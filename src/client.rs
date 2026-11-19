//! Xmip's side: the calls a Location and an archive make, each one request
//! with a bearer token over one connection.
//!
//! The JSON API, because it is one HTTP call per operation and answers in
//! a shape serde reads. Obtaining the token is outside this crate: a
//! Location is configured with one — from a service account, a metadata
//! server, or an operator — and hands it on as `Authorization: Bearer`.
//!
//! A listing stamps each object with its `generation`, and a receive gets
//! and deletes it on the condition `ifGenerationMatch` that it still lies
//! in that generation: one written again or deleted since is answered
//! `412` or `404` and nothing is done to it ([`AsListed::Changed`]). The
//! generation and not the `etag`, on Google's advice: "You should
//! generally use generation and metageneration preconditions instead of
//! `ETag` preconditions" (Cloud Storage, *Request preconditions*,
//! docs.cloud.google.com/storage/docs/request-preconditions, read
//! 2026-11-19). An upload that must write over nothing carries
//! `ifGenerationMatch=0`, which "only proceeds if no object with the
//! specified name exists in the bucket" (the same page).

use std::time::Duration;

use serde_json::Value;
use transport::error::{Result, protocol_error};
use transport::listed::{AsListed, Object};

use codec::percent::encode;
use http::endpoint::{Connections, Offer};
use http::status;
use net::Endpoint;
use net::http::{Request, Response};

pub struct Client {
    endpoint: Endpoint,
    token: String,
    timeout: Option<Duration>,
    /// The connections kept to the service, shared with the transport
    /// that made this client.
    connections: Connections,
}

impl Client {
    /// Speak to the Cloud Storage endpoint at `endpoint` — `http://host:port`
    /// or `https://host:port` — presenting `token`.
    ///
    /// # Errors
    /// Where `endpoint` is not an HTTP URL.
    pub fn new(endpoint: &str, token: &str) -> Result<Self> {
        Ok(Self {
            endpoint: Endpoint::parse(endpoint)?,
            token: token.to_string(),
            timeout: None,
            connections: Connections::new(),
        })
    }

    /// Give up on an endpoint that stops answering after `timeout`.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Keep connections among `connections`, which the transport holds
    /// across every client it makes.
    #[must_use]
    pub fn sharing(mut self, connections: Connections) -> Self {
        self.connections = connections;
        self
    }

    /// The object names under `prefix` in `bucket`, each with its
    /// `generation`, as many as one listing carries — a thousand — so a
    /// fuller prefix is taken a thousand at a time.
    ///
    /// # Errors
    /// Where the endpoint refused, could not be reached, or did not answer
    /// with a listing naming each object's `generation`.
    pub fn list(&self, bucket: &str, prefix: &str) -> Result<Vec<Object>> {
        let request = Request::new("GET", objects(bucket)).query("prefix", prefix);
        let listing: Value = serde_json::from_slice(&self.call(request)?.body)
            .map_err(|e| protocol_error(format!("a listing that is not JSON: {e}")))?;
        let Some(items) = listing["items"].as_array() else {
            return Ok(Vec::new());
        };
        items
            .iter()
            .map(|item| {
                let named = |field: &str| {
                    item[field].as_str().map(str::to_string).ok_or_else(|| {
                        protocol_error(format!("a listed object without its {field}"))
                    })
                };
                Ok((named("name")?, named("generation")?))
            })
            .collect()
    }

    /// The object `name` in `bucket` as listed, in `generation`.
    ///
    /// # Errors
    /// Where the endpoint refused for another reason or could not be
    /// reached.
    pub fn get(&self, bucket: &str, name: &str, generation: &str) -> Result<AsListed<Vec<u8>>> {
        let request = Request::new("GET", object(bucket, name)).query("alt", "media");
        self.as_listed(request, generation, |answer| answer.body)
    }

    /// Delete the object `name` in `bucket` as listed, in `generation`.
    ///
    /// # Errors
    /// Where the endpoint refused for another reason or could not be
    /// reached.
    pub fn delete(&self, bucket: &str, name: &str, generation: &str) -> Result<AsListed<()>> {
        let request = Request::new("DELETE", object(bucket, name));
        self.as_listed(request, generation, |_| ())
    }

    /// The object `name` in `bucket` in whichever generation it lies, or
    /// `None` where none does.
    ///
    /// # Errors
    /// Where the endpoint refused for another reason or could not be
    /// reached.
    pub fn read(&self, bucket: &str, name: &str) -> Result<Option<Vec<u8>>> {
        let request = Request::new("GET", object(bucket, name)).query("alt", "media");
        let answer = self.exchange(request)?;
        if answer.status == NOT_FOUND {
            return Ok(None);
        }
        judge(answer).map(|answer| Some(answer.body))
    }

    /// Upload `bytes` as the object `name` in `bucket`, in one piece, over
    /// whatever lies there.
    ///
    /// # Errors
    /// Where the endpoint refused or could not be reached.
    pub fn put(&self, bucket: &str, name: &str, bytes: &[u8]) -> Result<()> {
        self.call(upload(bucket, name, bytes)).map(|_| ())
    }

    /// Upload `bytes` as the object `name` in `bucket` only where no object
    /// lies under the name: `true` where it was written, `false` where one
    /// lay there and nothing was written.
    ///
    /// # Errors
    /// Where the endpoint refused for another reason or could not be
    /// reached.
    pub fn put_new(&self, bucket: &str, name: &str, bytes: &[u8]) -> Result<bool> {
        let answer = self.exchange(upload(bucket, name, bytes).query(IF_GENERATION_MATCH, "0"))?;
        if answer.status == PRECONDITION_FAILED {
            return Ok(false);
        }
        judge(answer).map(|_| true)
    }

    /// `request` made on the condition that its object lies in
    /// `generation`: what `held` makes of the answer, or
    /// [`AsListed::Changed`] where it lies in another (`412`) or not at all
    /// (`404`).
    fn as_listed<T>(
        &self,
        request: Request,
        generation: &str,
        held: impl FnOnce(Response) -> T,
    ) -> Result<AsListed<T>> {
        let answer = self.exchange(request.query(IF_GENERATION_MATCH, generation))?;
        if matches!(answer.status, PRECONDITION_FAILED | NOT_FOUND) {
            return Ok(AsListed::Changed);
        }
        judge(answer).map(|answer| AsListed::Held(held(answer)))
    }

    fn call(&self, request: Request) -> Result<Response> {
        judge(self.exchange(request)?)
    }

    /// The answer to `request`, whatever its status.
    fn exchange(&self, request: Request) -> Result<Response> {
        let request = request
            .header("Host", &self.endpoint.authority())
            .header("Authorization", &format!("Bearer {}", self.token));
        self.connections
            .exchange(&self.endpoint, self.timeout, Offer::Http11, &request)
    }
}

/// The condition a request carries on the generation its object lies in.
const IF_GENERATION_MATCH: &str = "ifGenerationMatch";

/// The answer to a request whose object is not there.
const NOT_FOUND: u16 = 404;

/// The answer to a request whose condition does not hold.
const PRECONDITION_FAILED: u16 = 412;

fn objects(bucket: &str) -> String {
    format!("/storage/v1/b/{}/o", encode(bucket, false))
}

fn object(bucket: &str, name: &str) -> String {
    format!("{}/{}", objects(bucket), encode(name, false))
}

/// The media upload of `bytes` as the object `name` in `bucket`.
fn upload(bucket: &str, name: &str, bytes: &[u8]) -> Request {
    Request::new("POST", format!("/upload{}", objects(bucket)))
        .query("uploadType", "media")
        .query("name", name)
        .header("Content-Type", "application/octet-stream")
        .body(bytes)
}

/// A 2xx answer as it is; anything else as a failure naming the status and
/// the message the service put in the body, retryable where HTTP says come
/// back.
fn judge(response: Response) -> Result<Response> {
    status::judge("Cloud Storage", response, reason, |_| false)
}

/// The message an error answer carries, or nothing.
fn reason(response: &Response) -> String {
    serde_json::from_slice::<Value>(&response.body)
        .ok()
        .and_then(|error| error["error"]["message"].as_str().map(str::to_string))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::{Event, Session};
    use std::assert_matches;
    use std::thread::JoinHandle;
    use transport::{Taken, socket};

    /// A session answering `requests` from a client presenting its token,
    /// and the client.
    fn both_ends(requests: usize) -> (Client, JoinHandle<(Session, Vec<Event>)>) {
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        let far_end = std::thread::spawn(move || {
            let mut session = Session::new("ya29.token").timing_out_after(Duration::from_secs(2));
            let events: Vec<Event> = (0..requests)
                .map(|_| session.serve_one(&listener).expect("served"))
                .collect();
            (session, events)
        });
        let client = Client::new(&format!("http://{address}"), "ya29.token")
            .expect("endpoint")
            .timing_out_after(Duration::from_secs(2));
        (client, far_end)
    }

    /// What `answer` held, or `None` where the object changed.
    fn held<T>(answer: AsListed<T>) -> Option<T> {
        match answer {
            AsListed::Held(held) => Some(held),
            AsListed::Changed => None,
        }
    }

    #[test]
    fn the_calls_reach_a_session_and_come_back_shaped_as_the_json_api_shapes_them() {
        let (client, far_end) = both_ends(6);
        client.put("orders", "in/a b.edi", b"UNA").expect("put");
        client.put("orders", "out/c.edi", b"UNB").expect("put");
        assert_eq!(
            client.list("orders", "in/").expect("list"),
            vec![("in/a b.edi".to_string(), "1".to_string())],
            "the first object written, in its generation"
        );
        let got = client.get("orders", "in/a b.edi", "1").expect("get");
        assert_eq!(held(got), Some(b"UNA".to_vec()));
        let deleted = client.delete("orders", "in/a b.edi", "1").expect("delete");
        assert_eq!(held(deleted), Some(()));
        assert_eq!(client.read("orders", "in/a b.edi").expect("read"), None);
        let (session, events) = far_end.join().expect("thread");
        assert_eq!(session.objects().len(), 1);
        assert_matches!(&events[2], Event::Listed { prefix, .. } if prefix == "in/");
        let origin = "google-cloud-storage://orders/in/a b.edi".to_string();
        assert_eq!(events[3], Event::Retrieved(origin.clone()));
        assert_eq!(events[4], Event::Deleted(origin));
        assert_eq!(events[5], Event::Refused("notFound".to_string()));
    }

    #[test]
    fn a_condition_not_met_or_an_object_gone_is_changed_and_one_already_there_exists() {
        let (client, far_end) = both_ends(9);
        client.put("orders", "in/a", b"one").expect("generation 1");
        assert!(
            !client.put_new("orders", "in/a", b"two").expect("412"),
            "exists"
        );
        assert!(
            client
                .put_new("orders", "in/b", b"new")
                .expect("generation 2")
        );
        assert!(held(client.get("orders", "in/a", "2").expect("412")).is_none());
        assert!(held(client.get("orders", "in/none", "1").expect("404")).is_none());
        assert!(held(client.delete("orders", "in/a", "2").expect("412")).is_none());
        assert!(held(client.delete("orders", "in/none", "1").expect("404")).is_none());
        assert_eq!(
            client.read("orders", "in/a").expect("read"),
            Some(b"one".to_vec())
        );
        assert_eq!(
            held(client.delete("orders", "in/b", "2").expect("ok")),
            Some(())
        );
        let (session, events) = far_end.join().expect("thread");
        let not_met = Event::Refused("conditionNotMet".to_string());
        let not_found = Event::Refused("notFound".to_string());
        assert_eq!(
            events[1..7],
            [
                not_met.clone(),
                Event::Stored(Taken::new(
                    "google-cloud-storage://orders/in/b",
                    b"new".to_vec()
                )),
                not_met.clone(),
                not_found.clone(),
                not_met,
                not_found
            ]
        );
        assert_eq!(
            session.objects().keys().collect::<Vec<_>>(),
            ["orders/in/a"],
            "nothing written over, nothing deleted that changed"
        );
    }

    #[test]
    fn a_server_failure_is_worth_repeating_and_a_client_one_is_not() {
        assert!(judge(Response::new(503)).expect_err("server").retryable);
        assert!(judge(Response::new(429)).expect_err("throttled").retryable);
        let forbidden = Response::new(403).body(br#"{"error":{"code":403,"message":"no"}}"#);
        let failure = judge(forbidden).expect_err("forbidden");
        assert!(!failure.retryable);
        assert_eq!(failure.message, "Cloud Storage answered 403 no");
        assert!(Client::new("orders.local", "t").is_err());
    }
}

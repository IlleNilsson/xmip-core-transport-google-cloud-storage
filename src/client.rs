//! Xmip's side: the four calls a Location makes, each one request with a
//! bearer token over one connection.
//!
//! The JSON API, because it is one HTTP call per operation and answers in
//! a shape serde reads. Obtaining the token is outside this crate: a
//! Location is configured with one — from a service account, a metadata
//! server, or an operator — and hands it on as `Authorization: Bearer`.

use std::time::Duration;

use serde_json::Value;
use transport::error::{Result, TransportError, protocol_error};

use http::endpoint;
use http::message::{self, Request, Response};
use http::percent::encode;

pub struct Client {
    endpoint: String,
    host: String,
    token: String,
    timeout: Option<Duration>,
}

impl Client {
    /// Speak to the Cloud Storage endpoint at `endpoint` — `http://host:port`
    /// or `https://host:port` — presenting `token`.
    ///
    /// # Errors
    /// Where `endpoint` is not an HTTP URL.
    pub fn new(endpoint: &str, token: &str) -> Result<Self> {
        Ok(Self {
            endpoint: endpoint.to_string(),
            host: endpoint::authority(endpoint)?,
            token: token.to_string(),
            timeout: None,
        })
    }

    /// Give up on an endpoint that stops answering after `timeout`.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// The object names under `prefix` in `bucket`, as many as one listing
    /// carries — a thousand — so a fuller prefix is taken a thousand at a
    /// time.
    ///
    /// # Errors
    /// Where the endpoint refused, could not be reached, or did not answer
    /// with a listing.
    pub fn list(&self, bucket: &str, prefix: &str) -> Result<Vec<String>> {
        let request = Request::new("GET", objects(bucket)).query("prefix", prefix);
        let listing: Value = serde_json::from_slice(&self.call(request)?.body)
            .map_err(|e| protocol_error(format!("a listing that is not JSON: {e}")))?;
        Ok(listing["items"]
            .as_array()
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| item["name"].as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default())
    }

    /// The object `name` in `bucket`.
    ///
    /// # Errors
    /// Where there is no such object, or the endpoint refused or could not
    /// be reached.
    pub fn get(&self, bucket: &str, name: &str) -> Result<Vec<u8>> {
        let request = Request::new("GET", object(bucket, name)).query("alt", "media");
        Ok(self.call(request)?.body)
    }

    /// Upload `bytes` as the object `name` in `bucket`, in one piece.
    ///
    /// # Errors
    /// Where the endpoint refused or could not be reached.
    pub fn put(&self, bucket: &str, name: &str, bytes: &[u8]) -> Result<()> {
        let request = Request::new("POST", format!("/upload{}", objects(bucket)))
            .query("uploadType", "media")
            .query("name", name)
            .header("Content-Type", "application/octet-stream")
            .body(bytes);
        self.call(request).map(|_| ())
    }

    /// Delete the object `name` in `bucket`.
    ///
    /// # Errors
    /// Where there is no such object, or the endpoint refused or could not
    /// be reached.
    pub fn delete(&self, bucket: &str, name: &str) -> Result<()> {
        self.call(Request::new("DELETE", object(bucket, name)))
            .map(|_| ())
    }

    fn call(&self, request: Request) -> Result<Response> {
        let request = request
            .header("Host", &self.host)
            .header("Authorization", &format!("Bearer {}", self.token));
        let stream = endpoint::connect(&self.endpoint, self.timeout)?;
        judge(message::exchange(stream, &request)?)
    }
}

fn objects(bucket: &str) -> String {
    format!("/storage/v1/b/{}/o", encode(bucket, false))
}

fn object(bucket: &str, name: &str) -> String {
    format!("{}/{}", objects(bucket), encode(name, false))
}

/// A 2xx answer as it is; anything else as a failure naming the status and
/// the message the service put in the body, retryable where it says come
/// back.
fn judge(response: Response) -> Result<Response> {
    if (200..300).contains(&response.status) {
        return Ok(response);
    }
    let message = serde_json::from_slice::<Value>(&response.body)
        .ok()
        .and_then(|error| error["error"]["message"].as_str().map(str::to_string))
        .unwrap_or_default();
    let retryable = response.status >= 500 || response.status == 408 || response.status == 429;
    Err(TransportError {
        message: format!("Cloud Storage answered {} {message}", response.status),
        retryable,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::{Event, Session};
    use transport::socket;

    #[test]
    fn the_four_calls_reach_a_session_and_come_back_shaped_as_the_json_api_shapes_them() {
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        let far_end = std::thread::spawn(move || {
            let mut session = Session::new("ya29.token").timing_out_after(Duration::from_secs(2));
            let events: Vec<Event> = (0..6)
                .map(|_| session.serve_one(&listener).expect("served"))
                .collect();
            (session, events)
        });
        let client = Client::new(&format!("http://{address}"), "ya29.token")
            .expect("endpoint")
            .timing_out_after(Duration::from_secs(2));
        client.put("orders", "in/a b.edi", b"UNA").expect("put");
        client.put("orders", "out/c.edi", b"UNB").expect("put");
        assert_eq!(
            client.list("orders", "in/").expect("list"),
            vec!["in/a b.edi".to_string()]
        );
        assert_eq!(client.get("orders", "in/a b.edi").expect("get"), b"UNA");
        client.delete("orders", "in/a b.edi").expect("delete");
        let missing = client.get("orders", "in/a b.edi").expect_err("gone");
        assert!(missing.message.contains("404 No such object"), "{missing}");
        assert!(!missing.retryable);
        let (session, events) = far_end.join().expect("thread");
        assert_eq!(session.objects().len(), 1);
        assert!(matches!(&events[2], Event::Listed { prefix, .. } if prefix == "in/"));
        let origin = "gs://orders/in/a b.edi".to_string();
        assert_eq!(events[3], Event::Retrieved(origin.clone()));
        assert_eq!(events[4], Event::Deleted(origin));
        assert_eq!(events[5], Event::Refused("notFound".to_string()));
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

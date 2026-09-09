//! The far end: enough of the JSON API to answer one Location, and what a
//! test or the playground puts on loopback.
//!
//! Not Cloud Storage. One session holds objects in memory, checks every
//! request for one bearer token, and answers the four calls with the
//! shapes the API answers them — the object list, the media, the object
//! resource, the error with its reason. A Location that needs durability,
//! generations or a token it obtains itself talks to the service through
//! [`crate::Client`].

use std::collections::BTreeMap;
use std::net::TcpListener;
use std::time::Duration;

use serde_json::{Value, json};
use transport::Arrived;
use transport::error::{Result, protocol_error};
use transport::socket;

use crate::percent::decode;
use crate::wire::{self, Request, Response};

/// What the client did, as [`Session::serve_one`] reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// The client listed `prefix` in `bucket`.
    Listed { bucket: String, prefix: String },
    /// The client fetched this object.
    Retrieved(String),
    /// The client uploaded an object; here is the Stream.
    Stored(Arrived),
    /// The client deleted this object.
    Deleted(String),
    /// The client was answered with this error reason.
    Refused(String),
}

pub struct Session {
    token: String,
    objects: BTreeMap<String, Vec<u8>>,
    timeout: Option<Duration>,
}

impl Session {
    /// Answer requests presenting `token`.
    #[must_use]
    pub fn new(token: &str) -> Self {
        Self {
            token: token.to_string(),
            objects: BTreeMap::new(),
            timeout: None,
        }
    }

    /// Give up on a client that stops mid-request after `timeout`.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Hold these objects, keyed `bucket/name`.
    #[must_use]
    pub fn with_objects(mut self, objects: BTreeMap<String, Vec<u8>>) -> Self {
        self.objects = objects;
        self
    }

    /// What is held now, keyed `bucket/name`, uploads and deletes included.
    #[must_use]
    pub fn objects(&self) -> &BTreeMap<String, Vec<u8>> {
        &self.objects
    }

    /// Accept one connection on `listener`, answer its one request, and say
    /// what it was.
    ///
    /// # Errors
    /// Where the connection could not be accepted, broke, or sent nothing.
    pub fn serve_one(&mut self, listener: &TcpListener) -> Result<Event> {
        let (stream, _) = socket::accept_tcp(listener, self.timeout)?;
        let (mut reader, mut writer) = socket::split(stream)?;
        let request = wire::read_request(&mut reader)?
            .ok_or_else(|| protocol_error("a connection that sent no request"))?;
        let (event, response) = self.answer(&request);
        wire::write_response(&mut writer, &response)?;
        Ok(event)
    }

    fn answer(&mut self, request: &Request) -> (Event, Response) {
        let bearer = format!("Bearer {}", self.token);
        if request.header_value("authorization") != Some(bearer.as_str()) {
            return refused(401, "authError", "Invalid Credentials");
        }
        let (upload, rest) = match request.path.strip_prefix("/upload") {
            Some(rest) => (true, rest),
            None => (false, request.path.as_str()),
        };
        let Some(rest) = rest.strip_prefix("/storage/v1/b/") else {
            return refused(404, "notFound", "Not Found");
        };
        let (bucket, name) = match rest.split_once("/o") {
            Some((bucket, "")) => (decode(bucket), None),
            Some((bucket, name)) => (decode(bucket), name.strip_prefix('/').map(decode)),
            None => return refused(404, "notFound", "Not Found"),
        };
        match (request.method.as_str(), upload, name) {
            ("GET", false, None) => self.list(&bucket, request),
            ("GET", false, Some(name)) => self.get(&bucket, &name),
            ("POST", true, None) => self.put(&bucket, request),
            ("DELETE", false, Some(name)) => self.delete(&bucket, &name),
            _ => refused(405, "methodNotAllowed", "not one of the four calls"),
        }
    }

    fn list(&self, bucket: &str, request: &Request) -> (Event, Response) {
        let prefix = request
            .query_value("prefix")
            .unwrap_or_default()
            .to_string();
        let under = format!("{bucket}/{prefix}");
        let items: Vec<Value> = self
            .objects
            .iter()
            .filter(|(held, _)| held.starts_with(&under))
            .map(|(held, bytes)| resource(bucket, &held[bucket.len() + 1..], bytes.len()))
            .collect();
        let mut listing = json!({ "kind": "storage#objects" });
        if !items.is_empty() {
            listing["items"] = Value::Array(items);
        }
        (
            Event::Listed {
                bucket: bucket.to_string(),
                prefix,
            },
            answer(200, &listing),
        )
    }

    fn get(&self, bucket: &str, name: &str) -> (Event, Response) {
        match self.objects.get(&format!("{bucket}/{name}")) {
            Some(bytes) => (
                Event::Retrieved(origin(bucket, name)),
                Response::new(200)
                    .header("Content-Type", "application/octet-stream")
                    .body(bytes),
            ),
            None => refused(404, "notFound", &format!("No such object: {bucket}/{name}")),
        }
    }

    fn put(&mut self, bucket: &str, request: &Request) -> (Event, Response) {
        if request.query_value("uploadType") != Some("media") {
            return refused(400, "badRequest", "only uploadType=media is served");
        }
        let Some(name) = request.query_value("name") else {
            return refused(400, "required", "Required parameter: name");
        };
        self.objects
            .insert(format!("{bucket}/{name}"), request.body.clone());
        (
            Event::Stored(Arrived::new(origin(bucket, name), request.body.clone())),
            answer(200, &resource(bucket, name, request.body.len())),
        )
    }

    fn delete(&mut self, bucket: &str, name: &str) -> (Event, Response) {
        match self.objects.remove(&format!("{bucket}/{name}")) {
            Some(_) => (Event::Deleted(origin(bucket, name)), Response::new(204)),
            None => refused(404, "notFound", &format!("No such object: {bucket}/{name}")),
        }
    }
}

fn origin(bucket: &str, name: &str) -> String {
    format!("gs://{bucket}/{name}")
}

/// An object resource, as much of it as a listing or an upload answer
/// needs.
fn resource(bucket: &str, name: &str, size: usize) -> Value {
    json!({
        "kind": "storage#object",
        "name": name,
        "bucket": bucket,
        "size": size.to_string(),
    })
}

fn answer(status: u16, body: &Value) -> Response {
    Response::new(status)
        .header("Content-Type", "application/json; charset=UTF-8")
        .body(body.to_string().as_bytes())
}

fn refused(status: u16, reason: &str, message: &str) -> (Event, Response) {
    let body = json!({
        "error": {
            "code": status,
            "message": message,
            "errors": [{ "message": message, "domain": "global", "reason": reason }],
        }
    });
    (Event::Refused(reason.to_string()), answer(status, &body))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bearing(request: Request) -> Request {
        request
            .header("Host", "storage.local")
            .header("Authorization", "Bearer ya29.token")
    }

    #[test]
    fn a_session_answers_in_the_json_apis_shapes_and_refuses_a_wrong_token() {
        let mut session = Session::new("ya29.token");
        let upload = Request::new("POST", "/upload/storage/v1/b/b/o")
            .query("uploadType", "media")
            .query("name", "in/k")
            .body(b"x");
        let (event, response) = session.answer(&bearing(upload));
        assert_eq!(response.status, 200);
        assert!(response.text().contains(r#""name":"in/k""#));
        assert_eq!(
            event,
            Event::Stored(Arrived::new("gs://b/in/k", b"x".to_vec()))
        );
        let listing = Request::new("GET", "/storage/v1/b/b/o").query("prefix", "in/");
        let (_, response) = session.answer(&bearing(listing));
        assert!(response.text().contains(r#""items":[{"#));
        let listing = Request::new("GET", "/storage/v1/b/b/o").query("prefix", "z");
        let (_, response) = session.answer(&bearing(listing));
        assert!(!response.text().contains("items"));
        let media = Request::new("GET", "/storage/v1/b/b/o/in%2Fk").query("alt", "media");
        let (event, response) = session.answer(&bearing(media));
        assert_eq!(
            (event, response.body),
            (Event::Retrieved("gs://b/in/k".to_string()), b"x".to_vec())
        );
        let (event, response) =
            session.answer(&bearing(Request::new("DELETE", "/storage/v1/b/b/o/in%2Fk")));
        assert_eq!(
            (event, response.status),
            (Event::Deleted("gs://b/in/k".to_string()), 204)
        );
        assert!(session.objects().is_empty());
        let wrong =
            Request::new("GET", "/storage/v1/b/b/o").header("Authorization", "Bearer other");
        let (event, response) = session.answer(&wrong);
        assert_eq!(event, Event::Refused("authError".to_string()));
        assert_eq!(response.status, 401);
        assert!(response.text().contains("Invalid Credentials"));
        let (_, response) = session.answer(&bearing(Request::new("GET", "/elsewhere")));
        assert_eq!(response.status, 404);
        let (_, response) = session.answer(&bearing(Request::new("PUT", "/storage/v1/b/b/o/k")));
        assert_eq!(response.status, 405);
        let bare = Request::new("POST", "/upload/storage/v1/b/b/o").query("uploadType", "media");
        let (_, response) = session.answer(&bearing(bare));
        assert_eq!(response.status, 400);
    }
}

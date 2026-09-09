#![forbid(unsafe_code)]

//! Streams that arrive as objects in a Cloud Storage bucket. One object is
//! one Stream, its name kept beside it.
//!
//! Cloud Storage is the drop box of every organisation that lives in
//! Google Cloud, and its JSON API is four calls on a bucket: list under a
//! prefix, get as media, upload as media, delete. A Receive Location lists
//! a prefix, gets each object and deletes it once it is safely a Stream; a
//! Send Location uploads a Stream as an object. Both present a bearer token
//! over plain HTTP/1.1 on a socket — `https://` with the `tls` feature,
//! which is the http technology's TLS (ADR-0033). Obtaining the token is
//! outside: a Location is configured with it.
//!
//! ```text
//! client.rs    Xmip's side: list, get, put, delete, JSON read by serde
//! session.rs   the far end a test or the playground runs on loopback
//! ```
//!
//! The endpoint, the percent-encoding and HTTP itself come from the http
//! technology, the flat XML scan from the capability (ADR-0044).
//!
//! Cloud Storage has objects and a precondition this transport does not
//! yet use, so [`Transport::claims`] answers [`NoNativeClaim`], ADR-0024
//! clause 5. The native claim the record names — a generation precondition
//! — is a later step.
//!
//! The origin URI is the object in the protocol's own terms:
//! `gs://bucket/in/order-1.edi`. A send target is the same form, or a name
//! alone in this transport's bucket.

pub mod client;
pub mod session;

use std::time::Duration;

pub use client::Client;
pub use session::{Event, Session};
use transport::error::Result;
use transport::socket;
use transport::{Arrived, Directions, NoNativeClaim, ResourceClaim, Transport};

pub struct GcsTransport {
    endpoint: String,
    bucket: String,
    token: String,
    prefix: String,
    timeout: Option<Duration>,
}

impl GcsTransport {
    /// Speak to the endpoint at `endpoint` — `http://host:port` or
    /// `https://host:port`, `https://storage.googleapis.com` in the cloud —
    /// about `bucket`.
    #[must_use]
    pub fn new(endpoint: impl Into<String>, bucket: &str) -> Self {
        Self {
            endpoint: endpoint.into(),
            bucket: bucket.to_string(),
            token: String::new(),
            prefix: String::new(),
            timeout: None,
        }
    }

    /// Present this bearer token.
    #[must_use]
    pub fn with_token(mut self, token: &str) -> Self {
        self.token = token.to_string();
        self
    }

    /// Receive only what is under `prefix` — `in/`, say.
    #[must_use]
    pub fn with_prefix(mut self, prefix: &str) -> Self {
        self.prefix = prefix.to_string();
        self
    }

    /// Give up on an endpoint that stops answering after `timeout`.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// The client this transport speaks through.
    ///
    /// # Errors
    /// Where the endpoint is not an HTTP URL.
    pub fn client(&self) -> Result<Client> {
        let client = Client::new(&self.endpoint, &self.token)?;
        Ok(match self.timeout {
            Some(timeout) => client.timing_out_after(timeout),
            None => client,
        })
    }

    /// A far end that expects this transport's token, for a test or the
    /// playground to run on loopback.
    #[must_use]
    pub fn session(&self) -> Session {
        let session = Session::new(&self.token);
        match self.timeout {
            Some(timeout) => session.timing_out_after(timeout),
            None => session,
        }
    }

    /// Where a target names the bucket and object itself — `gs://bucket/name`
    /// — or is a name alone in this transport's bucket.
    fn resolve<'a>(&'a self, target: &'a str) -> (&'a str, &'a str) {
        socket::target("gs", target).unwrap_or((&self.bucket, target))
    }
}

impl Transport for GcsTransport {
    fn name(&self) -> &'static str {
        "google-cloud-storage"
    }

    fn directions(&self) -> Directions {
        Directions::BOTH
    }

    /// Every object under the prefix, each deleted once it is a Stream.
    fn receive(&self) -> Result<Vec<Arrived>> {
        let client = self.client()?;
        let mut arrived = Vec::new();
        for name in client.list(&self.bucket, &self.prefix)? {
            let bytes = client.get(&self.bucket, &name)?;
            client.delete(&self.bucket, &name)?;
            arrived.push(Arrived::new(format!("gs://{}/{name}", self.bucket), bytes));
        }
        Ok(arrived)
    }

    fn send(&self, target: &str, bytes: &[u8]) -> Result<()> {
        let (bucket, name) = self.resolve(target);
        self.client()?.put(bucket, name, bytes)
    }

    fn claims(&self) -> Option<&dyn ResourceClaim> {
        Some(&NoNativeClaim)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::thread::JoinHandle;

    fn node(endpoint: &str, token: &str) -> GcsTransport {
        GcsTransport::new(endpoint, "orders")
            .with_token(token)
            .with_prefix("in/")
            .timing_out_after(Duration::from_secs(2))
    }

    fn serve(
        mut session: Session,
        listener: TcpListener,
        requests: usize,
    ) -> JoinHandle<(Session, Vec<Event>)> {
        std::thread::spawn(move || {
            let events = (0..requests)
                .map(|_| session.serve_one(&listener).expect("served"))
                .collect();
            (session, events)
        })
    }

    #[test]
    fn what_is_sent_to_a_session_is_received_back_and_deleted() {
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        let near = node(&format!("http://{address}"), "ya29.token");
        // Two uploads, one list, then a get and a delete per object under in/.
        let far_end = serve(near.session(), listener, 7);
        near.send("in/1.edi", b"UNA:+.? '").expect("a name alone");
        near.send("gs://orders/in/2.edi", b"")
            .expect("a full target");
        let mut arrived = near.receive().expect("received");
        arrived.sort_by(|a, b| a.origin_uri.cmp(&b.origin_uri));
        assert_eq!(arrived.len(), 2);
        assert_eq!(arrived[0].origin_uri, "gs://orders/in/1.edi");
        assert_eq!(arrived[0].bytes, b"UNA:+.? '");
        assert_eq!(arrived[1].origin_uri, "gs://orders/in/2.edi");
        assert!(arrived[1].bytes.is_empty());
        let (session, events) = far_end.join().expect("thread");
        assert!(session.objects().is_empty(), "deleted after retrieve");
        assert_eq!(
            events[0],
            Event::Stored(Arrived::new("gs://orders/in/1.edi", b"UNA:+.? '".to_vec()))
        );
        let deleted = events.iter().filter(|e| matches!(e, Event::Deleted(_)));
        assert_eq!(deleted.count(), 2);
    }

    #[test]
    fn a_wrong_token_is_refused_with_the_apis_own_status_and_message() {
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        let far_end = serve(node("http://x", "ya29.token").session(), listener, 1);
        let failure = node(&format!("http://{address}"), "ya29.other")
            .send("in/1.edi", b"x")
            .expect_err("refused");
        assert!(
            failure.message.contains("401 Invalid Credentials"),
            "{failure}"
        );
        assert!(!failure.retryable);
        let (_, events) = far_end.join().expect("thread");
        assert_eq!(events, vec![Event::Refused("authError".to_string())]);
    }

    #[test]
    fn objects_are_artefacts_without_a_lock_and_an_unreachable_endpoint_is_retryable() {
        let near = node("http://127.0.0.1:1", "t");
        assert!(near.claims().is_some());
        assert_eq!(near.name(), "google-cloud-storage");
        assert!(near.directions().receives() && near.directions().sends());
        assert!(near.receive().expect_err("nothing listening").retryable);
        let failure = node("orders.local", "t")
            .send("k", b"")
            .expect_err("no scheme");
        assert!(!failure.retryable);
    }
}

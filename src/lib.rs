#![forbid(unsafe_code)]

//! Streams that arrive as objects in a Cloud Storage bucket. One object is
//! one Stream, its name kept beside it.
//!
//! Cloud Storage is the drop box of every organisation that lives in
//! Google Cloud, and its JSON API is four calls on a bucket: list under a
//! prefix, get as media, upload as media, delete. A Receive Location lists
//! a prefix, gets each object as the runtime first reads it, and deletes it
//! once the runtime accepts or refuses it after the whole receive cycle —
//! one whose cycle failed stays for the next receive; a Send Location
//! uploads a Stream as an object. Both present a bearer token over plain
//! HTTP/1.1 on a socket — `https://` with the `tls` feature, which is the
//! http technology's TLS (ADR-0033). Obtaining the token is outside: a
//! Location is configured with it.
//!
//! ```text
//! client.rs    Xmip's side: list, get, put, delete, JSON read by serde
//! session.rs   the far end a test or the playground runs on loopback
//! ```
//!
//! The endpoint, HTTP itself and the judgement of an answer come from the
//! http technology, the percent-encoding from `net`, the flat XML scan from
//! the capability (ADR-0044).
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

use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

pub use client::Client;
use http::endpoint::Connections;
use net::{Endpoint, Target};
pub use session::{Event, Session};
use transport::error::{Result, protocol_error};
use transport::listed::listed;
use transport::listening::Listening;
use transport::loopback::{FarEnd, LOOPBACK_TIMEOUT, Loopback};
use transport::socket;
use transport::{Arrived, Configured, Directions, NoNativeClaim, ResourceClaim, Transport};
use xcore::settings::{Applies, Kind, Presence, Read, Setting, Settings};

/// What the loopback pair agrees on: one bucket, one object uploaded
/// there, one bearer token the far end expects and the near end presents.
const LOOPBACK_BUCKET: &str = "probe";
const LOOPBACK_OBJECT: &str = "probe.bin";
const LOOPBACK_TOKEN: &str = "ya29.probe";

#[derive(Clone)]
pub struct GcsTransport {
    endpoint: String,
    bucket: String,
    token: String,
    prefix: String,
    timeout: Option<Duration>,
    /// The connections kept to the service, shared by every client this
    /// makes.
    connections: Connections,
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
            connections: Connections::new(),
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
        let client = client.sharing(self.connections.clone());
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
        Target::under(&["gs"], target).map_or((&self.bucket, target), |named| {
            (named.authority(), named.path())
        })
    }
}

impl Transport for GcsTransport {
    fn name(&self) -> &'static str {
        "google-cloud-storage"
    }

    fn directions(&self) -> Directions {
        Directions::BOTH
    }

    fn arrivals(&self) -> transport::Arrivals {
        transport::Arrivals::Ordered("a receive lists again what is not yet told")
    }

    /// Every object under the prefix, listed by [`listed`], the capability's
    /// one object-store receive: the receive gets and deletes nothing, each
    /// object's `GET` is made when the runtime first reads its body — whole,
    /// `net::http` reads a `GET` body whole — and its acknowledgement
    /// deletes it on [`transport::Verdict::Accepted`] and
    /// [`transport::Verdict::Refused`] (a bucket has no place for a rejected
    /// object) and leaves it on [`transport::Verdict::Failed`], for the next
    /// receive to list and get again.
    fn receive(&self) -> Result<Vec<Arrived>> {
        let client = Arc::new(self.client()?);
        let (getting, deleting) = (Arc::clone(&client), Arc::clone(&client));
        let (bucket, getting_from, deleting_from) = (
            self.bucket.as_str(),
            self.bucket.clone(),
            self.bucket.clone(),
        );
        listed(
            || client.list(bucket, &self.prefix),
            |name| format!("gs://{bucket}/{name}"),
            move |name| getting.get(&getting_from, name),
            move |name| deleting.delete(&deleting_from, name),
        )
    }

    fn send(&self, target: &str, bytes: &[u8]) -> Result<()> {
        let (bucket, name) = self.resolve(target);
        self.client()?.put(bucket, name, bytes)
    }

    fn claims(&self) -> Option<&dyn ResourceClaim> {
        Some(&NoNativeClaim)
    }
}

impl Configured for GcsTransport {
    /// The address is the JSON API's endpoint, `https://storage.googleapis.com`
    /// in the cloud.
    const SETTINGS: &'static Settings = &Settings {
        technology: env!("CARGO_PKG_NAME"),
        settings: &[
            Setting {
                name: "bucket",
                kind: Kind::Text,
                presence: Presence::Required,
                meaning: "The bucket a Location lists and gets from or uploads to.",
                applies: Applies::Both,
            },
            Setting {
                name: "prefix",
                kind: Kind::Text,
                presence: Presence::Optional,
                meaning: "Only the objects whose names start with it are received; every \
                          object when left out.",
                applies: Applies::Receive,
            },
            Setting {
                name: "timeout",
                kind: Kind::Duration,
                presence: Presence::Optional,
                meaning: "How long an endpoint that stops answering is waited on; unbounded \
                          when left out.",
                applies: Applies::Both,
            },
        ],
    };

    /// The bearer token comes through the Location's credentials, never a
    /// setting; the transport is built without it.
    fn configured(address: &str, settings: &Read) -> Result<Self> {
        let mut transport = Self::new(address, settings.text("bucket"));
        if let Some(prefix) = settings.optional_text("prefix") {
            transport = transport.with_prefix(prefix);
        }
        if let Some(timeout) = settings.optional_duration("timeout") {
            transport = transport.timing_out_after(timeout);
        }
        Ok(transport)
    }
}

impl GcsTransport {
    /// Both ends on this machine: an ephemeral local port, one token the
    /// far end expects and the near end presents, the loopback timeout.
    #[must_use]
    pub fn loopback() -> Self {
        Self::new("http://127.0.0.1:0", LOOPBACK_BUCKET)
            .with_token(LOOPBACK_TOKEN)
            .timing_out_after(LOOPBACK_TIMEOUT)
    }
}

impl Loopback for GcsTransport {
    /// A bound session waiting for its one upload. The JSON API opens a
    /// connection per call, so the session serves one request at a time
    /// until one stored.
    fn far_end(&self) -> Result<Box<dyn FarEnd>> {
        let mut session = self.session();
        Ok(Box::new(Listening::new(
            move |listener: &TcpListener| loop {
                match session.serve_one(listener)? {
                    Event::Stored(arrived) => return Ok(arrived),
                    Event::Refused(reason) => {
                        return Err(protocol_error(format!("the session refused: {reason}")));
                    }
                    _ => {}
                }
            },
            socket::bind_tcp(&Endpoint::parse(&self.endpoint)?.address())?,
        )))
    }

    /// Upload the payload as one object, from a fresh near end presenting
    /// this transport's token, at the endpoint on `address`.
    fn send_to(&self, address: &str, payload: &[u8]) -> Result<()> {
        let near = Self {
            endpoint: format!("http://{address}"),
            ..self.clone()
        };
        near.send(LOOPBACK_OBJECT, payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::JoinHandle;
    use transport::Taken;
    use xcore::settings::Given;

    #[test]
    fn google_cloud_storage_declares_its_settings_and_reads_through_them() {
        assert_eq!(GcsTransport::SETTINGS.problems(), Vec::<String>::new());
        let endpoint = "https://storage.googleapis.com";
        let given = [
            ("bucket".to_string(), Given::Text("orders".to_string())),
            ("prefix".to_string(), Given::Text("in/".to_string())),
            ("timeout".to_string(), Given::Text("10s".to_string())),
        ];
        let built = GcsTransport::open(endpoint, Applies::Receive, &given).expect("built");
        assert_eq!(built.bucket, "orders");
        assert_eq!(built.prefix, "in/");
        assert_eq!(built.timeout, Some(Duration::from_secs(10)));
        assert!(
            built.token.is_empty(),
            "the token is the Location's credentials"
        );
        let Err(refused) = GcsTransport::open(endpoint, Applies::Send, &given[1..2]) else {
            panic!("bucket is required, and a Send Location has no prefix");
        };
        assert!(
            refused.message.contains("\"bucket\""),
            "{}",
            refused.message
        );
        assert!(
            refused.message.contains("\"prefix\""),
            "{}",
            refused.message
        );
    }

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
    fn an_object_is_got_when_read_and_deleted_when_accepted_or_refused_and_kept_when_failed() {
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        let near = node(&format!("http://{address}"), "ya29.token");
        // Three uploads; a list, the one get read, two deletes; a list, a
        // get, a delete.
        let far_end = serve(near.session(), listener, 10);
        near.send("in/1.edi", b"UNA:+.? '").expect("a name alone");
        near.send("gs://orders/in/2.edi", b"")
            .expect("a full target");
        near.send("in/3.edi", b"C3").expect("a name alone");
        let mut arrived = near.receive().expect("received");
        arrived.sort_by(|a, b| a.origin_uri.cmp(&b.origin_uri));
        assert_eq!(arrived.len(), 3);
        assert!(arrived.iter().all(Arrived::defers));
        let third = arrived.pop().expect("third");
        let second = arrived.pop().expect("second");
        assert_eq!(second.origin_uri, "gs://orders/in/2.edi");
        let first = arrived.pop().expect("first").taken().expect("accepted");
        assert_eq!(first.origin_uri, "gs://orders/in/1.edi");
        assert_eq!(first.bytes, b"UNA:+.? '");
        second.failed().expect("left");
        third
            .refused(transport::Refusal::Unacceptable)
            .expect("deleted");
        let again = near.receive().expect("received again");
        assert_eq!(again.len(), 1, "the failed one, and only it");
        let again = again.into_iter().next().expect("one").taken().expect("ok");
        assert_eq!(again.origin_uri, "gs://orders/in/2.edi");
        assert!(again.bytes.is_empty());
        let (session, events) = far_end.join().expect("thread");
        assert!(session.objects().is_empty(), "deleted once answered");
        assert_eq!(
            events[0],
            Event::Stored(Taken::new("gs://orders/in/1.edi", b"UNA:+.? '".to_vec()))
        );
        let named = |name: &str| format!("gs://orders/in/{name}");
        assert!(matches!(events[3], Event::Listed { .. }));
        assert_eq!(
            events[4..7],
            [
                Event::Retrieved(named("1.edi")),
                Event::Deleted(named("1.edi")),
                Event::Deleted(named("3.edi")),
            ],
            "only what was read was got"
        );
        assert!(matches!(events[7], Event::Listed { .. }));
        assert_eq!(
            events[8..],
            [
                Event::Retrieved(named("2.edi")),
                Event::Deleted(named("2.edi"))
            ]
        );
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

    #[test]
    fn the_loopback_uploads_one_object_through_its_own_session() {
        let pair = GcsTransport::loopback();
        let arrived = pair.round(b"an upload").expect("round");
        assert_eq!(arrived.bytes, b"an upload");
        assert_eq!(arrived.origin_uri, "gs://probe/probe.bin");
        assert_eq!(pair.name(), "google-cloud-storage");
        assert_eq!(pair.ceiling(), None);
    }

    /// The Playground's edge payloads, written here so the crate does not
    /// depend on it.
    fn edge_payloads() -> Vec<(&'static str, Vec<u8>)> {
        vec![
            ("empty", Vec::new()),
            ("one byte", vec![0x2a]),
            ("every byte", (0..=255).collect()),
            ("nul run", vec![0; 512]),
            ("high bytes", vec![0xff; 512]),
            ("crlf storm", b"\r\n".repeat(400)),
        ]
    }

    #[test]
    fn the_loopback_returns_the_edge_payloads_whole() {
        let pair = GcsTransport::loopback();
        for (name, payload) in edge_payloads() {
            assert!(pair.refuses(&payload).is_none(), "{name}");
            let arrived = pair.round(&payload).expect(name);
            assert_eq!(arrived.bytes, payload, "{name}");
        }
    }
}

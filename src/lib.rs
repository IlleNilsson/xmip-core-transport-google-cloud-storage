#![forbid(unsafe_code)]

//! Streams that arrive as objects in a Cloud Storage bucket. One object is
//! one Stream, its name kept beside it.
//!
//! Cloud Storage is the drop box of every organization that lives in
//! Google Cloud, and its JSON API is four calls on a bucket: list under a
//! prefix, get as media, upload as media, delete. A Receive Location lists
//! a prefix, gets each object as the runtime first reads it, and deletes it
//! once the runtime accepts it after the whole receive cycle, each in the
//! `generation` its listing gave it — one written again or deleted since is
//! neither read nor deleted, and the next listing takes it as a new
//! arrival; one refused stays where it lies and is not received again
//! while its generation is unchanged, one whose cycle failed stays for the
//! next receive; a Send Location uploads a Stream as an object. Both
//! present a bearer token over plain HTTP/1.1 on a socket — `https://` with
//! the `tls` feature, which is the http technology's TLS (ADR-0033).
//! Obtaining the token is outside: a Location is configured with it.
//!
//! ```text
//! client.rs    Xmip's side: list, get, delete, read, put, put_new; serde JSON
//! session.rs   the far end a test or the essentials runs on loopback
//! ```
//!
//! The endpoint, HTTP itself and the judgment of an answer come from the
//! http technology, the percent-encoding from `net`, the receive from the
//! capability (ADR-0044).
//!
//! Cloud Storage has no lock or lease a receive takes, so
//! [`Transport::claims`] answers [`NoNativeClaim`], ADR-0024 clause 5.
//!
//! The origin URI is the object under the scheme that is the technology's
//! name: `google-cloud-storage://bucket/in/order-1.edi`. A send target is
//! the same form, or a name alone in this transport's bucket.

pub mod client;
pub mod session;

use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

pub use client::Client;
use http::endpoint::Connections;
use net::{Endpoint, Target};
pub use session::{Event, Session};
use transport::ArrivalIdentity;
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
    /// The objects this Location refused and left in the bucket, each with
    /// its `generation`; a clone shares them.
    refused: transport::Refused<String, String>,
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
            refused: transport::Refused::default(),
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
    /// essentials to run on loopback.
    #[must_use]
    pub fn session(&self) -> Session {
        let session = Session::new(&self.token);
        match self.timeout {
            Some(timeout) => session.timing_out_after(timeout),
            None => session,
        }
    }

    /// Where a target names the bucket and object itself —
    /// `google-cloud-storage://bucket/name` — or is a name alone in this
    /// transport's bucket.
    fn resolve<'a>(&'a self, target: &'a str) -> (&'a str, &'a str) {
        Target::under(Self::SCHEMES, target).map_or((&self.bucket, target), |named| {
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

    fn waiting(&self) -> transport::Waiting {
        transport::Waiting::Polled("a receive lists the bucket and returns")
    }

    /// Every object under the prefix, listed by [`listed`], the capability's
    /// one object-store receive: the receive gets and deletes nothing, each
    /// object's `GET` is made when the runtime first reads its body — whole,
    /// `net::http` reads a `GET` body whole — and its acknowledgement
    /// deletes it on [`transport::Verdict::Accepted`] only, both in the
    /// generation listed. On [`transport::Verdict::Refused`] it is left
    /// where it lies and this Location does not receive it again while its
    /// generation is unchanged — one written again is a new arrival; the
    /// memory is the node process's, so a node started again receives it
    /// once more. On [`transport::Verdict::Failed`] it is left for the next
    /// receive to list and get again.
    fn receive_up_to(&self, most: usize) -> Result<Vec<Arrived>> {
        let client = Arc::new(self.client()?);
        let (getting, deleting) = (Arc::clone(&client), Arc::clone(&client));
        let (bucket, getting_from, deleting_from) = (
            self.bucket.as_str(),
            self.bucket.clone(),
            self.bucket.clone(),
        );
        listed(
            || client.list(bucket, &self.prefix),
            most,
            &self.refused,
            |name| origin(bucket, name),
            move |name, generation| getting.get(&getting_from, name, generation),
            move |name, generation| deleting.delete(&deleting_from, name, generation),
        )
    }

    fn sending(&self) -> transport::Sending {
        transport::Sending::Gathered(
            "the HTTP client writes a request whole, its length counted from it",
        )
    }

    fn send_from(&self, target: &str, outgoing: transport::Outgoing<'_>) -> Result<()> {
        let bytes: &[u8] = &outgoing.gather()?;
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
            transport::configured::timeout(
                Applies::Both,
                None,
                "How long an endpoint that stops answering is waited on; unbounded \
                 when left out.",
            ),
        ],
    };

    /// The schemes a Location's address is entered with to choose this
    /// technology: its name (ADR-0011, amendment 2026-11-19).
    const SCHEMES: &'static [&'static str] = &["google-cloud-storage"];

    fn timeout_kept(&mut self) -> Option<&mut dyn transport::configured::TimeoutKept> {
        Some(&mut self.timeout)
    }

    /// The bearer token comes through the Location's credentials, never a
    /// setting; the transport is built without it.
    fn configured(address: &str, settings: &Read) -> Result<Self> {
        let mut transport = Self::new(address, settings.text("bucket"));
        if let Some(prefix) = settings.optional_text("prefix") {
            transport = transport.with_prefix(prefix);
        }
        Ok(transport)
    }

    /// The connections its clients share, an HTTP/2 one's frames in a span
    /// counted on `clock`, its node's (ADR-0068, amendment 2026-11-16).
    fn timed_by(mut self, clock: &std::sync::Arc<dyn xcore::Clock>) -> Self {
        self.connections = self.connections.timed_by(clock);
        self
    }
}

/// The object `name` in `bucket`, as an origin says it, under the scheme
/// above: `google-cloud-storage://bucket/name`. The one place the form is
/// written: the transport's receive, the session's answers and the
/// google-cloud-storage archive's receipt all name an object by it.
#[must_use]
pub fn origin(bucket: &str, name: &str) -> String {
    format!("google-cloud-storage://{bucket}/{name}")
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
    fn arrival_identity(&self) -> ArrivalIdentity {
        ArrivalIdentity::Unnamed(
            "an object names no sender: the bucket it was taken from is in its origin",
        )
    }

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
    use std::assert_matches;
    use std::io::Read;
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
    fn an_object_is_got_when_read_deleted_when_accepted_and_left_when_refused_or_failed() {
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        let near = node(&format!("http://{address}"), "ya29.token");
        // Three uploads; a list, the one get read, its delete; a list, a
        // get, a delete; a list to see what is left.
        let far_end = serve(near.session(), listener, 10);
        near.send("in/1.edi", b"UNA:+.? '").expect("a name alone");
        near.send("google-cloud-storage://orders/in/2.edi", b"")
            .expect("a full target");
        near.send("in/3.edi", b"third").expect("a name alone");
        let mut arrived = near.receive().expect("received");
        arrived.sort_by(|a, b| a.origin_uri.cmp(&b.origin_uri));
        assert_eq!(arrived.len(), 3);
        assert!(arrived.iter().all(Arrived::defers));
        let third = arrived.pop().expect("third");
        let second = arrived.pop().expect("second");
        assert_eq!(second.origin_uri, "google-cloud-storage://orders/in/2.edi");
        let first = arrived.pop().expect("first").taken().expect("accepted");
        assert_eq!(first.origin_uri, "google-cloud-storage://orders/in/1.edi");
        assert_eq!(first.bytes, b"UNA:+.? '");
        second.failed().expect("left");
        third
            .refused(transport::Refusal::Unacceptable)
            .expect("left");
        let again = near.receive().expect("received again");
        assert_eq!(again.len(), 1, "the failed one, not the refused one");
        let again = again.into_iter().next().expect("one").taken().expect("ok");
        assert_eq!(again.origin_uri, "google-cloud-storage://orders/in/2.edi");
        assert_eq!(again.bytes, [] as [u8; 0]);
        let left = near
            .client()
            .expect("client")
            .list("orders", "in/")
            .expect("list");
        assert_eq!(
            left,
            [("in/3.edi".to_string(), "3".to_string())],
            "still there"
        );
        let (session, events) = far_end.join().expect("thread");
        assert_eq!(session.objects().len(), 1, "only the refused one is held");
        assert_eq!(
            events[0],
            Event::Stored(Taken::new(
                "google-cloud-storage://orders/in/1.edi",
                b"UNA:+.? '".to_vec()
            ))
        );
        let named = |name: &str| format!("google-cloud-storage://orders/in/{name}");
        assert_matches!(events[3], Event::Listed { .. });
        assert_eq!(
            events[4..6],
            [
                Event::Retrieved(named("1.edi")),
                Event::Deleted(named("1.edi"))
            ],
            "only what was read was got, only what was accepted deleted"
        );
        assert_matches!(events[6], Event::Listed { .. });
        assert_eq!(
            events[7..9],
            [
                Event::Retrieved(named("2.edi")),
                Event::Deleted(named("2.edi"))
            ]
        );
        assert_matches!(events[9], Event::Listed { .. });
    }

    #[test]
    fn a_refused_object_is_not_received_again_until_it_is_written_again() {
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        let near = node(&format!("http://{address}"), "ya29.token");
        // An upload; a list; a list; an upload again; a list, a get, a delete.
        let far_end = serve(near.session(), listener, 7);
        near.send("in/1.edi", b"first").expect("upload");
        let refused = near.receive().expect("received").remove(0);
        refused
            .refused(transport::Refusal::Forbidden)
            .expect("left");
        assert!(near.receive().expect("listed").is_empty(), "unchanged");
        near.send("in/1.edi", b"second").expect("written again");
        let again = near
            .receive()
            .expect("listed")
            .remove(0)
            .taken()
            .expect("ok");
        assert_eq!(again.bytes, b"second", "another generation, a new arrival");
        let (session, _) = far_end.join().expect("thread");
        assert!(session.objects().is_empty(), "accepted, so deleted");
    }

    #[test]
    fn a_replacement_between_read_and_acceptance_stays_for_the_next_listing() {
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        let near = node(&format!("http://{address}"), "ya29.token");
        // An upload; a list, a get; an upload over it; the delete of the
        // generation listed; a list, a get, a delete.
        let far_end = serve(near.session(), listener, 8);
        near.send("in/1.edi", b"first").expect("upload");
        let listed = near.receive().expect("listed").remove(0);
        let (_, mut body, told) = listed.into_parts();
        let mut read = Vec::new();
        body.read_to_end(&mut read).expect("read");
        assert_eq!(read, b"first");
        near.send("in/1.edi", b"second").expect("written over it");
        told.acknowledge(transport::Verdict::Accepted)
            .expect("what was accepted is done with");
        let again = near.receive().expect("listed again");
        assert_eq!(again.len(), 1, "the replacement is a new arrival");
        let again = again.into_iter().next().expect("one").taken().expect("ok");
        assert_eq!(again.bytes, b"second");
        let (session, events) = far_end.join().expect("thread");
        assert_eq!(
            events[4],
            Event::Refused("conditionNotMet".to_string()),
            "the delete names the generation listed, so the replacement stays"
        );
        assert!(session.objects().is_empty(), "the replacement accepted too");
    }

    #[test]
    fn a_replacement_between_listing_and_first_read_is_not_read_under_the_old_stamp() {
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        let near = node(&format!("http://{address}"), "ya29.token");
        // An upload; a list; an upload over it; the get of the generation
        // listed; a list, a get, a delete.
        let far_end = serve(near.session(), listener, 7);
        near.send("in/1.edi", b"first").expect("upload");
        let listed = near.receive().expect("listed").remove(0);
        near.send("in/1.edi", b"second").expect("written over it");
        let failure = listed.taken().expect_err("not read under the old stamp");
        assert!(
            failure.message.contains("changed since it was listed"),
            "{failure}"
        );
        let again = near.receive().expect("listed again");
        assert_eq!(again.len(), 1, "left by the failed cycle");
        let again = again.into_iter().next().expect("one").taken().expect("ok");
        assert_eq!(again.bytes, b"second", "read under its own stamp");
        let (session, events) = far_end.join().expect("thread");
        assert_eq!(events[3], Event::Refused("conditionNotMet".to_string()));
        assert!(session.objects().is_empty(), "accepted, so deleted");
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
    fn objects_are_artifacts_without_a_lock_and_an_unreachable_endpoint_is_retryable() {
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
        assert_eq!(arrived.origin_uri, "google-cloud-storage://probe/probe.bin");
        assert_eq!(pair.name(), "google-cloud-storage");
        assert_eq!(pair.ceiling(), None);
    }

    /// The Essentials' edge payloads, written here so the crate does not
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

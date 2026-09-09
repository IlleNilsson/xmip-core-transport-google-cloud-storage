//! The endpoint a Location is configured with, and a connection to it.
//!
//! `http://host:port` or `https://host:port`, read by the http technology's
//! own target parser so the two agree on what an authority is. TLS is that
//! technology's `tls` module behind this crate's `tls` feature — one TLS
//! stack in the estate (ADR-0033) — and without the feature an `https://`
//! endpoint is refused with a message saying so rather than sent in the
//! clear.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use http::target::HttpTarget;
use transport::error::Result;
use transport::socket;
use transport::wire::host_of;

/// Anything a request can travel over: a plain socket, or one wrapped in
/// TLS.
pub trait Connection: Read + Write {}

impl<S: Read + Write> Connection for S {}

/// The authority a `Host` header names for `endpoint`.
///
/// # Errors
/// Where `endpoint` is not `http://host:port` or `https://host:port`.
pub fn authority(endpoint: &str) -> Result<String> {
    Ok(HttpTarget::parse(endpoint)?.authority.to_string())
}

/// Open a connection to `endpoint` with `timeout` on its reads.
///
/// # Errors
/// Where the endpoint is malformed, could not be reached, or asks for TLS
/// this build does not carry.
pub fn connect(endpoint: &str, timeout: Option<Duration>) -> Result<Box<dyn Connection>> {
    let target = HttpTarget::parse(endpoint)?;
    let tcp = socket::connect_tcp(&target.address(), timeout)?;
    if target.secure {
        return secure(host_of(target.authority), tcp);
    }
    Ok(Box::new(tcp))
}

#[cfg(feature = "tls")]
fn secure(host: &str, tcp: TcpStream) -> Result<Box<dyn Connection>> {
    Ok(Box::new(http::tls::client(host, tcp)?))
}

#[cfg(not(feature = "tls"))]
fn secure(_host: &str, tcp: TcpStream) -> Result<Box<dyn Connection>> {
    drop(tcp);
    Err(transport::error::protocol_error(
        "https was asked for and this build has no tls feature compiled in",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_https_endpoint_without_tls_says_so() {
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        let failure = connect(&format!("https://{address}"), None).err();
        drop(listener);
        #[cfg(not(feature = "tls"))]
        assert!(failure.expect("no tls").message.contains("tls"));
        #[cfg(feature = "tls")]
        assert!(failure.is_none());
        assert!(connect("ftp://x", None).is_err());
        assert_eq!(authority("http://x:9000/").expect("parsed"), "x:9000");
        assert!(authority("x:9000").is_err());
    }
}

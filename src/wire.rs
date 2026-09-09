//! Plain HTTP/1.1 on the wire: one request and its answer over one
//! connection, `Content-Length` framed, `Connection: close`.
//!
//! What the JSON API is underneath the bearer token. Xmip's side writes a
//! request and reads the answer; the far end in [`crate::session`] reads a
//! request and writes an answer. Both halves are here so the two cannot
//! drift: a header written one way is read the same way.
//!
//! Chunked transfer encoding is not read. Every answer Cloud Storage gives to the four
//! calls this crate makes carries a `Content-Length`; one that carries
//! neither ends where the connection closes.

use std::io::{BufRead, BufReader, Read, Write};

use transport::error::{Result, classify, protocol_error};
use transport::wire::{MAX_BODY, header, read_head};

use crate::percent::{decode, encode};

/// One request, as Xmip's side builds it and the far end reads it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Request {
    pub method: String,
    /// The path as it travels: percent-encoded, opening with `/`.
    pub path: String,
    /// The query as names and values, before encoding.
    pub query: Vec<(String, String)>,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Request {
    #[must_use]
    pub fn new(method: &str, path: impl Into<String>) -> Self {
        Self {
            method: method.to_string(),
            path: path.into(),
            ..Self::default()
        }
    }

    #[must_use]
    pub fn query(mut self, name: &str, value: &str) -> Self {
        self.query.push((name.to_string(), value.to_string()));
        self
    }

    #[must_use]
    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_string(), value.to_string()));
        self
    }

    #[must_use]
    pub fn body(mut self, bytes: &[u8]) -> Self {
        self.body = bytes.to_vec();
        self
    }

    /// One header's value, however it was capitalised.
    #[must_use]
    pub fn header_value(&self, name: &str) -> Option<&str> {
        find(&self.headers, name)
    }

    /// One query parameter's value.
    #[must_use]
    pub fn query_value(&self, name: &str) -> Option<&str> {
        self.query
            .iter()
            .find(|(candidate, _)| candidate == name)
            .map(|(_, value)| value.as_str())
    }

    /// The request target as the request line carries it: the path, and the
    /// query encoded and joined.
    #[must_use]
    pub fn target(&self) -> String {
        if self.query.is_empty() {
            return self.path.clone();
        }
        let query: Vec<String> = self
            .query
            .iter()
            .map(|(name, value)| format!("{}={}", encode(name, false), encode(value, false)))
            .collect();
        format!("{}?{}", self.path, query.join("&"))
    }
}

/// One answer.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Response {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Response {
    #[must_use]
    pub fn new(status: u16) -> Self {
        Self {
            status,
            ..Self::default()
        }
    }

    #[must_use]
    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_string(), value.to_string()));
        self
    }

    #[must_use]
    pub fn body(mut self, bytes: &[u8]) -> Self {
        self.body = bytes.to_vec();
        self
    }

    /// One header's value, however it was capitalised.
    #[must_use]
    pub fn header_value(&self, name: &str) -> Option<&str> {
        find(&self.headers, name)
    }

    /// The body as text, lossily.
    #[must_use]
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

/// Write `request` and read the answer.
///
/// # Errors
/// Where the connection broke, or the answer is not HTTP Xmip can read.
pub fn exchange<S: Read + Write>(mut stream: S, request: &Request) -> Result<Response> {
    let head = format!(
        "{} {} HTTP/1.1\r\n{}Content-Length: {}\r\nConnection: close\r\n\r\n",
        request.method,
        request.target(),
        lines(&request.headers),
        request.body.len()
    );
    stream
        .write_all(head.as_bytes())
        .map_err(|e| classify("writing the request head", &e))?;
    stream
        .write_all(&request.body)
        .map_err(|e| classify("writing the request body", &e))?;
    stream
        .flush()
        .map_err(|e| classify("flushing the request", &e))?;
    read_response(&mut BufReader::new(stream))
}

fn read_response(reader: &mut impl BufRead) -> Result<Response> {
    let head = read_head(reader)?;
    let status = head
        .first()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .ok_or_else(|| protocol_error("an answer with no status line"))?;
    if header(&head, "transfer-encoding").is_some_and(|v| v.eq_ignore_ascii_case("chunked")) {
        return Err(protocol_error(
            "a chunked answer, which this transport does not read",
        ));
    }
    let body = if header(&head, "content-length").is_some() {
        read_body(reader, body_length(&head)?)?
    } else {
        let mut body = Vec::new();
        reader
            .read_to_end(&mut body)
            .map_err(|e| classify("reading the answer body", &e))?;
        body
    };
    Ok(Response {
        status,
        headers: headers_of(&head),
        body,
    })
}

/// The far end's side: one request off a connection, or `None` where the
/// peer closed without sending one.
///
/// # Errors
/// Where the connection broke, the request line is unreadable, or the body
/// is over what Xmip will read.
pub fn read_request(reader: &mut impl BufRead) -> Result<Option<Request>> {
    let head = read_head(reader)?;
    let Some(line) = head.first() else {
        return Ok(None);
    };
    let mut words = line.split_whitespace();
    let (Some(method), Some(target)) = (words.next(), words.next()) else {
        return Err(protocol_error(format!(
            "a request line Xmip cannot read: {line}"
        )));
    };
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let query = query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            (decode(name), decode(value))
        })
        .collect();
    let body = read_body(reader, body_length(&head)?)?;
    Ok(Some(Request {
        method: method.to_string(),
        path: path.to_string(),
        query,
        headers: headers_of(&head),
        body,
    }))
}

/// The far end's answer, written and flushed.
///
/// # Errors
/// Where the connection broke.
pub fn write_response(writer: &mut impl Write, response: &Response) -> Result<()> {
    let head = format!(
        "HTTP/1.1 {} {}\r\n{}Content-Length: {}\r\nConnection: close\r\n\r\n",
        response.status,
        reason(response.status),
        lines(&response.headers),
        response.body.len()
    );
    writer
        .write_all(head.as_bytes())
        .map_err(|e| classify("writing the answer head", &e))?;
    writer
        .write_all(&response.body)
        .map_err(|e| classify("writing the answer body", &e))?;
    writer
        .flush()
        .map_err(|e| classify("flushing the answer", &e))
}

fn lines(headers: &[(String, String)]) -> String {
    let lines: Vec<String> = headers
        .iter()
        .map(|(name, value)| format!("{name}: {value}\r\n"))
        .collect();
    lines.concat()
}

fn headers_of(head: &[String]) -> Vec<(String, String)> {
    head.iter()
        .skip(1)
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_string(), value.trim().to_string()))
        .collect()
}

fn find<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

fn body_length(head: &[String]) -> Result<usize> {
    let Some(value) = header(head, "content-length") else {
        return Ok(0);
    };
    let length: usize = value
        .parse()
        .map_err(|_| protocol_error(format!("a content-length that is not a number: {value}")))?;
    if length > MAX_BODY {
        return Err(protocol_error(format!(
            "a body of {length} bytes, over the {MAX_BODY} byte limit"
        )));
    }
    Ok(length)
}

fn read_body(reader: &mut impl Read, length: usize) -> Result<Vec<u8>> {
    let mut bytes = vec![0u8; length];
    reader
        .read_exact(&mut bytes)
        .map_err(|e| classify("reading the body", &e))?;
    Ok(bytes)
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        202 => "Accepted",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        _ => "Status",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stream that records what is written and serves a canned answer.
    struct Both(Vec<u8>, std::io::Cursor<Vec<u8>>);

    impl Read for Both {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            self.1.read(buf)
        }
    }

    impl Write for Both {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn a_request_round_trips_through_its_own_reader() {
        let request = Request::new("PUT", "/bucket/in/1%20a.edi")
            .query("prefix", "in/")
            .header("Host", "storage.local")
            .body(b"UNA");
        let answer = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok".to_vec();
        let mut both = Both(Vec::new(), std::io::Cursor::new(answer));
        let response = exchange(&mut both, &request).expect("exchanged");
        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"ok");
        let written = both.0;
        assert!(written.starts_with(b"PUT /bucket/in/1%20a.edi?prefix=in%2F HTTP/1.1\r\n"));
        let read = read_request(&mut &written[..]).expect("read").expect("one");
        assert_eq!(read.method, "PUT");
        assert_eq!(read.path, "/bucket/in/1%20a.edi");
        assert_eq!(read.query_value("prefix"), Some("in/"));
        assert_eq!(read.query_value("absent"), None);
        assert_eq!(read.header_value("host"), Some("storage.local"));
        assert_eq!(read.body, b"UNA");
        assert!(read_request(&mut &b""[..]).expect("closed").is_none());
        assert!(read_request(&mut &b"GET\r\n\r\n"[..]).is_err());
    }

    #[test]
    fn an_answer_is_written_as_it_is_read_back() {
        let mut written = Vec::new();
        write_response(&mut written, &Response::new(404).body(b"<Error/>")).expect("written");
        let response = read_response(&mut &written[..]).expect("read");
        assert_eq!(response.status, 404);
        assert_eq!(response.body, b"<Error/>");
        assert!(read_response(&mut &b"nonsense\r\n\r\n"[..]).is_err());
        let chunked = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n";
        assert!(read_response(&mut &chunked[..]).is_err());
        let unframed = b"HTTP/1.1 200 OK\r\n\r\nto the end";
        assert_eq!(
            read_response(&mut &unframed[..]).expect("read").body,
            b"to the end"
        );
        let over = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n",
            MAX_BODY + 1
        );
        assert!(read_response(&mut over.as_bytes()).is_err());
    }
}

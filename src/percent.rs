//! Percent-encoding, as a key travels in a path or a prefix in a query.
//!
//! RFC 3986's unreserved set and nothing more, which is what an object
//! name in a JSON API path demands: `a b` is `a%20b` and never `a+b`,
//! and a `/` in a name is escaped, because the name is one path segment.

use std::fmt::Write as _;

/// `text` with every byte outside the unreserved set as `%XX`, and `/` kept
/// where it separates the segments of a path.
#[must_use]
pub fn encode(text: &str, keep_slash: bool) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        let plain = byte.is_ascii_alphanumeric()
            || matches!(byte, b'-' | b'_' | b'.' | b'~')
            || (keep_slash && byte == b'/');
        if plain {
            out.push(char::from(byte));
        } else {
            write!(out, "%{byte:02X}").expect("writing to a String cannot fail");
        }
    }
    out
}

/// `%XX` back to bytes, lossily where they are not UTF-8; a `%` that is not
/// followed by two hex digits is left as it is.
#[must_use]
pub fn decode(text: &str) -> String {
    let raw = text.as_bytes();
    let mut out = Vec::with_capacity(raw.len());
    let mut at = 0;
    while at < raw.len() {
        let escaped = raw
            .get(at + 1..at + 3)
            .and_then(|hex| u8::from_str_radix(std::str::from_utf8(hex).ok()?, 16).ok());
        match (raw[at], escaped) {
            (b'%', Some(byte)) => {
                out.push(byte);
                at += 3;
            }
            (byte, _) => {
                out.push(byte);
                at += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoding_keeps_the_unreserved_and_the_path_slash() {
        assert_eq!(encode("in/a b+c.edi", true), "in/a%20b%2Bc.edi");
        assert_eq!(encode("in/", false), "in%2F");
        assert_eq!(encode("räksmörgås", false), "r%C3%A4ksm%C3%B6rg%C3%A5s");
        assert_eq!(decode("in%2Fa%20b"), "in/a b");
        assert_eq!(decode("r%C3%A4k"), "räk");
        assert_eq!(decode("%zz%4"), "%zz%4");
    }
}

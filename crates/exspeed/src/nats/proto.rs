//! The core NATS client protocol: a line-oriented text protocol
//! (<https://docs.nats.io/reference/reference-protocols/nats-protocol>).
//!
//! Clients send `CONNECT`, `PUB`, `HPUB`, `SUB`, `UNSUB`, `PING` and `PONG`;
//! the server sends `INFO`, `MSG`, `HMSG`, `PING`, `PONG`, `+OK` and `-ERR`.
//! Operation names are case-insensitive and arguments are separated by
//! spaces or tabs. Payloads follow their control line and end with CRLF.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::Deserialize;

/// Longest control line accepted (the NATS default).
pub const MAX_CONTROL_LINE: usize = 4096;

/// First line of a header block.
const HEADER_VERSION: &str = "NATS/1.0";

/// A client operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientOp {
    Connect(Box<ConnectOptions>),
    Pub {
        subject: String,
        reply_to: Option<String>,
        headers: Vec<(String, String)>,
        payload: Bytes,
    },
    Sub {
        subject: String,
        queue: Option<String>,
        sid: String,
    },
    Unsub {
        sid: String,
        max: Option<u64>,
    },
    Ping,
    Pong,
    /// `+OK` / `-ERR` / `INFO` from a client: ignored.
    Ignored,
}

/// The `CONNECT` options this server reads. Unknown fields are ignored.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct ConnectOptions {
    pub verbose: bool,
    pub pedantic: bool,
    pub name: Option<String>,
    pub lang: Option<String>,
    pub version: Option<String>,
    pub auth_token: Option<String>,
    pub user: Option<String>,
    pub pass: Option<String>,
    #[serde(default = "yes")]
    pub echo: bool,
    pub headers: bool,
    pub no_responders: bool,
}

fn yes() -> bool {
    true
}

/// A protocol error. `fatal` errors close the connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtoError {
    pub message: String,
    pub fatal: bool,
}

impl ProtoError {
    fn fatal(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            fatal: true,
        }
    }
}

/// Parse the next complete operation from `buf`, consuming it. `Ok(None)`
/// means more bytes are needed. `max_payload` bounds a `PUB`/`HPUB` body.
pub fn parse(buf: &mut BytesMut, max_payload: usize) -> Result<Option<ClientOp>, ProtoError> {
    let Some(nl) = buf.iter().position(|&b| b == b'\n') else {
        if buf.len() > MAX_CONTROL_LINE {
            return Err(ProtoError::fatal("Maximum Control Line Exceeded"));
        }
        return Ok(None);
    };
    if nl > MAX_CONTROL_LINE {
        return Err(ProtoError::fatal("Maximum Control Line Exceeded"));
    }
    let line_end = if nl > 0 && buf[nl - 1] == b'\r' {
        nl - 1
    } else {
        nl
    };
    let line = std::str::from_utf8(&buf[..line_end])
        .map_err(|_| ProtoError::fatal("Parser Error"))?
        .to_string();
    let (op, rest) = match line.find([' ', '\t']) {
        Some(i) => (&line[..i], line[i..].trim_matches([' ', '\t'])),
        None => (line.as_str(), ""),
    };
    let args: Vec<&str> = rest.split([' ', '\t']).filter(|s| !s.is_empty()).collect();
    let body_start = nl + 1;
    let op = op.to_ascii_uppercase();
    let parsed = match op.as_str() {
        "PING" => ClientOp::Ping,
        "PONG" => ClientOp::Pong,
        "+OK" | "-ERR" | "INFO" => ClientOp::Ignored,
        "CONNECT" => {
            let opts: ConnectOptions = serde_json::from_str(rest)
                .map_err(|e| ProtoError::fatal(format!("invalid CONNECT: {e}")))?;
            ClientOp::Connect(Box::new(opts))
        }
        "SUB" => {
            let (subject, queue, sid) = match args.as_slice() {
                [s, sid] => (*s, None, *sid),
                [s, q, sid] => (*s, Some(q.to_string()), *sid),
                _ => return Err(ProtoError::fatal("Parser Error")),
            };
            ClientOp::Sub {
                subject: subject.to_string(),
                queue,
                sid: sid.to_string(),
            }
        }
        "UNSUB" => {
            let (sid, max) = match args.as_slice() {
                [sid] => (*sid, None),
                [sid, max] => (
                    *sid,
                    Some(
                        max.parse::<u64>()
                            .map_err(|_| ProtoError::fatal("Parser Error"))?,
                    ),
                ),
                _ => return Err(ProtoError::fatal("Parser Error")),
            };
            ClientOp::Unsub {
                sid: sid.to_string(),
                max,
            }
        }
        "PUB" | "HPUB" => {
            let num = |s: &str| {
                s.parse::<usize>()
                    .map_err(|_| ProtoError::fatal("Parser Error"))
            };
            let (subject, reply_to, hdr_len, total) = match (op.as_str(), args.as_slice()) {
                ("PUB", [s, n]) => (*s, None, 0, num(n)?),
                ("PUB", [s, r, n]) => (*s, Some(*r), 0, num(n)?),
                ("HPUB", [s, h, n]) => (*s, None, num(h)?, num(n)?),
                ("HPUB", [s, r, h, n]) => (*s, Some(*r), num(h)?, num(n)?),
                _ => return Err(ProtoError::fatal("Parser Error")),
            };
            if hdr_len > total {
                return Err(ProtoError::fatal("Parser Error"));
            }
            if total > max_payload {
                return Err(ProtoError::fatal("Maximum Payload Violation"));
            }
            // Body plus its CRLF.
            if buf.len() < body_start + total + 2 {
                return Ok(None);
            }
            if &buf[body_start + total..body_start + total + 2] != b"\r\n" {
                return Err(ProtoError::fatal("Parser Error"));
            }
            let subject = subject.to_string();
            let reply_to = reply_to.map(str::to_string);
            buf.advance(body_start);
            let mut body = buf.split_to(total).freeze();
            buf.advance(2);
            let headers = if op == "HPUB" {
                let block = body.split_to(hdr_len);
                parse_headers(&block).map_err(ProtoError::fatal)?
            } else {
                Vec::new()
            };
            return Ok(Some(ClientOp::Pub {
                subject,
                reply_to,
                headers,
                payload: body,
            }));
        }
        _ => return Err(ProtoError::fatal("Unknown Protocol Operation")),
    };
    buf.advance(body_start);
    Ok(Some(parsed))
}

/// Parse a header block (`NATS/1.0[ status]\r\nKey: Value\r\n…\r\n\r\n`).
/// An inline status on a client's block is ignored.
pub fn parse_headers(block: &[u8]) -> Result<Vec<(String, String)>, String> {
    let text = std::str::from_utf8(block).map_err(|_| "headers are not UTF-8".to_string())?;
    let mut lines = text.split("\r\n");
    match lines.next() {
        Some(first) if first.starts_with(HEADER_VERSION) => {}
        _ => return Err("headers must start with NATS/1.0".to_string()),
    }
    let mut out = Vec::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let (k, v) = line
            .split_once(':')
            .ok_or_else(|| format!("malformed header line {line:?}"))?;
        let k = k.trim();
        if k.is_empty() {
            return Err("empty header name".to_string());
        }
        out.push((k.to_string(), v.trim().to_string()));
    }
    Ok(out)
}

/// Encode a header block. Line breaks inside names or values (which would
/// break the framing) become spaces.
pub fn encode_headers(headers: &[(String, String)], status: Option<u16>) -> Vec<u8> {
    let clean = |s: &str| s.replace(['\r', '\n'], " ");
    let mut out = Vec::with_capacity(32 + headers.len() * 32);
    out.extend_from_slice(HEADER_VERSION.as_bytes());
    if let Some(code) = status {
        out.extend_from_slice(format!(" {code}").as_bytes());
    }
    out.extend_from_slice(b"\r\n");
    for (k, v) in headers {
        out.extend_from_slice(clean(k).as_bytes());
        out.extend_from_slice(b": ");
        out.extend_from_slice(clean(v).as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(b"\r\n");
    out
}

/// `MSG` (or `HMSG` when `headers` is given) for subscription `sid`.
pub fn encode_msg(
    out: &mut BytesMut,
    subject: &str,
    sid: &str,
    reply_to: Option<&str>,
    headers: Option<&[u8]>,
    payload: &[u8],
) {
    let reply = reply_to.map(|r| format!(" {r}")).unwrap_or_default();
    match headers {
        Some(h) => {
            out.put_slice(
                format!(
                    "HMSG {subject} {sid}{reply} {} {}\r\n",
                    h.len(),
                    h.len() + payload.len()
                )
                .as_bytes(),
            );
            out.put_slice(h);
        }
        None => {
            out.put_slice(format!("MSG {subject} {sid}{reply} {}\r\n", payload.len()).as_bytes())
        }
    }
    out.put_slice(payload);
    out.put_slice(b"\r\n");
}

/// `-ERR '<message>'`.
pub fn encode_err(message: &str) -> Bytes {
    Bytes::from(format!("-ERR '{}'\r\n", message.replace('\'', "\"")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_all(input: &[u8]) -> Vec<ClientOp> {
        let mut buf = BytesMut::from(input);
        let mut ops = Vec::new();
        while let Some(op) = parse(&mut buf, 1 << 20).unwrap() {
            ops.push(op);
        }
        assert!(buf.is_empty(), "left over: {buf:?}");
        ops
    }

    #[test]
    fn parses_control_operations_case_insensitively() {
        let ops =
            parse_all(b"PING\r\npong\r\nSub foo.* 1\r\nSUB foo q 2\r\nunsub 1\r\nUNSUB 2 5\r\n");
        assert_eq!(
            ops,
            vec![
                ClientOp::Ping,
                ClientOp::Pong,
                ClientOp::Sub {
                    subject: "foo.*".into(),
                    queue: None,
                    sid: "1".into()
                },
                ClientOp::Sub {
                    subject: "foo".into(),
                    queue: Some("q".into()),
                    sid: "2".into()
                },
                ClientOp::Unsub {
                    sid: "1".into(),
                    max: None
                },
                ClientOp::Unsub {
                    sid: "2".into(),
                    max: Some(5)
                },
            ]
        );
    }

    #[test]
    fn parses_connect_with_defaults() {
        let ops = parse_all(
            br#"CONNECT {"verbose":false,"pedantic":false,"name":"svc","auth_token":"t","headers":true,"no_responders":true,"protocol":1}"#
                .iter()
                .chain(b"\r\n")
                .copied()
                .collect::<Vec<u8>>()
                .as_slice(),
        );
        let ClientOp::Connect(o) = &ops[0] else {
            panic!("{ops:?}")
        };
        assert_eq!(o.name.as_deref(), Some("svc"));
        assert_eq!(o.auth_token.as_deref(), Some("t"));
        assert!(o.echo, "echo defaults to true");
        assert!(o.headers && o.no_responders);
    }

    #[test]
    fn parses_pub_and_hpub_with_and_without_reply() {
        let hdr = b"NATS/1.0\r\nA: 1\r\nB:  two \r\n\r\n";
        let mut input = Vec::new();
        input.extend_from_slice(b"PUB a.b 5\r\nhello\r\nPUB a.b _INBOX.x.1 0\r\n\r\n");
        input.extend_from_slice(format!("HPUB c {} {}\r\n", hdr.len(), hdr.len() + 2).as_bytes());
        input.extend_from_slice(hdr);
        input.extend_from_slice(b"hi\r\n");
        input.extend_from_slice(format!("HPUB c r {} {}\r\n", hdr.len(), hdr.len()).as_bytes());
        input.extend_from_slice(hdr);
        input.extend_from_slice(b"\r\n");
        let ops = parse_all(&input);
        assert_eq!(
            ops[0],
            ClientOp::Pub {
                subject: "a.b".into(),
                reply_to: None,
                headers: vec![],
                payload: Bytes::from_static(b"hello")
            }
        );
        assert_eq!(
            ops[1],
            ClientOp::Pub {
                subject: "a.b".into(),
                reply_to: Some("_INBOX.x.1".into()),
                headers: vec![],
                payload: Bytes::new()
            }
        );
        let want_headers = vec![
            ("A".to_string(), "1".to_string()),
            ("B".to_string(), "two".to_string()),
        ];
        assert_eq!(
            ops[2],
            ClientOp::Pub {
                subject: "c".into(),
                reply_to: None,
                headers: want_headers.clone(),
                payload: Bytes::from_static(b"hi")
            }
        );
        assert_eq!(
            ops[3],
            ClientOp::Pub {
                subject: "c".into(),
                reply_to: Some("r".into()),
                headers: want_headers,
                payload: Bytes::new()
            }
        );
    }

    #[test]
    fn waits_for_a_complete_operation() {
        let full = b"PUB a 5\r\nhello\r\nPING\r\n";
        for cut in 0..full.len() {
            let mut buf = BytesMut::from(&full[..cut]);
            let mut got = Vec::new();
            while let Some(op) = parse(&mut buf, 1024).unwrap() {
                got.push(op);
            }
            buf.extend_from_slice(&full[cut..]);
            while let Some(op) = parse(&mut buf, 1024).unwrap() {
                got.push(op);
            }
            assert_eq!(got.len(), 2, "cut at {cut}");
        }
    }

    #[test]
    fn rejects_bad_input() {
        let err = |input: &[u8], max: usize| parse(&mut BytesMut::from(input), max).unwrap_err();
        assert_eq!(err(b"PUB a 10\r\n", 5).message, "Maximum Payload Violation");
        assert_eq!(err(b"PUB a 2\r\nhiXX", 5).message, "Parser Error");
        assert_eq!(err(b"FOO\r\n", 5).message, "Unknown Protocol Operation");
        assert_eq!(err(b"SUB\r\n", 5).message, "Parser Error");
        assert_eq!(err(b"HPUB a 5 2\r\n", 5).message, "Parser Error");
        let long = vec![b'x'; MAX_CONTROL_LINE + 1];
        assert_eq!(err(&long, 5).message, "Maximum Control Line Exceeded");
        assert!(err(b"PUB a 1\r\n", 0).fatal);
    }

    #[test]
    fn header_blocks_round_trip() {
        let h = vec![
            ("k".to_string(), "v".to_string()),
            ("x-a".to_string(), "b c".to_string()),
        ];
        let block = encode_headers(&h, None);
        assert_eq!(block, b"NATS/1.0\r\nk: v\r\nx-a: b c\r\n\r\n");
        assert_eq!(parse_headers(&block).unwrap(), h);
        assert_eq!(encode_headers(&[], Some(503)), b"NATS/1.0 503\r\n\r\n");
        assert_eq!(
            encode_headers(&[("a".into(), "1\r\n2".into())], None),
            b"NATS/1.0\r\na: 1  2\r\n\r\n"
        );
        assert!(parse_headers(b"HTTP/1.1\r\n\r\n").is_err());
    }

    #[test]
    fn encodes_msg_and_hmsg() {
        let mut out = BytesMut::new();
        encode_msg(&mut out, "a.b", "7", None, None, b"hi");
        encode_msg(&mut out, "a.b", "7", Some("r.1"), None, b"");
        let h = encode_headers(&[], Some(503));
        encode_msg(&mut out, "r", "9", None, Some(&h), b"");
        assert_eq!(
            &out[..],
            b"MSG a.b 7 2\r\nhi\r\nMSG a.b 7 r.1 0\r\n\r\nHMSG r 9 16 16\r\nNATS/1.0 503\r\n\r\n\r\n"
        );
        assert_eq!(&encode_err("it's bad")[..], b"-ERR 'it\"s bad'\r\n");
    }
}

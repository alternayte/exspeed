use crate::auth::error::AuthError;
use crate::types::StreamName;

/// Compiled stream-name glob. The grammar is deliberately tiny:
/// character class `[a-zA-Z0-9_-]` for literals, `*` for "zero or more
/// allowed chars". No `?`, no `[...]`, no escapes. Match is anchored at
/// both ends.
///
/// Matching is implemented by splitting the pattern on `*` and walking
/// the name left-to-right looking for each literal chunk in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamGlob {
    raw: String,
    /// Literal chunks between stars. For `"orders-*"` this is `["orders-", ""]`.
    chunks: Vec<String>,
    starts_with_star: bool,
    ends_with_star: bool,
}

impl StreamGlob {
    /// Compile a glob string. Returns `AuthError::InvalidGlob` on any
    /// disallowed character. The `credential_name` is only used to build
    /// the error message.
    pub fn compile(raw: &str, credential_name: &str) -> Result<Self, AuthError> {
        if raw.is_empty() {
            return Err(AuthError::InvalidGlob {
                name: credential_name.to_string(),
                glob: raw.to_string(),
            });
        }
        for ch in raw.chars() {
            let ok = ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' || ch == '*';
            if !ok {
                return Err(AuthError::InvalidGlob {
                    name: credential_name.to_string(),
                    glob: raw.to_string(),
                });
            }
        }
        let starts_with_star = raw.starts_with('*');
        let ends_with_star = raw.ends_with('*');
        let chunks: Vec<String> = raw.split('*').map(|s| s.to_string()).collect();
        Ok(Self {
            raw: raw.to_string(),
            chunks,
            starts_with_star,
            ends_with_star,
        })
    }

    /// True iff this glob is the single `"*"` wildcard — matches every stream.
    pub fn is_wildcard_all(&self) -> bool {
        self.raw == "*"
    }

    /// Match against a validated stream name.
    pub fn matches(&self, name: &StreamName) -> bool {
        let haystack = name.as_str();

        // Fast path: no wildcards → exact match.
        if self.chunks.len() == 1 {
            return self.chunks[0] == haystack;
        }

        let mut pos = 0usize;
        let last = self.chunks.len() - 1;

        for (i, chunk) in self.chunks.iter().enumerate() {
            if chunk.is_empty() {
                continue;
            }
            if i == 0 && !self.starts_with_star {
                if !haystack[pos..].starts_with(chunk.as_str()) {
                    return false;
                }
                pos += chunk.len();
            } else if i == last && !self.ends_with_star {
                if !haystack[pos..].ends_with(chunk.as_str()) {
                    return false;
                }
                // Nothing to advance past; we're at the end.
            } else {
                match haystack[pos..].find(chunk.as_str()) {
                    Some(off) => pos += off + chunk.len(),
                    None => return false,
                }
            }
        }
        true
    }

    pub fn as_str(&self) -> &str {
        &self.raw
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::StreamName;

    fn n(s: &str) -> StreamName {
        StreamName::try_from(s).unwrap()
    }

    #[test]
    fn exact_match_no_wildcard() {
        let g = StreamGlob::compile("orders", "t").unwrap();
        assert!(g.matches(&n("orders")));
        assert!(!g.matches(&n("orders-placed")));
    }

    #[test]
    fn trailing_wildcard() {
        let g = StreamGlob::compile("orders-*", "t").unwrap();
        assert!(g.matches(&n("orders-placed")));
        assert!(g.matches(&n("orders-")));
        assert!(!g.matches(&n("payments-orders-placed")));
    }

    #[test]
    fn leading_wildcard() {
        let g = StreamGlob::compile("*-orders", "t").unwrap();
        assert!(g.matches(&n("team-a-orders")));
        assert!(!g.matches(&n("orders")));
    }

    #[test]
    fn middle_wildcard() {
        let g = StreamGlob::compile("team-*-orders", "t").unwrap();
        assert!(g.matches(&n("team-a-orders")));
        assert!(g.matches(&n("team-alpha-beta-orders")));
        assert!(!g.matches(&n("team-orders")));
    }

    #[test]
    fn double_wildcard() {
        let g = StreamGlob::compile("team-*-orders-*", "t").unwrap();
        assert!(g.matches(&n("team-a-orders-placed")));
        assert!(!g.matches(&n("team-a-payments-placed")));
    }

    #[test]
    fn match_all() {
        let g = StreamGlob::compile("*", "t").unwrap();
        assert!(g.is_wildcard_all());
        assert!(g.matches(&n("anything")));
        assert!(g.matches(&n("x")));
    }

    #[test]
    fn rejects_invalid_chars() {
        assert!(matches!(
            StreamGlob::compile("orders.*", "t"),
            Err(AuthError::InvalidGlob { .. })
        ));
        assert!(matches!(
            StreamGlob::compile("orders?", "t"),
            Err(AuthError::InvalidGlob { .. })
        ));
        assert!(matches!(
            StreamGlob::compile("", "t"),
            Err(AuthError::InvalidGlob { .. })
        ));
    }
}

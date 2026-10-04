/// A parsed, validated subject filter. Parse once (e.g. per consumer) and
/// call [`SubjectFilter::matches`] per record without allocating.
///
/// Syntax: dot-delimited tokens; `*` matches exactly one token, `>` matches
/// one or more trailing tokens and must be the last token. An empty filter
/// matches everything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubjectFilter {
    tokens: Vec<FilterToken>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum FilterToken {
    Literal(String),
    One,
    Rest,
}

impl SubjectFilter {
    /// The filter that matches every subject.
    pub fn all() -> Self {
        Self { tokens: Vec::new() }
    }

    pub fn parse(pattern: &str) -> Result<Self, String> {
        if pattern.is_empty() {
            return Ok(Self::all());
        }
        let parts: Vec<&str> = pattern.split('.').collect();
        let mut tokens = Vec::with_capacity(parts.len());
        for (i, part) in parts.iter().enumerate() {
            let tok = match *part {
                "" => return Err(format!("subject filter '{pattern}' has an empty token")),
                "*" => FilterToken::One,
                ">" if i + 1 == parts.len() => FilterToken::Rest,
                ">" => {
                    return Err(format!(
                        "subject filter '{pattern}': '>' must be the last token"
                    ))
                }
                p if p.contains('*') || p.contains('>') || p.contains(char::is_whitespace) => {
                    return Err(format!(
                        "subject filter '{pattern}': wildcards must be whole tokens"
                    ))
                }
                p => FilterToken::Literal(p.to_string()),
            };
            tokens.push(tok);
        }
        Ok(Self { tokens })
    }

    pub fn is_all(&self) -> bool {
        self.tokens.is_empty()
    }

    /// Whether every subject `other` matches also matches `self`.
    pub fn covers(&self, other: &SubjectFilter) -> bool {
        if self.is_all() {
            return true;
        }
        if other.is_all() {
            return false;
        }
        let (a, b) = (&self.tokens, &other.tokens);
        for (i, ta) in a.iter().enumerate() {
            let Some(tb) = b.get(i) else {
                return false;
            };
            match (ta, tb) {
                (FilterToken::Rest, _) => return true,
                (_, FilterToken::Rest) => return false,
                (FilterToken::One, _) => {}
                (FilterToken::Literal(x), FilterToken::Literal(y)) if x == y => {}
                _ => return false,
            }
        }
        a.len() == b.len()
    }

    /// The filter's text form.
    pub fn as_pattern(&self) -> String {
        self.tokens
            .iter()
            .map(|t| match t {
                FilterToken::Literal(l) => l.as_str(),
                FilterToken::One => "*",
                FilterToken::Rest => ">",
            })
            .collect::<Vec<_>>()
            .join(".")
    }

    /// True when the filter has no wildcard (it names one subject).
    pub fn is_literal(&self) -> bool {
        !self.tokens.is_empty()
            && self
                .tokens
                .iter()
                .all(|t| matches!(t, FilterToken::Literal(_)))
    }

    /// Whether some subject matches both filters.
    pub fn overlaps(&self, other: &SubjectFilter) -> bool {
        if self.is_all() || other.is_all() {
            return true;
        }
        let (a, b) = (&self.tokens, &other.tokens);
        for i in 0..a.len().max(b.len()) {
            match (a.get(i), b.get(i)) {
                (Some(FilterToken::Rest), Some(_)) | (Some(_), Some(FilterToken::Rest)) => {
                    return true
                }
                (Some(FilterToken::Literal(x)), Some(FilterToken::Literal(y))) if x != y => {
                    return false
                }
                (Some(_), Some(_)) => {}
                // One filter ran out: only a trailing `>` on the other could
                // still match, and it needs at least one more token, which the
                // shorter filter can't produce.
                _ => return false,
            }
        }
        true
    }

    pub fn matches(&self, subject: &str) -> bool {
        if self.tokens.is_empty() {
            return true;
        }
        let mut parts = subject.split('.');
        for tok in &self.tokens {
            match tok {
                FilterToken::Rest => return parts.next().is_some(),
                FilterToken::One => {
                    if parts.next().is_none() {
                        return false;
                    }
                }
                FilterToken::Literal(l) => {
                    if parts.next() != Some(l.as_str()) {
                        return false;
                    }
                }
            }
        }
        parts.next().is_none()
    }
}

/// Several filters OR-ed together (empty set = match all).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SubjectFilters(Vec<SubjectFilter>);

impl SubjectFilters {
    pub fn parse<S: AsRef<str>>(patterns: &[S]) -> Result<Self, String> {
        let mut out = Vec::new();
        for p in patterns {
            let f = SubjectFilter::parse(p.as_ref())?;
            if f.is_all() {
                return Ok(Self(Vec::new()));
            }
            out.push(f);
        }
        Ok(Self(out))
    }

    pub fn matches(&self, subject: &str) -> bool {
        self.0.is_empty() || self.0.iter().any(|f| f.matches(subject))
    }

    /// True when every subject matches (no filters).
    pub fn is_all(&self) -> bool {
        self.0.is_empty()
    }

    /// Whether some subject matches both filter sets.
    pub fn overlaps(&self, other: &SubjectFilters) -> bool {
        if self.is_all() || other.is_all() {
            return true;
        }
        self.0.iter().any(|a| other.0.iter().any(|b| a.overlaps(b)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn subject_matches(subject: &str, pattern: &str) -> bool {
        SubjectFilter::parse(pattern).unwrap().matches(subject)
    }

    #[test]
    fn covers() {
        let c = |a: &str, b: &str| {
            SubjectFilter::parse(a)
                .unwrap()
                .covers(&SubjectFilter::parse(b).unwrap())
        };
        assert!(c("", "a.b"));
        assert!(!c("a.b", ""));
        assert!(c("orders.>", "orders.*"));
        assert!(c("orders.>", "orders.eu.>"));
        assert!(c("orders.*", "orders.eu"));
        assert!(!c("orders.*", "orders.>"));
        assert!(!c("orders.*", "orders.eu.x"));
        assert!(!c("orders.eu", "orders.*"));
        assert!(!c("orders.>", "orders"));
        assert!(!c("orders.>", ">"));
        assert!(c("*.created", "eu.created"));
        assert_eq!(SubjectFilter::parse("a.*.>").unwrap().as_pattern(), "a.*.>");
        assert!(SubjectFilter::parse("a.b").unwrap().is_literal());
        assert!(!SubjectFilter::parse("a.*").unwrap().is_literal());
    }

    #[test]
    fn overlap() {
        let o = |a: &str, b: &str| {
            let (fa, fb) = (
                SubjectFilter::parse(a).unwrap(),
                SubjectFilter::parse(b).unwrap(),
            );
            assert_eq!(
                fa.overlaps(&fb),
                fb.overlaps(&fa),
                "symmetric for {a} / {b}"
            );
            fa.overlaps(&fb)
        };
        assert!(o("", "a.b"));
        assert!(o("a.b", "a.b"));
        assert!(!o("a.b", "a.c"));
        assert!(o("a.*", "a.b"));
        assert!(o("a.>", "a.b.c"));
        assert!(!o("a.>", "a"));
        assert!(!o("a.*", "a.b.c"));
        assert!(o("*.b", "a.*"));
        assert!(!o("orders.eu.*", "orders.us.>"));
        assert!(o("orders.*.created", "orders.eu.>"));
        assert!(!o("a.b", "a.b.c"));
        let fs = |v: &[&str]| SubjectFilters::parse(v).unwrap();
        assert!(!fs(&["eu.>"]).overlaps(&fs(&["us.>", "asia.*"])));
        assert!(fs(&["eu.>"]).overlaps(&fs(&["us.>", "eu.x"])));
        assert!(fs(&[]).overlaps(&fs(&["us.>"])));
    }

    #[test]
    fn empty_pattern_matches_all() {
        assert!(subject_matches("anything", ""));
        assert!(subject_matches("orders.eu.created", ""));
        assert!(subject_matches("", ""));
    }

    #[test]
    fn exact_match() {
        assert!(subject_matches("orders.created", "orders.created"));
        assert!(!subject_matches("orders.created", "orders.shipped"));
    }

    #[test]
    fn single_level_wildcard() {
        assert!(subject_matches("orders.created", "orders.*"));
        assert!(subject_matches("orders.shipped", "orders.*"));
        assert!(!subject_matches("orders.eu.created", "orders.*"));
        assert!(!subject_matches("orders", "orders.*"));
    }

    #[test]
    fn multi_level_wildcard() {
        assert!(subject_matches("orders.eu.created", "orders.>"));
        assert!(subject_matches("orders.us.shipped.late", "orders.>"));
        assert!(subject_matches("orders.created", "orders.>"));
        assert!(!subject_matches("payments.created", "orders.>"));
    }

    #[test]
    fn mixed_patterns() {
        assert!(subject_matches("orders.eu.created", "orders.eu.*"));
        assert!(subject_matches("orders.eu.cancelled", "orders.eu.*"));
        assert!(!subject_matches("orders.us.created", "orders.eu.*"));
        assert!(subject_matches("orders.eu.created", "orders.*.created"));
        assert!(subject_matches("orders.us.created", "orders.*.created"));
        assert!(!subject_matches("orders.eu.shipped", "orders.*.created"));
    }

    #[test]
    fn wildcard_at_start() {
        assert!(subject_matches("orders.created", "*.created"));
        assert!(subject_matches("payments.created", "*.created"));
        assert!(!subject_matches("orders.eu.created", "*.created"));
    }

    #[test]
    fn no_match_different_depth() {
        assert!(!subject_matches("orders", "orders.created"));
        assert!(!subject_matches("orders.eu.created", "orders.created"));
    }

    #[test]
    fn gt_must_match_at_least_one() {
        assert!(!subject_matches("orders", "orders.>"));
    }

    #[test]
    fn filter_validation() {
        assert!(SubjectFilter::parse("a.>.c").is_err());
        assert!(SubjectFilter::parse("a..b").is_err());
        assert!(SubjectFilter::parse("a.b*").is_err());
        assert!(SubjectFilter::parse("a.*b").is_err());
        assert!(SubjectFilter::parse("a. b").is_err());
        assert!(SubjectFilter::parse(".a").is_err());
        assert!(SubjectFilter::parse("a.").is_err());
        assert!(SubjectFilter::parse("a.>").is_ok());
        // A subject with an empty token (never publishable) still matches
        // a wildcard filter sensibly.
        assert!(subject_matches("a..b", "a.*.b"));
        assert!(subject_matches("a.", "a.>"));
    }

    #[test]
    fn multiple_filters() {
        let f = SubjectFilters::parse(&["a.*", "b.>"]).unwrap();
        assert!(f.matches("a.x"));
        assert!(f.matches("b.x.y"));
        assert!(!f.matches("c.x"));
        assert!(SubjectFilters::parse::<&str>(&[])
            .unwrap()
            .matches("anything"));
    }
}

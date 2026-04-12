/// Match a subject string against a pattern.
///
/// Pattern syntax (NATS-style):
/// - `*` matches exactly one token (tokens separated by `.`)
/// - `>` matches one or more tokens (must be the last token in pattern)
/// - Empty pattern matches everything
/// - Literal tokens match exactly
pub fn subject_matches(subject: &str, pattern: &str) -> bool {
    if pattern.is_empty() {
        return true;
    }

    let subject_tokens: Vec<&str> = subject.split('.').collect();
    let pattern_tokens: Vec<&str> = pattern.split('.').collect();

    let mut si = 0;
    let mut pi = 0;

    while pi < pattern_tokens.len() {
        let pt = pattern_tokens[pi];

        if pt == ">" {
            return si < subject_tokens.len();
        }

        if si >= subject_tokens.len() {
            return false;
        }

        if pt == "*" {
            si += 1;
            pi += 1;
        } else if pt == subject_tokens[si] {
            si += 1;
            pi += 1;
        } else {
            return false;
        }
    }

    si == subject_tokens.len()
}

#[cfg(test)]
mod tests {
    use super::*;

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
}

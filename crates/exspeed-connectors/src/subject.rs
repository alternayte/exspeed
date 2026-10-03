//! `subject_template` rendering, shared by every source.
//!
//! - `{name}` is a plugin variable (e.g. `{schema}`, `{table}`, `{op}`,
//!   `{aggregate_type}`, `{routing_key}`).
//! - `{$.a.b}` reads a field from the record's JSON value.
//!
//! Substituted values are made subject-safe: whitespace, `*` and `>` become
//! `_`, and an empty or missing value becomes `unknown`. Unknown
//! placeholders are left as written.

pub fn render(template: &str, vars: &[(&str, &str)], json: Option<&serde_json::Value>) -> String {
    let mut out = String::with_capacity(template.len() + 16);
    let mut rest = template;
    while let Some(start) = rest.find('{') {
        out.push_str(&rest[..start]);
        let after = &rest[start + 1..];
        let Some(end) = after.find('}') else {
            out.push_str(&rest[start..]);
            return out;
        };
        let name = &after[..end];
        let value: Option<String> = if let Some(path) = name.strip_prefix("$.") {
            Some(
                json.and_then(|j| lookup(j, path))
                    .map(json_scalar)
                    .unwrap_or_default(),
            )
        } else {
            vars.iter()
                .find(|(k, _)| *k == name)
                .map(|(_, v)| v.to_string())
        };
        match value {
            Some(v) => out.push_str(&sanitize(&v)),
            None => {
                out.push('{');
                out.push_str(name);
                out.push('}');
            }
        }
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    out
}

/// Look up a dotted path in a JSON value.
pub fn lookup<'a>(v: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let path = path.strip_prefix("$.").unwrap_or(path);
    if path.is_empty() || path == "$" {
        return Some(v);
    }
    let mut cur = v;
    for seg in path.split('.') {
        cur = match cur {
            serde_json::Value::Object(o) => o.get(seg)?,
            serde_json::Value::Array(a) => a.get(seg.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(cur)
}

fn json_scalar(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// Make one substituted value safe inside a subject.
pub fn sanitize(v: &str) -> String {
    if v.is_empty() {
        return "unknown".into();
    }
    let s: String = v
        .chars()
        .map(|c| {
            if c.is_whitespace() || c == '*' || c == '>' {
                '_'
            } else {
                c
            }
        })
        .collect();
    // No empty tokens: collapse leading/trailing/double dots.
    let tokens: Vec<&str> = s.split('.').filter(|t| !t.is_empty()).collect();
    if tokens.is_empty() {
        "unknown".into()
    } else {
        tokens.join(".")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn vars_and_json_paths() {
        let j = json!({"type": "order.created", "n": 3, "a": {"b": "deep"}});
        assert_eq!(
            render(
                "{schema}.{table}.{$.type}",
                &[("schema", "public"), ("table", "users")],
                Some(&j)
            ),
            "public.users.order.created"
        );
        assert_eq!(render("x.{$.n}.{$.a.b}", &[], Some(&j)), "x.3.deep");
        assert_eq!(render("x.{$.missing}", &[], Some(&j)), "x.unknown");
        assert_eq!(render("x.{other}", &[], None), "x.{other}");
        assert_eq!(render("literal", &[], None), "literal");
    }

    #[test]
    fn values_are_sanitized() {
        assert_eq!(render("a.{v}", &[("v", "has space")], None), "a.has_space");
        assert_eq!(render("a.{v}", &[("v", "x.>")], None), "a.x._");
        assert_eq!(render("a.{v}", &[("v", "")], None), "a.unknown");
    }
}

//! Typed plugin settings.
//!
//! Every plugin declares a `serde` struct with `#[serde(deny_unknown_fields)]`
//! and parses its `[settings]` with [`parse`]. The helpers in [`de`] accept
//! both native TOML/JSON types and strings, because `${VAR}` substitution
//! always produces strings (`port = "${PG_PORT}"`).

use serde::de::{DeserializeOwned, Error as _};
use serde::{Deserialize, Deserializer};

use crate::config::Settings;
use crate::traits::ConnectorError;

/// Deserialize `settings` into the plugin's settings struct. Errors name the
/// plugin and the offending key.
pub fn parse<T: DeserializeOwned>(plugin: &str, settings: &Settings) -> Result<T, ConnectorError> {
    serde_json::from_value(serde_json::Value::Object(settings.clone()))
        .map_err(|e| ConnectorError::config(format!("invalid settings for plugin '{plugin}': {e}")))
}

pub mod de {
    use super::*;
    use std::collections::BTreeMap;

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum NumOrStr {
        Num(serde_json::Number),
        Str(String),
    }

    fn num<'de, D, T>(d: D) -> Result<T, D::Error>
    where
        D: Deserializer<'de>,
        T: std::str::FromStr + TryFrom<u64>,
        <T as std::str::FromStr>::Err: std::fmt::Display,
    {
        match NumOrStr::deserialize(d)? {
            NumOrStr::Num(n) => n.as_u64().and_then(|v| T::try_from(v).ok()).ok_or_else(|| {
                D::Error::custom(format!("expected a non-negative integer, got {n}"))
            }),
            NumOrStr::Str(s) => s
                .trim()
                .parse::<T>()
                .map_err(|e| D::Error::custom(format!("invalid number '{s}': {e}"))),
        }
    }

    pub fn u64<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
        num(d)
    }
    pub fn u32<'de, D: Deserializer<'de>>(d: D) -> Result<u32, D::Error> {
        num(d)
    }
    pub fn u16<'de, D: Deserializer<'de>>(d: D) -> Result<u16, D::Error> {
        num(d)
    }
    pub fn usize<'de, D: Deserializer<'de>>(d: D) -> Result<usize, D::Error> {
        num(d)
    }

    pub fn opt_u64<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u64>, D::Error> {
        num(d).map(Some)
    }

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum BoolOrStr {
        Bool(bool),
        Str(String),
    }

    pub fn bool<'de, D: Deserializer<'de>>(d: D) -> Result<bool, D::Error> {
        match BoolOrStr::deserialize(d)? {
            BoolOrStr::Bool(b) => Ok(b),
            BoolOrStr::Str(s) => match s.trim().to_ascii_lowercase().as_str() {
                "true" | "yes" | "1" | "on" => Ok(true),
                "false" | "no" | "0" | "off" | "" => Ok(false),
                other => Err(D::Error::custom(format!("invalid boolean '{other}'"))),
            },
        }
    }

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum ListOrStr {
        List(Vec<String>),
        Str(String),
    }

    /// An array of strings, or a comma-separated string.
    pub fn string_list<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<String>, D::Error> {
        Ok(match ListOrStr::deserialize(d)? {
            ListOrStr::List(v) => v
                .into_iter()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
            ListOrStr::Str(s) => s
                .split(',')
                .map(|p| p.trim().to_string())
                .filter(|p| !p.is_empty())
                .collect(),
        })
    }

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum MapOrStr {
        Map(BTreeMap<String, serde_json::Value>),
        Str(String),
    }

    /// A table of string values (`{ Authorization = "Bearer x" }`), or the
    /// legacy `"Key: Value, Key2: Value2"` string.
    pub fn string_map<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<(String, String)>, D::Error> {
        Ok(match MapOrStr::deserialize(d)? {
            MapOrStr::Map(m) => m
                .into_iter()
                .map(|(k, v)| {
                    let v = match v {
                        serde_json::Value::String(s) => s,
                        other => other.to_string(),
                    };
                    (k, v)
                })
                .collect(),
            MapOrStr::Str(s) => {
                let mut out = Vec::new();
                for part in s.split(',') {
                    let part = part.trim();
                    if part.is_empty() {
                        continue;
                    }
                    let Some(colon) = part.find(':') else {
                        return Err(D::Error::custom(format!(
                            "invalid header '{part}': expected 'Name: value'"
                        )));
                    };
                    out.push((
                        part[..colon].trim().to_string(),
                        part[colon + 1..].trim().to_string(),
                    ));
                }
                out
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct S {
        #[serde(deserialize_with = "de::u64")]
        n: u64,
        #[serde(default, deserialize_with = "de::bool")]
        b: bool,
        #[serde(default, deserialize_with = "de::string_list")]
        l: Vec<String>,
        #[serde(default, deserialize_with = "de::string_map")]
        m: Vec<(String, String)>,
    }

    fn settings(v: serde_json::Value) -> Settings {
        v.as_object().unwrap().clone()
    }

    #[test]
    fn native_and_string_forms() {
        let a: S = parse(
            "p",
            &settings(json!({"n": 5, "b": true, "l": ["x","y"], "m": {"A": "1"}})),
        )
        .unwrap();
        let b: S = parse(
            "p",
            &settings(json!({"n": "5", "b": "true", "l": "x, y", "m": "A: 1"})),
        )
        .unwrap();
        assert_eq!(a.n, b.n);
        assert_eq!(a.b, b.b);
        assert_eq!(a.l, b.l);
        assert_eq!(a.m, b.m);
    }

    #[test]
    fn misspelled_key_fails_with_its_name() {
        let err = parse::<S>("p", &settings(json!({"n": 1, "bb": true}))).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("bb") && msg.contains("plugin 'p'"), "{msg}");
    }

    #[test]
    fn bad_values_fail() {
        assert!(parse::<S>("p", &settings(json!({"n": -1}))).is_err());
        assert!(parse::<S>("p", &settings(json!({"n": "abc"}))).is_err());
        assert!(parse::<S>("p", &settings(json!({"n": 1, "b": "maybe"}))).is_err());
    }
}

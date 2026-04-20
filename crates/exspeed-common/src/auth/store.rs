use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use enumset::EnumSet;
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::auth::error::AuthError;
use crate::auth::glob::StreamGlob;
use crate::auth::types::{Action, Identity, IdentityRef, Permission};

/// Synthetic credential name used when `EXSPEED_AUTH_TOKEN` is set. Cannot
/// collide with a TOML-defined credential — the TOML loader rejects an
/// entry with this name when the env var is also set.
pub const LEGACY_ADMIN_NAME: &str = "legacy-admin";

// ---- Wire form (TOML) ------------------------------------------------------

#[derive(Debug, Deserialize)]
struct WireFile {
    #[serde(default)]
    credentials: Vec<WireCredential>,
}

#[derive(Debug, Deserialize)]
struct WireCredential {
    name: String,
    token_sha256: String,
    #[serde(default)]
    permissions: Vec<WirePermission>,
}

#[derive(Debug, Deserialize)]
struct WirePermission {
    streams: String,
    actions: Vec<String>,
}

// ---- Runtime form ----------------------------------------------------------

/// In-memory credential table. Built once at startup, shared via `Arc`.
/// Lookup by `sha256(raw_token)` is O(1).
#[derive(Debug)]
pub struct CredentialStore {
    by_hash: HashMap<[u8; 32], IdentityRef>,
    /// Breakdown for the startup log line.
    file_count: usize,
    legacy_admin_present: bool,
}

impl CredentialStore {
    /// Build a store from an optional file path + optional env-var token.
    ///
    /// - `from_file = Some(path)` → parse TOML at `path`. Missing file is
    ///   an error; caller decides whether to invoke with None.
    /// - `env_token = Some(raw)` → inject a synthetic `legacy-admin`
    ///   credential with global admin.
    /// - Both None is a programming error (caller should not wrap None
    ///   in a store); this returns an empty store anyway for robustness.
    pub fn build(
        from_file: Option<&Path>,
        env_token: Option<&str>,
    ) -> Result<Self, AuthError> {
        let mut by_hash: HashMap<[u8; 32], IdentityRef> = HashMap::new();
        let mut names: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut hash_to_name: HashMap<[u8; 32], String> = HashMap::new();
        let mut file_count = 0usize;

        if let Some(path) = from_file {
            // Open-once + match on NotFound rather than exists()+read: avoids a TOCTOU
            // race where the file vanishes between the two calls and `?` surfaces the
            // uglier FileIo(NotFound) instead of the intended FileMissing.
            let raw = match std::fs::read_to_string(path) {
                Ok(s) => s,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    return Err(AuthError::FileMissing(path.to_path_buf()));
                }
                Err(e) => return Err(e.into()),
            };
            let wire: WireFile = toml::from_str(&raw)?;

            // Validation order: cheap syntactic checks → dup checks → expensive compile.
            // Running dup checks before compile_credential means a double-pasted block
            // produces the more actionable DuplicateName (not DuplicateTokenHash) and
            // avoids wasted glob compilation on the error path.
            for wc in wire.credentials {
                if wc.name == LEGACY_ADMIN_NAME && env_token.is_some() {
                    return Err(AuthError::LegacyAdminReserved);
                }
                validate_name_charset(&wc.name)?;
                let digest = decode_hash(&wc.token_sha256, &wc.name)?;

                if names.contains(&wc.name) {
                    return Err(AuthError::DuplicateName(wc.name.clone()));
                }
                if let Some(other) = hash_to_name.get(&digest) {
                    return Err(AuthError::DuplicateTokenHash {
                        first: other.clone(),
                        second: wc.name.clone(),
                    });
                }

                let id = Arc::new(compile_credential(&wc)?);
                names.insert(wc.name.clone());
                hash_to_name.insert(digest, wc.name.clone());
                by_hash.insert(digest, id);
                file_count += 1;
            }
        }

        let mut legacy_admin_present = false;
        if let Some(token) = env_token {
            let digest: [u8; 32] = Sha256::digest(token.as_bytes()).into();
            // env-var token collides with a file entry's hash? Treat as DuplicateTokenHash.
            if let Some(other) = hash_to_name.get(&digest) {
                return Err(AuthError::DuplicateTokenHash {
                    first: other.clone(),
                    second: LEGACY_ADMIN_NAME.to_string(),
                });
            }
            let id = Arc::new(Identity {
                name: LEGACY_ADMIN_NAME.to_string(),
                permissions: vec![Permission {
                    streams: StreamGlob::compile("*", LEGACY_ADMIN_NAME)?,
                    actions: Action::Publish | Action::Subscribe | Action::Admin,
                }],
            });
            by_hash.insert(digest, id);
            legacy_admin_present = true;
        }

        Ok(Self { by_hash, file_count, legacy_admin_present })
    }

    /// O(1) lookup by sha256 of the raw token bytes.
    pub fn lookup(&self, digest: &[u8; 32]) -> Option<IdentityRef> {
        self.by_hash.get(digest).cloned()
    }

    pub fn len(&self) -> usize {
        self.by_hash.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_hash.is_empty()
    }

    /// `(file_count, legacy_admin_present)` for the startup log.
    pub fn source_breakdown(&self) -> (usize, bool) {
        (self.file_count, self.legacy_admin_present)
    }
}

fn validate_name_charset(name: &str) -> Result<(), AuthError> {
    if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-') {
        return Err(AuthError::InvalidName { name: name.to_string() });
    }
    Ok(())
}

fn compile_credential(wc: &WireCredential) -> Result<Identity, AuthError> {
    validate_name_charset(&wc.name)?;

    let mut permissions = Vec::with_capacity(wc.permissions.len());
    for wp in &wc.permissions {
        let streams = StreamGlob::compile(&wp.streams, &wc.name)?;
        let mut actions = EnumSet::<Action>::new();
        for a in &wp.actions {
            match a.as_str() {
                "publish" => { actions |= Action::Publish; }
                "subscribe" => { actions |= Action::Subscribe; }
                "admin" => { actions |= Action::Admin; }
                // `replicate` is the cluster-replication verb
                // (`Action::Replicate`, added in Plan G Wave 1). Follower
                // pods dial the leader's cluster port and the leader-side
                // server enforces this permission on Connect. A credential
                // with `actions = ["replicate"]` is the usual way to gate
                // replication access on a shared credentials.toml.
                "replicate" => { actions |= Action::Replicate; }
                other => {
                    return Err(AuthError::UnknownAction {
                        name: wc.name.clone(),
                        action: other.to_string(),
                    });
                }
            }
        }
        permissions.push(Permission { streams, actions });
    }

    Ok(Identity { name: wc.name.clone(), permissions })
}

fn decode_hash(raw: &str, credential_name: &str) -> Result<[u8; 32], AuthError> {
    if raw.len() != 64 || !raw.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()) {
        return Err(AuthError::InvalidTokenHash { name: credential_name.to_string() });
    }
    let mut out = [0u8; 32];
    for i in 0..32 {
        let byte = u8::from_str_radix(&raw[i * 2..i * 2 + 2], 16).map_err(|_| {
            AuthError::InvalidTokenHash { name: credential_name.to_string() }
        })?;
        out[i] = byte;
    }
    Ok(out)
}

/// Helper for tests and the CLI — sha256(raw) as lowercase hex.
pub fn sha256_hex(raw: &[u8]) -> String {
    let digest = Sha256::digest(raw);
    let mut out = String::with_capacity(64);
    for byte in digest {
        out.push_str(&format!("{:02x}", byte));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_tmp(content: &str) -> tempfile::NamedTempFile {
        use std::io::Write;
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f
    }

    fn hash_of(raw: &str) -> String {
        sha256_hex(raw.as_bytes())
    }

    #[test]
    fn build_from_env_only_injects_legacy_admin() {
        let store = CredentialStore::build(None, Some("raw-secret")).unwrap();
        assert_eq!(store.len(), 1);
        let (file_count, legacy) = store.source_breakdown();
        assert_eq!(file_count, 0);
        assert!(legacy);
        let digest: [u8; 32] = sha2::Sha256::digest(b"raw-secret").into();
        let id = store.lookup(&digest).expect("legacy-admin should resolve");
        assert_eq!(id.name, LEGACY_ADMIN_NAME);
        assert!(id.has_global_admin());
    }

    #[test]
    fn build_from_file_only() {
        let file = write_tmp(&format!(
            r#"
[[credentials]]
name = "orders-service"
token_sha256 = "{}"
permissions = [
  {{ streams = "orders-*", actions = ["publish", "subscribe"] }},
]
"#,
            hash_of("tok-orders")
        ));
        let store = CredentialStore::build(Some(file.path()), None).unwrap();
        assert_eq!(store.len(), 1);
        let digest: [u8; 32] = sha2::Sha256::digest(b"tok-orders").into();
        let id = store.lookup(&digest).unwrap();
        assert_eq!(id.name, "orders-service");
        assert!(id.authorize(Action::Publish, &crate::types::StreamName::try_from("orders-placed").unwrap()));
    }

    #[test]
    fn build_with_both_merges() {
        let file = write_tmp(&format!(
            r#"
[[credentials]]
name = "orders-service"
token_sha256 = "{}"
permissions = [{{ streams = "orders-*", actions = ["publish"] }}]
"#,
            hash_of("tok-orders")
        ));
        let store = CredentialStore::build(Some(file.path()), Some("legacy-token")).unwrap();
        assert_eq!(store.len(), 2);
        let (files, legacy) = store.source_breakdown();
        assert_eq!(files, 1);
        assert!(legacy);
    }

    #[test]
    fn reject_reserved_legacy_admin_name_when_env_set() {
        let file = write_tmp(&format!(
            r#"
[[credentials]]
name = "legacy-admin"
token_sha256 = "{}"
permissions = [{{ streams = "*", actions = ["admin"] }}]
"#,
            hash_of("x")
        ));
        let err = CredentialStore::build(Some(file.path()), Some("anything")).unwrap_err();
        assert!(matches!(err, AuthError::LegacyAdminReserved));
    }

    #[test]
    fn allow_legacy_admin_name_when_env_not_set() {
        let file = write_tmp(&format!(
            r#"
[[credentials]]
name = "legacy-admin"
token_sha256 = "{}"
permissions = [{{ streams = "*", actions = ["admin"] }}]
"#,
            hash_of("x")
        ));
        let store = CredentialStore::build(Some(file.path()), None).unwrap();
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn reject_duplicate_name() {
        let file = write_tmp(&format!(
            r#"
[[credentials]]
name = "dup"
token_sha256 = "{}"

[[credentials]]
name = "dup"
token_sha256 = "{}"
"#,
            hash_of("a"),
            hash_of("b"),
        ));
        let err = CredentialStore::build(Some(file.path()), None).unwrap_err();
        assert!(matches!(err, AuthError::DuplicateName(ref n) if n == "dup"));
    }

    #[test]
    fn reject_duplicate_token_hash() {
        let file = write_tmp(&format!(
            r#"
[[credentials]]
name = "a"
token_sha256 = "{h}"

[[credentials]]
name = "b"
token_sha256 = "{h}"
"#,
            h = hash_of("same-token"),
        ));
        let err = CredentialStore::build(Some(file.path()), None).unwrap_err();
        assert!(matches!(err, AuthError::DuplicateTokenHash { .. }));
    }

    #[test]
    fn reject_malformed_hex() {
        let file = write_tmp(
            r#"
[[credentials]]
name = "bad"
token_sha256 = "NOTHEX"
"#,
        );
        let err = CredentialStore::build(Some(file.path()), None).unwrap_err();
        assert!(matches!(err, AuthError::InvalidTokenHash { ref name } if name == "bad"));
    }

    #[test]
    fn reject_invalid_name() {
        let file = write_tmp(&format!(
            r#"
[[credentials]]
name = "has spaces"
token_sha256 = "{}"
"#,
            hash_of("x"),
        ));
        let err = CredentialStore::build(Some(file.path()), None).unwrap_err();
        assert!(matches!(err, AuthError::InvalidName { .. }));
    }

    #[test]
    fn reject_unknown_action() {
        let file = write_tmp(&format!(
            r#"
[[credentials]]
name = "a"
token_sha256 = "{}"
permissions = [{{ streams = "*", actions = ["manage"] }}]
"#,
            hash_of("x"),
        ));
        let err = CredentialStore::build(Some(file.path()), None).unwrap_err();
        assert!(matches!(err, AuthError::UnknownAction { ref action, .. } if action == "manage"));
    }

    #[test]
    fn reject_invalid_glob_chars() {
        let file = write_tmp(&format!(
            r#"
[[credentials]]
name = "a"
token_sha256 = "{}"
permissions = [{{ streams = "orders.*", actions = ["publish"] }}]
"#,
            hash_of("x"),
        ));
        let err = CredentialStore::build(Some(file.path()), None).unwrap_err();
        assert!(matches!(err, AuthError::InvalidGlob { .. }));
    }

    #[test]
    fn lookup_miss_returns_none() {
        let store = CredentialStore::build(None, Some("t")).unwrap();
        assert!(store.lookup(&[0u8; 32]).is_none());
    }

    #[test]
    fn replicate_action_compiles_from_toml() {
        // Regression test for Plan G Wave 5: the `replicate` string maps to
        // `Action::Replicate` and survives a round-trip through the TOML
        // loader. Followers rely on this verb to pass the leader-side
        // handshake gate in `replication/server.rs`; silently dropping the
        // mapping would make every replication session fail with 403.
        let file = write_tmp(&format!(
            r#"
[[credentials]]
name = "rep"
token_sha256 = "{}"
permissions = [{{ streams = "*", actions = ["replicate"] }}]
"#,
            hash_of("rep-token"),
        ));
        let store = CredentialStore::build(Some(file.path()), None).unwrap();
        let digest: [u8; 32] = sha2::Sha256::digest(b"rep-token").into();
        let id = store.lookup(&digest).expect("replicate credential resolves");
        assert!(
            id.permissions
                .iter()
                .any(|p| p.actions.contains(Action::Replicate)),
            "expected Replicate in action set; got {:?}",
            id.permissions,
        );
    }

    #[test]
    fn empty_permissions_is_deny_all() {
        let file = write_tmp(&format!(
            r#"
[[credentials]]
name = "noop"
token_sha256 = "{}"
"#,
            hash_of("x"),
        ));
        let store = CredentialStore::build(Some(file.path()), None).unwrap();
        let id = store
            .lookup(&sha2::Sha256::digest(b"x").into())
            .unwrap();
        assert!(!id.authorize(Action::Publish, &crate::types::StreamName::try_from("x").unwrap()));
        assert!(!id.has_any_admin_permission());
    }
}

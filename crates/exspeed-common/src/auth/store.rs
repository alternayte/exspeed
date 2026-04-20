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
        let mut names: HashMap<String, String> = HashMap::new(); // name → token_sha256 (for dup detection)
        let mut hash_to_name: HashMap<[u8; 32], String> = HashMap::new(); // for dup-hash msg
        let mut file_count = 0usize;

        if let Some(path) = from_file {
            if !path.exists() {
                return Err(AuthError::FileMissing(path.to_path_buf()));
            }
            let raw = std::fs::read_to_string(path)?;
            let wire: WireFile = toml::from_str(&raw)?;

            for wc in wire.credentials {
                if wc.name == LEGACY_ADMIN_NAME && env_token.is_some() {
                    return Err(AuthError::LegacyAdminReserved);
                }
                let id = Arc::new(compile_credential(&wc)?);
                let digest = decode_hash(&wc.token_sha256, &wc.name)?;

                if let Some(other) = hash_to_name.get(&digest) {
                    return Err(AuthError::DuplicateTokenHash {
                        first: other.clone(),
                        second: wc.name.clone(),
                    });
                }
                if names.contains_key(&wc.name) {
                    return Err(AuthError::DuplicateName(wc.name.clone()));
                }

                names.insert(wc.name.clone(), wc.token_sha256.clone());
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

fn compile_credential(wc: &WireCredential) -> Result<Identity, AuthError> {
    // Validate name charset.
    if wc.name.is_empty() || !wc.name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-') {
        return Err(AuthError::InvalidName { name: wc.name.clone() });
    }

    let mut permissions = Vec::with_capacity(wc.permissions.len());
    for wp in &wc.permissions {
        let streams = StreamGlob::compile(&wp.streams, &wc.name)?;
        let mut actions = EnumSet::<Action>::new();
        for a in &wp.actions {
            match a.as_str() {
                "publish" => { actions |= Action::Publish; }
                "subscribe" => { actions |= Action::Subscribe; }
                "admin" => { actions |= Action::Admin; }
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

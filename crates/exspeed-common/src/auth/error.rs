use thiserror::Error;

/// All failures produced by the auth module. Messages intentionally name the
/// offending entry (by name / line) so operators can fix `credentials.toml`
/// without guessing.
#[derive(Debug, Error)]
pub enum AuthError {
    #[error("credentials file not found: {0}")]
    FileMissing(std::path::PathBuf),

    #[error("credentials file unreadable: {0}")]
    FileIo(#[from] std::io::Error),

    #[error("credentials TOML parse error: {0}")]
    Toml(#[from] toml::de::Error),

    #[error("invalid credential name '{name}': must match [a-zA-Z0-9_-]+")]
    InvalidName { name: String },

    #[error("invalid token_sha256 on credential '{name}': must be 64 lowercase hex characters")]
    InvalidTokenHash { name: String },

    #[error("invalid stream glob '{glob}' on credential '{name}': only [a-zA-Z0-9_-*] allowed")]
    InvalidGlob { name: String, glob: String },

    #[error("unknown action '{action}' on credential '{name}'")]
    UnknownAction { name: String, action: String },

    #[error("duplicate credential name '{0}'")]
    DuplicateName(String),

    #[error("two credentials share the same token_sha256 (silent identity masquerade); check '{first}' and '{second}'")]
    DuplicateTokenHash { first: String, second: String },

    #[error("credential name 'legacy-admin' is reserved while EXSPEED_AUTH_TOKEN is set; rename the credential or unset the env var")]
    LegacyAdminReserved,
}

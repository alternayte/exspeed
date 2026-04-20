//! Shared auth primitives. Constant-time token comparison lives here so both
//! the TCP Connect gate (exspeed crate) and the HTTP bearer middleware
//! (exspeed-api crate) share a single implementation.

/// Constant-time comparison of a provided byte slice against an expected
/// token string. Uses `constant_time_eq` under the hood to defeat timing
/// oracles.
pub fn verify_token(provided: &[u8], expected: &str) -> bool {
    constant_time_eq::constant_time_eq(provided, expected.as_bytes())
}

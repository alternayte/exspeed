use s3::creds::Credentials;
use s3::{Bucket, Region};

/// Configuration for S3-backed tiered storage, built from environment variables.
#[derive(Debug)]
pub struct S3Config {
    pub bucket: Box<Bucket>,
    pub prefix: String,
    pub local_max_bytes: u64,
}

impl S3Config {
    /// Construct an `S3Config` from environment variables.
    ///
    /// Returns `Ok(None)` when `EXSPEED_STORAGE_S3_ENABLED` is absent or not
    /// equal to `"true"`, so callers can treat S3 tiering as opt-in.
    pub fn from_env() -> Result<Option<Self>, String> {
        // Guard: only proceed when explicitly enabled.
        let enabled = std::env::var("EXSPEED_STORAGE_S3_ENABLED").unwrap_or_default();
        if enabled != "true" {
            return Ok(None);
        }

        // Required: bucket name.
        let bucket_name = std::env::var("EXSPEED_STORAGE_S3_BUCKET").map_err(|_| {
            "EXSPEED_STORAGE_S3_BUCKET is required when S3 tiering is enabled".to_string()
        })?;

        // Optional with defaults.
        let prefix = std::env::var("EXSPEED_STORAGE_S3_PREFIX")
            .unwrap_or_else(|_| "exspeed/streams/".to_string());

        let region_name = std::env::var("EXSPEED_STORAGE_S3_REGION")
            .unwrap_or_else(|_| "us-east-1".to_string());

        // Build Region: use Custom when an endpoint is provided (e.g. MinIO).
        let region = match std::env::var("EXSPEED_STORAGE_S3_ENDPOINT") {
            Ok(endpoint) if !endpoint.is_empty() => Region::Custom {
                region: region_name,
                endpoint,
            },
            _ => region_name.parse().map_err(|e| format!("invalid S3 region: {e}"))?,
        };

        // Build Credentials: explicit keys or fall back to the default chain
        // (env vars / instance profile / etc.).
        let credentials = match (
            std::env::var("EXSPEED_STORAGE_S3_ACCESS_KEY"),
            std::env::var("EXSPEED_STORAGE_S3_SECRET_KEY"),
        ) {
            (Ok(access_key), Ok(secret_key)) => {
                Credentials::new(Some(&access_key), Some(&secret_key), None, None, None)
                    .map_err(|e| format!("S3 credentials error: {e}"))?
            }
            _ => Credentials::default().map_err(|e| format!("S3 credentials error: {e}"))?,
        };

        // Local disk cap before cold segments are evicted.
        let local_max_bytes = std::env::var("EXSPEED_STORAGE_LOCAL_MAX_BYTES")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(10_737_418_240); // 10 GiB

        // Create bucket handle; always use path-style for MinIO compatibility.
        let bucket = Bucket::new(&bucket_name, region, credentials)
            .map_err(|e| format!("failed to create S3 bucket handle: {e}"))?
            .with_path_style();

        Ok(Some(Self {
            bucket,
            prefix,
            local_max_bytes,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_none_when_not_enabled() {
        // With no env vars set (or ENABLED != "true"), we get Ok(None).
        // We can't easily unset vars in parallel tests, so we just check the
        // not-enabled path by inspecting the absence of the flag.
        std::env::remove_var("EXSPEED_STORAGE_S3_ENABLED");
        let result = S3Config::from_env();
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn errors_when_enabled_but_bucket_missing() {
        std::env::set_var("EXSPEED_STORAGE_S3_ENABLED", "true");
        std::env::remove_var("EXSPEED_STORAGE_S3_BUCKET");
        // Reset access/secret so we don't accidentally succeed via a real env.
        std::env::remove_var("EXSPEED_STORAGE_S3_ACCESS_KEY");
        std::env::remove_var("EXSPEED_STORAGE_S3_SECRET_KEY");

        let result = S3Config::from_env();
        assert!(result.is_err());
        let msg = result.unwrap_err();
        assert!(msg.contains("EXSPEED_STORAGE_S3_BUCKET"), "error: {msg}");

        // Clean up.
        std::env::remove_var("EXSPEED_STORAGE_S3_ENABLED");
    }
}

use std::env;

use async_trait::async_trait;
use s3::creds::Credentials;
use s3::{Bucket, Region};

use crate::consumer_state::ConsumerConfig;
use super::{ConsumerStore, ConsumerStoreError};

pub struct S3ConsumerStore {
    bucket: Box<Bucket>,
    prefix: String,
}

impl S3ConsumerStore {
    /// Build from environment variables. Reuses the offset store's S3 connection config.
    pub fn from_env() -> Result<Self, ConsumerStoreError> {
        let bucket_name = env::var("EXSPEED_OFFSET_STORE_S3_BUCKET").map_err(|_| {
            ConsumerStoreError::Connection(
                "EXSPEED_OFFSET_STORE_S3_BUCKET is required".to_string(),
            )
        })?;

        let prefix = env::var("EXSPEED_CONSUMER_STORE_S3_PREFIX")
            .unwrap_or_else(|_| "exspeed/consumers/".to_string());

        let region_name = env::var("EXSPEED_OFFSET_STORE_S3_REGION")
            .unwrap_or_else(|_| "us-east-1".to_string());

        let region = match env::var("EXSPEED_OFFSET_STORE_S3_ENDPOINT") {
            Ok(endpoint) => Region::Custom {
                region: region_name,
                endpoint,
            },
            Err(_) => region_name
                .parse()
                .map_err(|e: std::str::Utf8Error| {
                    ConsumerStoreError::Connection(format!("invalid S3 region: {}", e))
                })?,
        };

        let credentials = match (
            env::var("EXSPEED_OFFSET_STORE_S3_ACCESS_KEY"),
            env::var("EXSPEED_OFFSET_STORE_S3_SECRET_KEY"),
        ) {
            (Ok(access_key), Ok(secret_key)) => {
                Credentials::new(Some(&access_key), Some(&secret_key), None, None, None)
                    .map_err(|e| {
                        ConsumerStoreError::Connection(format!("S3 credentials error: {}", e))
                    })?
            }
            _ => Credentials::default().map_err(|e| {
                ConsumerStoreError::Connection(format!("S3 credentials error: {}", e))
            })?,
        };

        let bucket = Bucket::new(&bucket_name, region, credentials)
            .map_err(|e| ConsumerStoreError::Connection(format!("S3 bucket error: {}", e)))?
            .with_path_style();

        Ok(Self { bucket, prefix })
    }

    fn object_key(&self, name: &str) -> String {
        format!("{}{}.json", self.prefix, name)
    }
}

#[async_trait]
impl ConsumerStore for S3ConsumerStore {
    async fn save(&self, config: &ConsumerConfig) -> Result<(), ConsumerStoreError> {
        let key = self.object_key(&config.name);
        let json = serde_json::to_vec(config).map_err(|e| {
            ConsumerStoreError::Serialization(format!("json serialize: {}", e))
        })?;
        self.bucket
            .put_object(&key, &json)
            .await
            .map_err(|e| ConsumerStoreError::Connection(format!("S3 PUT failed: {}", e)))?;
        Ok(())
    }

    async fn load(&self, name: &str) -> Result<Option<ConsumerConfig>, ConsumerStoreError> {
        let key = self.object_key(name);
        match self.bucket.get_object(&key).await {
            Ok(resp) if resp.status_code() == 200 => {
                let cfg: ConsumerConfig = serde_json::from_slice(resp.as_slice()).map_err(|e| {
                    ConsumerStoreError::Serialization(format!("json deserialize: {}", e))
                })?;
                Ok(Some(cfg))
            }
            Ok(_) | Err(_) => Ok(None),
        }
    }

    async fn load_all(&self) -> Result<Vec<ConsumerConfig>, ConsumerStoreError> {
        let results = self
            .bucket
            .list(self.prefix.clone(), Some("/".to_string()))
            .await
            .map_err(|e| ConsumerStoreError::Connection(format!("S3 LIST failed: {}", e)))?;

        let mut configs = Vec::new();
        for result in &results {
            for obj in &result.contents {
                if obj.key.ends_with(".json") {
                    let name = obj
                        .key
                        .strip_prefix(&self.prefix)
                        .and_then(|k| k.strip_suffix(".json"))
                        .unwrap_or(&obj.key)
                        .to_string();
                    if let Some(cfg) = self.load(&name).await? {
                        configs.push(cfg);
                    }
                }
            }
        }
        configs.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(configs)
    }

    async fn delete(&self, name: &str) -> Result<(), ConsumerStoreError> {
        let key = self.object_key(name);
        self.bucket
            .delete_object(&key)
            .await
            .map_err(|e| ConsumerStoreError::Connection(format!("S3 DELETE failed: {}", e)))?;
        Ok(())
    }
}

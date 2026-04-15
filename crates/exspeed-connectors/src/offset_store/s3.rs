use async_trait::async_trait;
use s3::creds::Credentials;
use s3::{Bucket, Region};
use serde::{Deserialize, Serialize};

use super::{OffsetStore, OffsetStoreError};

#[derive(Debug, Serialize, Deserialize)]
struct OffsetData {
    offset_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    position: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sink_offset: Option<u64>,
}

pub struct S3OffsetStore {
    bucket: Box<Bucket>,
    prefix: String,
}

impl S3OffsetStore {
    pub fn from_env() -> Result<Self, OffsetStoreError> {
        let bucket_name =
            std::env::var("EXSPEED_OFFSET_STORE_S3_BUCKET").map_err(|_| {
                OffsetStoreError::Connection(
                    "EXSPEED_OFFSET_STORE_S3_BUCKET environment variable is required".to_string(),
                )
            })?;

        let prefix = std::env::var("EXSPEED_OFFSET_STORE_S3_PREFIX")
            .unwrap_or_else(|_| "exspeed/offsets/".to_string());

        let region_name = std::env::var("EXSPEED_OFFSET_STORE_S3_REGION")
            .unwrap_or_else(|_| "us-east-1".to_string());

        let region = match std::env::var("EXSPEED_OFFSET_STORE_S3_ENDPOINT") {
            Ok(endpoint) => Region::Custom {
                region: region_name,
                endpoint,
            },
            Err(_) => region_name
                .parse()
                .map_err(|e: std::str::Utf8Error| {
                    OffsetStoreError::Connection(format!("invalid S3 region: {}", e))
                })?,
        };

        let credentials = match (
            std::env::var("EXSPEED_OFFSET_STORE_S3_ACCESS_KEY"),
            std::env::var("EXSPEED_OFFSET_STORE_S3_SECRET_KEY"),
        ) {
            (Ok(access_key), Ok(secret_key)) => {
                Credentials::new(Some(&access_key), Some(&secret_key), None, None, None)
                    .map_err(|e| OffsetStoreError::Connection(format!("S3 credentials error: {}", e)))?
            }
            _ => Credentials::default()
                .map_err(|e| OffsetStoreError::Connection(format!("S3 credentials error: {}", e)))?,
        };

        let bucket = Bucket::new(&bucket_name, region, credentials)
            .map_err(|e| OffsetStoreError::Connection(format!("S3 bucket error: {}", e)))?
            .with_path_style();

        Ok(Self { bucket, prefix })
    }

    fn object_key(&self, connector: &str) -> String {
        format!("{}{}.json", self.prefix, connector)
    }

    async fn put_data(&self, connector: &str, data: &OffsetData) -> Result<(), OffsetStoreError> {
        let key = self.object_key(connector);
        let body = serde_json::to_vec(data)
            .map_err(|e| OffsetStoreError::Serialization(e.to_string()))?;
        self.bucket
            .put_object(&key, &body)
            .await
            .map_err(|e| OffsetStoreError::Connection(format!("S3 put error: {}", e)))?;
        Ok(())
    }

    async fn get_data(&self, connector: &str) -> Option<OffsetData> {
        let key = self.object_key(connector);
        match self.bucket.get_object(&key).await {
            Ok(response) => {
                if response.status_code() == 200 {
                    serde_json::from_slice(response.as_slice()).ok()
                } else {
                    None
                }
            }
            Err(_) => None,
        }
    }
}

#[async_trait]
impl OffsetStore for S3OffsetStore {
    async fn save_source_offset(
        &self,
        connector: &str,
        position: &str,
    ) -> Result<(), OffsetStoreError> {
        let data = OffsetData {
            offset_type: "source".to_string(),
            position: Some(position.to_string()),
            sink_offset: None,
        };
        self.put_data(connector, &data).await
    }

    async fn load_source_offset(
        &self,
        connector: &str,
    ) -> Result<Option<String>, OffsetStoreError> {
        match self.get_data(connector).await {
            Some(data) => Ok(data.position),
            None => Ok(None),
        }
    }

    async fn save_sink_offset(
        &self,
        connector: &str,
        offset: u64,
    ) -> Result<(), OffsetStoreError> {
        let data = OffsetData {
            offset_type: "sink".to_string(),
            position: None,
            sink_offset: Some(offset),
        };
        self.put_data(connector, &data).await
    }

    async fn load_sink_offset(&self, connector: &str) -> Result<u64, OffsetStoreError> {
        match self.get_data(connector).await {
            Some(data) => Ok(data.sink_offset.unwrap_or(0)),
            None => Ok(0),
        }
    }

    async fn delete(&self, connector: &str) -> Result<(), OffsetStoreError> {
        let key = self.object_key(connector);
        self.bucket
            .delete_object(&key)
            .await
            .map_err(|e| OffsetStoreError::Connection(format!("S3 delete error: {}", e)))?;
        Ok(())
    }
}

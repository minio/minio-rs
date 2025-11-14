// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2025 MinIO, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::s3::client::MinioClient;
use crate::s3::error::ValidationErr;
use crate::s3::inventory::{JobDefinition, PutInventoryConfigResponse};
use crate::s3::multimap_ext::Multimap;
use crate::s3::segmented_bytes::SegmentedBytes;
use crate::s3::types::{S3Api, S3Request, ToS3Request};
use crate::s3::utils::check_bucket_name;
use bytes::Bytes;
use http::Method;
use std::sync::Arc;
use typed_builder::TypedBuilder;

/// Argument builder for the MinIO inventory put configuration operation.
///
/// This struct constructs the parameters required for the
/// [`Client::put_inventory_config`](crate::s3::client::MinioClient::put_inventory_config) method.
#[derive(Clone, Debug, TypedBuilder)]
pub struct PutInventoryConfig {
    #[builder(!default)]
    client: MinioClient,
    #[builder(default, setter(into))]
    extra_headers: Option<Multimap>,
    #[builder(default, setter(into))]
    extra_query_params: Option<Multimap>,
    #[builder(default, setter(into))]
    region: Option<String>,
    #[builder(setter(into))]
    bucket: String,
    #[builder(setter(into))]
    id: String,
    #[builder(!default)]
    job_definition: JobDefinition,
}

/// Builder type for [`PutInventoryConfig`] that is returned by
/// [`MinioClient::put_inventory_config`](crate::s3::client::MinioClient::put_inventory_config).
pub type PutInventoryConfigBldr = PutInventoryConfigBuilder<(
    (MinioClient,),
    (),
    (),
    (),
    (String,),
    (String,),
    (JobDefinition,),
)>;

impl S3Api for PutInventoryConfig {
    type S3Response = PutInventoryConfigResponse;
}

impl ToS3Request for PutInventoryConfig {
    fn to_s3request(self) -> Result<S3Request, ValidationErr> {
        check_bucket_name(&self.bucket, true)?;

        if self.id.is_empty() {
            return Err(ValidationErr::InvalidInventoryJobId {
                id: self.id.clone(),
                reason: "Job ID cannot be empty".to_string(),
            });
        }

        self.job_definition
            .validate()
            .map_err(|e| ValidationErr::InvalidConfig {
                message: format!("Job definition validation failed: {e}"),
            })?;

        if self.job_definition.id != self.id {
            return Err(ValidationErr::InvalidConfig {
                message: format!(
                    "Job definition ID '{}' does not match provided ID '{}'",
                    self.job_definition.id, self.id
                ),
            });
        }

        let mut query_params = self.extra_query_params.unwrap_or_default();
        query_params.insert("minio-inventory".to_string(), "".to_string());
        query_params.insert("id".to_string(), self.id);

        let yaml_body = crate::s3::inventory::serialize_job_definition(&self.job_definition)
            .map_err(|e| match e {
                crate::s3::error::Error::Validation(v) => v,
                _ => ValidationErr::InvalidConfig {
                    message: format!("Failed to serialize job definition: {e}"),
                },
            })?;

        let body = Arc::new(SegmentedBytes::from(Bytes::from(yaml_body)));

        Ok(S3Request::builder()
            .client(self.client)
            .method(Method::PUT)
            .region(self.region)
            .bucket(self.bucket)
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .body(body)
            .build())
    }
}

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
use crate::s3::inventory::GenerateInventoryConfigResponse;
use crate::s3::multimap_ext::Multimap;
use crate::s3::types::{S3Api, S3Request, ToS3Request};
use crate::s3::utils::check_bucket_name;
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for the MinIO inventory generate configuration operation.
///
/// This struct constructs the parameters required for the
/// [`Client::generate_inventory_config`](crate::s3::client::MinioClient::generate_inventory_config) method.
#[derive(Clone, Debug, TypedBuilder)]
pub struct GenerateInventoryConfig {
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
}

/// Builder type for [`GenerateInventoryConfig`] that is returned by
/// [`MinioClient::generate_inventory_config`](crate::s3::client::MinioClient::generate_inventory_config).
pub type GenerateInventoryConfigBldr =
    GenerateInventoryConfigBuilder<((MinioClient,), (), (), (), (String,), (String,))>;

impl S3Api for GenerateInventoryConfig {
    type S3Response = GenerateInventoryConfigResponse;
}

impl ToS3Request for GenerateInventoryConfig {
    fn to_s3request(self) -> Result<S3Request, ValidationErr> {
        check_bucket_name(&self.bucket, true)?;

        if self.id.is_empty() {
            return Err(ValidationErr::InvalidInventoryJobId {
                id: self.id,
                reason: "Job ID cannot be empty".to_string(),
            });
        }

        let mut query_params = self.extra_query_params.unwrap_or_default();
        query_params.insert("minio-inventory".to_string(), "".to_string());
        query_params.insert("id".to_string(), self.id);
        query_params.insert("generate".to_string(), "".to_string());

        Ok(S3Request::builder()
            .client(self.client)
            .method(Method::GET)
            .region(self.region)
            .bucket(self.bucket)
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .build())
    }
}

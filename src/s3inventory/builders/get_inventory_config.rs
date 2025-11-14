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
use crate::s3::multimap_ext::Multimap;
use crate::s3::types::{BucketName, Region, S3Api, S3Request, ToS3Request};
use crate::s3::utils::insert;
use crate::s3inventory::{GetInventoryConfigResponse, InventoryJobId};
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for the MinIO inventory get configuration operation.
///
/// This struct constructs the parameters required for the
/// [`Client::get_inventory_config`](crate::s3::client::MinioClient::get_inventory_config) method.
#[derive(Clone, Debug, TypedBuilder)]
pub struct GetInventoryConfig {
    #[builder(!default)]
    client: MinioClient,
    #[builder(default, setter(into))]
    extra_headers: Option<Multimap>,
    #[builder(default, setter(into))]
    extra_query_params: Option<Multimap>,
    #[builder(default, setter(into))]
    region: Option<Region>,
    #[builder(setter(into))]
    #[builder(!default)]
    bucket: BucketName,
    #[builder(!default)]
    id: InventoryJobId,
}

/// Builder type for [`GetInventoryConfig`] that is returned by
/// [`MinioClient::get_inventory_config`](crate::s3::client::MinioClient::get_inventory_config).
pub type GetInventoryConfigBldr =
    GetInventoryConfigBuilder<((MinioClient,), (), (), (), (BucketName,), (InventoryJobId,))>;

impl S3Api for GetInventoryConfig {
    type S3Response = GetInventoryConfigResponse;
}

impl ToS3Request for GetInventoryConfig {
    fn to_s3request(self) -> Result<S3Request, ValidationErr> {
        let mut query_params: Multimap = insert(self.extra_query_params, "minio-inventory");
        query_params.insert("id".to_string(), self.id.into_inner());

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

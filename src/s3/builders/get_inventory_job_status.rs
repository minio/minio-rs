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
use crate::s3::inventory::GetInventoryJobStatusResponse;
use crate::s3::multimap_ext::Multimap;
use crate::s3::types::{S3Api, S3Request, ToS3Request};
use crate::s3::utils::check_bucket_name;
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for the MinIO inventory get job status operation.
///
/// This struct constructs the parameters required for the
/// [`Client::get_inventory_job_status`](crate::s3::client::MinioClient::get_inventory_job_status) method.
#[derive(Clone, Debug, TypedBuilder)]
pub struct GetInventoryJobStatus {
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

/// Builder type for [`GetInventoryJobStatus`] that is returned by
/// [`MinioClient::get_inventory_job_status`](crate::s3::client::MinioClient::get_inventory_job_status).
pub type GetInventoryJobStatusBldr =
    GetInventoryJobStatusBuilder<((MinioClient,), (), (), (), (String,), (String,))>;

impl S3Api for GetInventoryJobStatus {
    type S3Response = GetInventoryJobStatusResponse;
}

impl ToS3Request for GetInventoryJobStatus {
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
        query_params.insert("status".to_string(), "".to_string());

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

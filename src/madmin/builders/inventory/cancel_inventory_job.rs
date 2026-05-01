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

use crate::madmin::MinioAdminClient;
use crate::madmin::response::AdminInventoryControlResponse;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::Error;
use crate::s3::multimap_ext::Multimap;
use crate::s3::types::BucketName;
use crate::s3inventory::InventoryJobId;
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for canceling an inventory job.
///
/// This cancels a currently running inventory job. The job will stop processing
/// and will not be rescheduled.
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct CancelInventoryJob {
    #[builder(!default)]
    admin_client: MinioAdminClient,
    #[builder(
        default,
        setter(into, doc = "Optional extra HTTP headers to include in the request")
    )]
    extra_headers: Option<Multimap>,
    #[builder(
        default,
        setter(
            into,
            doc = "Optional extra query parameters to include in the request"
        )
    )]
    extra_query_params: Option<Multimap>,
    #[builder(!default)]
    bucket: BucketName,
    #[builder(!default)]
    id: InventoryJobId,
}

/// Builder type for [`CancelInventoryJob`].
pub type CancelInventoryJobBldr =
    CancelInventoryJobBuilder<((MinioAdminClient,), (), (), (BucketName,), (InventoryJobId,))>;

impl MadminApi for CancelInventoryJob {
    type MadminResponse = AdminInventoryControlResponse;
}

impl ToMadminRequest for CancelInventoryJob {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        let path = format!(
            "/minio/admin/v4/inventory/{}/{}/cancel",
            self.bucket.as_str(),
            self.id.as_str()
        );

        Ok(MadminRequest::builder()
            .client(self.admin_client.madmin_client())
            .method(Method::POST)
            .path(path)
            .query_params(self.extra_query_params.unwrap_or_default())
            .headers(self.extra_headers.unwrap_or_default())
            .bucket(Some(self.bucket.into_inner()))
            .build())
    }
}

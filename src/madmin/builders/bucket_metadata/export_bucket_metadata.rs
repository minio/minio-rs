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

use crate::madmin::madmin_client::MadminClient;
use crate::madmin::response::ExportBucketMetadataResponse;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::Error;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::types::BucketName;
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for the [ExportBucketMetadata](https://github.com/minio/madmin-go/blob/main/bucket-metadata.go) admin API operation.
///
/// This struct constructs the parameters required for the [`MadminClient::export_bucket_metadata`](crate::madmin::madmin_client::MadminClient::export_bucket_metadata) method.
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct ExportBucketMetadata {
    #[builder(!default)]
    client: MadminClient,
    #[builder(!default, setter(into, doc = "Bucket name to export metadata from"))]
    bucket: BucketName,
    #[builder(default, setter(into, doc = "Optional extra HTTP headers"))]
    extra_headers: Option<Multimap>,
    #[builder(default, setter(into, doc = "Optional extra query parameters"))]
    extra_query_params: Option<Multimap>,
}

pub type ExportBucketMetadataBldr =
    ExportBucketMetadataBuilder<((MadminClient,), (BucketName,), (), ())>;

impl ToMadminRequest for ExportBucketMetadata {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        let mut query_params = self.extra_query_params.unwrap_or_default();
        let bucket = self.bucket.into_inner();
        query_params.add("bucket", &bucket);

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::GET)
            .path("/export-bucket-metadata")
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .bucket(Some(bucket))
            .api_version(3)
            .build())
    }
}

impl MadminApi for ExportBucketMetadata {
    type MadminResponse = ExportBucketMetadataResponse;
}

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
use crate::madmin::response::ImportIAMV2Response;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::Error;
use crate::s3::multimap_ext::Multimap;
use crate::s3::segmented_bytes::SegmentedBytes;
use bytes::Bytes;
use http::Method;
use std::sync::Arc;
use typed_builder::TypedBuilder;

/// Argument builder for the ImportIAMV2 admin API.
///
/// This struct constructs the parameters required for the
/// [`MadminClient::import_iam_v2`](crate::madmin::madmin_client::MadminClient::import_iam_v2) method.
///
/// ImportIAMV2 provides detailed feedback about the import operation, including
/// which entities were skipped, removed, added, or failed.
///
/// ## Example
///
/// ```no_run
/// use minio::madmin::madmin_client::MadminClient;
/// use minio::s3::creds::StaticProvider;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let provider = StaticProvider::new("minioadmin", "minioadmin", None);
///     let client = MadminClient::new("http://localhost:9000".parse()?, Some(provider));
///
///     // Read IAM data from file
///     let iam_data = std::fs::read("iam-export.json")?;
///
///     let response = client
///         .import_iam_v2(iam_data)
///         .send()
///         .await?;
///
///     println!("Import Results:");
///     println!("  Added: {:?}", response.result.added);
///     println!("  Skipped: {:?}", response.result.skipped);
///     println!("  Removed: {:?}", response.result.removed);
///     println!("  Failed: {:?}", response.result.failed);
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone, TypedBuilder)]
#[builder(doc)]
pub struct ImportIAMV2 {
    #[builder(!default)]
    client: MadminClient,
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
    data: Vec<u8>,
}

impl ToMadminRequest for ImportIAMV2 {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::PUT)
            .path("/import-iam-v2")
            .query_params(self.extra_query_params.unwrap_or_default())
            .headers(self.extra_headers.unwrap_or_default())
            .body(Some(Arc::new(SegmentedBytes::from(Bytes::from(self.data)))))
            .build())
    }
}

impl MadminApi for ImportIAMV2 {
    type MadminResponse = ImportIAMV2Response;
}

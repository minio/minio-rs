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
use crate::madmin::response::BucketReplicationMRFResponse;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::Error;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::types::BucketName;
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for the [BucketReplicationMRF](https://pkg.go.dev/github.com/minio/madmin-go/v3#AdminClient.BucketReplicationMRF) admin API operation.
///
/// This struct constructs the parameters required for the [`MadminClient::bucket_replication_mrf`](crate::madmin::madmin_client::MadminClient::bucket_replication_mrf) method.
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct BucketReplicationMRF {
    #[builder(!default)]
    client: MadminClient,
    #[builder(!default, setter(into, doc = "Bucket name to check MRF backlog"))]
    bucket: BucketName,
    #[builder(
        default,
        setter(
            into,
            doc = "Optional node name to filter MRF entries (empty string for all nodes)"
        )
    )]
    node: Option<String>,
    #[builder(default, setter(into, doc = "Optional extra HTTP headers"))]
    extra_headers: Option<Multimap>,
    #[builder(default, setter(into, doc = "Optional extra query parameters"))]
    extra_query_params: Option<Multimap>,
}

pub type BucketReplicationMRFBldr =
    BucketReplicationMRFBuilder<((MadminClient,), (BucketName,), (), (), ())>;

impl ToMadminRequest for BucketReplicationMRF {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        let bucket = self.bucket.into_inner();
        let mut query_params = self.extra_query_params.unwrap_or_default();
        query_params.add("bucket", &bucket);

        // Add node parameter if specified
        if let Some(node) = self.node
            && !node.is_empty()
        {
            query_params.add("node", &node);
        }

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::GET)
            .path("/api/v1/replicate/mrf")
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .bucket(Some(bucket))
            .build())
    }
}

impl MadminApi for BucketReplicationMRF {
    type MadminResponse = BucketReplicationMRFResponse;
}

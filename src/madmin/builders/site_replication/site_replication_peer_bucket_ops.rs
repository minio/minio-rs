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
use crate::madmin::response::SiteReplicationPeerBucketOpsResponse;
use crate::madmin::types::site_replication::SRBucketOp;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::{Error, ValidationErr};
use crate::s3::multimap_ext::Multimap;
use crate::s3::segmented_bytes::SegmentedBytes;
use bytes::Bytes;
use http::Method;
use std::sync::Arc;
use typed_builder::TypedBuilder;

/// Argument builder for the SiteReplicationPeerBucketOps admin API operation.
///
/// This struct constructs the parameters required for the [`MadminClient::site_replication_peer_bucket_ops`](crate::madmin::madmin_client::MadminClient::site_replication_peer_bucket_ops) method.
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct SiteReplicationPeerBucketOps {
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
    #[builder(setter(doc = "Bucket operation to perform"))]
    operation: SRBucketOp,
}

/// Builder type for [`SiteReplicationPeerBucketOps`].
pub type SiteReplicationPeerBucketOpsBldr =
    SiteReplicationPeerBucketOpsBuilder<((MadminClient,), (), (), ())>;

impl ToMadminRequest for SiteReplicationPeerBucketOps {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        let bucket = self.operation.bucket.clone();
        let body_vec = serde_json::to_vec(&self.operation)
            .map_err(|e| Error::Validation(ValidationErr::JsonError(e)))?;
        let body = Arc::new(SegmentedBytes::from(Bytes::from(body_vec)));

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::PUT)
            .path("/site-replication/peer/bucket-ops")
            .query_params(self.extra_query_params.unwrap_or_default())
            .headers(self.extra_headers.unwrap_or_default())
            .body(Some(body))
            .bucket(Some(bucket))
            .build())
    }
}

impl MadminApi for SiteReplicationPeerBucketOps {
    type MadminResponse = SiteReplicationPeerBucketOpsResponse;
}

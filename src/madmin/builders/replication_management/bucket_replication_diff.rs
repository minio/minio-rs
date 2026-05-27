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
use crate::madmin::response::BucketReplicationDiffResponse;
use crate::madmin::types::replication::ReplDiffOpts;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::Error;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::types::BucketName;
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for the [BucketReplicationDiff](https://pkg.go.dev/github.com/minio/madmin-go/v3#AdminClient.BucketReplicationDiff) admin API operation.
///
/// This struct constructs the parameters required for the [`MadminClient::bucket_replication_diff`](crate::madmin::madmin_client::MadminClient::bucket_replication_diff) method.
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct BucketReplicationDiff {
    #[builder(!default)]
    client: MadminClient,
    #[builder(!default, setter(into, doc = "Bucket name to check replication diff"))]
    bucket: BucketName,
    #[builder(default, setter(into, doc = "Replication diff options"))]
    opts: Option<ReplDiffOpts>,
    #[builder(default, setter(into, doc = "Optional extra HTTP headers"))]
    extra_headers: Option<Multimap>,
    #[builder(default, setter(into, doc = "Optional extra query parameters"))]
    extra_query_params: Option<Multimap>,
}

pub type BucketReplicationDiffBldr =
    BucketReplicationDiffBuilder<((MadminClient,), (BucketName,), (), (), ())>;

impl ToMadminRequest for BucketReplicationDiff {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        let bucket = self.bucket.into_inner();
        let mut query_params = self.extra_query_params.unwrap_or_default();
        query_params.add("bucket", &bucket);

        // Add replication diff options as query parameters
        if let Some(opts) = self.opts {
            if let Some(arn) = opts.arn {
                query_params.add("arn", &arn);
            }
            if opts.verbose {
                query_params.add("verbose", "true");
            }
            if let Some(prefix) = opts.prefix {
                query_params.add("prefix", &prefix);
            }
        }

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::GET)
            .path("/api/v1/replicate/diff")
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .bucket(Some(bucket))
            .build())
    }
}

impl MadminApi for BucketReplicationDiff {
    type MadminResponse = BucketReplicationDiffResponse;
}

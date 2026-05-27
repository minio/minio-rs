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
use crate::madmin::response::UpdateRemoteTargetResponse;
use crate::madmin::types::bucket_target::{BucketTarget, TargetUpdateType};
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::Error;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::segmented_bytes::SegmentedBytes;
use crate::s3::types::BucketName;
use bytes::Bytes;
use http::Method;
use std::sync::Arc;
use typed_builder::TypedBuilder;

/// Argument builder for the Update Remote Target admin API operation.
///
/// This struct constructs the parameters required for the [`MadminClient::update_remote_target`](crate::madmin::madmin_client::MadminClient::update_remote_target) method.
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct UpdateRemoteTarget {
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
    #[builder(!default, setter(into, doc = "Name of the bucket"))]
    bucket: BucketName,
    #[builder(setter(into, doc = "Remote target configuration"))]
    target: BucketTarget,
    #[builder(default, setter(into, doc = "Type specification"))]
    update_type: TargetUpdateType,
}

/// Builder type for [`UpdateRemoteTarget`].
pub type UpdateRemoteTargetBldr = UpdateRemoteTargetBuilder<((MadminClient,), (), (), (), (), ())>;

impl MadminApi for UpdateRemoteTarget {
    type MadminResponse = UpdateRemoteTargetResponse;
}

impl ToMadminRequest for UpdateRemoteTarget {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        // Validate that ARN is present in the target
        if self.target.arn.as_ref().is_none_or(|arn| arn.is_empty()) {
            return Err(Error::Validation(
                crate::s3::error::ValidationErr::StrError {
                    message: "ARN is required for UpdateRemoteTarget".to_string(),
                    source: None,
                },
            ));
        }

        // Serialize target to JSON
        let json_data = serde_json::to_vec(&self.target)
            .map_err(|e| Error::Validation(crate::s3::error::ValidationErr::JsonError(e)))?;

        // Encrypt data using secret key as password (matching Go madmin behavior)
        let provider = self.client.shared.provider.as_ref().ok_or_else(|| {
            Error::Validation(crate::s3::error::ValidationErr::StrError {
                message: "Credentials required for UpdateRemoteTarget".to_string(),
                source: None,
            })
        })?;

        let creds = provider.fetch();
        let encrypted_data = crate::madmin::encrypt::encrypt_data(&creds.secret_key, &json_data)?;

        let body = Arc::new(SegmentedBytes::from(Bytes::from(encrypted_data)));

        let bucket = self.bucket.into_inner();
        let mut query_params: Multimap = self.extra_query_params.unwrap_or_default();
        query_params.add("bucket", &bucket);

        // Add update operation flag with specific update type
        query_params.add("update", self.update_type.as_query_param());

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::POST)
            .path("/update-remote-target")
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .body(Some(body))
            .bucket(Some(bucket))
            .build())
    }
}

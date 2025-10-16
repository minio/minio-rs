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

use crate::s3::error::{Error, ValidationErr};
use crate::s3::response::a_response_traits::{HasBucket, HasRegion, HasS3Fields};
use crate::s3::types::{FromS3Response, ReplicationConfig, S3Request};
use crate::{impl_from_s3response, impl_has_s3fields};
use bytes::{Buf, Bytes};
use http::HeaderMap;
use std::mem;
use xmltree::Element;

/// Response from the [`get_bucket_replication`](crate::s3::client::MinioClient::get_bucket_replication) API call,
/// providing the replication configuration of an S3 bucket.
///
/// This includes the rules and settings that define how objects in the bucket are replicated to other buckets.
///
/// For more information, refer to the [AWS S3 GetBucketReplication API documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketReplication.html).
#[derive(Clone, Debug)]
pub struct GetBucketReplicationResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_s3response!(GetBucketReplicationResponse);
impl_has_s3fields!(GetBucketReplicationResponse);

impl HasBucket for GetBucketReplicationResponse {}
impl HasRegion for GetBucketReplicationResponse {}

impl GetBucketReplicationResponse {
    /// Returns the replication configuration of the bucket.
    ///
    /// This includes the IAM role that Amazon S3 assumes to replicate objects on your behalf,
    /// and one or more replication rules that specify the conditions under which objects are replicated.
    ///
    /// For more details on replication configuration elements, see the [AWS S3 Replication Configuration documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/replication-add-config.html).
    pub fn config(&self) -> Result<ReplicationConfig, ValidationErr> {
        let root = Element::parse(self.body.clone().reader())?;
        ReplicationConfig::from_xml(&root)
    }
}

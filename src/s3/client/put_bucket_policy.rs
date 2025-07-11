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

use crate::s3::builders::{PutBucketPolicy, PutBucketPolicyBldr};
use crate::s3::client::MinioClient;

impl MinioClient {
    /// Creates a [`PutBucketPolicy`] request builder.
    ///
    /// To execute the request, call [`SetBucketPolicy::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`SetBucketPolicyResponse`](crate::s3::response::PutBucketPolicyResponse).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use std::collections::HashMap;
    /// use minio::s3::MinioClient;
    /// use minio::s3::builders::VersioningStatus;
    /// use minio::s3::response::PutBucketPolicyResponse;
    /// use minio::s3::types::{S3Api, AndOperator, Destination, Filter, ReplicationConfig, ReplicationRule};
    /// use minio::s3::response::a_response_traits::HasBucket;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = MinioClient::create_client_on_localhost().unwrap(); // configure your client here
    ///     
    ///     let config = r#"{
    ///         "Version": "2012-10-17",
    ///         "Statement": [
    ///         {
    ///             "Effect": "Allow",
    ///             "Principal": {
    ///                 "AWS": "*"
    ///             },
    ///             "Action": [
    ///                 "s3:GetBucketLocation",
    ///                 "s3:ListBucket"
    ///             ],
    ///             "Resource": "arn:aws:s3:::bucket-name"
    ///         },
    ///         {
    ///             "Effect": "Allow",
    ///             "Principal": {
    ///                 "AWS": "*"
    ///             },
    ///             "Action": "s3:GetObject",
    ///             "Resource": "arn:aws:s3:::bucket-name/*"
    ///         }]
    ///     }"#;
    ///
    ///     let resp: PutBucketPolicyResponse = client
    ///         .put_bucket_policy("bucket-name")
    ///         .config(config.to_owned())
    ///         .build().send().await.unwrap();
    ///     println!("set bucket replication policy on bucket '{}'", resp.bucket());
    /// }
    /// ```
    pub fn put_bucket_policy<S: Into<String>>(&self, bucket: S) -> PutBucketPolicyBldr {
        PutBucketPolicy::builder()
            .client(self.clone())
            .bucket(bucket)
    }
}

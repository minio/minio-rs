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

use crate::s3::builders::{DeleteBucketReplication, DeleteBucketReplicationBldr};
use crate::s3::client::MinioClient;

impl MinioClient {
    /// Creates a [`DeleteBucketReplication`] request builder.
    ///
    /// To execute the request, call [`DeleteBucketReplication::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`DeleteBucketReplicationResponse`](crate::s3::response::DeleteBucketReplicationResponse).    
    ///
    /// 🛈 This operation is not supported for express buckets.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    /// use minio::s3::response::DeleteBucketReplicationResponse;
    /// use minio::s3::types::S3Api;
    /// use minio::s3::response::a_response_traits::HasBucket;
    ///
    /// #[tokio::main]
    /// async fn main() {    
    ///     let base_url = "http://localhost:9000/".parse::<BaseUrl>().unwrap();
    ///     let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let client = MinioClient::new(base_url, Some(static_provider), None, None).unwrap();
    ///     let resp: DeleteBucketReplicationResponse = client
    ///         .delete_bucket_replication("bucket-name")
    ///         .build().send().await.unwrap();
    ///     println!("replication of bucket '{}' is deleted", resp.bucket());
    /// }
    /// ```
    pub fn delete_bucket_replication<S: Into<String>>(
        &self,
        bucket: S,
    ) -> DeleteBucketReplicationBldr {
        DeleteBucketReplication::builder()
            .client(self.clone())
            .bucket(bucket)
    }
}

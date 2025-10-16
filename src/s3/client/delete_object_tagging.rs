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

use crate::s3::builders::{DeleteObjectTagging, DeleteObjectTaggingBldr};
use crate::s3::client::MinioClient;

impl MinioClient {
    /// Creates a [`DeleteObjectTagging`] request builder.
    ///
    /// To execute the request, call [`DeleteObjectTagging::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`DeleteObjectTagsResponse`](crate::s3::response::DeleteObjectTaggingResponse).
    ///
    /// 🛈 This operation is not supported for express buckets.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    /// use minio::s3::response::DeleteObjectTaggingResponse;
    /// use minio::s3::types::S3Api;
    /// use minio::s3::response::a_response_traits::{HasBucket, HasObject};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let base_url = "http://localhost:9000/".parse::<BaseUrl>().unwrap();
    ///     let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let client = MinioClient::new(base_url, Some(static_provider), None, None).unwrap();
    ///     let resp: DeleteObjectTaggingResponse = client
    ///         .delete_object_tagging("bucket-name", "object_name")
    ///         .build().send().await.unwrap();
    ///     println!("legal hold of object '{}' in bucket '{}' is deleted", resp.object(), resp.bucket());
    /// }
    /// ```
    pub fn delete_object_tagging<S1: Into<String>, S2: Into<String>>(
        &self,
        bucket: S1,
        object: S2,
    ) -> DeleteObjectTaggingBldr {
        DeleteObjectTagging::builder()
            .client(self.clone())
            .bucket(bucket)
            .object(object)
    }
}

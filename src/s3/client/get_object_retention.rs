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

use crate::s3::builders::{GetObjectRetention, GetObjectRetentionBldr};
use crate::s3::client::MinioClient;
use crate::s3::error::ValidationErr;
use crate::s3::types::{BucketName, ObjectKey};

impl MinioClient {
    /// Creates a [`GetObjectRetention`] request builder.
    ///
    /// To execute the request, call [`GetObjectRetention::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`GetObjectRetentionResponse`](crate::s3::response::GetObjectRetentionResponse).
    ///
    /// 🛈 This operation is not supported for express buckets.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    /// use minio::s3::response::GetObjectRetentionResponse;
    /// use minio::s3::types::S3Api;
    /// use minio::s3::response_traits::HasBucket;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let base_url = "http://localhost:9000/".parse::<BaseUrl>().unwrap();
    ///     let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let client = MinioClient::new(base_url, Some(static_provider), None, None).unwrap();
    ///     let resp: GetObjectRetentionResponse = client
    ///         .get_object_retention("bucket-name", "object-name")
    ///         .unwrap().build().send().await.unwrap();
    ///     println!("retrieved retention mode '{:?}' until '{:?}' from bucket '{}' is enabled", resp.retention_mode(), resp.retain_until_date(), resp.bucket().unwrap());
    /// }
    /// ```
    pub fn get_object_retention<B, O>(
        &self,
        bucket: B,
        object: O,
    ) -> Result<GetObjectRetentionBldr, ValidationErr>
    where
        B: TryInto<BucketName>,
        B::Error: Into<ValidationErr>,
        O: TryInto<ObjectKey>,
        O::Error: Into<ValidationErr>,
    {
        Ok(GetObjectRetention::builder()
            .client(self.clone())
            .bucket(bucket.try_into().map_err(Into::into)?)
            .object(object.try_into().map_err(Into::into)?))
    }
}

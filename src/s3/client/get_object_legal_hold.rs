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

use crate::s3::builders::{GetObjectLegalHold, GetObjectLegalHoldBldr};
use crate::s3::client::MinioClient;
use crate::s3::error::ValidationErr;
use crate::s3::types::{BucketName, ObjectKey};

impl MinioClient {
    /// Creates a [`GetObjectLegalHold`] request builder.
    ///
    /// To execute the request, call [`GetObjectLegalHold::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`GetObjectLegalHoldResponse`](crate::s3::response::GetObjectLegalHoldResponse).
    ///
    /// 🛈 This operation is not supported for express buckets.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    /// use minio::s3::response::GetObjectLegalHoldResponse;
    /// use minio::s3::types::S3Api;
    /// use minio::s3::response_traits::{HasBucket, HasObject};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let base_url = "http://localhost:9000/".parse::<BaseUrl>().unwrap();
    ///     let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let client = MinioClient::new(base_url, Some(static_provider), None, None).unwrap();
    ///     let resp: GetObjectLegalHoldResponse = client
    ///         .get_object_legal_hold("bucket-name", "object-name")
    ///         .unwrap().build().send().await.unwrap();
    ///     println!("legal hold of object '{}' in bucket '{}' is enabled: {:?}", resp.object().unwrap(), resp.bucket().unwrap(), resp.enabled());
    /// }
    /// ```
    pub fn get_object_legal_hold<B, O>(
        &self,
        bucket: B,
        object: O,
    ) -> Result<GetObjectLegalHoldBldr, ValidationErr>
    where
        B: TryInto<BucketName>,
        B::Error: Into<ValidationErr>,
        O: TryInto<ObjectKey>,
        O::Error: Into<ValidationErr>,
    {
        Ok(GetObjectLegalHold::builder()
            .client(self.clone())
            .bucket(bucket.try_into().map_err(Into::into)?)
            .object(object.try_into().map_err(Into::into)?))
    }
}

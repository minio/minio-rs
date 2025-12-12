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

use crate::s3::builders::{PutObjectRetention, PutObjectRetentionBldr};
use crate::s3::client::MinioClient;
use crate::s3::error::ValidationErr;
use crate::s3::types::{BucketName, ObjectKey};

impl MinioClient {
    /// Creates a [`PutObjectRetention`] request builder.
    ///
    /// To execute the request, call [`SetObjectRetention::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`SetObjectRetentionResponse`](crate::s3::response::PutObjectRetentionResponse).
    ///
    /// 🛈 This operation is not supported for express buckets.
    ///
    /// Note: there is no separate delete object retention API. To remove object retention, you must
    /// call put_object_retention without '.retention_mode()' or '.retain_until_date()' to remove the retention.
    /// You must set '.bypass_governance_mode(true)' to remove retention from objects in GOVERNANCE mode.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    /// use minio::s3::response::PutObjectRetentionResponse;
    /// use minio::s3::builders::ObjectToDelete;
    /// use minio::s3::types::{RetentionMode, S3Api};
    /// use minio::s3::utils::utc_now;
    /// use minio::s3::response_traits::HasObject;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let base_url = "http://localhost:9000/".parse::<BaseUrl>().unwrap();
    ///     let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let client = MinioClient::new(base_url, Some(static_provider), None, None).unwrap();
    ///     let retain_until_date = utc_now() + chrono::Duration::days(1);
    ///     let resp: PutObjectRetentionResponse = client
    ///         .put_object_retention("bucket-name", "object-name")
    ///         .unwrap()
    ///         .retention_mode(RetentionMode::GOVERNANCE)
    ///         .retain_until_date(Some(retain_until_date))
    ///         .build().send().await.unwrap();
    ///     println!("set the object retention for object '{}'", resp.object().unwrap());
    /// }
    /// ```
    pub fn put_object_retention<B, O>(
        &self,
        bucket: B,
        object: O,
    ) -> Result<PutObjectRetentionBldr, ValidationErr>
    where
        B: TryInto<BucketName>,
        B::Error: Into<ValidationErr>,
        O: TryInto<ObjectKey>,
        O::Error: Into<ValidationErr>,
    {
        Ok(PutObjectRetention::builder()
            .client(self.clone())
            .bucket(bucket.try_into().map_err(Into::into)?)
            .object(object.try_into().map_err(Into::into)?))
    }
}

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

use crate::s3::builders::{UpdateObjectEncryption, UpdateObjectEncryptionBldr};
use crate::s3::client::MinioClient;
use crate::s3::error::ValidationErr;
use crate::s3::types::{BucketName, ObjectKey};

impl MinioClient {
    /// Creates an [`UpdateObjectEncryption`] request builder (MinIO extension).
    ///
    /// This rotates the SSE-KMS encryption key envelope of an existing object in-place without
    /// re-reading or re-writing the object data. The object must already be encrypted with SSE-S3
    /// or SSE-KMS; SSE-C objects are not supported. The `kms_key_arn` is required.
    ///
    /// To execute the request, call [`UpdateObjectEncryption::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing an [`UpdateObjectEncryptionResponse`](crate::s3::response::UpdateObjectEncryptionResponse).
    ///
    /// See: <https://min.io/docs/minio/linux/developers/minio-drivers.html>
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    /// use minio::s3::response::UpdateObjectEncryptionResponse;
    /// use minio::s3::types::S3Api;
    /// use minio::s3::response_traits::HasVersion;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let base_url = "http://localhost:9000/".parse::<BaseUrl>().unwrap();
    ///     let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let client = MinioClient::new(base_url, Some(static_provider), None, None).unwrap();
    ///     let resp: UpdateObjectEncryptionResponse = client
    ///         .update_object_encryption("bucket-name", "object-name", "my-kms-key")
    ///         .unwrap()
    ///         .bucket_key_enabled(true)
    ///         .build().send().await.unwrap();
    ///     println!("updated encryption, version: {:?}", resp.version_id());
    /// }
    /// ```
    pub fn update_object_encryption<B, O>(
        &self,
        bucket: B,
        object: O,
        kms_key_arn: impl Into<String>,
    ) -> Result<UpdateObjectEncryptionBldr, ValidationErr>
    where
        B: TryInto<BucketName>,
        B::Error: Into<ValidationErr>,
        O: TryInto<ObjectKey>,
        O::Error: Into<ValidationErr>,
    {
        Ok(UpdateObjectEncryption::builder()
            .client(self.clone())
            .bucket(bucket.try_into().map_err(Into::into)?)
            .object(object.try_into().map_err(Into::into)?)
            .kms_key_arn(kms_key_arn))
    }
}

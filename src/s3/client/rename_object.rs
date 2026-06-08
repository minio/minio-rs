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

use crate::s3::builders::{RenameObject, RenameObjectBldr};
use crate::s3::client::MinioClient;
use crate::s3::error::ValidationErr;
use crate::s3::types::{BucketName, ObjectKey};

impl MinioClient {
    /// Creates a [`RenameObject`] request builder (MinIO extension).
    ///
    /// Renames an object within a single bucket in one server-side request. The
    /// source and destination are object keys in the same `bucket`. Renames are
    /// only supported on unversioned buckets.
    ///
    /// To execute the request, call [`RenameObject::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`RenameObjectResponse`](crate::s3::response::RenameObjectResponse).
    ///
    /// See: <https://min.io/docs/minio/linux/developers/minio-drivers.html>
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    /// use minio::s3::response::RenameObjectResponse;
    /// use minio::s3::response_traits::HasEtagFromHeaders;
    /// use minio::s3::types::S3Api;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let base_url = "http://localhost:9000/".parse::<BaseUrl>().unwrap();
    ///     let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let client = MinioClient::new(base_url, Some(static_provider), None, None).unwrap();
    ///     let resp: RenameObjectResponse = client
    ///         .rename_object("bucket-name", "old-name", "new-name")
    ///         .unwrap().build().send().await.unwrap();
    ///     println!("the renamed object etag is: '{:?}'", resp.etag());
    /// }
    /// ```
    pub fn rename_object<B, S, D>(
        &self,
        bucket: B,
        src_object: S,
        dst_object: D,
    ) -> Result<RenameObjectBldr, ValidationErr>
    where
        B: TryInto<BucketName>,
        B::Error: Into<ValidationErr>,
        S: TryInto<ObjectKey>,
        S::Error: Into<ValidationErr>,
        D: TryInto<ObjectKey>,
        D::Error: Into<ValidationErr>,
    {
        Ok(RenameObject::builder()
            .client(self.clone())
            .bucket(bucket.try_into().map_err(Into::into)?)
            .src_object(src_object.try_into().map_err(Into::into)?)
            .dst_object(dst_object.try_into().map_err(Into::into)?))
    }
}

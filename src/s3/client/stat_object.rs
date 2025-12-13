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

use crate::s3::builders::{StatObject, StatObjectBldr};
use crate::s3::client::MinioClient;
use crate::s3::types::{BucketName, ObjectKey};

impl MinioClient {
    /// Creates a [`StatObject`] request builder to retrieve object metadata.
    ///
    /// This operation uses the HTTP HEAD method (S3 HeadObject API) to efficiently
    /// retrieve object metadata without downloading the object body. This is the
    /// standard and most efficient way to check if an object exists and get its
    /// metadata (size, ETag, Content-Type, user metadata, etc.).
    ///
    /// To execute the request, call [`StatObject::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`StatObjectResponse`](crate::s3::response::StatObjectResponse).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    /// use minio::s3::response::StatObjectResponse;
    /// use minio::s3::types::{BucketName, ObjectKey, S3Api};
    /// use minio::s3::response_traits::HasObject;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let base_url = "http://localhost:9000/".parse::<BaseUrl>().unwrap();
    ///     let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let client = MinioClient::new(base_url, Some(static_provider), None, None).unwrap();
    ///     let bucket = BucketName::new("bucket-name").unwrap();
    ///     let object = ObjectKey::new("object-name").unwrap();
    ///     let resp: StatObjectResponse =
    ///         client.stat_object(bucket, object).build().send().await.unwrap();
    ///     println!("stat of object '{}' are {:#?}", resp.object(), resp);
    /// }
    /// ```
    pub fn stat_object(&self, bucket: BucketName, object: ObjectKey) -> StatObjectBldr {
        StatObject::builder()
            .client(self.clone())
            .bucket(bucket)
            .object(object)
    }
}

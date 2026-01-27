// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2022-2024 MinIO, Inc.
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

use crate::s3::builders::{
    DeleteObject, DeleteObjectBldr, DeleteObjects, DeleteObjectsBldr, DeleteObjectsStreaming,
    ObjectToDelete, ObjectsStream,
};
use crate::s3::client::MinioClient;
use crate::s3::error::ValidationErr;
use crate::s3::types::BucketName;

impl MinioClient {
    /// Creates a [`DeleteObject`] request builder to delete a single object from an S3 bucket.
    ///
    /// To execute the request, call [`DeleteObject::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`DeleteObjectResponse`](crate::s3::response::DeleteObjectResponse).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    /// use minio::s3::response::DeleteObjectResponse;
    /// use minio::s3::types::S3Api;
    /// use minio::s3::response_traits::HasVersion;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let base_url = "http://localhost:9000/".parse::<BaseUrl>().unwrap();
    ///     let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let client = MinioClient::new(base_url, Some(static_provider), None, None).unwrap();
    ///     let resp: DeleteObjectResponse = client
    ///         .delete_object("bucket-name", "object-name")
    ///         .unwrap().build().send().await.unwrap();
    ///     println!("the object is deleted. The delete marker has version '{:?}'", resp.version_id());
    /// }
    /// ```
    pub fn delete_object<B, D>(
        &self,
        bucket: B,
        object: D,
    ) -> Result<DeleteObjectBldr, ValidationErr>
    where
        B: TryInto<BucketName>,
        B::Error: Into<ValidationErr>,
        D: TryInto<ObjectToDelete>,
        D::Error: Into<ValidationErr>,
    {
        Ok(DeleteObject::builder()
            .client(self.clone())
            .bucket(bucket.try_into().map_err(Into::into)?)
            .object(object.try_into().map_err(Into::into)?))
    }

    /// Creates a [`DeleteObjects`] request builder to delete multiple objects from an S3 bucket.
    ///
    /// To execute the request, call [`DeleteObjects::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`DeleteObjectsResponse`](crate::s3::response::DeleteObjectsResponse).
    pub fn delete_objects<B>(
        &self,
        bucket: B,
        objects: Vec<ObjectToDelete>,
    ) -> Result<DeleteObjectsBldr, ValidationErr>
    where
        B: TryInto<BucketName>,
        B::Error: Into<ValidationErr>,
    {
        Ok(DeleteObjects::builder()
            .client(self.clone())
            .bucket(bucket.try_into().map_err(Into::into)?)
            .objects(objects))
    }

    /// Creates a [`DeleteObjectsStreaming`] request builder to delete a stream of objects from an S3 bucket.
    ///
    /// To execute the request, call [`DeleteObjectsStreaming::to_stream()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`DeleteObjectsResponse`](crate::s3::response::DeleteObjectsResponse).
    pub fn delete_objects_streaming<B, D>(
        &self,
        bucket: B,
        objects: D,
    ) -> Result<DeleteObjectsStreaming, ValidationErr>
    where
        B: TryInto<BucketName>,
        B::Error: Into<ValidationErr>,
        D: Into<ObjectsStream>,
    {
        Ok(DeleteObjectsStreaming::new(
            self.clone(),
            bucket.try_into().map_err(Into::into)?,
            objects,
        ))
    }
}

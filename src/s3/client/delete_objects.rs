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
    /// use minio::s3::response::DeleteObjectResponse;
    /// use minio::s3::builders::ObjectToDelete;
    /// use minio::s3::types::S3Api;
    /// use minio::s3::response::a_response_traits::HasVersion;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = MinioClient::create_client_on_localhost().unwrap(); // configure your client here
    ///     let resp: DeleteObjectResponse = client
    ///         .delete_object("bucket-name", ObjectToDelete::from("object-name"))
    ///         .build().send().await.unwrap();
    ///     println!("the object is deleted. The delete marker has version '{:?}'", resp.version_id());
    /// }
    /// ```
    pub fn delete_object<S: Into<String>, D: Into<ObjectToDelete>>(
        &self,
        bucket: S,
        object: D,
    ) -> DeleteObjectBldr {
        DeleteObject::builder()
            .client(self.clone())
            .bucket(bucket)
            .object(object)
    }

    /// Creates a [`DeleteObjects`] request builder to delete multiple objects from an S3 bucket.
    ///
    /// To execute the request, call [`DeleteObjects::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`DeleteObjectsResponse`](crate::s3::response::DeleteObjectsResponse).
    pub fn delete_objects<S: Into<String>>(
        &self,
        bucket: S,
        objects: Vec<ObjectToDelete>,
    ) -> DeleteObjectsBldr {
        DeleteObjects::builder()
            .client(self.clone())
            .bucket(bucket)
            .objects(objects)
    }

    /// Creates a [`DeleteObjectsStreaming`] request builder to delete a stream of objects from an S3 bucket.
    ///
    /// To execute the request, call [`DeleteObjectsStreaming::to_stream()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`DeleteObjectsResponse`](crate::s3::response::DeleteObjectsResponse).
    pub fn delete_objects_streaming<S: Into<String>, D: Into<ObjectsStream>>(
        &self,
        bucket: S,
        objects: D,
    ) -> DeleteObjectsStreaming {
        DeleteObjectsStreaming::new(self.clone(), bucket.into(), objects)
    }
}

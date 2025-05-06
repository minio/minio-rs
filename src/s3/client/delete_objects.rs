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

//! APIs to remove objects.

use crate::s3::builders::RemoveObjectsApi;
use crate::s3::{
    builders::{DeleteObjects, ObjectToDelete, RemoveObject, RemoveObjects},
    client::Client,
};

impl Client {
    /// Creates a [`RemoveObject`] request builder.
    ///
    /// To execute the request, call [`RemoveObject::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`RemoveObjectResponse`](crate::s3::response::RemoveObjectResponse).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::Client;
    /// use minio::s3::response::RemoveObjectResponse;
    /// use minio::s3::builders::ObjectToDelete;
    /// use minio::s3::types::S3Api;
    ///
    /// #[tokio::main]
    /// async fn main() {
    /// let client: Client = Default::default(); // configure your client here
    ///     let resp: RemoveObjectResponse = client
    ///         .remove_object("bucket-name", ObjectToDelete::from("object-name"))
    ///         .send().await.unwrap();
    ///     println!("the object is deleted. The delete marker has version '{:?}'", resp.version_id);
    /// }
    /// ```
    pub fn remove_object<S: Into<String>, D: Into<ObjectToDelete>>(
        &self,
        bucket: S,
        object: D,
    ) -> RemoveObject {
        RemoveObject::new(self.clone(), bucket.into(), object)
    }

    pub fn remove_objects<S: Into<String>, D: Into<DeleteObjects>>(
        &self,
        bucket: S,
        objects: D,
    ) -> RemoveObjects {
        RemoveObjects::new(self.clone(), bucket.into(), objects)
    }

    /// Creates a builder to execute
    /// [DeleteObjects](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html)
    /// S3 API
    pub fn delete_objects<S: Into<String>, D: Into<ObjectToDelete>>(
        &self,
        bucket: S,
        object: Vec<ObjectToDelete>,
    ) -> RemoveObjectsApi {
        RemoveObjectsApi::new(self.clone(), bucket.into(), object)
    }
}

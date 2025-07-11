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

// ! S3 APIs for appending objects.

use super::MinioClient;
use crate::s3::builders::{
    AppendObject, AppendObjectBldr, AppendObjectContent, AppendObjectContentBldr, ObjectContent,
};
use crate::s3::segmented_bytes::SegmentedBytes;
use std::sync::Arc;

impl MinioClient {
    /// Creates a [`AppendObject`] request builder to append data to the end of an (existing) object.
    /// This is a lower-level API that performs a non-multipart object upload.
    ///
    /// To execute the request, call [`AppendObject::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`AppendObjectResponse`](crate::s3::response::AppendObjectResponse).    
    ///
    /// 🛈 This operation is not supported for regular non-express buckets.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::response::{AppendObjectResponse, PutObjectResponse};
    /// use minio::s3::segmented_bytes::SegmentedBytes;
    /// use minio::s3::types::S3Api;
    /// use minio::s3::response::a_response_traits::HasObjectSize;
    ///
    /// #[tokio::main]
    /// async fn main() {    
    ///     let client = MinioClient::create_client_on_localhost().unwrap(); // configure your client here
    ///     let data1: SegmentedBytes = SegmentedBytes::from("aaaa".to_string());
    ///     let data2: SegmentedBytes = SegmentedBytes::from("bbbb".to_string());
    ///     let resp: PutObjectResponse = client
    ///         .put_object("bucket-name", "object-name", data1)
    ///         .build().send().await.unwrap();
    ///     let offset_bytes = 4; // the offset at which to append the data
    ///     let resp: AppendObjectResponse = client
    ///         .append_object("bucket-name", "object-name", data2, offset_bytes)
    ///         .build().send().await.unwrap();
    ///     println!("size of the final object is {} bytes", resp.object_size());
    /// }
    /// ```
    pub fn append_object<S1: Into<String>, S2: Into<String>>(
        &self,
        bucket: S1,
        object: S2,
        data: SegmentedBytes,
        offset_bytes: u64,
    ) -> AppendObjectBldr {
        AppendObject::builder()
            .client(self.clone())
            .bucket(bucket.into())
            .object(object.into())
            .data(Arc::new(data))
            .offset_bytes(offset_bytes)
    }

    /// Creates an [`AppendObjectContent`] request builder to append data to the end of an (existing)
    /// object. The content is streamed and appended to MinIO/S3. This is a higher-level API that
    /// handles multipart appends transparently.
    ///
    /// To execute the request, call [`AppendObjectContent::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`AppendObjectResponse`](crate::s3::response::AppendObjectResponse).    
    ///
    /// 🛈 This operation is not supported for regular non-express buckets.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::response::{AppendObjectResponse, PutObjectResponse};
    /// use minio::s3::builders::ObjectContent;
    /// use minio::s3::segmented_bytes::SegmentedBytes;
    /// use minio::s3::types::S3Api;
    /// use minio::s3::response::a_response_traits::HasObjectSize;
    ///
    /// #[tokio::main]
    /// async fn main() {    
    ///     let client = MinioClient::create_client_on_localhost().unwrap(); // configure your client here
    ///     let data1: SegmentedBytes = SegmentedBytes::from("aaaa".to_string());
    ///     let content2: String = "bbbb".to_string();
    ///     let resp: PutObjectResponse = client
    ///         .put_object("bucket-name", "object-name", data1)
    ///         .build().send().await.unwrap();
    ///     let resp: AppendObjectResponse = client
    ///         .append_object_content("bucket-name", "object-name", content2)
    ///         .build().send().await.unwrap();
    ///     println!("size of the final object is {} bytes", resp.object_size());
    /// }
    /// ```
    pub fn append_object_content<S1: Into<String>, S2: Into<String>, C: Into<ObjectContent>>(
        &self,
        bucket: S1,
        object: S2,
        content: C,
    ) -> AppendObjectContentBldr {
        AppendObjectContent::builder()
            .client(self.clone())
            .bucket(bucket)
            .object(object)
            .input_content(content)
    }
}

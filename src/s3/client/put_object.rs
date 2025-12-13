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

use crate::s3::client::MinioClient;
use crate::s3::segmented_bytes::SegmentedBytes;
use crate::s3::types::{BucketName, ObjectKey, UploadId};
use crate::s3::{
    builders::{
        AbortMultipartUpload, AbortMultipartUploadBldr, CompleteMultipartUpload,
        CompleteMultipartUploadBldr, CreateMultipartUpload, CreateMultipartUploadBldr,
        ObjectContent, PutObject, PutObjectBldr, PutObjectContent, PutObjectContentBldr,
        UploadPart, UploadPartBldr,
    },
    types::PartInfo,
};
use std::sync::Arc;

impl MinioClient {
    /// Creates a [`PutObject`] request builder to upload an object to a specified bucket in S3-compatible storage.
    /// This method performs a simple, non-multipart upload of the provided content as an object.
    ///
    /// For handling large files requiring multipart upload, see [`create_multipart_upload`](#method.create_multipart_upload).
    ///
    /// To execute the request, call [`PutObject::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`PutObjectResponse`](crate::s3::response::PutObjectResponse).
    ///
    /// For more information, refer to the [AWS S3 PutObject API documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    /// use minio::s3::response::PutObjectResponse;
    /// use minio::s3::types::{BucketName, ObjectKey, S3Api};
    /// use minio::s3::segmented_bytes::SegmentedBytes;
    /// use minio::s3::response_traits::HasObject;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let base_url = "http://localhost:9000/".parse::<BaseUrl>().unwrap();
    ///     let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let client = MinioClient::new(base_url, Some(static_provider), None, None).unwrap();
    ///     let data = SegmentedBytes::from("Hello world".to_string());
    ///     let resp: PutObjectResponse = client
    ///         .put_object(BucketName::new("bucket-name").unwrap(), ObjectKey::new("object-name").unwrap(), data)
    ///         .build().send().await.unwrap();
    ///     println!("successfully put object '{}'", resp.object());
    /// }
    /// ```
    pub fn put_object(
        &self,
        bucket: BucketName,
        object: ObjectKey,
        data: SegmentedBytes,
    ) -> PutObjectBldr {
        let inner = UploadPart::builder()
            .client(self.clone())
            .bucket(bucket)
            .object(object)
            .data(Arc::new(data))
            .build();
        PutObject::builder().inner(inner)
    }

    /// Creates a [`CreateMultipartUpload`] request builder to initiate a new multipart upload for a specified object in a bucket.
    /// This allows uploading large objects as a series of parts, which can be uploaded independently and in parallel.
    ///
    /// To execute the request, call [`CreateMultipartUpload::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`CreateMultipartUploadResponse`](crate::s3::response::CreateMultipartUploadResponse).
    ///
    /// For more information, refer to the [AWS S3 CreateMultipartUpload API documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html).
    ///
    /// # Example
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    /// use minio::s3::response::CreateMultipartUploadResponse;
    /// use minio::s3::types::{BucketName, ObjectKey, S3Api};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let base_url = "http://localhost:9000/".parse::<BaseUrl>().unwrap();
    ///     let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let client = MinioClient::new(base_url, Some(static_provider), None, None).unwrap();
    ///     let resp: CreateMultipartUploadResponse = client
    ///         .create_multipart_upload(BucketName::new("bucket-name").unwrap(), ObjectKey::new("large-object").unwrap())
    ///         .build().send().await.unwrap();
    ///     println!("Initiated multipart upload with UploadId '{:?}'", resp.upload_id().await);
    /// }
    /// ```
    pub fn create_multipart_upload(
        &self,
        bucket: BucketName,
        object: ObjectKey,
    ) -> CreateMultipartUploadBldr {
        CreateMultipartUpload::builder()
            .client(self.clone())
            .bucket(bucket)
            .object(object)
    }

    /// Creates an [`AbortMultipartUpload`] request builder to abort an ongoing multipart upload for an object.
    /// This operation stops the multipart upload and discards all uploaded parts, freeing storage.
    ///
    /// To execute the request, call [`AbortMultipartUpload::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`AbortMultipartUploadResponse`](crate::s3::response::AbortMultipartUploadResponse).
    ///
    /// For more information, refer to the [AWS S3 AbortMultipartUpload API documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html).
    ///
    /// # Example
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    /// use minio::s3::response::AbortMultipartUploadResponse;
    /// use minio::s3::types::{BucketName, ObjectKey, UploadId, S3Api};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let base_url = "http://localhost:9000/".parse::<BaseUrl>().unwrap();
    ///     let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let client = MinioClient::new(base_url, Some(static_provider), None, None).unwrap();
    ///     let resp: AbortMultipartUploadResponse = client
    ///         .abort_multipart_upload(BucketName::new("bucket-name").unwrap(), ObjectKey::new("object-name").unwrap(), UploadId::new("upload-id-123").unwrap())
    ///         .build().send().await.unwrap();
    ///     println!("Aborted multipart upload for '{}', upload id '{}'", "object-name", "upload-id-123");
    /// }
    /// ```
    pub fn abort_multipart_upload(
        &self,
        bucket: BucketName,
        object: ObjectKey,
        upload_id: UploadId,
    ) -> AbortMultipartUploadBldr {
        AbortMultipartUpload::builder()
            .client(self.clone())
            .bucket(bucket)
            .object(object)
            .upload_id(upload_id)
    }

    /// Creates a [`CompleteMultipartUpload`] request builder to complete a multipart upload by assembling previously uploaded parts into a single object.
    /// This finalizes the upload and makes the object available in the bucket.
    ///
    /// To execute the request, call [`CompleteMultipartUpload::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`CompleteMultipartUploadResponse`](crate::s3::response::CompleteMultipartUploadResponse).
    ///
    /// For more information, refer to the [AWS S3 CompleteMultipartUpload API documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html).
    ///
    /// # Example
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    /// use minio::s3::response::CompleteMultipartUploadResponse;
    /// use minio::s3::types::{BucketName, ObjectKey, UploadId, S3Api, PartInfo};
    /// use minio::s3::response_traits::HasObject;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let base_url = "http://localhost:9000/".parse::<BaseUrl>().unwrap();
    ///     let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let client = MinioClient::new(base_url, Some(static_provider), None, None).unwrap();
    ///     let parts: Vec<PartInfo> = vec![]; // fill with your uploaded part info
    ///     let resp: CompleteMultipartUploadResponse = client
    ///         .complete_multipart_upload(BucketName::new("bucket-name").unwrap(), ObjectKey::new("object-name").unwrap(), UploadId::new("upload-id-123").unwrap(), parts)
    ///         .build().send().await.unwrap();
    ///     println!("Completed multipart upload for '{}'", resp.object());
    /// }
    /// ```
    pub fn complete_multipart_upload(
        &self,
        bucket: BucketName,
        object: ObjectKey,
        upload_id: UploadId,
        parts: Vec<PartInfo>,
    ) -> CompleteMultipartUploadBldr {
        CompleteMultipartUpload::builder()
            .client(self.clone())
            .bucket(bucket)
            .object(object)
            .upload_id(upload_id)
            .parts(parts)
    }

    /// Creates an [`UploadPart`] request builder to upload a single part as part of a multipart upload.
    /// Each part is uploaded independently, enabling parallel uploads for large objects.
    ///
    /// To execute the request, call [`UploadPart::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`UploadPartResponse`](crate::s3::response::UploadPartResponse).
    ///
    /// For more information, refer to the [AWS S3 UploadPart API documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html).
    ///
    /// # Example
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    /// use minio::s3::response::UploadPartResponse;
    /// use minio::s3::types::{BucketName, ObjectKey, UploadId, S3Api};
    /// use minio::s3::segmented_bytes::SegmentedBytes;
    /// use minio::s3::response_traits::HasObject;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let base_url = "http://localhost:9000/".parse::<BaseUrl>().unwrap();
    ///     let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let client = MinioClient::new(base_url, Some(static_provider), None, None).unwrap();
    ///     let data = SegmentedBytes::from("Some part data".to_string());
    ///     let resp: UploadPartResponse = client
    ///         .upload_part(BucketName::new("bucket-name").unwrap(), ObjectKey::new("object-name").unwrap(), UploadId::new("upload-id").unwrap(), 1, data)
    ///         .build().send().await.unwrap();
    ///     println!("Uploaded object: {}", resp.object());
    /// }
    /// ```
    pub fn upload_part(
        &self,
        bucket: BucketName,
        object: ObjectKey,
        upload_id: UploadId,
        part_number: u16,
        data: SegmentedBytes,
    ) -> UploadPartBldr {
        UploadPart::builder()
            .client(self.clone())
            .bucket(bucket)
            .object(object)
            .upload_id(upload_id.to_string())
            .part_number(part_number)
            .data(Arc::new(data))
    }

    /// Creates a [`PutObjectContent`] request builder to upload data to MinIO/S3, automatically handling multipart uploads for large content.
    /// This higher-level API efficiently streams and uploads content, splitting it into parts as needed.
    ///
    /// To execute the request, call [`PutObjectContent::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`PutObjectContentResponse`](crate::s3::response::PutObjectContentResponse).
    ///
    /// For more information, see the [AWS S3 Multipart Upload Overview](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html).
    ///
    /// # Example
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    /// use minio::s3::response::PutObjectContentResponse;
    /// use minio::s3::types::{BucketName, ObjectKey, S3Api};
    /// use minio::s3::response_traits::{HasObject, HasEtagFromHeaders};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let base_url = "http://localhost:9000/".parse::<BaseUrl>().unwrap();
    ///     let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let client = MinioClient::new(base_url, Some(static_provider), None, None).unwrap();
    ///     let content = "Hello, world!".to_string();
    ///     let resp: PutObjectContentResponse = client
    ///         .put_object_content(BucketName::new("bucket").unwrap(), ObjectKey::new("object").unwrap(), content)
    ///         .build().send().await.unwrap();
    ///     println!("Uploaded object '{}' with ETag '{:?}'", resp.object(), resp.etag());
    /// }
    /// ```
    pub fn put_object_content<C: Into<ObjectContent>>(
        &self,
        bucket: BucketName,
        object: ObjectKey,
        content: C,
    ) -> PutObjectContentBldr {
        PutObjectContent::builder()
            .client(self.clone())
            .bucket(bucket)
            .object(object)
            .input_content(content)
    }
}

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

use crate::s3::builders::{DeleteBucket, DeleteBucketBldr, DeleteObject, ObjectToDelete};
use crate::s3::client::MinioClient;
use crate::s3::error::Error;
use crate::s3::error::S3ServerError::S3Error;
use crate::s3::minio_error_response::MinioErrorCode;
use crate::s3::response::{
    BucketExistsResponse, DeleteBucketResponse, DeleteObjectResponse, DeleteObjectsResponse,
    DeleteResult, PutObjectLegalHoldResponse,
};
use crate::s3::types::{BucketName, ObjectKey, VersionId};
use crate::s3::types::{S3Api, S3Request, ToStream};
use bytes::Bytes;
use futures_util::StreamExt;
use http::Method;
use multimap::MultiMap;

impl MinioClient {
    /// Creates a [`DeleteBucket`] request builder.
    ///
    /// To execute the request, call [`DeleteBucket::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`DeleteBucketResponse`].
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    /// use minio::s3::response::DeleteBucketResponse;
    /// use minio::s3::types::{BucketName, S3Api};
    /// use minio::s3::response_traits::{HasBucket, HasRegion};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let base_url = "http://localhost:9000/".parse::<BaseUrl>().unwrap();
    ///     let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let client = MinioClient::new(base_url, Some(static_provider), None, None).unwrap();
    ///     let resp: DeleteBucketResponse = client
    ///         .delete_bucket(BucketName::new("bucket-name").unwrap())
    ///         .build().send().await.unwrap();
    ///     println!("bucket '{}' in region '{}' is removed", resp.bucket(), resp.region());
    /// }
    /// ```
    pub fn delete_bucket(&self, bucket: BucketName) -> DeleteBucketBldr {
        DeleteBucket::builder().client(self.clone()).bucket(bucket)
    }

    /// Deletes a bucket and also deletes non-empty buckets by first removing all objects before
    /// deleting the bucket. Bypasses governance mode and legal hold.
    pub async fn delete_and_purge_bucket(
        &self,
        bucket: BucketName,
    ) -> Result<DeleteBucketResponse, Error> {
        let resp: BucketExistsResponse = self.bucket_exists(bucket.clone()).build().send().await?;
        if !resp.exists {
            // if the bucket does not exist, we can return early
            let dummy: S3Request = S3Request::builder()
                .client(self.clone())
                .method(Method::DELETE)
                .bucket(bucket)
                .headers(MultiMap::default())
                .build();

            return Ok(DeleteBucketResponse {
                request: dummy, //TODO consider how to handle this
                body: Bytes::new(),
                headers: Default::default(),
            });
        }

        let is_express = self.is_minio_express().await;

        let mut stream = self
            .list_objects(bucket.clone())
            .include_versions(!is_express)
            .recursive(true)
            .build()
            .to_stream()
            .await;

        if is_express {
            while let Some(items) = stream.next().await {
                let object_names = items?.contents.into_iter().map(ObjectToDelete::from);
                let mut resp = self
                    .delete_objects_streaming(bucket.clone(), object_names)
                    .bypass_governance_mode(false) // Express does not support governance mode
                    .to_stream()
                    .await;

                while let Some(item) = resp.next().await {
                    let _resp: DeleteObjectsResponse = item?;
                }
            }
        } else {
            while let Some(items) = stream.next().await {
                let object_names = items?.contents.into_iter().map(ObjectToDelete::from);
                let mut resp = self
                    .delete_objects_streaming(bucket.clone(), object_names)
                    .bypass_governance_mode(true)
                    .to_stream()
                    .await;

                while let Some(item) = resp.next().await {
                    let resp: DeleteObjectsResponse = item?;
                    for obj in resp.result()?.into_iter() {
                        match obj {
                            DeleteResult::Deleted(_) => {}
                            DeleteResult::Error(v) => {
                                // the object is not deleted. try to disable legal hold and try again.
                                let object_key =
                                    ObjectKey::try_from(v.object_name.as_str()).unwrap();

                                let result = match &v.version_id {
                                    Some(vid) => {
                                        self.put_object_legal_hold(
                                            bucket.clone(),
                                            object_key,
                                            false,
                                        )
                                        .version_id(VersionId::try_from(vid.as_str()).unwrap())
                                        .build()
                                        .send()
                                        .await
                                    }
                                    None => {
                                        self.put_object_legal_hold(
                                            bucket.clone(),
                                            object_key,
                                            false,
                                        )
                                        .build()
                                        .send()
                                        .await
                                    }
                                };

                                let _resp: PutObjectLegalHoldResponse = result?;

                                let _resp: DeleteObjectResponse = DeleteObject::builder()
                                    .client(self.clone())
                                    .bucket(bucket.clone())
                                    .object(v)
                                    .bypass_governance_mode(true)
                                    .build()
                                    .send()
                                    .await?;
                            }
                        }
                    }
                }
            }
        }

        let request: DeleteBucket = self.delete_bucket(bucket.clone()).build();
        match request.send().await {
            Ok(resp) => Ok(resp),
            Err(Error::S3Server(S3Error(mut e))) => {
                if matches!(e.code(), MinioErrorCode::NoSuchBucket) {
                    let dummy: S3Request = S3Request::builder()
                        .client(self.clone())
                        .method(Method::DELETE)
                        .bucket(bucket)
                        .headers(MultiMap::default())
                        .build();

                    Ok(DeleteBucketResponse {
                        request: dummy, //TODO consider how to handle this
                        body: Bytes::new(),
                        headers: e.take_headers(),
                    })
                } else if matches!(e.code(), MinioErrorCode::BucketNotEmpty) {
                    // for convenience, add the first 5 documents that were are still in the bucket
                    // to the error message
                    let mut stream = self
                        .list_objects(bucket.clone())
                        .include_versions(!is_express)
                        .recursive(true)
                        .build()
                        .to_stream()
                        .await;

                    let mut objs = Vec::new();
                    while let Some(items_result) = stream.next().await {
                        if let Ok(items) = items_result {
                            objs.extend(items.contents);
                            if objs.len() >= 5 {
                                break;
                            }
                        }
                        // else: silently ignore the error and keep looping
                    }

                    let new_msg = match e.message() {
                        None => format!("found content: {objs:?}"),
                        Some(msg) => format!("{msg}, found content: {objs:?}"),
                    };
                    e.set_message(new_msg);
                    Err(Error::S3Server(S3Error(e)))
                } else if e
                    .message()
                    .as_ref()
                    .map(|msg| msg.contains("Use DeleteWarehouse API"))
                    .unwrap_or(false)
                {
                    // This is a warehouse bucket - provide helpful guidance
                    let original_msg = e.message().clone().unwrap_or_default();
                    let new_msg = format!(
                        "Cannot delete warehouse bucket '{}' using DeleteBucket API. \
                         Warehouse buckets must be deleted using the DeleteWarehouse S3 Tables API. \
                         Original error: {}",
                        bucket, original_msg
                    );
                    e.set_code(MinioErrorCode::WarehouseBucketOperationNotSupported);
                    e.set_message(new_msg);
                    Err(Error::S3Server(S3Error(e)))
                } else {
                    Err(Error::S3Server(S3Error(e)))
                }
            }
            Err(e) => Err(e),
        }
    }
}

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

use super::Client;
use crate::s3::builders::{DeleteBucket, DeleteObject, ObjectToDelete};
use crate::s3::error::{Error, ErrorCode};
use crate::s3::response::DeleteResult;
use crate::s3::response::{
    DeleteBucketResponse, DeleteObjectResponse, DeleteObjectsResponse, PutObjectLegalHoldResponse,
};
use crate::s3::types::{S3Api, ToStream};
use bytes::Bytes;
use futures::StreamExt;

impl Client {
    /// Creates a [`DeleteBucket`] request builder.
    ///
    /// To execute the request, call [`DeleteBucket::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`DeleteBucketResponse`].
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::Client;
    /// use minio::s3::response::DeleteBucketResponse;
    /// use minio::s3::types::S3Api;
    /// use minio::s3::response::a_response_traits::{HasBucket, HasRegion};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client: Client = Default::default(); // configure your client here
    ///     let resp: DeleteBucketResponse =
    ///         client.delete_bucket("bucket-name").send().await.unwrap();
    ///     println!("bucket '{}' in region '{}' is removed", resp.bucket(), resp.region());
    /// }
    /// ```
    pub fn delete_bucket<S: Into<String>>(&self, bucket: S) -> DeleteBucket {
        DeleteBucket::new(self.clone(), bucket.into())
    }

    /// Deletes a bucket and also deletes non-empty buckets by first removing all objects before
    /// deleting the bucket. Bypasses governance mode and legal hold.
    pub async fn delete_and_purge_bucket<S: Into<String>>(
        &self,
        bucket: S,
    ) -> Result<DeleteBucketResponse, Error> {
        let bucket: String = bucket.into();
        let is_express = self.is_minio_express().await;

        let mut stream = self
            .list_objects(&bucket)
            .include_versions(!is_express)
            .recursive(true)
            .to_stream()
            .await;

        if is_express {
            while let Some(items) = stream.next().await {
                let object_names = items?.contents.into_iter().map(ObjectToDelete::from);
                let mut resp = self
                    .delete_objects_streaming(&bucket, object_names)
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
                    .delete_objects_streaming(&bucket, object_names)
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
                                let _resp: PutObjectLegalHoldResponse = self
                                    .put_object_legal_hold(&bucket, &v.object_name, false)
                                    .version_id(v.version_id.clone())
                                    .send()
                                    .await?;

                                let _resp: DeleteObjectResponse = DeleteObject::new(
                                    self.clone(),
                                    bucket.clone(),
                                    ObjectToDelete::from(v),
                                )
                                .bypass_governance_mode(true)
                                .send()
                                .await?;
                            }
                        }
                    }
                }
            }
        }

        let request: DeleteBucket = self.delete_bucket(&bucket);
        match request.send().await {
            Ok(resp) => Ok(resp),
            Err(Error::S3Error(e)) => {
                if e.code == ErrorCode::NoSuchBucket {
                    Ok(DeleteBucketResponse {
                        request: Default::default(), //TODO consider how to handle this
                        body: Bytes::new(),
                        headers: e.headers,
                    })
                } else if e.code == ErrorCode::BucketNotEmpty {
                    // for convenience, add the first 5 documents that were are still in the bucket
                    let mut stream = self
                        .list_objects(&bucket)
                        .include_versions(!is_express)
                        .recursive(true)
                        .to_stream()
                        .await;

                    let mut objs = Vec::new();
                    while let Some(items) = stream.next().await {
                        objs.append(items?.contents.as_mut());
                        if objs.len() >= 5 {
                            break;
                        }
                    }
                    println!("Bucket '{bucket}' is not empty. The first 5 objects are: {objs:?}");
                    Err(Error::S3Error(e))
                } else {
                    Err(Error::S3Error(e))
                }
            }
            Err(e) => Err(e),
        }
    }
}

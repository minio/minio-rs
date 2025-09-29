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
    /// use minio::s3::response::DeleteBucketResponse;
    /// use minio::s3::types::S3Api;
    /// use minio::s3::response::a_response_traits::{HasBucket, HasRegion};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = MinioClient::create_client_on_localhost().unwrap(); // configure your client here
    ///     let resp: DeleteBucketResponse = client
    ///         .delete_bucket("bucket-name")
    ///         .build().send().await.unwrap();
    ///     println!("bucket '{}' in region '{}' is removed", resp.bucket(), resp.region());
    /// }
    /// ```
    pub fn delete_bucket<S: Into<String>>(&self, bucket: S) -> DeleteBucketBldr {
        DeleteBucket::builder().client(self.clone()).bucket(bucket)
    }

    /// Deletes a bucket and also deletes non-empty buckets by first removing all objects before
    /// deleting the bucket. Bypasses governance mode and legal hold.
    pub async fn delete_and_purge_bucket<S: Into<String>>(
        &self,
        bucket: S,
    ) -> Result<DeleteBucketResponse, Error> {
        let bucket: String = bucket.into();

        let resp: BucketExistsResponse = self.bucket_exists(&bucket).build().send().await?;
        if !resp.exists {
            // if the bucket does not exist, we can return early
            let dummy: S3Request = S3Request::builder().client(self.clone()).method(Method::DELETE).bucket(bucket).headers(MultiMap::default()).build(/* S3RequestBuilder_Error_Missing_required_field_headers */);

            return Ok(DeleteBucketResponse {
                request: dummy, //TODO consider how to handle this
                body: Bytes::new(),
                headers: Default::default(),
            });
        }

        let is_express = self.is_minio_express().await;

        let mut stream = self
            .list_objects(&bucket)
            .include_versions(!is_express)
            .recursive(true)
            .build()
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
                                    .build()
                                    .send()
                                    .await?;

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

        let request: DeleteBucket = self.delete_bucket(&bucket).build();
        match request.send().await {
            Ok(resp) => Ok(resp),
            Err(Error::S3Server(S3Error(mut e))) => {
                if matches!(e.code(), MinioErrorCode::NoSuchBucket) {
                    let dummy: S3Request = S3Request::builder().client(self.clone()).method(Method::DELETE).bucket(bucket).headers(MultiMap::default()).build(/* S3RequestBuilder_Error_Missing_required_field_headers */);

                    Ok(DeleteBucketResponse {
                        request: dummy, //TODO consider how to handle this
                        body: Bytes::new(),
                        headers: e.take_headers(),
                    })
                } else if matches!(e.code(), MinioErrorCode::BucketNotEmpty) {
                    // for convenience, add the first 5 documents that were are still in the bucket
                    // to the error message
                    let mut stream = self
                        .list_objects(&bucket)
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
                } else {
                    Err(Error::S3Server(S3Error(e)))
                }
            }
            Err(e) => Err(e),
        }
    }
}

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

//! S3 APIs for bucket objects.

use super::Client;
use crate::s3::builders::{DeleteBucket, ObjectToDelete, RemoveObject};
use crate::s3::error::{Error, ErrorCode};
use crate::s3::response::DeleteResult;
use crate::s3::response::{
    DeleteBucketResponse, PutObjectLegalHoldResponse, RemoveObjectResponse, RemoveObjectsResponse,
};
use crate::s3::types::{S3Api, ToStream};
use futures::StreamExt;

impl Client {
    /// Creates a [`DeleteBucket`] request builder.
    ///
    /// To execute the request, call [`DeleteBucket::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`DeleteBucketResponse`](crate::s3::response::DeleteBucketResponse).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::Client;
    /// use minio::s3::response::DeleteBucketResponse;
    /// use minio::s3::types::S3Api;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client: Client = Default::default(); // configure your client here
    ///     let resp: DeleteBucketResponse =
    ///         client.delete_bucket("bucket-name").send().await.unwrap();
    ///     println!("bucket '{}' in region '{}' is removed", resp.bucket, resp.region);
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
        if self.is_minio_express() {
            let mut stream = self.list_objects(&bucket).to_stream().await;

            while let Some(items) = stream.next().await {
                let mut resp = self
                    .remove_objects(
                        &bucket,
                        items?.contents.into_iter().map(ObjectToDelete::from),
                    )
                    .to_stream()
                    .await;
                while let Some(item) = resp.next().await {
                    let _resp: RemoveObjectsResponse = item?;
                }
            }
        } else {
            let mut stream = self
                .list_objects(&bucket)
                .include_versions(true)
                .to_stream()
                .await;

            while let Some(items) = stream.next().await {
                let mut resp = self
                    .remove_objects(
                        &bucket,
                        items?.contents.into_iter().map(ObjectToDelete::from),
                    )
                    .bypass_governance_mode(true)
                    .to_stream()
                    .await;

                while let Some(item) = resp.next().await {
                    let resp: RemoveObjectsResponse = item?;
                    for obj in resp.result.into_iter() {
                        match obj {
                            DeleteResult::Deleted(_) => {}
                            DeleteResult::Error(v) => {
                                // the object is not deleted. try to disable legal hold and try again.
                                let _resp: PutObjectLegalHoldResponse = self
                                    .put_object_legal_hold(&bucket, &v.object_name, false)
                                    .version_id(v.version_id.clone())
                                    .send()
                                    .await?;

                                let _resp: RemoveObjectResponse = RemoveObject::new(
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
        match self.delete_bucket(bucket).send().await {
            Ok(resp) => Ok(resp),
            Err(Error::S3Error(e)) => {
                if e.code == ErrorCode::NoSuchBucket {
                    Ok(DeleteBucketResponse {
                        headers: e.headers,
                        bucket: e.bucket_name,
                        region: String::new(),
                    })
                } else {
                    Err(Error::S3Error(e))
                }
            }
            Err(e) => Err(e),
        }
    }
}

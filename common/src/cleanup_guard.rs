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

use minio::s3::MinioClient;

/// Cleanup guard that removes the bucket when it is dropped
pub struct CleanupGuard {
    client: MinioClient,
    bucket_name: String,
}

impl CleanupGuard {
    #[allow(dead_code)]
    pub fn new<S: Into<String>>(client: MinioClient, bucket_name: S) -> Self {
        Self {
            client,
            bucket_name: bucket_name.into(),
        }
    }

    pub async fn cleanup(&self) {
        cleanup(self.client.clone(), &self.bucket_name).await;
    }
}

pub async fn cleanup(client: MinioClient, bucket_name: &str) {
    tokio::select!(
        _ = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
            eprintln!("Cleanup timeout after 60s while removing bucket {bucket_name}");
        },
        outcome = client.delete_and_purge_bucket(bucket_name) => {
            match outcome {
                Ok(_) => {
                    //eprintln!("Bucket {} removed successfully", bucket_name);
                }
                Err(e) => {
                    eprintln!("Error removing bucket '{bucket_name}':\n{e}");
                }
            }
        }
    );
}

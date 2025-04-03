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

use async_std::future::timeout;
use minio::s3::Client;
use std::sync::Arc;
use std::thread;

/// Cleanup guard that removes the bucket when it is dropped
pub struct CleanupGuard {
    client: Arc<Client>,
    bucket_name: String,
}

impl CleanupGuard {
    #[allow(dead_code)]
    pub fn new(client: &Arc<Client>, bucket_name: &str) -> Self {
        Self {
            client: Arc::clone(client),
            bucket_name: bucket_name.to_string(),
        }
    }
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        let client = self.client.clone();
        let bucket_name = self.bucket_name.clone();
        //println!("Going to remove bucket {}", bucket_name);

        // Spawn the cleanup task in a way that detaches it from the current runtime
        thread::spawn(move || {
            // Create a new runtime for this thread
            let rt = tokio::runtime::Runtime::new().unwrap();

            // Execute the async cleanup in this new runtime
            rt.block_on(async {
                // do the actual removal of the bucket
                match timeout(
                    std::time::Duration::from_secs(60),
                    client.remove_and_purge_bucket(&bucket_name),
                )
                .await
                {
                    Ok(result) => match result {
                        Ok(_) => {
                            //println!("Bucket {} removed successfully", bucket_name),
                        }
                        Err(e) => println!("Error removing bucket {}: {:?}", bucket_name, e),
                    },
                    Err(_) => println!("Timeout after 60s while removing bucket {}", bucket_name),
                }
            });
        })
        .join()
        .unwrap(); // This blocks the current thread until cleanup is done
    }
}

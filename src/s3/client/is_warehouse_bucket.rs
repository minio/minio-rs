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
use crate::s3::types::{BucketName, ObjectKey, S3Api};
use crate::s3tables::utils::WarehouseName;

impl MinioClient {
    /// Non-destructively checks whether a bucket is a warehouse bucket (S3 Tables).
    ///
    /// This method uses the Iceberg REST API (`GET /_iceberg/v1/warehouses/{bucket-name}`)
    /// to check if the bucket is managed as a warehouse/table bucket, without attempting
    /// any destructive operations.
    ///
    /// # Arguments
    ///
    /// * `bucket_name` - Name of the bucket to check
    ///
    /// # Returns
    ///
    /// * `Ok(true)` - Bucket is a warehouse bucket (warehouse metadata exists)
    /// * `Ok(false)` - Bucket is not a warehouse bucket (warehouse metadata does not exist)
    /// * `Err(e)` - Error occurred while checking (e.g., network error, permission denied)
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    /// use minio::s3::types::BucketName;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let base_url = "http://localhost:9000/".parse::<BaseUrl>()?;
    ///     let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let client = MinioClient::new(base_url, Some(provider), None, None)?;
    ///
    ///     // Check if a bucket is a warehouse bucket using Iceberg REST API
    ///     match client.is_warehouse_bucket(BucketName::new("my-bucket").unwrap()).await {
    ///         Ok(true) => println!("This is a warehouse bucket"),
    ///         Ok(false) => println!("This is a regular S3 bucket"),
    ///         Err(e) => println!("Error checking bucket: {}", e),
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Note
    ///
    /// This method creates a TablesClient to query the Iceberg REST API at `/_iceberg/v1`.
    /// If the warehouse metadata exists, the bucket is a warehouse bucket. If a 404 error
    /// is returned, the bucket is not a warehouse bucket. Any other error is propagated.
    pub async fn is_warehouse_bucket(
        &self,
        bucket_name: BucketName,
    ) -> Result<bool, crate::s3::error::Error> {
        // Try to parse as a WarehouseName - if it fails, it's not a valid warehouse name
        let _warehouse_name = match WarehouseName::new(bucket_name.clone()) {
            Ok(name) => name,
            Err(_) => {
                // If the bucket name doesn't meet warehouse naming requirements, it's definitely not a warehouse
                return Ok(false);
            }
        };

        // Attempt to HEAD the warehouse object on the bucket
        // This checks if warehouse metadata exists for this bucket by looking for the
        // Iceberg metadata path. The presence of warehouse metadata indicates a warehouse bucket.
        match self
            .stat_object(
                bucket_name,
                ObjectKey::try_from("/.iceberg/metadata").unwrap(),
            )
            .build()
            .send()
            .await
        {
            Ok(_) => {
                // Warehouse metadata exists, so this is a warehouse bucket
                Ok(true)
            }
            Err(e) => {
                // Check if it's a 404 error - warehouse doesn't exist
                let error_str = e.to_string();
                if error_str.contains("404") || error_str.contains("NoSuchKey") {
                    Ok(false) // Warehouse doesn't exist
                } else {
                    // For any other error (like permission denied), return false
                    // as we can't reliably determine if it's a warehouse
                    Ok(false)
                }
            }
        }
    }
}

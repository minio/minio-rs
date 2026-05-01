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

//! Demonstrates comprehensive error handling for inventory operations.
//!
//! This example shows how to handle different error types when working with
//! inventory jobs, including inventory-specific errors, generic S3 errors,
//! validation errors, and network errors.

use minio::s3::MinioClient;
use minio::s3::creds::StaticProvider;
use minio::s3::error::{Error, S3ServerError, ValidationErr};
use minio::s3::http::BaseUrl;
use minio::s3::types::{InventoryError, S3Api};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let base_url = "http://localhost:9000".parse::<BaseUrl>()?;
    let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    let client = MinioClient::new(base_url, Some(static_provider), None, None)?;

    let bucket = "test-bucket";
    let job_id = "test-job";

    println!("=== Inventory Error Handling Examples ===\n");

    // Example 1: Get inventory config with error handling
    println!("1. Getting inventory configuration...");
    match client
        .get_inventory_config(bucket, job_id)?
        .build()
        .send()
        .await
    {
        Ok(response) => {
            println!("   ✓ Success! Job user: {}", response.user());
            println!("   Config:\n{}", response.yaml_definition());
        }
        Err(e) => {
            println!("   ✗ Error occurred:");
            handle_error(e);
        }
    }

    println!("\n2. Deleting inventory configuration...");
    match client
        .delete_inventory_config(bucket, job_id)?
        .build()
        .send()
        .await
    {
        Ok(_) => {
            println!("   ✓ Successfully deleted job '{}'", job_id);
        }
        Err(e) => {
            println!("   ✗ Error occurred:");
            handle_error(e);
        }
    }

    // Example 3: Demonstrating validation error (empty job ID)
    println!("\n3. Testing validation error (empty job ID)...");
    match client
        .delete_inventory_config(bucket, "")?
        .build()
        .send()
        .await
    {
        Ok(_) => {
            println!("   ✓ Unexpected success");
        }
        Err(e) => {
            println!("   ✗ Expected validation error:");
            handle_error(e);
        }
    }

    // Example 4: Demonstrating bucket not found error
    println!("\n4. Testing bucket not found error...");
    match client
        .get_inventory_config("nonexistent-bucket", job_id)?
        .build()
        .send()
        .await
    {
        Ok(_) => {
            println!("   ✓ Unexpected success");
        }
        Err(e) => {
            println!("   ✗ Expected bucket not found error:");
            handle_error(e);
        }
    }

    Ok(())
}

/// Handles different error types with detailed information
fn handle_error(error: Error) {
    match error {
        // Inventory-specific errors from MinIO server
        Error::S3Server(S3ServerError::InventoryError(inv_err)) => {
            println!("   [INVENTORY ERROR]");
            handle_inventory_error(*inv_err);
        }

        // Generic S3 errors
        Error::S3Server(S3ServerError::S3Error(s3_err)) => {
            println!("   [S3 ERROR]");
            println!("   Code: {:?}", s3_err.code());
            if let Some(msg) = s3_err.message() {
                println!("   Message: {}", msg);
            }
            println!("   Resource: {}", s3_err.resource());
            println!("   Request ID: {}", s3_err.request_id());
        }

        // Invalid server response
        Error::S3Server(S3ServerError::InvalidServerResponse {
            message,
            http_status_code,
            content_type,
        }) => {
            println!("   [INVALID SERVER RESPONSE]");
            println!("   Status: {}", http_status_code);
            println!("   Content-Type: {}", content_type);
            println!("   Message: {}", message);
        }

        // HTTP error with status code
        Error::S3Server(S3ServerError::HttpError(status, body)) => {
            println!("   [HTTP ERROR]");
            println!("   Status: {}", status);
            println!("   Body: {}", body);
        }

        // Client-side validation errors
        Error::Validation(val_err) => {
            println!("   [VALIDATION ERROR]");
            handle_validation_error(val_err);
        }

        // Network errors
        Error::Network(net_err) => {
            println!("   [NETWORK ERROR]");
            println!("   {}", net_err);
        }

        // File I/O errors
        Error::DriveIo(io_err) => {
            println!("   [I/O ERROR]");
            println!("   {}", io_err);
        }

        // Tables errors
        Error::TablesError(tbl_err) => {
            println!("   [TABLES ERROR]");
            println!("   {}", tbl_err);
        }
    }
}

/// Handles inventory-specific errors with detailed information
fn handle_inventory_error(error: InventoryError) {
    match error {
        InventoryError::NoSuchConfiguration { bucket, job_id } => {
            println!("   Type: Configuration Not Found");
            println!("   Bucket: {}", bucket);
            println!("   Job ID: {}", job_id);
            println!("   → The inventory job '{}' does not exist in bucket '{}'", job_id, bucket);
        }

        InventoryError::InvalidJobId { job_id, reason } => {
            println!("   Type: Invalid Job ID");
            println!("   Job ID: {}", job_id);
            println!("   Reason: {}", reason);
            println!("   → Check that the job ID meets requirements (no special characters, etc.)");
        }

        InventoryError::NoSuchSourceBucket { bucket } => {
            println!("   Type: Source Bucket Not Found");
            println!("   Bucket: {}", bucket);
            println!("   → Create the source bucket before creating inventory jobs");
        }

        InventoryError::NoSuchDestinationBucket { bucket } => {
            println!("   Type: Destination Bucket Not Found");
            println!("   Bucket: {}", bucket);
            println!("   → Create the destination bucket before creating inventory jobs");
        }

        InventoryError::PermissionDenied { bucket, message } => {
            println!("   Type: Permission Denied");
            println!("   Bucket: {}", bucket);
            println!("   Message: {}", message);
            println!("   → Check IAM policies and bucket permissions");
        }

        InventoryError::JobAlreadyCanceled { bucket, job_id } => {
            println!("   Type: Job Already Canceled");
            println!("   Bucket: {}", bucket);
            println!("   Job ID: {}", job_id);
            println!("   → Cannot perform operation on a canceled job");
        }

        InventoryError::JobAlreadyCompleted { bucket, job_id } => {
            println!("   Type: Job Already Completed");
            println!("   Bucket: {}", bucket);
            println!("   Job ID: {}", job_id);
            println!("   → The job has finished execution");
        }

        InventoryError::JobNotRunning { bucket, job_id } => {
            println!("   Type: Job Not Running");
            println!("   Bucket: {}", bucket);
            println!("   Job ID: {}", job_id);
            println!("   → Cannot cancel a job that hasn't started yet");
        }

        InventoryError::JobAlreadySuspended { bucket, job_id } => {
            println!("   Type: Job Already Suspended");
            println!("   Bucket: {}", bucket);
            println!("   Job ID: {}", job_id);
            println!("   → The job is currently suspended");
        }

        InventoryError::CannotSuspendCompletedJob { bucket, job_id } => {
            println!("   Type: Cannot Suspend Completed Job");
            println!("   Bucket: {}", bucket);
            println!("   Job ID: {}", job_id);
            println!("   → Cannot suspend a job that has already completed");
        }

        InventoryError::CannotSuspendCanceledOnceJob { bucket, job_id } => {
            println!("   Type: Cannot Suspend Canceled Once Job");
            println!("   Bucket: {}", bucket);
            println!("   Job ID: {}", job_id);
            println!("   → Once-off jobs cannot be suspended after cancellation");
        }

        InventoryError::InvalidJobStateForSuspend {
            bucket,
            job_id,
            state,
        } => {
            println!("   Type: Invalid State for Suspend");
            println!("   Bucket: {}", bucket);
            println!("   Job ID: {}", job_id);
            println!("   Current State: {}", state);
            println!("   → Job must be in a running state to suspend");
        }

        InventoryError::JobNotSuspended {
            bucket,
            job_id,
            state,
        } => {
            println!("   Type: Job Not Suspended");
            println!("   Bucket: {}", bucket);
            println!("   Job ID: {}", job_id);
            println!("   Current State: {}", state);
            println!("   → Job must be suspended before it can be resumed");
        }

        InventoryError::JobMetadataUpdateFailed {
            bucket,
            job_id,
            reason,
        } => {
            println!("   Type: Metadata Update Failed");
            println!("   Bucket: {}", bucket);
            println!("   Job ID: {}", job_id);
            println!("   Reason: {}", reason);
            println!("   → Internal server error updating job metadata");
        }

        InventoryError::CorruptJobMetadata { bucket, job_id } => {
            println!("   Type: Corrupt Metadata");
            println!("   Bucket: {}", bucket);
            println!("   Job ID: {}", job_id);
            println!("   → Job metadata is corrupted, may need to recreate the job");
        }

        InventoryError::UnexpectedError { bucket, message } => {
            println!("   Type: Unexpected Server Error");
            println!("   Bucket: {}", bucket);
            println!("   Message: {}", message);
            println!("   → An unexpected error occurred on the server");
        }
    }
}

/// Handles validation errors with detailed information
fn handle_validation_error(error: ValidationErr) {
    match error {
        ValidationErr::InvalidInventoryJobId { id, reason } => {
            println!("   Type: Invalid Inventory Job ID");
            println!("   Job ID: {}", id);
            println!("   Reason: {}", reason);
        }
        ValidationErr::InvalidBucketName { name, reason } => {
            println!("   Type: Invalid Bucket Name");
            println!("   Bucket: {}", name);
            println!("   Reason: {}", reason);
        }
        _ => {
            println!("   {}", error);
        }
    }
}

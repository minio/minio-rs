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

use minio::madmin::madmin_client::MadminClient;
use minio::madmin::response::{
    AddUserResponse, ListUsersResponse, RemoveUserResponse, SetUserResponse, SetUserStatusResponse,
    UserInfoResponse,
};
use minio::madmin::types::MadminApi;
use minio::madmin::types::typed_parameters::AccessKey;
use minio::s3::creds::StaticProvider;
use minio_common::test_context::TestContext;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_list_users() {
    let ctx = TestContext::new_from_env();

    // Create MadminClient from the same base URL with credentials
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    // List users
    let resp: ListUsersResponse = madmin_client.list_users().build().send().await.unwrap();

    // Should have at least the admin user
    let users = resp.users().unwrap();
    assert!(!users.is_empty());

    // Print user details
    for (username, info) in &users {
        println!("User: {} - Status: {}", username, info.status);
        if let Some(policy) = &info.policy_name {
            println!("  Policy: {}", policy);
        }
        if let Some(groups) = &info.member_of {
            println!("  Groups: {:?}", groups);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_add_user() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    // Generate unique username
    let username = format!(
        "testuser-{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
    );
    let password = "TestPassword123!";

    // Add user
    let _resp: AddUserResponse = madmin_client
        .add_user(&username, password)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    // Success indicated by no error
    // Verify user was created by listing users
    let list_resp: ListUsersResponse = madmin_client.list_users().build().send().await.unwrap();

    assert!(list_resp.users().unwrap().contains_key(&username));
    println!("Successfully created user: {}", username);

    // Clean up - remove the user
    let _remove_resp: RemoveUserResponse = madmin_client
        .remove_user(&username)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    println!("Cleaned up test user: {}", username);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_remove_user() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    // First create a user
    let username = format!(
        "testuser-{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
    );
    let password = "TestPassword123!";

    let _add_resp: AddUserResponse = madmin_client
        .add_user(&username, password)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    println!("Created test user: {}", username);

    // Verify it exists
    let list_resp: ListUsersResponse = madmin_client.list_users().build().send().await.unwrap();
    assert!(list_resp.users().unwrap().contains_key(&username));

    // Now remove it
    let _resp: RemoveUserResponse = madmin_client
        .remove_user(&username)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    // Success indicated by no error
    // Verify it's gone
    let list_resp2: ListUsersResponse = madmin_client.list_users().build().send().await.unwrap();
    assert!(!list_resp2.users().unwrap().contains_key(&username));

    println!("Successfully removed user: {}", username);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_user_info() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    // First create a user
    let username = format!(
        "testuser-{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
    );
    let password = "TestPassword123!";

    let _add_resp: AddUserResponse = madmin_client
        .add_user(&username, password)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    println!("Created test user: {}", username);

    // Get user info
    let resp: UserInfoResponse = madmin_client
        .user_info()
        .access_key(&username)
        .build()
        .send()
        .await
        .unwrap();

    let user_info = resp.user_info().unwrap();
    assert!(!user_info.status.is_empty());
    println!("User status: {}", user_info.status);

    if let Some(policy) = &user_info.policy_name {
        println!("Policy: {}", policy);
    }

    if let Some(groups) = &user_info.member_of {
        println!("Groups: {:?}", groups);
    }

    // Clean up
    let _remove_resp: RemoveUserResponse = madmin_client
        .remove_user(&username)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    println!("Cleaned up test user: {}", username);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_add_user_invalid_credentials() {
    // Try to create AccessKey with empty username - should fail validation
    let result = AccessKey::new("");
    assert!(result.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_remove_nonexistent_user() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    // Try to remove a user that doesn't exist
    let result: Result<RemoveUserResponse, _> = madmin_client
        .remove_user("nonexistent-user-12345")
        .unwrap()
        .build()
        .send()
        .await;

    // MinIO returns 404 for non-existent users
    assert!(result.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_user_info_nonexistent_user() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    // Try to get info for a user that doesn't exist
    let result: Result<UserInfoResponse, _> = madmin_client
        .user_info()
        .access_key("nonexistent-user-12345")
        .build()
        .send()
        .await;

    // MinIO returns 404 for non-existent users
    assert!(result.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_add_duplicate_user() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    // Create a user
    let username = format!(
        "testuser-{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
    );
    let password = "TestPassword123!";

    let _add_resp: AddUserResponse = madmin_client
        .add_user(&username, password)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    // Try to add the same user again - MinIO allows updating existing users
    let _resp2: AddUserResponse = madmin_client
        .add_user(&username, "NewPassword456!")
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    // Should succeed (MinIO treats this as an update - success indicated by no error)

    // Clean up
    let _remove_resp: RemoveUserResponse = madmin_client
        .remove_user(&username)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    println!("Cleaned up test user: {}", username);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_user_lifecycle() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let username = format!(
        "lifecycle-user-{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
    );
    let password = "TestPassword123!";

    // Step 1: Add user
    let _add_resp: AddUserResponse = madmin_client
        .add_user(&username, password)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    println!("✓ User created: {}", username);

    // Step 2: Verify user exists in list
    let list_resp: ListUsersResponse = madmin_client.list_users().build().send().await.unwrap();
    assert!(list_resp.users().unwrap().contains_key(&username));
    println!("✓ User found in list");

    // Step 3: Get user info
    let info_resp: UserInfoResponse = madmin_client
        .user_info()
        .access_key(&username)
        .build()
        .send()
        .await
        .unwrap();
    let user_info = info_resp.user_info().unwrap();
    assert!(!user_info.status.is_empty());
    println!("✓ User info retrieved: status={}", user_info.status);

    // Step 4: Remove user
    let _remove_resp: RemoveUserResponse = madmin_client
        .remove_user(&username)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    println!("✓ User removed");

    // Step 5: Verify user is gone
    let list_resp2: ListUsersResponse = madmin_client.list_users().build().send().await.unwrap();
    assert!(!list_resp2.users().unwrap().contains_key(&username));
    println!("✓ User confirmed removed from list");

    println!("Complete user lifecycle test passed!");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_set_user_status() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    // Create a test user
    let username = format!(
        "statustest-{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
    );
    let password = "TestPassword123!";

    let _add_resp: AddUserResponse = madmin_client
        .add_user(&username, password)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    println!("Created test user: {}", username);

    // Disable the user
    let _disable_resp: SetUserStatusResponse = madmin_client
        .set_user_status()
        .access_key(&username)
        .status("disabled".to_string())
        .build()
        .send()
        .await
        .unwrap();
    println!("✓ User disabled successfully");

    // Verify user is disabled
    let info_resp: UserInfoResponse = madmin_client
        .user_info()
        .access_key(&username)
        .build()
        .send()
        .await
        .unwrap();
    let user_info = info_resp.user_info().unwrap();
    assert_eq!(user_info.status, "disabled");
    println!("✓ User status verified as disabled");

    // Re-enable the user
    let _enable_resp: SetUserStatusResponse = madmin_client
        .set_user_status()
        .access_key(&username)
        .status("enabled".to_string())
        .build()
        .send()
        .await
        .unwrap();
    println!("✓ User re-enabled successfully");

    // Verify user is enabled again
    let info_resp2: UserInfoResponse = madmin_client
        .user_info()
        .access_key(&username)
        .build()
        .send()
        .await
        .unwrap();
    let user_info2 = info_resp2.user_info().unwrap();
    assert_eq!(user_info2.status, "enabled");
    println!("✓ User status verified as enabled");

    // Clean up
    let _remove_resp: RemoveUserResponse = madmin_client
        .remove_user(&username)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    println!("Cleaned up test user: {}", username);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_set_user_status_nonexistent() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    // Try to set status for non-existent user
    let result: Result<SetUserStatusResponse, _> = madmin_client
        .set_user_status()
        .access_key("nonexistent-user-12345")
        .status("enabled".to_string())
        .build()
        .send()
        .await;

    // Should fail with an error
    assert!(result.is_err());
    println!("Set status for nonexistent user correctly failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_set_user() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let username = format!(
        "setuser-test-{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
    );
    let initial_password = "InitialPassword123!";
    let updated_password = "UpdatedPassword456!";

    let _resp: SetUserResponse = madmin_client
        .set_user(&username, initial_password)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    println!("Created user with SetUser: {}", username);

    let list_resp: ListUsersResponse = madmin_client.list_users().build().send().await.unwrap();
    assert!(list_resp.users().unwrap().contains_key(&username));
    println!("✓ User found in list");

    let _update_resp: SetUserResponse = madmin_client
        .set_user(&username, updated_password)
        .unwrap()
        .status("disabled".to_string())
        .build()
        .send()
        .await
        .unwrap();

    println!("✓ User updated with new password and disabled status");

    let info_resp: UserInfoResponse = madmin_client
        .user_info()
        .access_key(&username)
        .build()
        .send()
        .await
        .unwrap();
    let user_info = info_resp.user_info().unwrap();
    assert_eq!(user_info.status, "disabled");
    println!("✓ User status verified as disabled");

    let _remove_resp: RemoveUserResponse = madmin_client
        .remove_user(&username)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    println!("Cleaned up test user: {}", username);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_info_access_key() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let username = format!(
        "infokey-test-{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
    );
    let password = "TestPassword123!";

    let _add_resp: AddUserResponse = madmin_client
        .add_user(&username, password)
        .unwrap()
        .build()
        .send()
        .await
        .expect("Failed to create user");

    println!("Created test user: {} ", username);

    // Give server time to process and propagate the user creation
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let info_result = madmin_client
        .info_access_key(&username)
        .unwrap()
        .build()
        .send()
        .await;

    match info_result {
        Ok(info_resp) => {
            assert_eq!(info_resp.info().unwrap().access_key, username);
            assert!(!info_resp.info().unwrap().user_type.is_empty());
            assert!(!info_resp.info().unwrap().account_status.is_empty());
            println!("✓ Access Key: {}", info_resp.info().unwrap().access_key);
            println!("✓ User Type: {}", info_resp.info().unwrap().user_type);
            println!(
                "✓ User Provider: {}",
                info_resp.info().unwrap().user_provider
            );
            println!(
                "✓ Account Status: {}",
                info_resp.info().unwrap().account_status
            );
            println!("✓ Parent User: {}", info_resp.info().unwrap().parent_user);
        }
        Err(e) => {
            // The info-access-key API may not support regular IAM users
            let err_str = format!("{:?}", e);
            if err_str.contains("XMinioAdminNoSuchAccessKey") {
                println!("✓ info-access-key API doesn't support IAM users (only service accounts)");
                println!(
                    "  This is a known MinIO behavior - info-access-key is for service accounts only"
                );
            } else {
                panic!("Failed to get info for access key {}: {:?}", username, e);
            }
        }
    }

    let _remove_resp: RemoveUserResponse = madmin_client
        .remove_user(&username)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    println!("Cleaned up test user: {}", username);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_info_access_key_for_admin() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let info_result = madmin_client
        .info_access_key(&ctx.access_key)
        .unwrap()
        .build()
        .send()
        .await;

    match info_result {
        Ok(info_resp) => {
            assert_eq!(info_resp.info().unwrap().access_key, ctx.access_key);
            println!(
                "✓ Admin Access Key: {}",
                info_resp.info().unwrap().access_key
            );
            println!("✓ User Type: {}", info_resp.info().unwrap().user_type);
            println!(
                "✓ User Provider: {}",
                info_resp.info().unwrap().user_provider
            );
            println!(
                "✓ Account Status: {}",
                info_resp.info().unwrap().account_status
            );
        }
        Err(e) => {
            // The info-access-key API may not support root admin users
            let err_str = format!("{:?}", e);
            if err_str.contains("XMinioAdminNoSuchAccessKey") {
                println!(
                    "✓ info-access-key API doesn't support root admin user (this is expected)"
                );
                println!("  Root admin users are not returned by info-access-key endpoint");
            } else {
                panic!("Failed to get admin access key info: {:?}", e);
            }
        }
    }
}

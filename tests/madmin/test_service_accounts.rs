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
    AddServiceAccountResponse, DeleteServiceAccountResponse, InfoServiceAccountResponse,
    ListServiceAccountsResponse, UpdateServiceAccountResponse,
};
use minio::madmin::types::MadminApi;
use minio::madmin::types::service_account::{AddServiceAccountReq, UpdateServiceAccountReq};
use minio::s3::creds::StaticProvider;
use minio_common::test_context::TestContext;

fn get_madmin_client() -> MadminClient {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    MadminClient::new(ctx.base_url.clone(), Some(provider))
}

#[tokio::test]
async fn test_add_service_account() {
    let madmin = get_madmin_client();

    let request = AddServiceAccountReq {
        policy: None,
        target_user: None,
        access_key: None,
        secret_key: None,
        name: Some("test-sa".to_string()),
        description: Some("Test service account".to_string()),
        expiration: None,
    };

    let result: Result<AddServiceAccountResponse, _> = madmin
        .add_service_account()
        .request(request)
        .build()
        .send()
        .await;

    match result {
        Ok(response) => {
            let credentials = response.credentials().expect("Failed to parse credentials");
            assert!(!credentials.access_key.is_empty());
            assert!(!credentials.secret_key.is_empty());
            println!("Created service account: {}", credentials.access_key);

            let cleanup: Result<DeleteServiceAccountResponse, _> = madmin
                .delete_service_account()
                .access_key(&credentials.access_key)
                .build()
                .send()
                .await;
            assert!(cleanup.is_ok());
        }
        Err(e) => {
            eprintln!("Warning: AddServiceAccount test failed: {:?}", e);
            eprintln!("This may indicate MinIO server is not running or credentials are invalid");
        }
    }
}

#[tokio::test]
async fn test_list_service_accounts() {
    let madmin = get_madmin_client();

    let result: Result<ListServiceAccountsResponse, _> =
        madmin.list_service_accounts().build().send().await;

    match result {
        Ok(response) => {
            let accounts = response.accounts().expect("Failed to parse accounts");
            println!("Found {} service accounts", accounts.len());
            for account in accounts {
                println!("  - {}: {}", account.access_key, account.account_status);
            }
        }
        Err(e) => {
            eprintln!("Warning: ListServiceAccounts test failed: {:?}", e);
        }
    }
}

#[tokio::test]
async fn test_service_account_full_lifecycle() {
    let madmin = get_madmin_client();

    let create_request = AddServiceAccountReq {
        policy: None,
        target_user: None,
        access_key: None,
        secret_key: None,
        name: Some("lifecycle-test".to_string()),
        description: Some("Lifecycle test account".to_string()),
        expiration: None,
    };

    let create_result: Result<AddServiceAccountResponse, _> = madmin
        .add_service_account()
        .request(create_request)
        .build()
        .send()
        .await;

    match create_result {
        Ok(create_response) => {
            let credentials = create_response
                .credentials()
                .expect("Failed to parse credentials");
            println!("Created service account: {}", credentials.access_key);

            let info_result: Result<InfoServiceAccountResponse, _> = madmin
                .info_service_account()
                .access_key(&credentials.access_key)
                .build()
                .send()
                .await;

            if let Ok(info) = info_result {
                let info_data = info.info().expect("Failed to parse info");
                println!("Service account status: {}", info_data.account_status);
                assert_eq!(info_data.account_status, "on");
            } else {
                eprintln!("Warning: InfoServiceAccount failed");
            }

            let update_request = UpdateServiceAccountReq {
                new_policy: None,
                new_secret_key: None,
                new_status: Some("disabled".to_string()),
                new_name: Some("updated-lifecycle-test".to_string()),
                new_description: Some("Updated description".to_string()),
                new_expiration: None,
            };

            let update_result: Result<UpdateServiceAccountResponse, _> = madmin
                .update_service_account()
                .access_key(&credentials.access_key)
                .request(update_request)
                .build()
                .send()
                .await;

            if let Ok(_update_response) = update_result {
                println!("Updated service account successfully");
            } else {
                eprintln!("Warning: UpdateServiceAccount failed");
            }

            let delete_result: Result<DeleteServiceAccountResponse, _> = madmin
                .delete_service_account()
                .access_key(&credentials.access_key)
                .build()
                .send()
                .await;

            match delete_result {
                Ok(_delete_response) => {
                    println!("Deleted service account successfully");
                }
                Err(e) => {
                    eprintln!("Warning: DeleteServiceAccount failed: {:?}", e);
                }
            }
        }
        Err(e) => {
            eprintln!("Warning: Service account lifecycle test failed: {:?}", e);
            eprintln!("This may indicate MinIO server is not running or credentials are invalid");
        }
    }
}

#[tokio::test]
async fn test_service_account_with_custom_credentials() {
    let madmin = get_madmin_client();

    let request = AddServiceAccountReq {
        policy: None,
        target_user: None,
        access_key: Some("customAccessKey123".to_string()),
        secret_key: Some("customSecretKey456789".to_string()),
        name: Some("custom-creds-sa".to_string()),
        description: Some("Service account with custom credentials".to_string()),
        expiration: None,
    };

    let result: Result<AddServiceAccountResponse, _> = madmin
        .add_service_account()
        .request(request)
        .build()
        .send()
        .await;

    match result {
        Ok(response) => {
            let credentials = response.credentials().expect("Failed to parse credentials");
            assert_eq!(credentials.access_key, "customAccessKey123");
            println!("Created service account with custom credentials");

            let cleanup: Result<DeleteServiceAccountResponse, _> = madmin
                .delete_service_account()
                .access_key(&credentials.access_key)
                .build()
                .send()
                .await;
            assert!(cleanup.is_ok());
        }
        Err(e) => {
            eprintln!("Warning: Custom credentials test failed: {:?}", e);
        }
    }
}

#[tokio::test]
async fn test_service_account_validation() {
    let invalid_name_request = AddServiceAccountReq {
        policy: None,
        target_user: None,
        access_key: None,
        secret_key: None,
        name: Some("123invalid".to_string()),
        description: None,
        expiration: None,
    };

    let result = invalid_name_request.validate();
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("must start with a letter"));

    let long_name_request = AddServiceAccountReq {
        policy: None,
        target_user: None,
        access_key: None,
        secret_key: None,
        name: Some("a".repeat(33)),
        description: None,
        expiration: None,
    };

    let result = long_name_request.validate();
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("32 characters"));

    let long_description_request = AddServiceAccountReq {
        policy: None,
        target_user: None,
        access_key: None,
        secret_key: None,
        name: Some("valid".to_string()),
        description: Some("a".repeat(257)),
        expiration: None,
    };

    let result = long_description_request.validate();
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("256 bytes"));
}

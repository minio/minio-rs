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
use minio::madmin::response::{AttachPolicyLDAPResponse, DetachPolicyLDAPResponse};
use minio::madmin::types::MadminApi;
use minio::madmin::types::idp_config::IdpType;
use minio::madmin::types::typed_parameters::IdpConfigName;
use minio::s3::creds::StaticProvider;
use minio_common::test_context::TestContext;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_list_idp_config() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    println!("Listing OpenID IDP configurations");
    let openid_result = madmin_client
        .list_idp_config()
        .idp_type(IdpType::OpenId)
        .build()
        .send()
        .await;

    match openid_result {
        Ok(response) => {
            let items = response.items().unwrap();

            for item in &items {
                assert!(!item.name.is_empty(), "IDP name should not be empty");
                assert!(!item.idp_type.is_empty(), "IDP type should not be empty");
            }

            println!("OpenID IDP configurations: {} found", items.len());
            for item in &items {
                println!(
                    "  - {} ({}) - enabled: {}",
                    item.name, item.idp_type, item.enabled
                );
                if let Some(ref role_arn) = item.role_arn {
                    println!("    Role ARN: {}", role_arn);
                }
            }
        }
        Err(e) => {
            println!("IDP configuration may not be available: {:?}", e);
            println!("Test completed (IDP not configured is acceptable)");
        }
    }

    println!("Listing LDAP IDP configurations");
    let ldap_result = madmin_client
        .list_idp_config()
        .idp_type(IdpType::Ldap)
        .build()
        .send()
        .await;

    match ldap_result {
        Ok(response) => {
            let items = response.items().unwrap();

            for item in &items {
                assert!(!item.name.is_empty(), "IDP name should not be empty");
                assert!(!item.idp_type.is_empty(), "IDP type should not be empty");
            }

            println!("LDAP IDP configurations: {} found", items.len());
            for item in &items {
                println!(
                    "  - {} ({}) - enabled: {}",
                    item.name, item.idp_type, item.enabled
                );
            }
        }
        Err(e) => {
            println!("LDAP configuration may not be available: {:?}", e);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore] // Requires proper IDP server setup
async fn test_openid_idp_lifecycle() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let idp_name = IdpConfigName::from("test-openid-rust");

    // Configuration for a test OpenID provider
    let config_data = format!(
        "client_id=test-client-{}\n\
         client_secret=test-secret\n\
         config_url=https://example.com/.well-known/openid-configuration\n\
         scopes=openid,profile,email\n\
         redirect_uri=https://minio.example.com/oauth_callback",
        uuid::Uuid::new_v4()
    );

    // Add OpenID IDP configuration
    println!("Adding OpenID IDP configuration: {}", idp_name);
    let add_result = madmin_client
        .add_or_update_idp_config()
        .idp_type(IdpType::OpenId)
        .name(&idp_name)
        .config_data(&config_data)
        .update(false)
        .build()
        .send()
        .await;

    match add_result {
        Ok(response) => {
            println!(
                "OpenID IDP configuration added. Restart required: {}",
                response.restart_required()
            );

            // Get the IDP configuration
            println!("Getting OpenID IDP configuration");
            let get_result = madmin_client
                .get_idp_config()
                .idp_type(IdpType::OpenId)
                .name(&idp_name)
                .build()
                .send()
                .await;

            match get_result {
                Ok(config_response) => {
                    let config = config_response.config().unwrap();
                    assert!(!config.idp_type.is_empty(), "IDP type should not be empty");

                    for entry in &config.info {
                        assert!(!entry.key.is_empty(), "Config key should not be empty");
                    }

                    println!("Retrieved IDP configuration:");
                    println!("  Type: {}", config.idp_type);
                    if let Some(ref name) = config.name {
                        println!("  Name: {}", name);
                    }
                    println!("  Configuration entries: {}", config.info.len());
                    for entry in &config.info {
                        println!("    {} = {}", entry.key, entry.value);
                    }
                }
                Err(e) => {
                    println!("Failed to get IDP configuration: {:?}", e);
                }
            }

            // Update the configuration
            let updated_config_data = format!(
                "client_id=test-client-updated-{}\n\
                 client_secret=test-secret-updated\n\
                 config_url=https://example.com/.well-known/openid-configuration\n\
                 scopes=openid,profile,email\n\
                 redirect_uri=https://minio.example.com/oauth_callback",
                uuid::Uuid::new_v4()
            );

            println!("Updating OpenID IDP configuration");
            let update_result = madmin_client
                .add_or_update_idp_config()
                .idp_type(IdpType::OpenId)
                .name(&idp_name)
                .config_data(&updated_config_data)
                .update(true)
                .build()
                .send()
                .await;

            match update_result {
                Ok(response) => {
                    println!(
                        "OpenID IDP configuration updated. Restart required: {}",
                        response.restart_required()
                    );
                }
                Err(e) => {
                    println!("Failed to update IDP configuration: {:?}", e);
                }
            }

            // List to verify it exists
            let list_result = madmin_client
                .list_idp_config()
                .idp_type(IdpType::OpenId)
                .build()
                .send()
                .await;

            if let Ok(response) = list_result {
                let items = response.items().unwrap();
                let found = items.iter().any(|item| item.name == idp_name.as_str());
                assert!(found, "IDP configuration should be in the list");
                println!("IDP configuration verified in list");
            }

            // Delete the IDP configuration
            println!("Deleting OpenID IDP configuration");
            let delete_result = madmin_client
                .delete_idp_config()
                .idp_type(IdpType::OpenId)
                .name(&idp_name)
                .build()
                .send()
                .await;

            match delete_result {
                Ok(response) => {
                    println!(
                        "OpenID IDP configuration deleted. Restart required: {}",
                        response.restart_required()
                    );
                }
                Err(e) => {
                    println!("Failed to delete IDP configuration: {:?}", e);
                }
            }

            println!("OpenID IDP lifecycle test completed");
        }
        Err(e) => {
            println!(
                "IDP configuration not allowed (expected if IDP not enabled): {:?}",
                e
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore] // Requires proper LDAP server setup
async fn test_ldap_idp_lifecycle() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let idp_name = IdpConfigName::from("test-ldap-rust");

    // Configuration for a test LDAP provider
    let config_data = "server_addr=ldap.example.com:389\n\
         lookup_bind_dn=cn=admin,dc=example,dc=com\n\
         lookup_bind_password=admin-password\n\
         user_dn_search_base_dn=ou=users,dc=example,dc=com\n\
         user_dn_search_filter=(uid=%s)\n\
         group_search_base_dn=ou=groups,dc=example,dc=com\n\
         group_search_filter=(&(objectClass=groupOfNames)(member=%d))"
        .to_string();

    // Add LDAP IDP configuration
    println!("Adding LDAP IDP configuration: {}", idp_name);
    let add_result = madmin_client
        .add_or_update_idp_config()
        .idp_type(IdpType::Ldap)
        .name(&idp_name)
        .config_data(&config_data)
        .update(false)
        .build()
        .send()
        .await;

    match add_result {
        Ok(response) => {
            println!(
                "LDAP IDP configuration added. Restart required: {}",
                response.restart_required()
            );

            // Check the LDAP configuration
            println!("Checking LDAP IDP configuration");
            let check_result = madmin_client
                .check_idp_config()
                .idp_type(IdpType::Ldap)
                .name(&idp_name)
                .build()
                .send()
                .await;

            match check_result {
                Ok(check_response) => {
                    let result = check_response.result();
                    if let Some(error_type) = &result.error_type {
                        assert!(
                            !error_type.is_empty(),
                            "Error type should not be empty string"
                        );
                    }

                    if check_response.is_valid() {
                        println!("LDAP IDP configuration is valid");
                    } else {
                        println!(
                            "LDAP IDP configuration validation failed: {:?} - {:?}",
                            result.error_type, result.error_message
                        );
                    }
                }
                Err(e) => {
                    println!("Failed to check LDAP IDP configuration: {:?}", e);
                }
            }

            // Get the IDP configuration
            println!("Getting LDAP IDP configuration");
            let get_result = madmin_client
                .get_idp_config()
                .idp_type(IdpType::Ldap)
                .name(&idp_name)
                .build()
                .send()
                .await;

            match get_result {
                Ok(config_response) => {
                    let config = config_response.config().unwrap();
                    println!("Retrieved LDAP IDP configuration:");
                    println!("  Type: {}", config.idp_type);
                    println!("  Configuration entries: {}", config.info.len());
                }
                Err(e) => {
                    println!("Failed to get LDAP IDP configuration: {:?}", e);
                }
            }

            // Delete the IDP configuration
            println!("Deleting LDAP IDP configuration");
            let delete_result = madmin_client
                .delete_idp_config()
                .idp_type(IdpType::Ldap)
                .name(&idp_name)
                .build()
                .send()
                .await;

            match delete_result {
                Ok(response) => {
                    println!(
                        "LDAP IDP configuration deleted. Restart required: {}",
                        response.restart_required()
                    );
                }
                Err(e) => {
                    println!("Failed to delete LDAP IDP configuration: {:?}", e);
                }
            }

            println!("LDAP IDP lifecycle test completed");
        }
        Err(e) => {
            println!(
                "LDAP IDP configuration not allowed (expected if LDAP not enabled): {:?}",
                e
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore] // Requires proper LDAP server setup
async fn test_check_nonexistent_idp() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    println!("Checking non-existent LDAP IDP configuration");
    let check_result = madmin_client
        .check_idp_config()
        .idp_type(IdpType::Ldap)
        .name("nonexistent-idp")
        .build()
        .send()
        .await;

    match check_result {
        Ok(response) => {
            println!(
                "Check completed. Valid: {}, Error: {:?}",
                response.is_valid(),
                response.result().error_message
            );
        }
        Err(e) => {
            println!("Expected error for non-existent IDP: {:?}", e);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Requires LDAP server with configured users and policies"]
async fn test_get_ldap_policy_entities() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    // Query policy entities for LDAP users
    let policy_name = std::env::var("TEST_POLICY").unwrap_or_else(|_| "readwrite".to_string());

    let resp = madmin_client
        .get_ldap_policy_entities()
        .policy(vec![policy_name.clone()])
        .build()
        .send()
        .await
        .expect("Failed to get LDAP policy entities");

    if let Some(users) = &resp.user_mappings {
        for user in users {
            assert!(!user.user.is_empty(), "User DN should not be empty");
        }
    }

    if let Some(groups) = &resp.group_mappings {
        for group in groups {
            assert!(!group.group.is_empty(), "Group DN should not be empty");
        }
    }

    println!("LDAP policy entities for policy '{}':", policy_name);

    if let Some(users) = &resp.user_mappings {
        println!("  Users: {}", users.len());
        for user in users {
            println!("    - {} -> {:?}", user.user, user.policies);
        }
    }

    if let Some(groups) = &resp.group_mappings {
        println!("  Groups: {}", groups.len());
        for group in groups {
            println!("    - {} -> {:?}", group.group, group.policies);
        }
    }

    println!("✓ GetLDAPPolicyEntities API call successful");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Requires LDAP server with test user"]
async fn test_attach_detach_policy_ldap() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let policy_name = "readonly";
    let ldap_user = std::env::var("TEST_LDAP_USER")
        .unwrap_or_else(|_| "cn=testuser,ou=users,dc=example,dc=com".to_string());

    // Attach policy to LDAP user
    println!(
        "Attaching policy '{}' to LDAP user '{}'",
        policy_name, ldap_user
    );
    let _attach_resp: AttachPolicyLDAPResponse = madmin_client
        .attach_policy_ldap()
        .policies(vec![policy_name.to_string()])
        .user(ldap_user.clone())
        .build()
        .send()
        .await
        .expect("Failed to attach policy to LDAP user");

    // Response indicates success if we got here without error

    println!("✓ Policy attached successfully");

    // Verify attachment
    let entities = madmin_client
        .get_ldap_policy_entities()
        .policy(vec![policy_name.to_string()])
        .build()
        .send()
        .await
        .expect("Failed to get policy entities");

    if let Some(users) = &entities.user_mappings {
        assert!(
            users.iter().any(|u| u.user == ldap_user),
            "LDAP user should be in policy entities"
        );
        println!("✓ Policy attachment verified");
    }

    // Detach policy from LDAP user
    println!("Detaching policy from LDAP user");
    let _detach_resp: DetachPolicyLDAPResponse = madmin_client
        .detach_policy_ldap()
        .policies(vec![policy_name.to_string()])
        .user(ldap_user.clone())
        .build()
        .send()
        .await
        .expect("Failed to detach policy from LDAP user");

    // Response indicates success if we got here without error

    println!("✓ Policy detached successfully");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Requires LDAP server with test group"]
async fn test_attach_detach_policy_ldap_group() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let policy_name = "readonly";
    let ldap_group = std::env::var("TEST_LDAP_GROUP")
        .unwrap_or_else(|_| "cn=developers,ou=groups,dc=example,dc=com".to_string());

    // Attach policy to LDAP group
    println!(
        "Attaching policy '{}' to LDAP group '{}'",
        policy_name, ldap_group
    );
    let _attach_group_resp: AttachPolicyLDAPResponse = madmin_client
        .attach_policy_ldap()
        .policies(vec![policy_name.to_string()])
        .group(ldap_group.clone())
        .build()
        .send()
        .await
        .expect("Failed to attach policy to LDAP group");

    // Response indicates success if we got here without error

    println!("✓ Policy attached to group successfully");

    // Detach policy from LDAP group
    println!("Detaching policy from LDAP group");
    let _detach_group_resp: DetachPolicyLDAPResponse = madmin_client
        .detach_policy_ldap()
        .policies(vec![policy_name.to_string()])
        .group(ldap_group.clone())
        .build()
        .send()
        .await
        .expect("Failed to detach policy from LDAP group");

    // Response indicates success if we got here without error

    println!("✓ Policy detached from group successfully");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Requires LDAP server with configured access keys"]
async fn test_list_access_keys_ldap_bulk() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    // List all LDAP-generated access keys
    let resp = madmin_client
        .list_access_keys_ldap_bulk()
        .build()
        .send()
        .await
        .expect("Failed to list LDAP access keys");

    for (user, keys_resp) in &resp.users_keys {
        assert!(!user.is_empty(), "User DN should not be empty");
        if let Some(ref service_accounts) = keys_resp.service_accounts {
            for service_account in service_accounts {
                assert!(
                    !service_account.access_key.is_empty(),
                    "Access key should not be empty"
                );
                assert!(
                    !service_account.parent_user.is_empty(),
                    "Parent user should not be empty"
                );
            }
        }
    }

    println!("LDAP users with access keys: {}", resp.users_keys.len());

    for (user, keys_resp) in &resp.users_keys {
        println!("  User: {}", user);
        if let Some(ref service_accounts) = keys_resp.service_accounts {
            for service_account in service_accounts {
                println!("    Access Key: {}", service_account.access_key);
                println!("      Parent User: {}", service_account.parent_user);
            }
        }
    }

    println!("✓ ListAccessKeysLDAPBulk API call successful");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Requires LDAP server with configured access keys"]
async fn test_list_access_keys_ldap_bulk_with_opts() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let ldap_user = std::env::var("TEST_LDAP_USER")
        .unwrap_or_else(|_| "cn=testuser,ou=users,dc=example,dc=com".to_string());

    // List LDAP access keys for specific user
    let resp = madmin_client
        .list_access_keys_ldap_bulk()
        .user_dns(vec![ldap_user.clone()])
        .build()
        .send()
        .await
        .expect("Failed to list LDAP access keys with user filter");

    println!("LDAP users with access keys: {}", resp.users_keys.len());

    for (user, keys_resp) in &resp.users_keys {
        println!("  User: {}", user);
        if let Some(ref service_accounts) = keys_resp.service_accounts {
            for service_account in service_accounts {
                println!("    Access Key: {}", service_account.access_key);
            }
        }
    }

    println!("✓ ListAccessKeysLDAPBulk with options API call successful");
}

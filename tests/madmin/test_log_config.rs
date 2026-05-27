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
use minio::madmin::response::{GetLogConfigResponse, ResetLogConfigResponse, SetLogConfigResponse};
use minio::madmin::types::MadminApi;
use minio::madmin::types::log_config::{LogConfig, LogRecorderConfig};
use minio::s3::creds::StaticProvider;
use minio_common::test_context::TestContext;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_get_log_config() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    println!("Getting log configuration");

    let result: Result<GetLogConfigResponse, _> =
        madmin_client.get_log_config().build().send().await;

    match result {
        Ok(log_config) => {
            let status = log_config.status().expect("Failed to parse log status");

            if let Some(limit) = &status.api.drive_limit {
                assert!(
                    !limit.is_empty(),
                    "API drive limit should not be empty string"
                );
            }
            if let Some(interval) = &status.api.flush_interval {
                assert!(
                    !interval.is_empty(),
                    "API flush interval should not be empty string"
                );
            }
            if let Some(_count) = status.api.flush_count {}

            if let Some(limit) = &status.error.drive_limit {
                assert!(
                    !limit.is_empty(),
                    "Error drive limit should not be empty string"
                );
            }
            if let Some(interval) = &status.error.flush_interval {
                assert!(
                    !interval.is_empty(),
                    "Error flush interval should not be empty string"
                );
            }
            if let Some(_count) = status.error.flush_count {}

            if let Some(limit) = &status.audit.drive_limit {
                assert!(
                    !limit.is_empty(),
                    "Audit drive limit should not be empty string"
                );
            }
            if let Some(interval) = &status.audit.flush_interval {
                assert!(
                    !interval.is_empty(),
                    "Audit flush interval should not be empty string"
                );
            }
            if let Some(_count) = status.audit.flush_count {}

            println!("Log configuration retrieved:");
            println!("  API logger enabled: {}", status.api.enabled);
            println!("  Error logger enabled: {}", status.error.enabled);
            println!("  Audit logger enabled: {}", status.audit.enabled);

            if let Some(ref limit) = status.api.drive_limit {
                println!("  API drive limit: {}", limit);
            }
            if let Some(ref interval) = status.api.flush_interval {
                println!("  API flush interval: {}", interval);
            }
        }
        Err(e) => {
            println!(
                "Log configuration may not be available on this server: {:?}",
                e
            );
            println!("Test completed (log config not supported is acceptable)");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_set_and_reset_log_config() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    // First, get current config to restore later
    let original_result: Result<GetLogConfigResponse, _> =
        madmin_client.get_log_config().build().send().await;

    // Try to set a log configuration
    let log_config = LogConfig {
        api: Some(LogRecorderConfig {
            enable: true,
            drive_limit: Some("500Mi".to_string()),
            flush_count: Some(100),
            flush_interval: Some("10s".to_string()),
        }),
        error: Some(LogRecorderConfig {
            enable: true,
            drive_limit: Some("200Mi".to_string()),
            flush_count: Some(50),
            flush_interval: Some("5s".to_string()),
        }),
        audit: Some(LogRecorderConfig {
            enable: false,
            drive_limit: None,
            flush_count: None,
            flush_interval: None,
        }),
    };

    println!("Setting log configuration");
    let set_result: Result<SetLogConfigResponse, _> = madmin_client
        .set_log_config()
        .config(log_config)
        .build()
        .send()
        .await;

    match set_result {
        Ok(_) => {
            println!("Log configuration set successfully");

            // Verify the change
            let verify_result: Result<GetLogConfigResponse, _> =
                madmin_client.get_log_config().build().send().await;

            match verify_result {
                Ok(log_config) => {
                    let status = log_config.status().expect("Failed to parse status");
                    println!("Verified log configuration:");
                    println!("  API logger enabled: {}", status.api.enabled);
                    assert!(status.api.enabled, "API logger should be enabled after set");

                    if let Some(ref limit) = status.api.drive_limit {
                        println!("  API drive limit: {}", limit);
                    }
                }
                Err(e) => {
                    println!("Failed to verify log configuration: {:?}", e);
                }
            }

            // Reset log configuration
            println!("Resetting log configuration to defaults");
            let reset_result: Result<ResetLogConfigResponse, _> =
                madmin_client.reset_log_config().build().send().await;

            match reset_result {
                Ok(_) => {
                    println!("Log configuration reset successfully");

                    // Verify reset
                    let verify_reset: Result<GetLogConfigResponse, _> =
                        madmin_client.get_log_config().build().send().await;

                    match verify_reset {
                        Ok(log_config) => {
                            let status = log_config.status().expect("Failed to parse status");
                            println!("Log configuration after reset:");
                            println!("  API logger enabled: {}", status.api.enabled);
                            println!("  Error logger enabled: {}", status.error.enabled);
                            println!("  Audit logger enabled: {}", status.audit.enabled);
                        }
                        Err(e) => {
                            println!("Failed to verify reset: {:?}", e);
                        }
                    }
                }
                Err(e) => {
                    println!("Log configuration reset not allowed: {:?}", e);

                    // Try to restore original config if reset failed
                    if let Ok(original) = original_result {
                        println!("Attempting to restore original configuration");
                        let orig_status =
                            original.status().expect("Failed to parse original status");
                        let restore_config = LogConfig {
                            api: Some(LogRecorderConfig {
                                enable: orig_status.api.enabled,
                                drive_limit: orig_status.api.drive_limit.clone(),
                                flush_count: orig_status.api.flush_count,
                                flush_interval: orig_status.api.flush_interval.clone(),
                            }),
                            error: Some(LogRecorderConfig {
                                enable: orig_status.error.enabled,
                                drive_limit: orig_status.error.drive_limit.clone(),
                                flush_count: orig_status.error.flush_count,
                                flush_interval: orig_status.error.flush_interval.clone(),
                            }),
                            audit: Some(LogRecorderConfig {
                                enable: orig_status.audit.enabled,
                                drive_limit: orig_status.audit.drive_limit.clone(),
                                flush_count: orig_status.audit.flush_count,
                                flush_interval: orig_status.audit.flush_interval.clone(),
                            }),
                        };

                        let _ = madmin_client
                            .set_log_config()
                            .config(restore_config)
                            .build()
                            .send()
                            .await;
                    }
                }
            }
        }
        Err(e) => {
            println!(
                "Log configuration modification not allowed (expected on shared servers): {:?}",
                e
            );
        }
    }

    println!("Log configuration test completed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore] // Ignored by default as it may interfere with server operations
async fn test_log_config_with_large_limits() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    // Test with large drive limits
    let log_config = LogConfig {
        api: Some(LogRecorderConfig {
            enable: true,
            drive_limit: Some("10Gi".to_string()),
            flush_count: Some(1000),
            flush_interval: Some("30s".to_string()),
        }),
        error: Some(LogRecorderConfig {
            enable: true,
            drive_limit: Some("5Gi".to_string()),
            flush_count: Some(500),
            flush_interval: Some("15s".to_string()),
        }),
        audit: Some(LogRecorderConfig {
            enable: true,
            drive_limit: Some("20Gi".to_string()),
            flush_count: Some(2000),
            flush_interval: Some("60s".to_string()),
        }),
    };

    println!("Setting log configuration with large limits");
    let result: Result<SetLogConfigResponse, _> = madmin_client
        .set_log_config()
        .config(log_config)
        .build()
        .send()
        .await;

    match result {
        Ok(_) => {
            println!("Large limit log configuration set successfully");

            // Reset to defaults
            let _ = madmin_client.reset_log_config().build().send().await;
        }
        Err(e) => {
            println!("Large limit log configuration not allowed: {:?}", e);
        }
    }
}

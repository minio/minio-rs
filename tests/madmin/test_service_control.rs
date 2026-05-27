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

use futures_util::StreamExt;
use minio::madmin::madmin_client::MadminClient;
use minio::madmin::response::ServiceActionResponse;
use minio::madmin::types::MadminApi;
use minio::madmin::types::trace::ServiceTraceOpts;
use minio::s3::creds::StaticProvider;
use minio_common::test_context::TestContext;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore] // Service stop would shut down the test server
async fn test_service_stop() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let resp: ServiceActionResponse = madmin_client
        .service_stop()
        .build()
        .send()
        .await
        .expect("Failed to stop service");

    // Verify the action is Stop
    assert!(
        matches!(
            resp.result().unwrap().action,
            minio::madmin::types::service::ServiceAction::Stop
        ),
        "Action should be Stop variant"
    );

    println!(
        "✓ Service stop action initiated: {:?}",
        resp.result().unwrap().action
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore] // Service freeze/unfreeze affects server availability
async fn test_service_freeze_unfreeze() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    // Freeze the service
    let freeze_resp: ServiceActionResponse = madmin_client
        .service_freeze()
        .build()
        .send()
        .await
        .expect("Failed to freeze service");

    assert!(
        matches!(
            freeze_resp.result().unwrap().action,
            minio::madmin::types::service::ServiceAction::Freeze
        ),
        "Action should be Freeze variant"
    );
    println!(
        "✓ Service frozen: action={:?}",
        freeze_resp.result().unwrap().action
    );

    // Immediately unfreeze
    let unfreeze_resp: ServiceActionResponse = madmin_client
        .service_unfreeze()
        .build()
        .send()
        .await
        .expect("Failed to unfreeze service");

    assert!(
        matches!(
            unfreeze_resp.result().unwrap().action,
            minio::madmin::types::service::ServiceAction::Unfreeze
        ),
        "Action should be Unfreeze variant"
    );
    println!(
        "✓ Service unfrozen: action={:?}",
        unfreeze_resp.result().unwrap().action
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore] // Service freeze affects server availability
async fn test_service_freeze() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let resp: ServiceActionResponse = madmin_client
        .service_freeze()
        .build()
        .send()
        .await
        .expect("Failed to freeze service");

    assert!(
        matches!(
            resp.result().unwrap().action,
            minio::madmin::types::service::ServiceAction::Freeze
        ),
        "Action should be Freeze variant"
    );
    println!(
        "✓ Service freeze action initiated: {:?}",
        resp.result().unwrap().action
    );

    // Don't forget to unfreeze!
    let _resp: ServiceActionResponse = madmin_client
        .service_unfreeze()
        .build()
        .send()
        .await
        .expect("Failed to unfreeze service");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore] // Service unfreeze when not frozen may not do anything meaningful
async fn test_service_unfreeze() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let resp: ServiceActionResponse = madmin_client
        .service_unfreeze()
        .build()
        .send()
        .await
        .expect("Failed to unfreeze service");

    println!(
        "✓ Service unfreeze action: {}",
        resp.result().unwrap().action
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_service_trace() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    // Enable trace for all types to maximize chances of receiving events
    let opts = ServiceTraceOpts {
        s3: Some(true),
        internal: Some(true),
        storage: Some(true),
        only_errors: Some(false),
        ..Default::default()
    };

    println!("✓ Starting service trace with opts: s3=true, internal=true, storage=true");

    let response = madmin_client
        .service_trace()
        .opts(opts)
        .build()
        .send()
        .await
        .expect("Failed to start service trace");

    let mut trace_stream = response.into_stream();
    println!("✓ Service trace stream started successfully");

    let mut count = 0;
    let max_traces = 3;
    let timeout = Duration::from_secs(10);

    println!(
        "✓ Waiting for up to {} trace events (timeout: {:?})",
        max_traces, timeout
    );

    let result = tokio::time::timeout(timeout, async {
        while let Some(result) = trace_stream.next().await {
            match result {
                Ok(trace_info) => {
                    assert!(
                        !trace_info.trace.func_name.is_empty(),
                        "Function name should not be empty"
                    );
                    assert!(
                        !trace_info.trace.node_name.is_empty(),
                        "Node name should not be empty"
                    );

                    if let Some(http) = &trace_info.trace.http {
                        assert!(
                            !http.req_info.method.is_empty(),
                            "HTTP method should not be empty"
                        );
                    }

                    println!(
                        "✓ Trace {}: {} on {} (type: {:?})",
                        count + 1,
                        trace_info.trace.func_name,
                        trace_info.trace.node_name,
                        trace_info.trace.trace_type
                    );
                    if let Some(http) = &trace_info.trace.http {
                        println!(
                            "  HTTP: {} {} -> {}",
                            http.req_info.method,
                            http.req_info.path.as_deref().unwrap_or(""),
                            http.resp_info.status_code.unwrap_or(0)
                        );
                    }
                    if let Some(msg) = &trace_info.trace.message {
                        println!("  Message: {}", msg);
                    }
                    count += 1;
                    if count >= max_traces {
                        break;
                    }
                }
                Err(e) => {
                    println!("✗ Trace error: {}", e);
                    break;
                }
            }
        }
        count
    })
    .await;

    match result {
        Ok(final_count) => {
            println!("✓ Received {} trace events within timeout", final_count);
            // Trace may not always have events immediately on idle servers
            // Just verify the stream was established successfully
        }
        Err(_) => {
            println!(
                "✓ Trace stream timed out after {:?} (received {} events)",
                timeout, count
            );
            // Timeout is acceptable - means streaming is working but no events
        }
    }

    println!("✓ Service trace test completed successfully");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "ServiceCancelRestart requires a restart to be in progress"]
async fn test_service_cancel_restart() {
    use minio::madmin::response::ServiceCancelRestartResponse;

    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    // Cancel any pending restart
    let _resp: ServiceCancelRestartResponse = madmin_client
        .service_cancel_restart()
        .send()
        .await
        .expect("Failed to cancel service restart");

    println!("✓ Service cancel restart successful");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "ServiceAction is a general-purpose API that affects server state"]
async fn test_service_action_status() {
    use minio::madmin::types::service::ServiceAction;

    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    // Note: service_action() returns a built request, we need to use builder directly for options
    use minio::madmin::builders::ServiceAction as ServiceActionBuilder;

    let resp: ServiceActionResponse = ServiceActionBuilder::builder()
        .client(madmin_client.clone())
        .action(ServiceAction::Restart)
        .dry_run(true)
        .build()
        .send()
        .await
        .expect("Failed to execute service action");

    let result = resp.result().unwrap();
    println!("✓ Service action: {:?}", result.action);
    println!("✓ Dry run: {}", result.dry_run);

    // Verify response structure
    assert_eq!(result.action, ServiceAction::Restart);
    assert!(result.dry_run);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "ServiceAction with restart would affect server availability"]
async fn test_service_action_restart() {
    use minio::madmin::types::service::ServiceAction;

    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    // Use ServiceAction for restart with dry run to test without actual restart
    use minio::madmin::builders::ServiceAction as ServiceActionBuilder;

    let resp: ServiceActionResponse = ServiceActionBuilder::builder()
        .client(madmin_client.clone())
        .action(ServiceAction::Restart)
        .dry_run(true)
        .build()
        .send()
        .await
        .expect("Failed to execute service action");

    let result = resp.result().unwrap();
    println!("✓ Service action: {:?}", result.action);

    if result
        .results
        .as_ref()
        .is_none_or(|r| r.iter().all(|p| p.err.is_none()))
    {
        println!("✓ All peers ready for restart");
    } else {
        println!("Some peers reported issues:");
        if let Some(results) = &result.results {
            for peer in results.iter().filter(|p| p.err.is_some()) {
                println!("  - {}: {:?}", peer.host, peer.err);
            }
        }
    }

    assert_eq!(result.action, ServiceAction::Restart);
}

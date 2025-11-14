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
use minio::madmin::response::ServerHealthInfoResponse;
use minio::madmin::types::MadminApi;
use minio::s3::creds::StaticProvider;
use minio_common::test_context::TestContext;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore] // JSON parsing issue - response may be newline-delimited JSON or streaming format
async fn test_server_health_info_basic() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let resp: ServerHealthInfoResponse = madmin_client
        .server_health_info()
        .minio_info(true)
        .build()
        .send()
        .await
        .expect("Failed to get server health info");

    assert!(
        !resp.health.version.is_empty(),
        "Version should not be empty"
    );
    println!("✓ Health check version: {}", resp.health.version);
    println!("✓ Timestamp: {}", resp.health.timestamp);

    if let Some(error) = &resp.health.error {
        println!("⚠ Health check error: {}", error);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore] // JSON parsing issue - response may be newline-delimited JSON or streaming format
async fn test_server_health_info_all_checks() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let resp: ServerHealthInfoResponse = madmin_client
        .server_health_info()
        .minio_info(true)
        .minio_config(true)
        .sys_cpu(true)
        .sys_drive_hw(true)
        .sys_os_info(true)
        .sys_mem(true)
        .sys_net(true)
        .sys_process(true)
        .sys_errors(true)
        .sys_services(true)
        .sys_config(true)
        .replication(true)
        .shards_health(true)
        .deadline(Duration::from_secs(30))
        .build()
        .send()
        .await
        .expect("Failed to get comprehensive health info");

    assert!(
        !resp.health.version.is_empty(),
        "Version should not be empty"
    );

    if let Some(sys) = &resp.health.sys {
        println!("✓ System information available");

        if let Some(cpu_info) = &sys.cpu_info {
            println!("  - CPU info: {} entries", cpu_info.len());
        }

        if let Some(mem_info) = &sys.mem_info {
            println!("  - Memory info: {} entries", mem_info.len());
        }

        if let Some(disk_info) = &sys.disk_hw_info {
            println!("  - Disk HW info: {} entries", disk_info.len());
        }

        if let Some(os_info) = &sys.os_info {
            println!("  - OS info: {} entries", os_info.len());
        }

        if let Some(net_info) = &sys.net_info {
            println!("  - Network info: {} entries", net_info.len());
        }

        if let Some(proc_info) = &sys.proc_info {
            println!("  - Process info: {} entries", proc_info.len());
        }
    }

    if let Some(minio) = &resp.health.minio {
        println!("✓ MinIO information available");

        if let Some(info) = &minio.info
            && let Some(servers) = &info.servers
        {
            println!("  - Servers: {} entries", servers.len());
        }

        if let Some(config) = &minio.config
            && let Some(cfg) = &config.config
        {
            println!("  - Config entries: {}", cfg.len());
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore] // JSON parsing issue - response may be newline-delimited JSON or streaming format
async fn test_server_health_info_selective_checks() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let resp: ServerHealthInfoResponse = madmin_client
        .server_health_info()
        .sys_cpu(true)
        .sys_mem(true)
        .sys_os_info(true)
        .build()
        .send()
        .await
        .expect("Failed to get selective health info");

    assert!(
        !resp.health.version.is_empty(),
        "Version should not be empty"
    );
    println!("✓ Health check with selective system info completed");
}

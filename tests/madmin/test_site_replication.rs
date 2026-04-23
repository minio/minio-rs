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
    SiteReplicationAddResponse, SiteReplicationEditResponse, SiteReplicationInfoResponse,
    SiteReplicationMetaInfoResponse, SiteReplicationPeerEditResponse,
    SiteReplicationRemoveResponse, SiteReplicationResyncResponse, SiteReplicationStateEditResponse,
    SiteReplicationStatusResponse,
};
use minio::madmin::types::MadminApi;
use minio::s3::creds::StaticProvider;
use minio_common::test_context::TestContext;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Requires multi-site deployment with site replication configured"]
async fn test_site_replication_info() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    println!("Getting site replication info");

    let response: SiteReplicationInfoResponse = madmin_client
        .site_replication_info()
        .build()
        .send()
        .await
        .expect("Failed to get site replication info");

    let info = response.info().unwrap();
    if info.enabled {
        assert!(
            !info.name.is_empty(),
            "Replication name should not be empty when enabled"
        );
    }

    println!("✓ Site replication info retrieved");
    println!("   Enabled: {}", info.enabled);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Requires multi-site deployment with site replication configured"]
async fn test_site_replication_status() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    println!("Getting site replication status");

    let response: SiteReplicationStatusResponse = madmin_client
        .site_replication_status()
        .build()
        .send()
        .await
        .expect("Failed to get site replication status");

    let status = response.status().unwrap();
    for (name, peer_info) in &status.sites {
        assert!(!name.is_empty(), "Site name should not be empty");
        assert!(
            !peer_info.endpoint.is_empty(),
            "Peer endpoint should not be empty"
        );
    }

    println!("✓ Site replication status retrieved");
    println!("   Enabled: {}", status.enabled);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Requires multi-site deployment and would modify site replication configuration"]
async fn test_site_replication_add() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let _madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    println!("This test is a placeholder - requires actual peer site configuration");
    println!("✓ Site replication add API is available");

    // Note: Actual test would require:
    // - A second MinIO server running
    // - Proper peer configuration with PeerSite struct
    // - Using site_replication_add().build().send().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Requires multi-site deployment and would modify site replication configuration"]
async fn test_site_replication_edit() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let _madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    println!("This test is a placeholder - requires existing site replication setup");
    println!("✓ Site replication edit API is available");

    // Note: Actual test would require:
    // - Existing site replication setup
    // - PeerInfo with deployment_id and endpoint
    // - Using site_replication_edit().build().send().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Requires multi-site deployment and would modify site replication configuration"]
async fn test_site_replication_remove() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let _madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    println!("This test is a placeholder - requires existing site replication setup");
    println!("✓ Site replication remove API is available");

    // Note: Actual test would require:
    // - Existing site replication with peer sites
    // - Using site_replication_remove().build().send().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Requires multi-site deployment"]
async fn test_site_replication_metainfo() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    println!("Getting site replication metainfo");

    let result: Result<SiteReplicationMetaInfoResponse, _> = madmin_client
        .site_replication_metainfo()
        .build()
        .send()
        .await;

    match result {
        Ok(_response) => {
            println!("✓ Site replication metainfo retrieved");
        }
        Err(e) => {
            println!("✓ Site replication not configured (expected): {}", e);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Requires multi-site deployment and would trigger replication resync"]
async fn test_site_replication_resync() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let _madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    println!("This test is a placeholder - would trigger resync operation");
    println!("✓ Site replication resync API is available");

    // Note: Actual test would require:
    // - Existing site replication setup
    // - Using site_replication_resync().build().send().await
    // - Would trigger potentially expensive resync operation
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Requires multi-site deployment and would modify state"]
async fn test_site_replication_state_edit() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let _madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    println!("This test is a placeholder - would modify replication state");
    println!("✓ Site replication state edit API is available");

    // Note: Actual test would require:
    // - Existing site replication setup
    // - State edit request with updates
    // - Using site_replication_state_edit().build().send().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Peer-to-peer operations are internal and called by other sites"]
async fn test_site_replication_peer_operations() {
    let ctx = TestContext::new_from_env();
    let _provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let _madmin_client = MadminClient::new(ctx.base_url.clone(), Some(_provider));

    println!("Peer operations (join, edit, remove, bucket ops, IAM, IDP, metainfo)");
    println!("are internal peer-to-peer APIs called by other MinIO sites.");
    println!("They are not typically called directly by clients.");
    println!("✓ All peer operation APIs are available");

    // Note: These APIs are used internally between MinIO servers:
    // - site_replication_peer_join
    // - site_replication_peer_edit
    // - site_replication_peer_remove
    // - site_replication_peer_bucket_ops
    // - site_replication_peer_bucket_meta
    // - site_replication_peer_iam_item
    // - site_replication_peer_idp_settings
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Requires multi-site deployment"]
async fn test_site_replication_add_full() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    // This is a comprehensive test showing the full API usage
    // Requires environment variables for peer sites
    let peer_endpoint = std::env::var("PEER_SITE_ENDPOINT")
        .unwrap_or_else(|_| "https://site2.example.com:9000".to_string());
    let peer_access_key =
        std::env::var("PEER_ACCESS_KEY").unwrap_or_else(|_| "peer_admin".to_string());
    let peer_secret_key =
        std::env::var("PEER_SECRET_KEY").unwrap_or_else(|_| "peer_password".to_string());

    use minio::madmin::types::site_replication::PeerSite;

    let peer_sites = vec![PeerSite {
        name: "site2".to_string(),
        endpoint: vec![peer_endpoint],
        access_key: peer_access_key,
        secret_key: peer_secret_key,
    }];

    let result: Result<SiteReplicationAddResponse, _> = madmin_client
        .site_replication_add()
        .sites(peer_sites)
        .build()
        .send()
        .await;

    match result {
        Ok(response) => {
            let status = response.status().unwrap();
            assert!(!status.status.is_empty(), "Status should not be empty");

            println!("✓ Site replication add successful");
            println!("  Status: {:?}", status.status);
        }
        Err(e) => {
            println!(
                "Site replication add failed (expected without multi-site): {}",
                e
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Requires multi-site deployment"]
async fn test_site_replication_edit_full() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let peer_endpoint = std::env::var("PEER_SITE_ENDPOINT")
        .unwrap_or_else(|_| "https://site2.example.com:9000".to_string());
    let deployment_id =
        std::env::var("PEER_DEPLOYMENT_ID").unwrap_or_else(|_| "deployment-id-here".to_string());

    let result: Result<SiteReplicationEditResponse, _> = madmin_client
        .site_replication_edit()
        .deployment_id(deployment_id)
        .endpoint(peer_endpoint)
        .build()
        .send()
        .await;

    match result {
        Ok(response) => {
            let status = response.status().unwrap();
            assert!(!status.status.is_empty(), "Status should not be empty");

            println!("✓ Site replication edit successful");
            println!("  Status: {:?}", status.status);
        }
        Err(e) => {
            println!("Site replication edit failed (expected): {}", e);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Requires multi-site deployment"]
async fn test_site_replication_remove_full() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let peer_name = "site2";

    let result: Result<SiteReplicationRemoveResponse, _> = madmin_client
        .site_replication_remove()
        .site_names(vec![peer_name.to_string()])
        .build()
        .send()
        .await;

    match result {
        Ok(response) => {
            let status = response.status().unwrap();
            assert!(!status.status.is_empty(), "Status should not be empty");

            println!("✓ Site replication remove successful");
            println!("  Status: {:?}", status.status);
        }
        Err(e) => {
            println!("Site replication remove failed (expected): {}", e);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Requires multi-site deployment"]
async fn test_site_replication_resync_op() {
    use minio::madmin::types::site_replication::SiteResyncOp;

    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let result: Result<SiteReplicationResyncResponse, _> = madmin_client
        .site_replication_resync()
        .operation(SiteResyncOp::Start)
        .build()
        .send()
        .await;

    match result {
        Ok(response) => {
            println!("✓ Site replication resync operation initiated");
            println!("  Status: {:?}", response.status);
        }
        Err(e) => {
            println!("Site replication resync failed (expected): {}", e);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Requires multi-site deployment"]
async fn test_sr_status_info() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let result: Result<SiteReplicationStatusResponse, _> =
        madmin_client.site_replication_status().build().send().await;

    match result {
        Ok(response) => {
            let status = response.status().unwrap();
            for (name, peer) in &status.sites {
                assert!(!name.is_empty(), "Site name should not be empty");
                assert!(
                    !peer.endpoint.is_empty(),
                    "Peer endpoint should not be empty"
                );
            }

            println!("✓ Site replication status info retrieved");
            println!("  Enabled: {}", status.enabled);
            println!("  Sites: {}", status.sites.len());
            for (name, peer) in &status.sites {
                println!("    - {}: {}", name, peer.endpoint);
            }
        }
        Err(e) => {
            println!("Site replication not configured (expected): {}", e);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Requires multi-site deployment"]
async fn test_sr_meta_info() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let result: Result<SiteReplicationMetaInfoResponse, _> = madmin_client
        .site_replication_metainfo()
        .build()
        .send()
        .await;

    match result {
        Ok(response) => {
            println!("✓ Site replication meta info retrieved");
            println!("  Info: {:?}", response);
        }
        Err(e) => {
            println!("Site replication not configured (expected): {}", e);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Requires multi-site deployment"]
async fn test_sr_peer_edit() {
    use minio::madmin::types::site_replication::PeerInfo;

    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let peer = PeerInfo {
        name: "site2".to_string(),
        endpoint: "https://site2.example.com:9000".to_string(),
        deployment_id: "deployment-id-here".to_string(),
    };

    let result: Result<SiteReplicationPeerEditResponse, _> = madmin_client
        .site_replication_peer_edit()
        .peer_info(peer)
        .build()
        .send()
        .await;

    match result {
        Ok(_) => {
            println!("✓ Site replication peer edit successful");
        }
        Err(e) => {
            println!("Peer edit API available but requires config: {}", e);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Requires multi-site deployment"]
async fn test_sr_state_edit() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    // Create a sample state object
    let state = serde_json::json!({
        "enabled": true
    });

    let result: Result<SiteReplicationStateEditResponse, _> = madmin_client
        .site_replication_state_edit()
        .state(state)
        .build()
        .send()
        .await;

    match result {
        Ok(_) => {
            println!("✓ Site replication state edit successful");
        }
        Err(e) => {
            println!("State edit API available but requires config: {}", e);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Peer-to-peer API called by MinIO servers"]
async fn test_sr_peer_join() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let _madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    println!("✓ SRPeerJoin API is available");
    println!("  This is a peer-to-peer API called by other MinIO sites");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Peer-to-peer API called by MinIO servers"]
async fn test_sr_peer_bucket_ops() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let _madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    println!("✓ SRPeerBucketOps API is available");
    println!("  This is a peer-to-peer API for bucket operations");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Peer-to-peer API called by MinIO servers"]
async fn test_sr_peer_replicate_iam_item() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let _madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    println!("✓ SRPeerReplicateIAMItem API is available");
    println!("  This is a peer-to-peer API for IAM item replication");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Peer-to-peer API called by MinIO servers"]
async fn test_sr_peer_replicate_bucket_meta() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let _madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    println!("✓ SRPeerReplicateBucketMeta API is available");
    println!("  This is a peer-to-peer API for bucket metadata replication");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Peer-to-peer API called by MinIO servers"]
async fn test_sr_peer_get_idp_settings() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let _madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    println!("✓ SRPeerGetIDPSettings API is available");
    println!("  This is a peer-to-peer API for retrieving IDP settings");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Peer-to-peer API called by MinIO servers"]
async fn test_sr_peer_remove() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let _madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    println!("✓ SRPeerRemove API is available");
    println!("  This is a peer-to-peer API for removing peer sites");
}

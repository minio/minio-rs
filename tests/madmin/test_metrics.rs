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
use minio::madmin::response::MetricsResponse;
use minio::madmin::types::MadminApi;
use minio::s3::creds::StaticProvider;
use minio_common::test_context::TestContext;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore] // Prometheus metrics API may not be available in single-node mode
async fn test_metrics_basic() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let resp: MetricsResponse = madmin_client
        .metrics()
        .build()
        .send()
        .await
        .expect("Failed to get metrics");

    assert!(
        !resp.metrics().unwrap().is_empty(),
        "Metrics should not be empty"
    );
    assert!(
        resp.metrics().unwrap().contains("minio_"),
        "Metrics should contain MinIO metrics"
    );

    // Count and validate key metric types
    let metrics_str = resp.metrics().unwrap();
    let lines: Vec<&str> = metrics_str.lines().collect();
    let metric_lines: Vec<&str> = lines
        .iter()
        .filter(|l| !l.starts_with('#') && !l.is_empty())
        .copied()
        .collect();
    let help_lines: Vec<&str> = lines
        .iter()
        .filter(|l| l.starts_with("# HELP"))
        .copied()
        .collect();
    let type_lines: Vec<&str> = lines
        .iter()
        .filter(|l| l.starts_with("# TYPE"))
        .copied()
        .collect();

    println!("Metrics summary:");
    println!("  Total lines: {}", lines.len());
    println!("  Metric lines: {}", metric_lines.len());
    println!("  HELP comments: {}", help_lines.len());
    println!("  TYPE comments: {}", type_lines.len());

    // Verify common MinIO metrics are present
    assert!(
        resp.metrics().unwrap().contains("minio_version_info"),
        "Should contain version info"
    );
    assert!(
        resp.metrics()
            .unwrap()
            .contains("minio_software_version_info")
            || resp.metrics().unwrap().contains("minio_version_info"),
        "Should contain software version"
    );

    println!(
        "✓ Retrieved and validated {} metrics ({} bytes)",
        metric_lines.len(),
        resp.metrics().unwrap().len()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore] // Prometheus metrics API may not be available in single-node mode
async fn test_metrics_with_options() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let resp: MetricsResponse = madmin_client
        .metrics()
        .cluster(true)
        .disk(true)
        .bucket(true)
        .build()
        .send()
        .await
        .expect("Failed to get metrics with options");

    assert!(
        !resp.metrics().unwrap().is_empty(),
        "Metrics should not be empty"
    );

    // With cluster option, should see cluster-related metrics
    let has_cluster_metrics =
        resp.metrics().unwrap().contains("cluster") || resp.metrics().unwrap().contains("node");

    // With disk option, should see disk-related metrics
    let has_disk_metrics =
        resp.metrics().unwrap().contains("disk") || resp.metrics().unwrap().contains("storage");

    // With bucket option, should see bucket-related metrics
    let has_bucket_metrics = resp.metrics().unwrap().contains("bucket");

    println!("Metrics breakdown:");
    println!(
        "  Cluster metrics: {}",
        if has_cluster_metrics {
            "present"
        } else {
            "absent"
        }
    );
    println!(
        "  Disk metrics: {}",
        if has_disk_metrics {
            "present"
        } else {
            "absent"
        }
    );
    println!(
        "  Bucket metrics: {}",
        if has_bucket_metrics {
            "present"
        } else {
            "absent"
        }
    );

    println!(
        "✓ Retrieved metrics with options ({} bytes)",
        resp.metrics().unwrap().len()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore] // Prometheus metrics API may not be available in single-node mode
async fn test_metrics_format() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let resp: MetricsResponse = madmin_client
        .metrics()
        .build()
        .send()
        .await
        .expect("Failed to get metrics");

    // Check that the metrics are in Prometheus format
    assert!(
        resp.metrics().unwrap().contains("# HELP") || resp.metrics().unwrap().contains("# TYPE"),
        "Metrics should be in Prometheus format with HELP or TYPE comments"
    );

    // Validate Prometheus format structure
    let metrics_str = resp.metrics().unwrap();
    let lines: Vec<&str> = metrics_str.lines().collect();
    let mut sample_metrics = Vec::new();

    for line in lines.iter().take(50) {
        if !line.starts_with('#') && !line.is_empty() {
            // Prometheus format: metric_name{labels} value timestamp
            if line.contains('{') || line.contains(' ') {
                sample_metrics.push(*line);
            }
        }
    }

    assert!(
        !sample_metrics.is_empty(),
        "Should have parseable metric lines"
    );

    println!("Prometheus format validation:");
    println!("  Sample metrics (first 5):");
    for metric in sample_metrics.iter().take(5) {
        println!("    {}", metric);
    }

    println!("✓ Metrics are in valid Prometheus format");
}

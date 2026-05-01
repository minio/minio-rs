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

//! Stress test: Ultra-aggressive state transition chaos test.
//!
//! This is an enhanced version of rapid_state_changes that pushes the system
//! to its limits by creating many jobs and hammering them with rapid state
//! transitions. The goal is to find race conditions, deadlocks, and breaking
//! points that only appear under extreme chaos.
//!
//! # Differences from rapid_state_changes
//!
//! - **50 jobs** instead of 5 (10x more)
//! - **20 control threads** instead of 8 (2.5x more)
//! - **10-50ms** between operations instead of 50-200ms (4x faster)
//! - **5 minutes** duration instead of 2 minutes
//! - **More operations**: Config updates, deletions, recreations
//! - **More chaos**: Random concurrent modifications
//!
//! # Test Scenario
//!
//! 1. Create 50 inventory jobs across 50 buckets
//! 2. Spawn 20 threads that aggressively perform:
//!    - Suspend/Resume (state flipping)
//!    - Cancel and immediate restart
//!    - Generate inventory (triggering actual work)
//!    - Delete and recreate configs
//!    - Update config parameters
//!    - Status checks (read operations)
//! 3. Run for 5 minutes of sustained chaos
//! 4. Monitor for:
//!    - Deadlocks (threads hanging)
//!    - Race conditions (inconsistent state)
//!    - Server crashes
//!    - Memory/resource leaks
//!    - API errors or panics
//!
//! # Expected Behavior
//!
//! - System should remain stable under chaos
//! - All operations should eventually complete or fail gracefully
//! - No deadlocks or stuck states
//! - No server crashes
//! - Errors are acceptable but system should recover
//!
//! # Requirements
//!
//! - MinIO server at http://localhost:9000
//! - Admin credentials: minioadmin/minioadmin
//! - Sufficient system resources (CPU, memory)

use minio::madmin::types::MadminApi;
use minio::madmin::{AdminControlJson, MinioAdminClient};
use minio::s3::builders::ObjectContent;
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;
use minio::s3::response::CreateBucketResponse;
use minio::s3::types::{BucketName, S3Api};
use minio::s3::MinioClient;
use minio::s3inventory::{
    DestinationSpec, JobDefinition, JobStatus, ModeSpec, OnOrOff, OutputFormat,
    PutInventoryConfigResponse, Schedule, VersionsSpec,
};
use rand::Rng;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

const NUM_JOBS: usize = 50;
const NUM_CONTROL_THREADS: usize = 20;
const TEST_DURATION_SECS: u64 = 300;
const MIN_OBJECTS_PER_BUCKET: usize = 20;

#[derive(Debug, Clone, Copy)]
enum ChaosOperation {
    Suspend,
    Resume,
    Cancel,
    CheckStatus,
    UpdateConfig,
    DeleteAndRecreate,
    SuspendResumePair,
}

struct ChaosMetrics {
    suspends: AtomicU64,
    resumes: AtomicU64,
    cancels: AtomicU64,
    status_checks: AtomicU64,
    config_updates: AtomicU64,
    config_deletes: AtomicU64,
    config_recreates: AtomicU64,
    suspend_resume_pairs: AtomicU64,
    total_errors: AtomicU64,
    deadlock_warnings: AtomicU64,
    start_time: Instant,
}

impl ChaosMetrics {
    fn new() -> Self {
        Self {
            suspends: AtomicU64::new(0),
            resumes: AtomicU64::new(0),
            cancels: AtomicU64::new(0),
            status_checks: AtomicU64::new(0),
            config_updates: AtomicU64::new(0),
            config_deletes: AtomicU64::new(0),
            config_recreates: AtomicU64::new(0),
            suspend_resume_pairs: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
            deadlock_warnings: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }

    fn print_progress(&self) {
        let elapsed = self.start_time.elapsed().as_secs();
        let total_ops = self.suspends.load(Ordering::Relaxed)
            + self.resumes.load(Ordering::Relaxed)
            + self.cancels.load(Ordering::Relaxed)
            + self.status_checks.load(Ordering::Relaxed)
            + self.config_updates.load(Ordering::Relaxed)
            + self.config_deletes.load(Ordering::Relaxed);

        println!("\n[{:>3}s] Chaos Progress:", elapsed);
        println!("  Total operations:     {}", total_ops);
        println!("  Ops/sec:              {:.1}", total_ops as f64 / elapsed as f64);
        println!("  Suspends:             {}", self.suspends.load(Ordering::Relaxed));
        println!("  Resumes:              {}", self.resumes.load(Ordering::Relaxed));
        println!("  Cancels:              {}", self.cancels.load(Ordering::Relaxed));
        println!("  Status checks:        {}", self.status_checks.load(Ordering::Relaxed));
        println!("  Config updates:       {}", self.config_updates.load(Ordering::Relaxed));
        println!("  Config del/recreate:  {}", self.config_deletes.load(Ordering::Relaxed));
        println!("  Suspend-resume pairs: {}", self.suspend_resume_pairs.load(Ordering::Relaxed));
        println!("  Total errors:         {}", self.total_errors.load(Ordering::Relaxed));
        println!("  Deadlock warnings:    {}", self.deadlock_warnings.load(Ordering::Relaxed));
    }

    fn print_summary(&self) {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        let total_ops = self.suspends.load(Ordering::Relaxed)
            + self.resumes.load(Ordering::Relaxed)
            + self.cancels.load(Ordering::Relaxed)
            + self.status_checks.load(Ordering::Relaxed)
            + self.config_updates.load(Ordering::Relaxed)
            + self.config_deletes.load(Ordering::Relaxed);

        println!("\n=== Final Chaos Statistics ===");
        println!("Duration:             {:.1} minutes", elapsed / 60.0);
        println!("Total operations:     {}", total_ops);
        println!("Operations/sec:       {:.2}", total_ops as f64 / elapsed);
        println!("\nOperation Breakdown:");
        println!("  Suspends:             {}", self.suspends.load(Ordering::Relaxed));
        println!("  Resumes:              {}", self.resumes.load(Ordering::Relaxed));
        println!("  Cancels:              {}", self.cancels.load(Ordering::Relaxed));
        println!("  Status checks:        {}", self.status_checks.load(Ordering::Relaxed));
        println!("  Config updates:       {}", self.config_updates.load(Ordering::Relaxed));
        println!("  Config del/recreate:  {}", self.config_deletes.load(Ordering::Relaxed));
        println!("  Suspend-resume pairs: {}", self.suspend_resume_pairs.load(Ordering::Relaxed));
        println!("\nErrors and Issues:");
        println!("  Total errors:         {}", self.total_errors.load(Ordering::Relaxed));
        println!("  Error rate:           {:.2}%", (self.total_errors.load(Ordering::Relaxed) as f64 / total_ops as f64) * 100.0);
        println!("  Deadlock warnings:    {}", self.deadlock_warnings.load(Ordering::Relaxed));

        if self.deadlock_warnings.load(Ordering::Relaxed) > 0 {
            println!("\n⚠️  WARNING: Potential deadlocks detected!");
        }

        if total_ops > 0 && self.total_errors.load(Ordering::Relaxed) as f64 / total_ops as f64 > 0.5 {
            println!("\n⚠️  WARNING: High error rate (>50%) - system may be unstable");
        }
    }
}

async fn chaos_thread_task(
    client: MinioClient,
    admin: MinioAdminClient,
    job_info: Vec<(BucketName, String)>,
    dest_bucket: BucketName,
    thread_id: usize,
    metrics: Arc<ChaosMetrics>,
    stop_signal: Arc<AtomicBool>,
) {
    let mut operation_count = 0;
    let thread_start = Instant::now();

    while !stop_signal.load(Ordering::Relaxed) {
        let (operation, job_idx, sleep_ms) = {
            let mut rng = rand::rng();
            let op = match rng.random_range(0..7) {
                0 => ChaosOperation::Suspend,
                1 => ChaosOperation::Resume,
                2 => ChaosOperation::Cancel,
                3 => ChaosOperation::CheckStatus,
                4 => ChaosOperation::UpdateConfig,
                5 => ChaosOperation::DeleteAndRecreate,
                _ => ChaosOperation::SuspendResumePair,
            };
            let idx = rng.random_range(0..job_info.len());
            let sleep = rng.random_range(10..50);
            (op, idx, sleep)
        };

        let (bucket, job_id) = &job_info[job_idx];

        let op_start = Instant::now();
        let mut error_occurred = false;

        match operation {
            ChaosOperation::Suspend => {
                match admin.suspend_inventory_job(bucket.clone(), job_id).build().send().await {
                    Ok(resp) => {
                        let _: Result<AdminControlJson, _> = resp.admin_control();
                        metrics.suspends.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        error_occurred = true;
                    }
                }
            }

            ChaosOperation::Resume => {
                match admin.resume_inventory_job(bucket.clone(), job_id).build().send().await {
                    Ok(resp) => {
                        let _: Result<AdminControlJson, _> = resp.admin_control();
                        metrics.resumes.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        error_occurred = true;
                    }
                }
            }

            ChaosOperation::Cancel => {
                match admin.cancel_inventory_job(bucket.clone(), job_id).build().send().await {
                    Ok(resp) => {
                        let _: Result<AdminControlJson, _> = resp.admin_control();
                        metrics.cancels.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        error_occurred = true;
                    }
                }
            }

            ChaosOperation::CheckStatus => {
                let builder = match client.get_inventory_job_status(bucket.clone(), job_id) {
                    Ok(b) => b,
                    Err(_) => {
                        error_occurred = true;
                        continue;
                    }
                };
                match builder.build().send().await {
                    Ok(resp) => {
                        let _: Result<JobStatus, _> = resp.status();
                        metrics.status_checks.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        error_occurred = true;
                    }
                }
            }

            ChaosOperation::UpdateConfig => {
                let job = JobDefinition {
                    api_version: "v1".to_string(),
                    id: job_id.clone(),
                    destination: DestinationSpec {
                        bucket: dest_bucket.to_string(),
                        prefix: Some(format!("{}/updated-{}/", job_id, operation_count)),
                        format: OutputFormat::CSV,
                        compression: OnOrOff::On,
                        max_file_size_hint: None,
                    },
                    schedule: Schedule::Weekly,
                    mode: ModeSpec::Fast,
                    versions: VersionsSpec::Current,
                    include_fields: vec![],
                    filters: None,
                };

                let builder = match client.put_inventory_config(bucket.clone(), job_id, job) {
                    Ok(b) => b,
                    Err(_) => {
                        error_occurred = true;
                        continue;
                    }
                };
                match builder.build().send().await {
                    Ok(_) => {
                        metrics.config_updates.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        error_occurred = true;
                    }
                }
            }

            ChaosOperation::DeleteAndRecreate => {
                let builder = match client.delete_inventory_config(bucket.clone(), job_id) {
                    Ok(b) => b,
                    Err(_) => {
                        error_occurred = true;
                        continue;
                    }
                };
                match builder.build().send().await {
                    Ok(_) => {
                        metrics.config_deletes.fetch_add(1, Ordering::Relaxed);

                        tokio::time::sleep(Duration::from_millis(50)).await;

                        let job = JobDefinition {
                            api_version: "v1".to_string(),
                            id: job_id.clone(),
                            destination: DestinationSpec {
                                bucket: dest_bucket.to_string(),
                                prefix: Some(format!("{}/", job_id)),
                                format: OutputFormat::CSV,
                                compression: OnOrOff::On,
                                max_file_size_hint: None,
                            },
                            schedule: Schedule::Daily,
                            mode: ModeSpec::Fast,
                            versions: VersionsSpec::Current,
                            include_fields: vec![],
                            filters: None,
                        };

                        let recreate_builder = match client.put_inventory_config(bucket.clone(), job_id, job) {
                            Ok(b) => b,
                            Err(_) => {
                                error_occurred = true;
                                continue;
                            }
                        };
                        match recreate_builder.build().send().await {
                            Ok(_) => {
                                metrics.config_recreates.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(_) => {
                                error_occurred = true;
                            }
                        }
                    }
                    Err(_) => {
                        error_occurred = true;
                    }
                }
            }

            ChaosOperation::SuspendResumePair => {
                match admin.suspend_inventory_job(bucket.clone(), job_id).build().send().await {
                    Ok(_) => {
                        tokio::time::sleep(Duration::from_millis(10)).await;

                        match admin.resume_inventory_job(bucket.clone(), job_id).build().send().await {
                            Ok(_) => {
                                metrics.suspend_resume_pairs.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(_) => {
                                error_occurred = true;
                            }
                        }
                    }
                    Err(_) => {
                        error_occurred = true;
                    }
                }
            }
        }

        if error_occurred {
            metrics.total_errors.fetch_add(1, Ordering::Relaxed);
        }

        let op_duration = op_start.elapsed();
        if op_duration > Duration::from_secs(5) {
            metrics.deadlock_warnings.fetch_add(1, Ordering::Relaxed);
            eprintln!("[Thread {}] ⚠️  Operation took {:.1}s (possible deadlock?)",
                     thread_id, op_duration.as_secs_f64());
        }

        operation_count += 1;

        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
    }

    let thread_duration = thread_start.elapsed().as_secs_f64();
    println!("[Thread {}] Completed {} operations in {:.1}s ({:.1} ops/s)",
             thread_id, operation_count, thread_duration, operation_count as f64 / thread_duration);
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("=== S3 Inventory Stress Test: ULTRA STATE CHAOS ===\n");
    println!("⚠️  WARNING: This is an aggressive chaos test!");
    println!("   - {} jobs with rapid state changes", NUM_JOBS);
    println!("   - {} concurrent control threads", NUM_CONTROL_THREADS);
    println!("   - 10-50ms between operations (very fast)");
    println!("   - {} seconds ({:.1} minutes) duration\n", TEST_DURATION_SECS, TEST_DURATION_SECS as f64 / 60.0);

    let base_url = "http://localhost:9000".parse::<BaseUrl>()?;
    let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    let client = MinioClient::new(
        base_url.clone(),
        Some(static_provider.clone()),
        None,
        None,
    )?;

    let dest_bucket = BucketName::new("chaos-reports").unwrap();
    println!("Step 1: Creating destination bucket...");
    let _: CreateBucketResponse = client.create_bucket(dest_bucket.clone()).build().send().await?;

    let mut job_info = Vec::new();

    println!("\nStep 2: Creating {} buckets with inventory jobs...", NUM_JOBS);
    println!("  (This may take a few minutes)");

    for i in 0..NUM_JOBS {
        let bucket = BucketName::new(&format!("chaos-{}", i)).unwrap();
        let job_id = format!("job-{}", i);

        let _: CreateBucketResponse = client.create_bucket(bucket.clone()).build().send().await?;

        for j in 0..MIN_OBJECTS_PER_BUCKET {
            let object_name = format!("obj-{:03}.dat", j);
            let content = vec![b'C'; 256];
            let object_content = ObjectContent::from(content);

            let _ = client
                .put_object_content(bucket.clone(), &object_name, object_content)
                .build()
                .send()
                .await;
        }

        let job = JobDefinition {
            api_version: "v1".to_string(),
            id: job_id.clone(),
            destination: DestinationSpec {
                bucket: dest_bucket.to_string(),
                prefix: Some(format!("job-{}/", i)),
                format: OutputFormat::CSV,
                compression: OnOrOff::On,
                max_file_size_hint: None,
            },
            schedule: Schedule::Daily,
            mode: ModeSpec::Fast,
            versions: VersionsSpec::Current,
            include_fields: vec![],
            filters: None,
        };

        let _: PutInventoryConfigResponse = client
            .put_inventory_config(bucket.clone(), &job_id, job)?
            .build()
            .send()
            .await?;

        job_info.push((bucket.clone(), job_id.clone()));

        if (i + 1) % 10 == 0 {
            println!("  Created {}/{} buckets...", i + 1, NUM_JOBS);
        }
    }

    println!("\n✓ Setup complete! Starting chaos test...\n");
    let metrics = Arc::new(ChaosMetrics::new());
    let stop_signal = Arc::new(AtomicBool::new(false));
    let mut tasks = JoinSet::new();

    for thread_id in 0..NUM_CONTROL_THREADS {
        let client_clone = MinioClient::new(
            base_url.clone(),
            Some(static_provider.clone()),
            None,
            None,
        )?;
        let admin_clone = client_clone.admin();
        let job_info_clone = job_info.clone();
        let dest_bucket_clone = dest_bucket.clone();
        let metrics_clone = Arc::clone(&metrics);
        let stop_signal_clone = Arc::clone(&stop_signal);

        tasks.spawn(async move {
            chaos_thread_task(
                client_clone,
                admin_clone,
                job_info_clone,
                dest_bucket_clone,
                thread_id,
                metrics_clone,
                stop_signal_clone,
            )
            .await;
        });
    }

    let progress_metrics = Arc::clone(&metrics);
    let progress_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            progress_metrics.print_progress();
        }
    });

    println!("🔥 CHAOS INITIATED - Running for {} seconds...\n", TEST_DURATION_SECS);
    tokio::time::sleep(Duration::from_secs(TEST_DURATION_SECS)).await;

    println!("\n\n🛑 Stopping all threads...");
    stop_signal.store(true, Ordering::Relaxed);

    while let Some(result) = tasks.join_next().await {
        if let Err(e) = result {
            eprintln!("⚠️  Thread panicked: {}", e);
        }
    }

    progress_handle.abort();

    println!("\n=== All Threads Stopped ===");
    metrics.print_summary();

    let total_state_ops = metrics.suspends.load(Ordering::Relaxed)
        + metrics.resumes.load(Ordering::Relaxed)
        + metrics.cancels.load(Ordering::Relaxed);

    if metrics.deadlock_warnings.load(Ordering::Relaxed) == 0 &&
       total_state_ops > 0 &&
       (metrics.total_errors.load(Ordering::Relaxed) as f64 / total_state_ops as f64) < 0.1 {
        println!("\n✅ System survived chaos test successfully!");
    } else {
        println!("\n⚠️  Issues detected during chaos test - review warnings above");
    }

    Ok(())
}

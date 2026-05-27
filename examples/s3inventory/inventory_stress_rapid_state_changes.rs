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

//! Stress test: Rapid inventory job state transitions.
//!
//! This stress test evaluates how the admin inventory control system handles
//! rapid state changes (suspend, resume, cancel, update, delete) across multiple jobs.
//! It tests for race conditions, deadlocks, and state consistency issues.
//!
//! # Test Scenario
//!
//! 1. Create multiple inventory jobs on separate buckets
//! 2. Spawn threads that randomly perform operations:
//!    - Suspend jobs
//!    - Resume jobs
//!    - Check job status
//!    - Cancel jobs
//!    - Update job configuration
//!    - Delete and recreate jobs
//!    - Get job configuration
//! 3. Monitor for state consistency and errors
//! 4. Distinguish between invalid state transitions (expected) and real errors
//! 5. Verify no deadlocks or stuck states
//!
//! # Configuration
//!
//! - `NUM_JOBS`: Number of inventory jobs to stress test
//! - `NUM_CONTROL_THREADS`: Concurrent threads controlling jobs
//! - `TEST_DURATION_SECS`: How long to run the stress test
//!
//! # Expected Behavior
//!
//! - Jobs should reach consistent states without hanging
//! - State transitions should be atomic
//! - No deadlocks or race conditions
//! - All operations should eventually complete
//!
//! # Requirements
//!
//! - MinIO server at http://localhost:9000
//! - Admin credentials: minioadmin/minioadmin

use minio::madmin::types::MadminApi;
use minio::madmin::{AdminControlJson, MinioAdminClient};
use minio::s3::MinioClient;
use minio::s3::builders::ObjectContent;
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;
use minio::s3::types::{BucketName, ObjectKey, S3Api};
use minio::s3inventory::{
    DestinationSpec, JobDefinition, JobStatus, ModeSpec, OnOrOff, OutputFormat, Schedule,
    VersionsSpec,
};
use rand::Rng;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

#[derive(Debug, Clone)]
struct StressConfig {
    num_jobs: usize,
    num_threads: usize,
    duration_secs: u64,
    objects_per_bucket: usize,
    min_delay_ms: u64,
    max_delay_ms: u64,
    server_url: String,
    access_key: String,
    secret_key: String,
    bucket_groups: Option<usize>, // If set, divides buckets into groups to reduce contention
}

impl Default for StressConfig {
    fn default() -> Self {
        Self {
            num_jobs: 10,
            num_threads: 20,
            duration_secs: 120,
            objects_per_bucket: 25,
            min_delay_ms: 1,
            max_delay_ms: 10,
            server_url: "http://localhost:9000".to_string(),
            access_key: "minioadmin".to_string(),
            secret_key: "minioadmin".to_string(),
            bucket_groups: None, // Default: all threads access all buckets (high contention)
        }
    }
}

impl StressConfig {
    fn from_args() -> Self {
        let mut config = Self::default();
        let args: Vec<String> = std::env::args().collect();

        let mut i = 1;
        while i < args.len() {
            match args[i].as_str() {
                "--jobs" | "-j" => {
                    if i + 1 < args.len() {
                        config.num_jobs = args[i + 1].parse().unwrap_or(config.num_jobs);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--threads" | "-t" => {
                    if i + 1 < args.len() {
                        config.num_threads = args[i + 1].parse().unwrap_or(config.num_threads);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--duration" | "-d" => {
                    if i + 1 < args.len() {
                        config.duration_secs = args[i + 1].parse().unwrap_or(config.duration_secs);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--objects" | "-o" => {
                    if i + 1 < args.len() {
                        config.objects_per_bucket =
                            args[i + 1].parse().unwrap_or(config.objects_per_bucket);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--min-delay" => {
                    if i + 1 < args.len() {
                        config.min_delay_ms = args[i + 1].parse().unwrap_or(config.min_delay_ms);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--max-delay" => {
                    if i + 1 < args.len() {
                        config.max_delay_ms = args[i + 1].parse().unwrap_or(config.max_delay_ms);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--server" | "-s" => {
                    if i + 1 < args.len() {
                        config.server_url = args[i + 1].clone();
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--access-key" => {
                    if i + 1 < args.len() {
                        config.access_key = args[i + 1].clone();
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--secret-key" => {
                    if i + 1 < args.len() {
                        config.secret_key = args[i + 1].clone();
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--bucket-groups" | "-g" => {
                    if i + 1 < args.len() {
                        config.bucket_groups = args[i + 1].parse().ok();
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--help" | "-h" => {
                    Self::print_help();
                    std::process::exit(0);
                }
                "--info" | "-i" => {
                    Self::print_info();
                    std::process::exit(0);
                }
                _ => {
                    i += 1;
                }
            }
        }

        config
    }

    fn print_info() {
        println!(
            "╔══════════════════════════════════════════════════════════════════════════════╗"
        );
        println!("║          S3 Inventory Heavy Stress Test - Detailed Documentation            ║");
        println!(
            "╚══════════════════════════════════════════════════════════════════════════════╝"
        );
        println!();
        println!("PURPOSE: BREAK THE CODE");
        println!("------------------------");
        println!(
            "This tool's PRIMARY GOAL is to BREAK the inventory control APIs and expose bugs."
        );
        println!("It is NOT about finding optimal performance - it's about finding:");
        println!();
        println!("  ✗ Race conditions and data corruption");
        println!("  ✗ Deadlocks and livelocks");
        println!("  ✗ Panics and crashes under extreme load");
        println!("  ✗ Memory leaks and resource exhaustion");
        println!("  ✗ Inconsistent state transitions");
        println!("  ✗ Concurrency bugs that only appear at scale");
        println!("  ✗ Edge cases in error handling");
        println!();
        println!("HIGHER THREAD COUNTS = MORE LIKELY TO EXPOSE BUGS");
        println!("If the test completes without crashes/panics/corruption → CODE IS GOOD");
        println!("If you find issues → EXCELLENT, that's the whole point!");
        println!();
        println!("WHAT IT TESTS");
        println!("-------------");
        println!(
            "• Admin Control APIs: suspend_inventory_job, resume_inventory_job, cancel_inventory_job"
        );
        println!(
            "• Inventory Management: put_inventory_config, delete_inventory_config, get_inventory_config"
        );
        println!("• Job Status Monitoring: get_inventory_job_status");
        println!("• Concurrent Access: Multiple threads hammering the same resources");
        println!("• Race Conditions: Intentional state changes happening simultaneously");
        println!("• System Breaking Points: Pushing to failure to find limits");
        println!();
        println!("ARCHITECTURE");
        println!("------------");
        println!("1. Setup Phase:");
        println!("   - Creates N buckets (state-stress-0, state-stress-1, ...)");
        println!("   - Creates one inventory job per bucket");
        println!("   - Populates buckets with test objects");
        println!("   - Creates shared destination bucket for reports");
        println!();
        println!("2. Stress Test Phase:");
        println!("   - Spawns M concurrent threads");
        println!("   - Each thread randomly selects operations and jobs");
        println!("   - Operations execute with configurable delays");
        println!("   - Metrics tracked in real-time");
        println!();
        println!("3. Contention Models:");
        println!("   a) High Contention (default): All threads access all jobs");
        println!("      → Good for testing worst-case race conditions");
        println!("      → Example: 100 threads / 10 jobs = 10 threads per job");
        println!();
        println!("   b) Grouped (--bucket-groups): Threads divided into isolated groups");
        println!("      → Each group has dedicated job subset");
        println!("      → Reduces contention, tests parallel scalability");
        println!("      → Example: 100 threads, 20 jobs, 5 groups:");
        println!("        • Group 0: Threads 0-19 → Jobs 0-3");
        println!("        • Group 1: Threads 20-39 → Jobs 4-7");
        println!("        • Group 2: Threads 40-59 → Jobs 8-11");
        println!("        • Group 3: Threads 60-79 → Jobs 12-15");
        println!("        • Group 4: Threads 80-99 → Jobs 16-19");
        println!();
        println!("PARAMETERS EXPLAINED");
        println!("--------------------");
        println!("--threads (-t):");
        println!("  Number of concurrent threads hammering the system.");
        println!("  • Low (20-50): Basic concurrency testing");
        println!("  • Medium (50-100): Heavy load testing");
        println!("  • High (100-200): Extreme stress testing");
        println!("  • Very High (200+): Breaking point identification");
        println!();
        println!("--jobs (-j):");
        println!("  Number of inventory jobs (and buckets) to create.");
        println!("  • Formula: contention = threads / jobs");
        println!("  • More jobs = less contention per job");
        println!("  • Fewer jobs = more intense contention");
        println!();
        println!("--bucket-groups (-g):");
        println!("  Divides workload into isolated groups.");
        println!("  • None: All threads compete for all jobs (max contention)");
        println!("  • Few groups (2-5): Moderate isolation");
        println!("  • Many groups (10+): Low contention, tests parallel scalability");
        println!("  • Rule: groups should divide evenly into threads and jobs");
        println!();
        println!("--duration (-d):");
        println!("  Test duration in seconds.");
        println!("  • Short (30-60s): Quick validation");
        println!("  • Medium (120-300s): Sustained load testing");
        println!("  • Long (600+s): Stability and memory leak detection");
        println!();
        println!("--min-delay / --max-delay:");
        println!("  Milliseconds between operations per thread.");
        println!("  • Fast (1-10ms): Maximum throughput, extreme stress");
        println!("  • Medium (10-50ms): Realistic high-load scenario");
        println!("  • Slow (50-200ms): Controlled load testing");
        println!();
        println!("--objects (-o):");
        println!("  Number of test objects per bucket.");
        println!("  • Affects setup time only");
        println!("  • Doesn't impact stress test performance");
        println!("  • Lower values = faster test startup");
        println!();
        println!("INTERPRETING RESULTS - DID WE BREAK ANYTHING?");
        println!("----------------------------------------------");
        println!("Test Completed Without Crashes:");
        println!("  ✓ GOOD: No panics, deadlocks, or crashes found");
        println!("  → The code handles this level of concurrency");
        println!("  → Try more aggressive parameters to push harder");
        println!();
        println!("Expected Errors (NOT bugs):");
        println!("  These are NORMAL in high-contention stress tests:");
        println!("  • \"invalid\" operations: State conflicts (suspend already-suspended, etc.)");
        println!("  • Some \"err\" operations: Transient conflicts during state changes");
        println!("  → High invalid/err rates with extreme contention = expected behavior");
        println!();
        println!("Unexpected Errors (POSSIBLE bugs):");
        println!("  Watch for these - they may indicate real issues:");
        println!("  • Network timeouts (consistent pattern, not occasional)");
        println!("  • Server errors: 500 Internal Server Error, 503 Service Unavailable");
        println!("  • Authentication failures (should never happen with valid credentials)");
        println!("  • Connection refused (server may have crashed!)");
        println!("  → Document these and investigate server logs");
        println!();
        println!("Red Flags (DEFINITE bugs):");
        println!("  • Thread panics in the Rust client");
        println!("  • Test hangs/freezes (deadlock)");
        println!("  • Jobs in impossible states (check with manual inspection)");
        println!("  • Memory usage growing unbounded (memory leak)");
        println!("  • Server crashes");
        println!();
        println!("Success Rates (context matters):");
        println!("  • Updates: Should be ~99% (most reliable operation)");
        println!("  • Lower success rates are OK with extreme contention");
        println!("  • The goal is NOT high success rates - it's finding bugs!");
        println!();
        println!("RECOMMENDED DESTRUCTIVE TEST SCENARIOS");
        println!("---------------------------------------");
        println!("Run these tests to try to BREAK the code:");
        println!();
        println!("1. Extreme Contention Attack (try to cause race conditions):");
        println!(
            "   cargo run --example inventory_stress_rapid_state_changes -- -t 1000 -j 5 --min-delay 0 --max-delay 1 -d 120"
        );
        println!("   → 200 threads per job, near-zero delays");
        println!("   → GOAL: Expose race conditions, deadlocks, state corruption");
        println!();
        println!("2. Resource Exhaustion Test (try to crash the server):");
        println!(
            "   cargo run --example inventory_stress_rapid_state_changes -- -t 2000 -j 100 --min-delay 0 --max-delay 1 -d 300"
        );
        println!("   → Maximum thread count, fast operations");
        println!("   → GOAL: Find connection limits, memory leaks, resource exhaustion");
        println!();
        println!("3. Sustained Torture Test (find memory leaks):");
        println!(
            "   cargo run --example inventory_stress_rapid_state_changes -- -t 500 -j 50 --min-delay 1 --max-delay 5 -d 3600"
        );
        println!("   → 1 hour of continuous heavy load");
        println!("   → GOAL: Expose memory leaks, file descriptor leaks, gradual degradation");
        println!();
        println!("4. State Chaos Test (break state machine logic):");
        println!(
            "   cargo run --example inventory_stress_rapid_state_changes -- -t 500 -j 10 --min-delay 0 --max-delay 2 -d 180"
        );
        println!("   → 50 threads per job rapidly changing states");
        println!("   → GOAL: Find state machine bugs, inconsistent transitions");
        println!();
        println!("5. Rapid Fire Test (overwhelm the API):");
        println!(
            "   cargo run --example inventory_stress_rapid_state_changes -- -t 1000 -j 50 --min-delay 0 --max-delay 0 -d 60"
        );
        println!("   → Zero delays = maximum possible throughput");
        println!("   → GOAL: Find rate limiting issues, queue overflows, dropped requests");
        println!();
        println!("6. Mixed Load Chaos (unpredictable patterns):");
        println!(
            "   cargo run --example inventory_stress_rapid_state_changes -- -t 800 -j 30 --min-delay 0 --max-delay 50 -d 600"
        );
        println!("   → Wide delay variance creates unpredictable timing");
        println!("   → GOAL: Find timing-dependent bugs, edge cases in scheduling");
        println!();
        println!("MAKING THE TEST EVEN MORE AGGRESSIVE");
        println!("-------------------------------------");
        println!("To maximize the chance of breaking the code:");
        println!();
        println!("1. Increase Contention (expose race conditions):");
        println!("   - DECREASE --jobs (fewer jobs = more threads per job)");
        println!("   - INCREASE --threads (more concurrent operations)");
        println!("   - Remove --bucket-groups (force all threads to compete)");
        println!("   → Example: -t 2000 -j 5 (400 threads per job!)");
        println!();
        println!("2. Maximize Throughput (overwhelm the server):");
        println!("   - Set --min-delay 0 --max-delay 0 (no delays)");
        println!("   - Increase --threads to maximum your system can handle");
        println!("   → Example: -t 5000 --min-delay 0 --max-delay 0");
        println!();
        println!("3. Increase Test Duration (find memory leaks):");
        println!("   - Run for hours: -d 7200 (2 hours) or -d 14400 (4 hours)");
        println!("   - Monitor server memory usage over time");
        println!("   → Goal: Find leaks that only appear after extended runtime");
        println!();
        println!("4. Chaos Mode (unpredictable timing):");
        println!("   - Wide delay range: --min-delay 0 --max-delay 100");
        println!("   - Creates unpredictable operation timing");
        println!("   → Exposes timing-dependent bugs");
        println!();
        println!("WHAT TO LOOK FOR (SIGNS OF SUCCESS IN BREAKING CODE)");
        println!("-----------------------------------------------------");
        println!("✓ Thread Panics/Crashes:");
        println!("  → EXCELLENT! You found a concurrency bug!");
        println!("  → Save the stack trace and report it");
        println!("  → Try to reproduce with same parameters");
        println!();
        println!("✓ Hangs or Freezes:");
        println!("  → GREAT! Possible deadlock or livelock");
        println!("  → This should NEVER happen - it's a critical bug");
        println!("  → Take thread dumps to identify the deadlock");
        println!();
        println!("✓ Data Corruption/Inconsistent State:");
        println!("  → JACKPOT! This is exactly what we're looking for");
        println!("  → Document the exact sequence that led to it");
        println!("  → Check if jobs are in impossible states");
        println!();
        println!("✓ Server Crashes:");
        println!("  → PERFECT! You overwhelmed the server");
        println!("  → Check server logs for panic/crash details");
        println!("  → This reveals resource exhaustion or server bugs");
        println!();
        println!("✓ Unexpected Errors (not state conflicts):");
        println!("  → GOOD! Network timeouts, 500 errors, etc.");
        println!("  → These reveal error handling issues");
        println!("  → Some errors are OK, but watch for patterns");
        println!();
        println!("✗ Test Completes Successfully:");
        println!("  → Code is robust! (but try even more aggressive tests)");
        println!();
        println!("BASIC TROUBLESHOOTING (if test won't run)");
        println!("------------------------------------------");
        println!("• All Operations Failing: Server not running or wrong credentials");
        println!("• Immediate Crash: Check --server URL is correct");
        println!("• Won't Start: Verify MinIO server is accessible");
        println!();
        println!("METRICS REFERENCE");
        println!("-----------------");
        println!("Each operation type shows: X ok / Y invalid / Z err");
        println!();
        println!("• Updates: Should have ~99% success (most reliable)");
        println!("• Gets: Should have 70-90% success");
        println!("• Status Checks: Should have 70-90% success");
        println!("• Deletes: 50-80% success (conflicts during recreation)");
        println!("• Suspends: 30-60% success (high contention)");
        println!("• Resumes: 10-30% success (must be suspended first)");
        println!("• Cancels: 30-50% success (only works on running jobs)");
        println!();
        println!("SAFETY NOTES");
        println!("------------");
        println!("• Test buckets are named 'state-stress-N' and can be safely deleted");
        println!("• Report bucket is 'state-stress-reports'");
        println!("• The tool cleans up job configs but not buckets");
        println!("• Running against production systems is NOT recommended");
        println!("• High thread counts can temporarily overload the server");
        println!();
        println!(
            "For basic usage, run: cargo run --example inventory_stress_rapid_state_changes -- --help"
        );
        println!();
    }

    fn print_help() {
        println!("S3 Inventory Heavy Stress Test");
        println!();
        println!("USAGE:");
        println!("    cargo run --example inventory_stress_rapid_state_changes -- [OPTIONS]");
        println!();
        println!("OPTIONS:");
        println!("    -j, --jobs <NUM>          Number of inventory jobs to create (default: 10)");
        println!("    -t, --threads <NUM>       Number of concurrent threads (default: 20)");
        println!("    -d, --duration <SECS>     Test duration in seconds (default: 120)");
        println!("    -o, --objects <NUM>       Objects per bucket (default: 25)");
        println!(
            "    --min-delay <MS>          Minimum delay between operations in ms (default: 1)"
        );
        println!(
            "    --max-delay <MS>          Maximum delay between operations in ms (default: 10)"
        );
        println!("    -g, --bucket-groups <N>   Divide buckets into N groups to reduce contention");
        println!("                              Each thread group works on its own bucket subset");
        println!("    -s, --server <URL>        MinIO server URL (default: http://localhost:9000)");
        println!("    --access-key <KEY>        Access key (default: minioadmin)");
        println!("    --secret-key <KEY>        Secret key (default: minioadmin)");
        println!("    -h, --help                Print this help message");
        println!("    -i, --info                Show detailed documentation and usage guide");
        println!();
        println!("EXAMPLES:");
        println!("    # Run with 100 threads and 20 jobs (high contention)");
        println!("    cargo run --example inventory_stress_rapid_state_changes -- -t 100 -j 20");
        println!();
        println!("    # Divide into 5 groups to reduce contention");
        println!(
            "    cargo run --example inventory_stress_rapid_state_changes -- -t 100 -j 20 -g 5"
        );
        println!("    # Result: 20 threads per group, 4 jobs per group");
        println!();
        println!("    # Quick 30-second test with 50 threads");
        println!("    cargo run --example inventory_stress_rapid_state_changes -- -t 50 -d 30");
        println!();
        println!("    # Extreme stress: 200 threads, minimal delays, 10 groups");
        println!(
            "    cargo run --example inventory_stress_rapid_state_changes -- -t 200 -g 10 --min-delay 0 --max-delay 5"
        );
        println!();
        println!("For detailed documentation and recommended test scenarios:");
        println!("    cargo run --example inventory_stress_rapid_state_changes -- --info");
    }
}

#[derive(Debug, Clone, Copy)]
enum ControlOperation {
    Suspend,
    Resume,
    CheckStatus,
    Cancel,
    UpdateConfig,
    DeleteAndRecreate,
    GetConfig,
}

struct StressMetrics {
    suspends_succeeded: AtomicU64,
    suspends_invalid: AtomicU64,
    suspends_failed: AtomicU64,
    resumes_succeeded: AtomicU64,
    resumes_invalid: AtomicU64,
    resumes_failed: AtomicU64,
    status_checks_succeeded: AtomicU64,
    status_checks_failed: AtomicU64,
    cancels_succeeded: AtomicU64,
    cancels_invalid: AtomicU64,
    cancels_failed: AtomicU64,
    updates_succeeded: AtomicU64,
    updates_failed: AtomicU64,
    deletes_succeeded: AtomicU64,
    deletes_failed: AtomicU64,
    gets_succeeded: AtomicU64,
    gets_failed: AtomicU64,
    start_time: Instant,
}

impl StressMetrics {
    fn new() -> Self {
        Self {
            suspends_succeeded: AtomicU64::new(0),
            suspends_invalid: AtomicU64::new(0),
            suspends_failed: AtomicU64::new(0),
            resumes_succeeded: AtomicU64::new(0),
            resumes_invalid: AtomicU64::new(0),
            resumes_failed: AtomicU64::new(0),
            status_checks_succeeded: AtomicU64::new(0),
            status_checks_failed: AtomicU64::new(0),
            cancels_succeeded: AtomicU64::new(0),
            cancels_invalid: AtomicU64::new(0),
            cancels_failed: AtomicU64::new(0),
            updates_succeeded: AtomicU64::new(0),
            updates_failed: AtomicU64::new(0),
            deletes_succeeded: AtomicU64::new(0),
            deletes_failed: AtomicU64::new(0),
            gets_succeeded: AtomicU64::new(0),
            gets_failed: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }

    fn print_progress(&self) {
        let elapsed = self.start_time.elapsed().as_secs();
        println!("\n[{:>3}s] Progress Report:", elapsed);
        println!(
            "  Suspends:  {} ok / {} invalid / {} err",
            self.suspends_succeeded.load(Ordering::Relaxed),
            self.suspends_invalid.load(Ordering::Relaxed),
            self.suspends_failed.load(Ordering::Relaxed)
        );
        println!(
            "  Resumes:   {} ok / {} invalid / {} err",
            self.resumes_succeeded.load(Ordering::Relaxed),
            self.resumes_invalid.load(Ordering::Relaxed),
            self.resumes_failed.load(Ordering::Relaxed)
        );
        println!(
            "  Cancels:   {} ok / {} invalid / {} err",
            self.cancels_succeeded.load(Ordering::Relaxed),
            self.cancels_invalid.load(Ordering::Relaxed),
            self.cancels_failed.load(Ordering::Relaxed)
        );
        println!(
            "  Status:    {} ok / {} err",
            self.status_checks_succeeded.load(Ordering::Relaxed),
            self.status_checks_failed.load(Ordering::Relaxed)
        );
        println!(
            "  Updates:   {} ok / {} err",
            self.updates_succeeded.load(Ordering::Relaxed),
            self.updates_failed.load(Ordering::Relaxed)
        );
        println!(
            "  Deletes:   {} ok / {} err",
            self.deletes_succeeded.load(Ordering::Relaxed),
            self.deletes_failed.load(Ordering::Relaxed)
        );
        println!(
            "  Gets:      {} ok / {} err",
            self.gets_succeeded.load(Ordering::Relaxed),
            self.gets_failed.load(Ordering::Relaxed)
        );
    }

    fn print_summary(&self) {
        let elapsed = self.start_time.elapsed().as_secs_f64();

        println!("\n=== Final Statistics ===");
        println!("Test Duration: {:.2}s\n", elapsed);

        let total_ops = self.suspends_succeeded.load(Ordering::Relaxed)
            + self.suspends_invalid.load(Ordering::Relaxed)
            + self.suspends_failed.load(Ordering::Relaxed)
            + self.resumes_succeeded.load(Ordering::Relaxed)
            + self.resumes_invalid.load(Ordering::Relaxed)
            + self.resumes_failed.load(Ordering::Relaxed)
            + self.status_checks_succeeded.load(Ordering::Relaxed)
            + self.status_checks_failed.load(Ordering::Relaxed)
            + self.cancels_succeeded.load(Ordering::Relaxed)
            + self.cancels_invalid.load(Ordering::Relaxed)
            + self.cancels_failed.load(Ordering::Relaxed)
            + self.updates_succeeded.load(Ordering::Relaxed)
            + self.updates_failed.load(Ordering::Relaxed)
            + self.deletes_succeeded.load(Ordering::Relaxed)
            + self.deletes_failed.load(Ordering::Relaxed)
            + self.gets_succeeded.load(Ordering::Relaxed)
            + self.gets_failed.load(Ordering::Relaxed);

        let total_success = self.suspends_succeeded.load(Ordering::Relaxed)
            + self.resumes_succeeded.load(Ordering::Relaxed)
            + self.status_checks_succeeded.load(Ordering::Relaxed)
            + self.cancels_succeeded.load(Ordering::Relaxed)
            + self.updates_succeeded.load(Ordering::Relaxed)
            + self.deletes_succeeded.load(Ordering::Relaxed)
            + self.gets_succeeded.load(Ordering::Relaxed);

        let total_invalid = self.suspends_invalid.load(Ordering::Relaxed)
            + self.resumes_invalid.load(Ordering::Relaxed)
            + self.cancels_invalid.load(Ordering::Relaxed);

        let total_errors = self.suspends_failed.load(Ordering::Relaxed)
            + self.resumes_failed.load(Ordering::Relaxed)
            + self.status_checks_failed.load(Ordering::Relaxed)
            + self.cancels_failed.load(Ordering::Relaxed)
            + self.updates_failed.load(Ordering::Relaxed)
            + self.deletes_failed.load(Ordering::Relaxed)
            + self.gets_failed.load(Ordering::Relaxed);

        println!("Total Operations: {}", total_ops);
        println!("Operations/sec: {:.2}", total_ops as f64 / elapsed);
        println!(
            "Success Rate: {:.1}% ({} valid, {} invalid transitions, {} errors)\n",
            (total_success as f64 / total_ops as f64) * 100.0,
            total_success,
            total_invalid,
            total_errors
        );

        self.print_progress();
    }
}

async fn control_thread_task(
    client: MinioClient,
    admin: MinioAdminClient,
    job_info: Vec<(BucketName, String)>,
    thread_id: usize,
    metrics: Arc<StressMetrics>,
    stop_signal: Arc<AtomicBool>,
    config: StressConfig,
) {
    let mut operation_count = 0;

    // Calculate which jobs this thread can access based on bucket groups
    let (job_start, job_end) = if let Some(num_groups) = config.bucket_groups {
        let threads_per_group = config.num_threads.div_ceil(num_groups);
        let jobs_per_group = job_info.len().div_ceil(num_groups);
        let group_id = thread_id / threads_per_group;
        let start = group_id * jobs_per_group;
        let end = ((group_id + 1) * jobs_per_group).min(job_info.len());
        (start, end)
    } else {
        // No grouping: access all jobs
        (0, job_info.len())
    };

    while !stop_signal.load(Ordering::Relaxed) {
        let (operation, job_idx, sleep_ms) = {
            let mut rng = rand::rng();
            let op = match rng.random_range(0..7) {
                0 => ControlOperation::Suspend,
                1 => ControlOperation::Resume,
                2 => ControlOperation::CheckStatus,
                3 => ControlOperation::Cancel,
                4 => ControlOperation::UpdateConfig,
                5 => ControlOperation::DeleteAndRecreate,
                _ => ControlOperation::GetConfig,
            };
            // Select job only from this thread's assigned range
            let idx = rng.random_range(job_start..job_end);
            let sleep = rng.random_range(config.min_delay_ms..=config.max_delay_ms);
            (op, idx, sleep)
        };

        let (bucket, job_id) = &job_info[job_idx];

        match operation {
            ControlOperation::Suspend => {
                match admin
                    .suspend_inventory_job(bucket, job_id)
                    .unwrap()
                    .build()
                    .send()
                    .await
                {
                    Ok(resp) => {
                        let control: Result<AdminControlJson, _> = resp.admin_control();
                        match control {
                            Ok(ctrl) => {
                                metrics.suspends_succeeded.fetch_add(1, Ordering::Relaxed);
                                if operation_count % 100 == 0 {
                                    println!(
                                        "[Thread {}] Suspended {} - Status: {:?}",
                                        thread_id, job_id, ctrl.status
                                    );
                                }
                            }
                            Err(e) => {
                                let err_msg = format!("{:?}", e);
                                if err_msg.contains("already suspended")
                                    || err_msg.contains("AlreadySuspended")
                                {
                                    metrics.suspends_invalid.fetch_add(1, Ordering::Relaxed);
                                } else {
                                    metrics.suspends_failed.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        let err_msg = format!("{:?}", e);
                        if err_msg.contains("already suspended")
                            || err_msg.contains("AlreadySuspended")
                        {
                            metrics.suspends_invalid.fetch_add(1, Ordering::Relaxed);
                        } else {
                            metrics.suspends_failed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }

            ControlOperation::Resume => {
                match admin
                    .resume_inventory_job(bucket, job_id)
                    .unwrap()
                    .build()
                    .send()
                    .await
                {
                    Ok(resp) => {
                        let control: Result<AdminControlJson, _> = resp.admin_control();
                        match control {
                            Ok(ctrl) => {
                                metrics.resumes_succeeded.fetch_add(1, Ordering::Relaxed);
                                if operation_count % 100 == 0 {
                                    println!(
                                        "[Thread {}] Resumed {} - Status: {:?}",
                                        thread_id, job_id, ctrl.status
                                    );
                                }
                            }
                            Err(e) => {
                                let err_msg = format!("{:?}", e);
                                if err_msg.contains("already resumed")
                                    || err_msg.contains("not suspended")
                                {
                                    metrics.resumes_invalid.fetch_add(1, Ordering::Relaxed);
                                } else {
                                    metrics.resumes_failed.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        let err_msg = format!("{:?}", e);
                        if err_msg.contains("already resumed") || err_msg.contains("not suspended")
                        {
                            metrics.resumes_invalid.fetch_add(1, Ordering::Relaxed);
                        } else {
                            metrics.resumes_failed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }

            ControlOperation::CheckStatus => {
                let builder = match client.get_inventory_job_status(bucket.clone(), job_id) {
                    Ok(b) => b,
                    Err(_) => continue,
                };
                match builder.build().send().await {
                    Ok(resp) => {
                        let status: Result<JobStatus, _> = resp.status();
                        match status {
                            Ok(_) => {
                                metrics
                                    .status_checks_succeeded
                                    .fetch_add(1, Ordering::Relaxed);
                            }
                            Err(_) => {
                                metrics.status_checks_failed.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    Err(_) => {
                        metrics.status_checks_failed.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }

            ControlOperation::Cancel => {
                match admin
                    .cancel_inventory_job(bucket, job_id)
                    .unwrap()
                    .build()
                    .send()
                    .await
                {
                    Ok(resp) => {
                        let control: Result<AdminControlJson, _> = resp.admin_control();
                        match control {
                            Ok(_) => {
                                metrics.cancels_succeeded.fetch_add(1, Ordering::Relaxed);
                                if operation_count % 100 == 0 {
                                    println!("[Thread {}] Cancelled {}", thread_id, job_id);
                                }
                            }
                            Err(e) => {
                                let err_msg = format!("{:?}", e);
                                if err_msg.contains("not running")
                                    || err_msg.contains("NoRunningJob")
                                {
                                    metrics.cancels_invalid.fetch_add(1, Ordering::Relaxed);
                                } else {
                                    metrics.cancels_failed.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        let err_msg = format!("{:?}", e);
                        if err_msg.contains("not running") || err_msg.contains("NoRunningJob") {
                            metrics.cancels_invalid.fetch_add(1, Ordering::Relaxed);
                        } else {
                            metrics.cancels_failed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }

            ControlOperation::UpdateConfig => {
                let updated_job = JobDefinition {
                    api_version: "v1".to_string(),
                    id: job_id.clone(),
                    destination: DestinationSpec {
                        bucket: "state-stress-reports".to_string(),
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

                let builder = match client.put_inventory_config(bucket.clone(), job_id, updated_job)
                {
                    Ok(b) => b,
                    Err(_) => continue,
                };
                match builder.build().send().await {
                    Ok(_) => {
                        metrics.updates_succeeded.fetch_add(1, Ordering::Relaxed);
                        if operation_count % 100 == 0 {
                            println!("[Thread {}] Updated config for {}", thread_id, job_id);
                        }
                    }
                    Err(_) => {
                        metrics.updates_failed.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }

            ControlOperation::DeleteAndRecreate => {
                let builder = match client.delete_inventory_config(bucket.clone(), job_id) {
                    Ok(b) => b,
                    Err(_) => continue,
                };
                match builder.build().send().await {
                    Ok(_) => {
                        metrics.deletes_succeeded.fetch_add(1, Ordering::Relaxed);

                        tokio::time::sleep(Duration::from_millis(100)).await;

                        let new_job = JobDefinition {
                            api_version: "v1".to_string(),
                            id: job_id.clone(),
                            destination: DestinationSpec {
                                bucket: "state-stress-reports".to_string(),
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

                        if let Ok(builder) =
                            client.put_inventory_config(bucket.clone(), job_id, new_job)
                        {
                            let _ = builder.build().send().await;
                        }

                        if operation_count % 100 == 0 {
                            println!("[Thread {}] Deleted and recreated {}", thread_id, job_id);
                        }
                    }
                    Err(_) => {
                        metrics.deletes_failed.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }

            ControlOperation::GetConfig => {
                let builder = match client.get_inventory_config(bucket.clone(), job_id) {
                    Ok(b) => b,
                    Err(_) => continue,
                };
                match builder.build().send().await {
                    Ok(_) => {
                        metrics.gets_succeeded.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        metrics.gets_failed.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }

        operation_count += 1;

        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
    }

    println!(
        "[Thread {}] Completed {} operations",
        thread_id, operation_count
    );
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = StressConfig::from_args();

    println!("=== S3 Inventory HEAVY Stress Test: Rapid State Transitions ===\n");
    println!("Configuration:");
    println!("  Jobs to manage:       {}", config.num_jobs);
    println!("  Control threads:      {}", config.num_threads);
    println!("  Test duration:        {} seconds", config.duration_secs);
    println!("  Objects per bucket:   {}", config.objects_per_bucket);
    println!(
        "  Operation delay:      {}-{}ms",
        config.min_delay_ms, config.max_delay_ms
    );
    println!("  Server URL:           {}", config.server_url);

    if let Some(num_groups) = config.bucket_groups {
        let threads_per_group = config.num_threads.div_ceil(num_groups);
        let jobs_per_group = config.num_jobs.div_ceil(num_groups);
        println!("  Bucket groups:        {} groups", num_groups);
        println!("    → ~{} threads per group", threads_per_group);
        println!("    → ~{} jobs per group", jobs_per_group);
        println!(
            "    → Contention: ~{:.1} threads/job",
            threads_per_group as f64 / jobs_per_group as f64
        );
    } else {
        println!("  Bucket groups:        None (all threads access all jobs)");
        println!(
            "    → Contention: ~{:.1} threads/job (high)",
            config.num_threads as f64 / config.num_jobs as f64
        );
    }

    let expected_ops_per_sec = config.num_threads as f64
        * (1000.0 / ((config.min_delay_ms + config.max_delay_ms) as f64 / 2.0));
    println!(
        "  Expected throughput:  ~{:.0} ops/sec\n",
        expected_ops_per_sec
    );

    let base_url = config.server_url.parse::<BaseUrl>()?;
    let static_provider = StaticProvider::new(&config.access_key, &config.secret_key, None);
    let client = MinioClient::new(base_url.clone(), Some(static_provider.clone()), None, None)?;

    let dest_bucket = BucketName::new("state-stress-reports").unwrap();
    println!("Step 1: Creating destination bucket...");
    let _ = client
        .create_bucket(dest_bucket.clone())
        .unwrap()
        .build()
        .send()
        .await; // Ignore if bucket already exists

    let mut job_info = Vec::new();

    println!(
        "\nStep 2: Creating {} test buckets with inventory jobs...",
        config.num_jobs
    );
    for i in 0..config.num_jobs {
        let bucket = BucketName::new(format!("state-stress-{}", i)).unwrap();
        let job_id = format!("job-{}", i);

        let _ = client.create_bucket(bucket.clone()).unwrap().build().send().await; // Ignore if bucket already exists

        for j in 0..config.objects_per_bucket {
            let object_name = format!("test-object-{:04}.dat", j);
            let content = vec![b'X'; 512];
            let object_content = ObjectContent::from(content);

            let _ = client
                .put_object_content(
                    bucket.clone(),
                    ObjectKey::new(&object_name).unwrap(),
                    object_content,
                )
                .unwrap()
                .build()
                .send()
                .await;
        }

        // Delete any existing job config first
        if let Ok(builder) = client.delete_inventory_config(bucket.clone(), &job_id) {
            let _ = builder.build().send().await;
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

        client
            .put_inventory_config(bucket.clone(), &job_id, job)?
            .build()
            .send()
            .await?;

        job_info.push((bucket.clone(), job_id.clone()));
        println!("  Created bucket '{}' with job '{}'", bucket, job_id);
    }

    println!("\nStep 3: Starting rapid state transition test...\n");
    let metrics = Arc::new(StressMetrics::new());
    let stop_signal = Arc::new(AtomicBool::new(false));
    let mut tasks = JoinSet::new();

    for thread_id in 0..config.num_threads {
        let client_clone =
            MinioClient::new(base_url.clone(), Some(static_provider.clone()), None, None)?;
        let admin_clone = client_clone.admin();
        let job_info_clone = job_info.clone();
        let metrics_clone = Arc::clone(&metrics);
        let stop_signal_clone = Arc::clone(&stop_signal);
        let config_clone = config.clone();

        tasks.spawn(async move {
            control_thread_task(
                client_clone,
                admin_clone,
                job_info_clone,
                thread_id,
                metrics_clone,
                stop_signal_clone,
                config_clone,
            )
            .await;
        });
    }

    let progress_metrics = Arc::clone(&metrics);
    let progress_interval = if config.num_threads >= 50 { 10 } else { 20 };
    let progress_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(progress_interval));
        loop {
            interval.tick().await;
            progress_metrics.print_progress();
        }
    });

    println!(
        "Running stress test for {} seconds...",
        config.duration_secs
    );
    tokio::time::sleep(Duration::from_secs(config.duration_secs)).await;

    println!("\n\nStopping all threads...");
    stop_signal.store(true, Ordering::Relaxed);

    while let Some(result) = tasks.join_next().await {
        if let Err(e) = result {
            eprintln!("Thread panicked: {}", e);
        }
    }

    progress_handle.abort();

    println!("\n=== All Threads Stopped ===");
    metrics.print_summary();

    println!("\n=== Stress Test Completed Successfully ===");

    Ok(())
}

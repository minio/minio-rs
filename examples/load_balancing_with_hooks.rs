// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2024 MinIO, Inc.
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

//! # Load Balancing with Request Hooks Example
//!
//! This example demonstrates how to implement client-side load balancing for MinIO
//! using the RequestHooks infrastructure. It shows multiple load balancing strategies
//! and how to monitor their effectiveness.
//!
//! ## Key Concepts
//!
//! 1. **Request Hooks**: Intercept and modify requests before they're signed and sent
//! 2. **Load Balancing**: Distribute requests across multiple MinIO nodes
//! 3. **Health Checking**: Track node availability and response times
//! 4. **Telemetry**: Monitor load distribution and performance
//! 5. **Redirect Headers**: When URL is modified, automatic headers are added:
//!    - `x-minio-redirect-from`: Original target URL
//!    - `x-minio-redirect-to`: New destination URL after load balancing
//!
//! ## Usage
//!
//! ```bash
//! # Set up your MinIO cluster nodes
//! export MINIO_NODES="node1.minio.local:9000,node2.minio.local:9000,node3.minio.local:9000"
//! export MINIO_ROOT_USER="minioadmin"
//! export MINIO_ROOT_PASSWORD="minioadmin"
//!
//! # Run with round-robin strategy (default)
//! cargo run --example load_balancing_with_hooks
//!
//! # Run with least-connections strategy
//! cargo run --example load_balancing_with_hooks -- --strategy least-connections
//!
//! # Run with weighted round-robin
//! cargo run --example load_balancing_with_hooks -- --strategy weighted
//! ```

use clap::{Parser, ValueEnum};
use http::{Extensions, Method};
use minio::s3::client::{MinioClientBuilder, RequestHooks};
use minio::s3::creds::StaticProvider;
use minio::s3::error::Error;
use minio::s3::http::{BaseUrl, Url};
use minio::s3::multimap_ext::Multimap;
use minio::s3::segmented_bytes::SegmentedBytes;
use minio::s3::types::{BucketName, ObjectKey, S3Api, ToStream};
use reqwest::Response;
use std::env;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Command-line arguments for the load balancing example
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Load balancing strategy to use
    #[arg(short, long, value_enum, default_value = "round-robin")]
    strategy: LoadBalanceStrategy,

    /// Number of requests to make for testing
    #[arg(short = 'n', long, default_value = "100")]
    requests: usize,

    /// Enable verbose debug output
    #[arg(short, long)]
    verbose: bool,
}

/// Available load balancing strategies
#[derive(Clone, Copy, Debug, ValueEnum)]
enum LoadBalanceStrategy {
    /// Simple round-robin: cycles through nodes sequentially
    RoundRobin,
    /// Least connections: routes to the node with fewest active connections
    LeastConnections,
    /// Weighted round-robin: distributes based on node weights/capacity
    Weighted,
    /// Random selection: randomly picks a node for each request
    Random,
}

/// Represents a MinIO node in the cluster with its health and performance metrics
#[derive(Debug, Clone)]
struct Node {
    /// The hostname or IP address of the node
    host: String,
    /// The port number (typically 9000 for MinIO)
    port: u16,
    /// Whether to use HTTPS for this node
    https: bool,
    /// Current number of active connections to this node
    active_connections: Arc<AtomicUsize>,
    /// Total number of requests sent to this node
    total_requests: Arc<AtomicU64>,
    /// Total response time in milliseconds for all requests
    total_response_time_ms: Arc<AtomicU64>,
    /// Number of failed requests to this node
    failed_requests: Arc<AtomicU64>,
    /// Weight for weighted round-robin (higher = more traffic)
    weight: u32,
    /// Whether the node is currently healthy
    is_healthy: Arc<RwLock<bool>>,
    /// Last time the node was checked for health
    last_health_check: Arc<RwLock<Instant>>,
}

impl Node {
    /// Creates a new node from a host:port string
    ///
    /// # Arguments
    /// * `host_port` - String in format "hostname:port" or just "hostname" (defaults to port 9000)
    /// * `https` - Whether to use HTTPS for this node
    /// * `weight` - Weight for weighted round-robin (default 1)
    fn new(host_port: &str, https: bool, weight: u32) -> Self {
        let parts: Vec<&str> = host_port.split(':').collect();
        let (host, port) = if parts.len() == 2 {
            (parts[0].to_string(), parts[1].parse().unwrap_or(9000))
        } else {
            (host_port.to_string(), 9000)
        };

        Self {
            host,
            port,
            https,
            active_connections: Arc::new(AtomicUsize::new(0)),
            total_requests: Arc::new(AtomicU64::new(0)),
            total_response_time_ms: Arc::new(AtomicU64::new(0)),
            failed_requests: Arc::new(AtomicU64::new(0)),
            weight,
            is_healthy: Arc::new(RwLock::new(true)),
            last_health_check: Arc::new(RwLock::new(Instant::now())),
        }
    }

    /// Returns the current number of active connections
    fn get_active_connections(&self) -> usize {
        self.active_connections.load(Ordering::Relaxed)
    }

    /// Returns whether the node is currently considered healthy
    fn is_healthy(&self) -> bool {
        *self.is_healthy.read().unwrap()
    }

    /// Calculates the average response time in milliseconds
    fn average_response_time_ms(&self) -> f64 {
        let total_requests = self.total_requests.load(Ordering::Relaxed);
        if total_requests == 0 {
            0.0
        } else {
            self.total_response_time_ms.load(Ordering::Relaxed) as f64 / total_requests as f64
        }
    }

    /// Updates the node's health status based on recent failures
    fn update_health_status(&self) {
        let total = self.total_requests.load(Ordering::Relaxed);
        let failed = self.failed_requests.load(Ordering::Relaxed);

        // Mark unhealthy if more than 50% of recent requests failed
        let is_healthy = if total > 10 {
            (failed as f64 / total as f64) < 0.5
        } else {
            true // Not enough data to determine
        };

        *self.is_healthy.write().unwrap() = is_healthy;
        *self.last_health_check.write().unwrap() = Instant::now();
    }
}

/// Main load balancer hook that implements various load balancing strategies
///
/// This hook intercepts all S3 requests and redirects them to different nodes
/// based on the selected strategy. It also tracks metrics for monitoring.
#[derive(Debug, Clone)]
struct LoadBalancerHook {
    /// List of available MinIO nodes
    nodes: Arc<Vec<Node>>,
    /// Selected load balancing strategy
    strategy: LoadBalanceStrategy,
    /// Counter for round-robin strategy
    round_robin_counter: Arc<AtomicUsize>,
    /// Whether to print verbose debug information
    verbose: bool,
}

impl LoadBalancerHook {
    /// Creates a new load balancer hook
    ///
    /// # Arguments
    /// * `nodes` - List of MinIO nodes to balance between
    /// * `strategy` - Load balancing strategy to use
    /// * `verbose` - Whether to print debug information
    fn new(nodes: Vec<Node>, strategy: LoadBalanceStrategy, verbose: bool) -> Self {
        if nodes.is_empty() {
            panic!("At least one node must be provided");
        }

        Self {
            nodes: Arc::new(nodes),
            strategy,
            round_robin_counter: Arc::new(AtomicUsize::new(0)),
            verbose,
        }
    }

    /// Selects the next node based on the configured strategy
    ///
    /// # Returns
    /// The selected node, or None if no healthy nodes are available
    fn select_node(&self) -> Option<Node> {
        // Filter to only healthy nodes
        let healthy_nodes: Vec<Node> = self
            .nodes
            .iter()
            .filter(|n| n.is_healthy())
            .cloned()
            .collect();

        if healthy_nodes.is_empty() {
            // If no healthy nodes, try all nodes
            if self.verbose {
                println!("WARNING: No healthy nodes available, using all nodes");
            }
            return self.select_from_nodes(&self.nodes);
        }

        self.select_from_nodes(&healthy_nodes)
    }

    /// Internal method to select from a given set of nodes
    fn select_from_nodes(&self, nodes: &[Node]) -> Option<Node> {
        if nodes.is_empty() {
            return None;
        }

        match self.strategy {
            LoadBalanceStrategy::RoundRobin => {
                // Simple round-robin: cycle through nodes sequentially
                let index = self.round_robin_counter.fetch_add(1, Ordering::SeqCst) % nodes.len();
                Some(nodes[index].clone())
            }

            LoadBalanceStrategy::LeastConnections => {
                // Select the node with the fewest active connections
                nodes
                    .iter()
                    .min_by_key(|n| n.get_active_connections())
                    .cloned()
            }

            LoadBalanceStrategy::Weighted => {
                // Weighted round-robin: distribute based on node weights
                // Build a weighted list where nodes appear multiple times based on weight
                let mut weighted_nodes = Vec::new();
                for node in nodes {
                    for _ in 0..node.weight {
                        weighted_nodes.push(node.clone());
                    }
                }

                if weighted_nodes.is_empty() {
                    Some(nodes[0].clone())
                } else {
                    let index = self.round_robin_counter.fetch_add(1, Ordering::SeqCst)
                        % weighted_nodes.len();
                    Some(weighted_nodes[index].clone())
                }
            }

            LoadBalanceStrategy::Random => {
                // Random selection using a simple pseudo-random approach
                let seed = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as usize;
                let index = seed % nodes.len();
                Some(nodes[index].clone())
            }
        }
    }

    /// Prints current statistics for all nodes
    fn print_stats(&self) {
        println!("\n=== Load Balancer Statistics ===");
        println!("Strategy: {:?}", self.strategy);
        println!("\nNode Statistics:");

        for (i, node) in self.nodes.iter().enumerate() {
            let total = node.total_requests.load(Ordering::Relaxed);
            let failed = node.failed_requests.load(Ordering::Relaxed);
            let active = node.get_active_connections();
            let avg_time = node.average_response_time_ms();
            let health_status = if node.is_healthy() {
                "HEALTHY"
            } else {
                "UNHEALTHY"
            };

            println!("\nNode {}: {}:{}", i + 1, node.host, node.port);
            println!("  Status: {}", health_status);
            println!("  Weight: {}", node.weight);
            println!("  Total Requests: {}", total);
            println!("  Failed Requests: {}", failed);
            println!("  Active Connections: {}", active);
            println!("  Avg Response Time: {:.2} ms", avg_time);

            if total > 0 {
                let success_rate = ((total - failed) as f64 / total as f64) * 100.0;
                println!("  Success Rate: {:.1}%", success_rate);
            }
        }
        println!("\n================================");
    }
}

#[async_trait::async_trait]
impl RequestHooks for LoadBalancerHook {
    fn name(&self) -> &'static str {
        "load-balancer"
    }

    /// Called before the request is signed - this is where we redirect to selected node
    async fn before_signing_mut(
        &self,
        method: &Method,
        url: &mut Url,
        _region: &str,
        _headers: &mut Multimap,
        _query_params: &Multimap,
        bucket_name: Option<&str>,
        object_name: Option<&str>,
        _body: Option<&SegmentedBytes>,
        extensions: &mut Extensions,
    ) -> Result<(), Error> {
        // Select the target node
        let node = self
            .select_node()
            .ok_or_else(|| Error::Validation(minio::s3::error::ValidationErr::MissingBucketName))?;

        // Store the selected node and start time in extensions for later use
        extensions.insert(node.clone());
        extensions.insert(Instant::now());

        // Increment active connections for this node
        node.active_connections.fetch_add(1, Ordering::SeqCst);
        node.total_requests.fetch_add(1, Ordering::SeqCst);

        // Update the URL to point to the selected node
        let original_host = url.host.clone();
        url.host = node.host.clone();
        url.port = node.port;
        url.https = node.https;

        // Note: When we modify the URL here, the MinIO client will automatically add
        // x-minio-redirect-from and x-minio-redirect-to headers to track the redirection
        // for server-side telemetry and debugging.

        if self.verbose {
            println!(
                "[{}] Routing {} request to {}:{} (was: {})",
                chrono::Local::now().format("%H:%M:%S%.3f"),
                method,
                node.host,
                node.port,
                original_host
            );

            if let Some(bucket) = bucket_name {
                print!(" - Bucket: {}", bucket);
                if let Some(object) = object_name {
                    print!(", Object: {}", object);
                }
                println!();
            }

            println!(
                "  Active connections on this node: {}",
                node.get_active_connections()
            );
        }

        Ok(())
    }

    /// Called after the request completes - update metrics and health status
    async fn after_execute(
        &self,
        method: &Method,
        _url: &Url,
        _region: &str,
        _headers: &Multimap,
        _query_params: &Multimap,
        _bucket_name: Option<&str>,
        _object_name: Option<&str>,
        resp: &Result<Response, reqwest::Error>,
        extensions: &mut Extensions,
    ) {
        // Retrieve the node and start time from extensions
        if let Some(node) = extensions.get::<Node>() {
            // Decrement active connections
            node.active_connections.fetch_sub(1, Ordering::SeqCst);

            // Calculate response time
            if let Some(start_time) = extensions.get::<Instant>() {
                let duration = start_time.elapsed();
                let response_time_ms = duration.as_millis() as u64;
                node.total_response_time_ms
                    .fetch_add(response_time_ms, Ordering::SeqCst);

                if self.verbose {
                    let status_str = match resp {
                        Ok(response) => format!("HTTP {}", response.status().as_u16()),
                        Err(err) => format!("Error: {}", err),
                    };

                    println!(
                        "[{}] Response from {}:{} - {} - {} - {:?}",
                        chrono::Local::now().format("%H:%M:%S%.3f"),
                        node.host,
                        node.port,
                        method,
                        status_str,
                        duration
                    );
                }
            }

            // Track failures
            if resp.is_err() || resp.as_ref().unwrap().status().is_server_error() {
                node.failed_requests.fetch_add(1, Ordering::SeqCst);

                // Update health status if too many failures
                node.update_health_status();

                if !node.is_healthy() && self.verbose {
                    println!(
                        "WARNING: Node {}:{} marked as UNHEALTHY due to high failure rate",
                        node.host, node.port
                    );
                }
            }
        }
    }
}

/// Example telemetry hook that works alongside the load balancer
///
/// This demonstrates how multiple hooks can work together to provide
/// comprehensive monitoring and logging.
#[derive(Debug)]
struct TelemetryHook {
    request_count: Arc<AtomicU64>,
    verbose: bool,
}

impl TelemetryHook {
    fn new(verbose: bool) -> Self {
        Self {
            request_count: Arc::new(AtomicU64::new(0)),
            verbose,
        }
    }
}

#[async_trait::async_trait]
impl RequestHooks for TelemetryHook {
    fn name(&self) -> &'static str {
        "telemetry"
    }

    async fn before_signing_mut(
        &self,
        _method: &Method,
        _url: &mut Url,
        _region: &str,
        _headers: &mut Multimap,
        _query_params: &Multimap,
        _bucket_name: Option<&str>,
        _object_name: Option<&str>,
        _body: Option<&SegmentedBytes>,
        _extensions: &mut Extensions,
    ) -> Result<(), Error> {
        let count = self.request_count.fetch_add(1, Ordering::SeqCst) + 1;

        if self.verbose && count.is_multiple_of(10) {
            println!("📊 Total requests processed: {}", count);
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse command-line arguments
    let args = Args::parse();

    println!("🚀 MinIO Load Balancing Example");
    println!("================================\n");

    // Get MinIO nodes from environment or use defaults
    let nodes_str = env::var("MINIO_NODES")
        .unwrap_or_else(|_| "localhost:9000,localhost:9001,localhost:9002".to_string());

    println!("Configuring nodes from: {}", nodes_str);

    // Parse nodes and create Node instances
    let mut nodes: Vec<Node> = Vec::new();
    for (i, node_str) in nodes_str.split(',').enumerate() {
        // For weighted strategy, give different weights to nodes
        let weight = if matches!(args.strategy, LoadBalanceStrategy::Weighted) {
            // Example: first node gets weight 3, second gets 2, others get 1
            match i {
                0 => 3,
                1 => 2,
                _ => 1,
            }
        } else {
            1
        };

        let node = Node::new(node_str.trim(), false, weight);
        println!(
            "  - Node {}: {}:{} (weight: {})",
            i + 1,
            node.host,
            node.port,
            weight
        );
        nodes.push(node);
    }

    if nodes.is_empty() {
        eprintln!("❌ Error: No nodes configured. Set MINIO_NODES environment variable.");
        std::process::exit(1);
    }

    println!("\nStrategy: {:?}", args.strategy);
    println!("Requests to make: {}", args.requests);
    println!("Verbose mode: {}\n", args.verbose);

    // Get credentials from environment
    let access_key = env::var("MINIO_ROOT_USER")
        .or_else(|_| env::var("MINIO_ACCESS_KEY"))
        .unwrap_or_else(|_| "minioadmin".to_string());

    let secret_key = env::var("MINIO_ROOT_PASSWORD")
        .or_else(|_| env::var("MINIO_SECRET_KEY"))
        .unwrap_or_else(|_| "minioadmin".to_string());

    // Create the load balancer hook
    let load_balancer = LoadBalancerHook::new(nodes.clone(), args.strategy, args.verbose);

    // Create the telemetry hook
    let telemetry = TelemetryHook::new(args.verbose);

    // Use the first node as the initial base URL (will be overridden by the hook)
    let base_url = BaseUrl::from_str(&format!("http://{}:{}", nodes[0].host, nodes[0].port))?;

    // Build the MinIO client with our hooks
    println!("Building MinIO client with load balancing hooks...");
    let client = MinioClientBuilder::new(base_url)
        .provider(Some(StaticProvider::new(&access_key, &secret_key, None)))
        .hook(Arc::new(load_balancer.clone()))
        .hook(Arc::new(telemetry))
        .build()?;

    println!("✅ Client configured successfully\n");

    // Create a test bucket name
    let test_bucket = format!("load-balance-test-{}", chrono::Utc::now().timestamp());

    println!("Creating test bucket: {}", test_bucket);

    // Try to create the bucket (might fail if it exists, that's ok)
    match client
        .create_bucket(BucketName::new(&test_bucket)?)
        .build()
        .send()
        .await
    {
        Ok(_) => println!("✅ Bucket created successfully"),
        Err(e) => {
            // Check if it's because the bucket already exists
            if e.to_string().contains("BucketAlreadyOwnedByYou")
                || e.to_string().contains("BucketAlreadyExists")
            {
                println!("ℹ️  Bucket already exists, continuing...");
            } else {
                println!("⚠️  Could not create bucket: {}", e);
                println!("    Continuing with tests anyway...");
            }
        }
    }

    println!("\nStarting load balanced requests...\n");

    // Perform test requests
    let start_time = Instant::now();
    let mut success_count = 0;
    let mut failure_count = 0;

    for i in 0..args.requests {
        // Mix of different operations to simulate real load
        let operation = i % 4;

        let result = match operation {
            0 => {
                // List buckets
                client.list_buckets().build().send().await.map(|_| ())
            }
            1 => {
                // Check if bucket exists
                client
                    .bucket_exists(BucketName::new(&test_bucket)?)
                    .build()
                    .send()
                    .await
                    .map(|_| ())
            }
            2 => {
                // Stat a non-existent object (will fail but that's ok)
                client
                    .stat_object(
                        BucketName::new(&test_bucket)?,
                        ObjectKey::new(format!("test-object-{}", i))?,
                    )
                    .build()
                    .send()
                    .await
                    .map(|_| ())
            }
            3 => {
                // List objects in bucket (just check if we can start the stream)
                drop(
                    client
                        .list_objects(BucketName::new(&test_bucket)?)
                        .build()
                        .to_stream(),
                );
                Ok(())
            }
            _ => unreachable!(),
        };

        match result {
            Ok(_) => success_count += 1,
            Err(_) => failure_count += 1,
        }

        // Small delay between requests to avoid overwhelming the servers
        if i < args.requests - 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Print progress
        if !args.verbose && (i + 1) % 10 == 0 {
            print!(".");
            use std::io::Write;
            std::io::stdout().flush().unwrap();
        }
    }

    if !args.verbose {
        println!(); // New line after progress dots
    }

    let total_duration = start_time.elapsed();

    println!("\n✅ Load testing complete!");
    println!("\n=== Final Results ===");
    println!("Total time: {:?}", total_duration);
    println!("Total requests: {}", args.requests);
    println!("Successful: {}", success_count);
    println!("Failed: {}", failure_count);
    println!(
        "Success rate: {:.1}%",
        (success_count as f64 / args.requests as f64) * 100.0
    );
    println!(
        "Requests/sec: {:.2}",
        args.requests as f64 / total_duration.as_secs_f64()
    );

    // Print detailed statistics from the load balancer
    load_balancer.print_stats();

    // Clean up: try to delete the test bucket
    println!("\nCleaning up test bucket...");
    match client
        .delete_bucket(BucketName::new(&test_bucket)?)
        .build()
        .send()
        .await
    {
        Ok(_) => println!("✅ Test bucket deleted"),
        Err(e) => println!("ℹ️  Could not delete test bucket: {}", e),
    }

    println!("\n🎉 Example complete!");

    Ok(())
}

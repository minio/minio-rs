// Profiling script to quantify overhead in minio-rs vs object_store
// Run with: cargo run --example profile_overhead --release

use futures::StreamExt;
use minio::s3::client::MinioClientBuilder;
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;
use minio::s3::types::{BucketName, ObjectKey, S3Api};
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

const ITERATIONS: usize = 50;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint =
        std::env::var("MINIO_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".to_string());
    let access_key = std::env::var("MINIO_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".to_string());
    let secret_key = std::env::var("MINIO_SECRET_KEY").unwrap_or_else(|_| "minioadmin".to_string());
    let bucket_str = std::env::var("MINIO_BUCKET").unwrap_or_else(|_| "benchmark-test".to_string());
    let bucket: BucketName = BucketName::new(&bucket_str).expect("Invalid bucket name");
    let object_str = "test_data.parquet";
    let object: ObjectKey = ObjectKey::new(object_str).expect("Invalid object key");

    println!("Profiling overhead breakdown");
    println!("============================");
    println!("Endpoint: {}", endpoint);
    println!("Bucket: {}", bucket_str);
    println!("Object: {}", object_str);
    println!("Iterations: {}", ITERATIONS);
    println!();

    // Create minio-rs client
    let base_url: BaseUrl = endpoint.parse()?;
    let provider = StaticProvider::new(&access_key, &secret_key, None);
    let minio_client = Arc::new(
        MinioClientBuilder::new(base_url.clone())
            .provider(Some(provider.clone()))
            .skip_region_lookup(true)
            .build()?,
    );

    // Create object_store client
    let object_store = AmazonS3Builder::new()
        .with_endpoint(&endpoint)
        .with_bucket_name(&bucket_str)
        .with_access_key_id(&access_key)
        .with_secret_access_key(&secret_key)
        .with_allow_http(true)
        .build()?;

    // Warm up connections
    println!("Warming up connections...");
    for _ in 0..3 {
        let _ = minio_client
            .get_object(bucket.clone(), object.clone())
            .build()
            .send()
            .await;
        let _ = object_store.get(&Path::from(object_str)).await;
    }

    println!();
    println!("=== MINIO-RS BREAKDOWN ===");
    profile_minio_rs(&minio_client, &bucket, &object).await?;

    println!();
    println!("=== OBJECT_STORE BREAKDOWN ===");
    profile_object_store(&object_store, object_str).await?;

    println!();
    println!("=== FAST PATH COMPARISON ===");
    profile_fast_path(&minio_client, &bucket, &object).await?;

    let object_store: Arc<dyn ObjectStore> = Arc::new(object_store);
    profile_range_requests(&minio_client, &object_store, &bucket, &object, object_str).await?;

    profile_parallel_requests(&minio_client, &object_store, &bucket, &object, object_str).await?;

    profile_head_requests(&minio_client, &object_store, &bucket, &object, object_str).await?;

    Ok(())
}

async fn profile_head_requests(
    minio_client: &Arc<minio::s3::MinioClient>,
    object_store: &Arc<dyn ObjectStore>,
    bucket: &BucketName,
    object: &ObjectKey,
    object_str: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    const HEAD_ITERATIONS: usize = 50;

    println!();
    println!("=== HEAD REQUEST COMPARISON ({}x) ===", HEAD_ITERATIONS);

    let path = Path::from(object_str);

    // minio-rs HEAD requests (stat_object)
    let mut minio_times = Vec::with_capacity(HEAD_ITERATIONS);
    for _ in 0..HEAD_ITERATIONS {
        let start = Instant::now();
        let _ = minio_client
            .stat_object(bucket.clone(), object.clone())
            .build()
            .send()
            .await?;
        minio_times.push(start.elapsed());
    }

    // object_store HEAD requests
    let mut os_times = Vec::with_capacity(HEAD_ITERATIONS);
    for _ in 0..HEAD_ITERATIONS {
        let start = Instant::now();
        let _ = object_store.head(&path).await?;
        os_times.push(start.elapsed());
    }

    println!(
        "  minio-rs:          {:>8.2}ms per request",
        avg_ms(&minio_times)
    );
    println!(
        "  object_store:      {:>8.2}ms per request",
        avg_ms(&os_times)
    );
    println!(
        "  Ratio:             {:>8.2}x",
        avg_ms(&minio_times) / avg_ms(&os_times)
    );

    Ok(())
}

async fn profile_minio_rs(
    client: &Arc<minio::s3::MinioClient>,
    bucket: &BucketName,
    object: &ObjectKey,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut total_times = Vec::with_capacity(ITERATIONS);
    let mut build_times = Vec::with_capacity(ITERATIONS);
    let mut send_times = Vec::with_capacity(ITERATIONS);
    let mut content_times = Vec::with_capacity(ITERATIONS);
    let mut stream_times = Vec::with_capacity(ITERATIONS);

    for _ in 0..ITERATIONS {
        // Measure build time
        let start = Instant::now();
        let builder = client.get_object(bucket.clone(), object.clone()).build();
        let build_time = start.elapsed();
        build_times.push(build_time);

        // Measure send time (network + signing)
        let start = Instant::now();
        let response = builder.send().await?;
        let send_time = start.elapsed();
        send_times.push(send_time);

        // Measure content extraction time
        let start = Instant::now();
        let content = response.content()?;
        let content_time = start.elapsed();
        content_times.push(content_time);

        // Measure stream consumption time
        let start = Instant::now();
        let (mut stream, _) = content.to_stream().await?;
        let mut _total_bytes = 0usize;
        while let Some(chunk) = stream.next().await {
            _total_bytes += chunk?.len();
        }
        let stream_time = start.elapsed();
        stream_times.push(stream_time);

        total_times.push(build_time + send_time + content_time + stream_time);
    }

    println!(
        "  Build request:     {:>8.2}ms (builder pattern)",
        avg_ms(&build_times)
    );
    println!(
        "  Send request:      {:>8.2}ms (sign + HTTP)",
        avg_ms(&send_times)
    );
    println!(
        "  Extract content:   {:>8.2}ms (ObjectContent)",
        avg_ms(&content_times)
    );
    println!(
        "  Stream consume:    {:>8.2}ms (read all bytes)",
        avg_ms(&stream_times)
    );
    println!("  --------------------------");
    println!("  TOTAL:             {:>8.2}ms", avg_ms(&total_times));

    Ok(())
}

async fn profile_object_store(
    store: &impl ObjectStore,
    object: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut total_times = Vec::with_capacity(ITERATIONS);
    let mut get_times = Vec::with_capacity(ITERATIONS);
    let mut stream_times = Vec::with_capacity(ITERATIONS);

    let path = Path::from(object);

    for _ in 0..ITERATIONS {
        // Measure get time (includes network)
        let start = Instant::now();
        let result = store.get(&path).await?;
        let get_time = start.elapsed();
        get_times.push(get_time);

        // Measure stream consumption time
        let start = Instant::now();
        let bytes = result.bytes().await?;
        let _ = bytes.len();
        let stream_time = start.elapsed();
        stream_times.push(stream_time);

        total_times.push(get_time + stream_time);
    }

    println!(
        "  Get request:       {:>8.2}ms (sign + HTTP)",
        avg_ms(&get_times)
    );
    println!(
        "  Stream consume:    {:>8.2}ms (read all bytes)",
        avg_ms(&stream_times)
    );
    println!("  --------------------------");
    println!("  TOTAL:             {:>8.2}ms", avg_ms(&total_times));

    Ok(())
}

async fn profile_fast_path(
    client: &Arc<minio::s3::MinioClient>,
    bucket: &BucketName,
    object: &ObjectKey,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut fast_times = Vec::with_capacity(ITERATIONS);
    let mut fast_stream_times = Vec::with_capacity(ITERATIONS);

    println!("Testing get_object_fast() path:");

    for _ in 0..ITERATIONS {
        // Measure fast path request time
        let start = Instant::now();
        let response = client
            .get_object_fast(bucket.as_str(), object.as_str(), None)
            .await?;
        let request_time = start.elapsed();
        fast_times.push(request_time);

        // Measure stream consumption (same as minio-rs standard)
        let start = Instant::now();
        let mut stream = response.bytes_stream();
        let mut _total_bytes = 0usize;
        while let Some(chunk) = stream.next().await {
            _total_bytes += chunk?.len();
        }
        let stream_time = start.elapsed();
        fast_stream_times.push(stream_time);
    }

    println!(
        "  Fast request:      {:>8.2}ms (sign + HTTP)",
        avg_ms(&fast_times)
    );
    println!(
        "  Stream consume:    {:>8.2}ms (read all bytes)",
        avg_ms(&fast_stream_times)
    );
    println!("  --------------------------");
    let total: f64 = avg_ms(&fast_times) + avg_ms(&fast_stream_times);
    println!("  TOTAL:             {:>8.2}ms", total);

    Ok(())
}

async fn profile_range_requests(
    minio_client: &Arc<minio::s3::MinioClient>,
    object_store: &Arc<dyn ObjectStore>,
    bucket: &BucketName,
    object: &ObjectKey,
    object_str: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    const RANGE_ITERATIONS: usize = 100;
    const RANGE_SIZE: u64 = 64 * 1024; // 64KB chunks (typical Parquet row group read)

    println!();
    println!(
        "=== RANGE REQUEST COMPARISON ({}x {}KB reads) ===",
        RANGE_ITERATIONS,
        RANGE_SIZE / 1024
    );

    let path = Path::from(object_str);

    // minio-rs range requests
    let mut minio_times = Vec::with_capacity(RANGE_ITERATIONS);
    for i in 0..RANGE_ITERATIONS {
        let offset = (i as u64 * RANGE_SIZE) % (20 * 1024 * 1024); // Cycle through first 20MB
        let start = Instant::now();
        let response = minio_client
            .get_object(bucket.clone(), object.clone())
            .offset(offset)
            .length(RANGE_SIZE)
            .build()
            .send()
            .await?;
        let content = response.content()?;
        let _ = content.to_segmented_bytes().await?;
        minio_times.push(start.elapsed());
    }

    // object_store range requests
    let mut os_times = Vec::with_capacity(RANGE_ITERATIONS);
    for i in 0..RANGE_ITERATIONS {
        let offset = ((i * RANGE_SIZE as usize) % (20 * 1024 * 1024)) as u64;
        let start = Instant::now();
        let _ = object_store
            .get_range(&path, offset..offset + RANGE_SIZE)
            .await?;
        os_times.push(start.elapsed());
    }

    println!(
        "  minio-rs:          {:>8.2}ms per request",
        avg_ms(&minio_times)
    );
    println!(
        "  object_store:      {:>8.2}ms per request",
        avg_ms(&os_times)
    );
    println!(
        "  Ratio:             {:>8.2}x",
        avg_ms(&minio_times) / avg_ms(&os_times)
    );

    Ok(())
}

async fn profile_parallel_requests(
    minio_client: &Arc<minio::s3::MinioClient>,
    object_store: &Arc<dyn ObjectStore>,
    bucket: &BucketName,
    object: &ObjectKey,
    object_str: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    const PARALLEL_COUNT: usize = 10;
    const RANGE_SIZE: u64 = 64 * 1024;

    println!();
    println!(
        "=== PARALLEL REQUEST COMPARISON ({} concurrent requests) ===",
        PARALLEL_COUNT
    );
    let path = Path::from(object_str);

    // minio-rs parallel requests - using into_bytes() for direct comparison
    let start = Instant::now();
    let mut handles = Vec::with_capacity(PARALLEL_COUNT);
    for i in 0..PARALLEL_COUNT {
        let client = minio_client.clone();
        let bucket_clone = bucket.clone();
        let object_clone = object.clone();
        let offset = (i as u64 * RANGE_SIZE) % (20 * 1024 * 1024);
        handles.push(tokio::spawn(async move {
            if let Ok(response) = client
                .get_object(bucket_clone, object_clone)
                .offset(offset)
                .length(RANGE_SIZE)
                .build()
                .send()
                .await
            {
                // Properly consume the response body using into_bytes()
                let _ = response.into_bytes().await;
            }
        }));
    }
    for handle in handles {
        let _ = handle.await;
    }
    let minio_time = start.elapsed();

    // object_store parallel requests
    let start = Instant::now();
    let mut handles = Vec::with_capacity(PARALLEL_COUNT);
    for i in 0..PARALLEL_COUNT {
        let store = object_store.clone();
        let path = path.clone();
        let offset = ((i * RANGE_SIZE as usize) % (20 * 1024 * 1024)) as u64;
        handles.push(tokio::spawn(async move {
            let _ = store.get_range(&path, offset..offset + RANGE_SIZE).await;
        }));
    }
    for handle in handles {
        let _ = handle.await;
    }
    let os_time = start.elapsed();

    println!(
        "  minio-rs total:    {:>8.2}ms",
        minio_time.as_secs_f64() * 1000.0
    );
    println!(
        "  object_store total: {:>7.2}ms",
        os_time.as_secs_f64() * 1000.0
    );
    println!(
        "  Ratio:             {:>8.2}x",
        minio_time.as_secs_f64() / os_time.as_secs_f64()
    );

    Ok(())
}

fn avg_ms(times: &[Duration]) -> f64 {
    times.iter().map(|d| d.as_secs_f64() * 1000.0).sum::<f64>() / times.len() as f64
}

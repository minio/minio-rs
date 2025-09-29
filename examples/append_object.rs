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

mod common;

use crate::common::create_bucket_if_not_exists;
use minio::s3::MinioClient;
use minio::s3::response::a_response_traits::HasObjectSize;
use minio::s3::response::{AppendObjectResponse, StatObjectResponse};
use minio::s3::segmented_bytes::SegmentedBytes;
use minio::s3::types::S3Api;
use rand::Rng;
use rand::distr::Alphanumeric;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::init(); // Note: set environment variable RUST_LOG="INFO" to log info and higher
    let client: MinioClient = MinioClient::create_client_on_localhost()?;

    if !client.is_minio_express().await {
        println!("Need (MinIO) Express mode to run this example");
        return Ok(());
    }

    let bucket_name: &str = "append-test-bucket";
    create_bucket_if_not_exists(bucket_name, &client).await?;

    let object_name: &str = "append-test-object";

    let n_segments = 1000;
    let segment_size = 1024 * 1024; // 1 KB
    let mut offset_bytes = 0;

    for i in 0..n_segments {
        let rand_str: String = random_string(segment_size);

        let data_size = rand_str.len() as u64;
        let data: SegmentedBytes = SegmentedBytes::from(rand_str);

        let resp: AppendObjectResponse = client
            .append_object(bucket_name, object_name, data, offset_bytes)
            .build()
            .send()
            .await?;

        offset_bytes += data_size;
        if resp.object_size() != offset_bytes {
            panic!(
                "from the append_object: size mismatch: expected {}, got {offset_bytes}",
                resp.object_size(),
            )
        }
        //println!("Append response: {resp:#?}");

        let resp: StatObjectResponse = client
            .stat_object(bucket_name, object_name)
            .build()
            .send()
            .await?;
        if resp.size()? != offset_bytes {
            panic!(
                "from the stat_Object: size mismatch: expected {}, got {offset_bytes}",
                resp.size()?,
            )
        }
        println!("{i}/{n_segments}");
        //println!("Stat response: {resp:#?}");
    }

    Ok(())
}

fn random_string(len: usize) -> String {
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

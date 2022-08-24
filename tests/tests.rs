// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2022 MinIO, Inc.
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

use rand::distributions::{Alphanumeric, DistString};
use std::io::BufReader;

use minio::s3::args::*;
use minio::s3::client::Client;
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;
use minio::s3::types::{
    CsvInputSerialization, CsvOutputSerialization, DeleteObject, FileHeaderInfo, QuoteFields,
    SelectRequest,
};

struct RandReader {
    size: usize,
}

impl RandReader {
    fn new(size: usize) -> RandReader {
        RandReader { size: size }
    }
}

impl std::io::Read for RandReader {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
        let bytes_read = match self.size > buf.len() {
            true => buf.len(),
            false => self.size,
        };

        if bytes_read > 0 {
            let random: &mut dyn rand::RngCore = &mut rand::thread_rng();
            random.fill_bytes(&mut buf[0..bytes_read]);
        }

        self.size -= bytes_read;

        Ok(bytes_read)
    }
}

fn rand_bucket_name() -> String {
    Alphanumeric
        .sample_string(&mut rand::thread_rng(), 8)
        .to_lowercase()
}

fn rand_object_name() -> String {
    Alphanumeric.sample_string(&mut rand::thread_rng(), 8)
}

struct ClientTest<'a> {
    client: &'a Client<'a>,
    test_bucket: String,
}

impl<'a> ClientTest<'a> {
    fn new(client: &'a Client<'_>, test_bucket: &'a str) -> ClientTest<'a> {
        ClientTest {
            client: client,
            test_bucket: test_bucket.to_string(),
        }
    }

    async fn bucket_exists(&self) {
        let bucket_name = rand_bucket_name();
        self.client
            .make_bucket(&MakeBucketArgs::new(&bucket_name).unwrap())
            .await
            .unwrap();
        let exists = self
            .client
            .bucket_exists(&BucketExistsArgs::new(&bucket_name).unwrap())
            .await
            .unwrap();
        assert_eq!(exists, true);
        self.client
            .remove_bucket(&RemoveBucketArgs::new(&bucket_name).unwrap())
            .await
            .unwrap();
    }

    async fn list_buckets(&self) {
        let mut names: Vec<String> = Vec::new();
        for _ in 1..=3 {
            names.push(rand_bucket_name());
        }

        for b in names.iter() {
            self.client
                .make_bucket(&MakeBucketArgs::new(&b).unwrap())
                .await
                .unwrap();
        }

        let mut count = 0;
        let resp = self
            .client
            .list_buckets(&ListBucketsArgs::new())
            .await
            .unwrap();
        for bucket in resp.buckets.iter() {
            if names.contains(&bucket.name) {
                count += 1;
            }
        }
        assert_eq!(count, 3);

        for b in names.iter() {
            self.client
                .remove_bucket(&RemoveBucketArgs::new(&b).unwrap())
                .await
                .unwrap();
        }
    }

    async fn put_object(&self) {
        let object_name = rand_object_name();
        let size = 16_usize;
        self.client
            .put_object(
                &mut PutObjectArgs::new(
                    &self.test_bucket,
                    &object_name,
                    &mut RandReader::new(size),
                    Some(size),
                    None,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let resp = self
            .client
            .stat_object(&StatObjectArgs::new(&self.test_bucket, &object_name).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.bucket_name, self.test_bucket);
        assert_eq!(resp.object_name, object_name);
        assert_eq!(resp.size, size);
        self.client
            .remove_object(&RemoveObjectArgs::new(&self.test_bucket, &object_name).unwrap())
            .await
            .unwrap();
    }

    async fn put_object_multipart(&self) {
        let object_name = rand_object_name();
        let size: usize = 16 + 5 * 1024 * 1024;
        self.client
            .put_object(
                &mut PutObjectArgs::new(
                    &self.test_bucket,
                    &object_name,
                    &mut RandReader::new(size),
                    Some(size),
                    None,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let resp = self
            .client
            .stat_object(&StatObjectArgs::new(&self.test_bucket, &object_name).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.bucket_name, self.test_bucket);
        assert_eq!(resp.object_name, object_name);
        assert_eq!(resp.size, size);
        self.client
            .remove_object(&RemoveObjectArgs::new(&self.test_bucket, &object_name).unwrap())
            .await
            .unwrap();
    }

    async fn get_object(&self) {
        let object_name = rand_object_name();
        let data = "hello, world";
        self.client
            .put_object(
                &mut PutObjectArgs::new(
                    &self.test_bucket,
                    &object_name,
                    &mut BufReader::new(data.as_bytes()),
                    Some(data.len()),
                    None,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let resp = self
            .client
            .get_object(&GetObjectArgs::new(&self.test_bucket, &object_name).unwrap())
            .await
            .unwrap();
        let got = resp.text().await.unwrap();
        assert_eq!(got, data);
        self.client
            .remove_object(&RemoveObjectArgs::new(&self.test_bucket, &object_name).unwrap())
            .await
            .unwrap();
    }

    async fn remove_objects(&self) {
        let bucket_name = rand_bucket_name();
        self.client
            .make_bucket(&MakeBucketArgs::new(&bucket_name).unwrap())
            .await
            .unwrap();

        let mut names: Vec<String> = Vec::new();
        for _ in 1..=3 {
            let object_name = rand_object_name();
            let size = 0_usize;
            self.client
                .put_object(
                    &mut PutObjectArgs::new(
                        &self.test_bucket,
                        &object_name,
                        &mut RandReader::new(size),
                        Some(size),
                        None,
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
            names.push(object_name);
        }
        let mut objects: Vec<DeleteObject> = Vec::new();
        for name in names.iter() {
            objects.push(DeleteObject {
                name: &name,
                version_id: None,
            });
        }

        self.client
            .remove_objects(
                &mut RemoveObjectsArgs::new(&self.test_bucket, &mut objects.iter()).unwrap(),
            )
            .await
            .unwrap();

        self.client
            .remove_bucket(&RemoveBucketArgs::new(&bucket_name).unwrap())
            .await
            .unwrap();
    }

    async fn list_objects(&self) {
        let bucket_name = rand_bucket_name();
        self.client
            .make_bucket(&MakeBucketArgs::new(&bucket_name).unwrap())
            .await
            .unwrap();

        let mut names: Vec<String> = Vec::new();
        for _ in 1..=3 {
            let object_name = rand_object_name();
            let size = 0_usize;
            self.client
                .put_object(
                    &mut PutObjectArgs::new(
                        &self.test_bucket,
                        &object_name,
                        &mut RandReader::new(size),
                        Some(size),
                        None,
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
            names.push(object_name);
        }

        self.client
            .list_objects(
                &mut ListObjectsArgs::new(&self.test_bucket, &|items| {
                    for item in items.iter() {
                        assert_eq!(names.contains(&item.name), true);
                    }
                    true
                })
                .unwrap(),
            )
            .await
            .unwrap();

        let mut objects: Vec<DeleteObject> = Vec::new();
        for name in names.iter() {
            objects.push(DeleteObject {
                name: &name,
                version_id: None,
            });
        }

        self.client
            .remove_objects(
                &mut RemoveObjectsArgs::new(&self.test_bucket, &mut objects.iter()).unwrap(),
            )
            .await
            .unwrap();

        self.client
            .remove_bucket(&RemoveBucketArgs::new(&bucket_name).unwrap())
            .await
            .unwrap();
    }

    async fn select_object_content(&self) {
        let object_name = rand_object_name();
        let mut data = String::new();
        data.push_str("1997,Ford,E350,\"ac, abs, moon\",3000.00\n");
        data.push_str("1999,Chevy,\"Venture \"\"Extended Edition\"\"\",,4900.00\n");
        data.push_str("1999,Chevy,\"Venture \"\"Extended Edition, Very Large\"\"\",,5000.00\n");
        data.push_str("1996,Jeep,Grand Cherokee,\"MUST SELL!\n");
        data.push_str("air, moon roof, loaded\",4799.00\n");
        let body = String::from("Year,Make,Model,Description,Price\n") + &data;

        self.client
            .put_object(
                &mut PutObjectArgs::new(
                    &self.test_bucket,
                    &object_name,
                    &mut BufReader::new(body.as_bytes()),
                    Some(body.len()),
                    None,
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let request = SelectRequest::new_csv_input_output(
            "select * from S3Object",
            CsvInputSerialization {
                compression_type: None,
                allow_quoted_record_delimiter: false,
                comments: None,
                field_delimiter: None,
                file_header_info: Some(FileHeaderInfo::USE),
                quote_character: None,
                quote_escape_character: None,
                record_delimiter: None,
            },
            CsvOutputSerialization {
                field_delimiter: None,
                quote_character: None,
                quote_escape_character: None,
                quote_fields: Some(QuoteFields::ASNEEDED),
                record_delimiter: None,
            },
        )
        .unwrap();
        let mut resp = self
            .client
            .select_object_content(
                &SelectObjectContentArgs::new(&self.test_bucket, &object_name, &request).unwrap(),
            )
            .await
            .unwrap();
        let mut got = String::new();
        let mut buf = [0_u8; 512];
        loop {
            let size = resp.read(&mut buf).await.unwrap();
            if size == 0 {
                break;
            }
            got += &String::from_utf8(buf[..size].to_vec()).unwrap();
        }
        assert_eq!(got, data);
        self.client
            .remove_object(&RemoveObjectArgs::new(&self.test_bucket, &object_name).unwrap())
            .await
            .unwrap();
    }
}

#[tokio::main]
#[test]
async fn s3_tests() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let host = std::env::var("SERVER_ENDPOINT")?;
    let access_key = std::env::var("ACCESS_KEY")?;
    let secret_key = std::env::var("SECRET_KEY")?;
    let secure = std::env::var("ENABLE_HTTPS").is_ok();
    let region = std::env::var("SERVER_REGION").ok();

    let mut burl = BaseUrl::from_string(host).unwrap();
    burl.https = secure;
    if let Some(v) = region {
        burl.region = v;
    }

    let provider = StaticProvider::new(&access_key, &secret_key, None);
    let client = Client::new(burl.clone(), Some(&provider));

    let test_bucket = rand_bucket_name();
    client
        .make_bucket(&MakeBucketArgs::new(&test_bucket).unwrap())
        .await
        .unwrap();

    let ctest = ClientTest::new(&client, &test_bucket);

    println!("make_bucket() + bucket_exists() + remove_bucket()");
    ctest.bucket_exists().await;

    println!("list_buckets()");
    ctest.list_buckets().await;

    println!("put_object() + stat_object() + remove_object()");
    ctest.put_object().await;

    println!("[Multipart] put_object()");
    ctest.put_object_multipart().await;

    println!("get_object()");
    ctest.get_object().await;

    println!("remove_objects()");
    ctest.remove_objects().await;

    println!("list_objects()");
    ctest.list_objects().await;

    println!("select_object_content()");
    ctest.select_object_content().await;

    client
        .remove_bucket(&RemoveBucketArgs::new(&test_bucket).unwrap())
        .await
        .unwrap();

    Ok(())
}

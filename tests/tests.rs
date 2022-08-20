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

use async_std::task;
use minio::s3::types::NotificationRecords;
use rand::distributions::{Alphanumeric, DistString};
use std::io::BufReader;
use tokio::sync::mpsc;

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
    base_url: BaseUrl,
    access_key: String,
    secret_key: String,
    ignore_cert_check: bool,
    ssl_cert_file: String,
    client: Client<'a>,
    test_bucket: String,
}

impl<'a> ClientTest<'_> {
    fn new(
        base_url: BaseUrl,
        access_key: String,
        secret_key: String,
        static_provider: &'a StaticProvider,
        ignore_cert_check: bool,
        ssl_cert_file: String,
    ) -> ClientTest<'a> {
        let mut client = Client::new(base_url.clone(), Some(static_provider));
        client.ignore_cert_check = ignore_cert_check;
        client.ssl_cert_file = ssl_cert_file.to_string();

        ClientTest {
            base_url: base_url,
            access_key: access_key,
            secret_key: secret_key,
            ignore_cert_check: ignore_cert_check,
            ssl_cert_file: ssl_cert_file,
            client: client,
            test_bucket: rand_bucket_name(),
        }
    }

    async fn init(&self) {
        self.client
            .make_bucket(&MakeBucketArgs::new(&self.test_bucket).unwrap())
            .await
            .unwrap();
    }

    async fn drop(&self) {
        self.client
            .remove_bucket(&RemoveBucketArgs::new(&self.test_bucket).unwrap())
            .await
            .unwrap();
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

    async fn listen_bucket_notification(&self) {
        let object_name = rand_object_name();

        let name = object_name.clone();
        let (sender, mut receiver): (mpsc::UnboundedSender<bool>, mpsc::UnboundedReceiver<bool>) =
            mpsc::unbounded_channel();

        let access_key = self.access_key.clone();
        let secret_key = self.secret_key.clone();
        let base_url = self.base_url.clone();
        let ignore_cert_check = self.ignore_cert_check;
        let ssl_cert_file = self.ssl_cert_file.clone();
        let test_bucket = self.test_bucket.clone();

        let listen_task = move || async move {
            let static_provider = StaticProvider::new(&access_key, &secret_key, None);
            let mut client = Client::new(base_url, Some(&static_provider));
            client.ignore_cert_check = ignore_cert_check;
            client.ssl_cert_file = ssl_cert_file;

            let event_fn = |event: NotificationRecords| {
                for record in event.records.iter() {
                    if let Some(s3) = &record.s3 {
                        if let Some(object) = &s3.object {
                            if let Some(key) = &object.key {
                                if name == *key {
                                    sender.send(true).unwrap();
                                }
                                return false;
                            }
                        }
                    }
                }
                sender.send(false).unwrap();
                return false;
            };

            let args = &ListenBucketNotificationArgs::new(&test_bucket, &event_fn).unwrap();
            client.listen_bucket_notification(&args).await.unwrap();
        };

        let spawned_task = task::spawn(listen_task());
        task::sleep(std::time::Duration::from_millis(100)).await;

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

        self.client
            .remove_object(&RemoveObjectArgs::new(&self.test_bucket, &object_name).unwrap())
            .await
            .unwrap();

        spawned_task.await;
        assert_eq!(receiver.recv().await.unwrap(), true);
    }
}

#[tokio::main]
#[test]
async fn s3_tests() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let host = std::env::var("SERVER_ENDPOINT")?;
    let access_key = std::env::var("ACCESS_KEY")?;
    let secret_key = std::env::var("SECRET_KEY")?;
    let secure = std::env::var("ENABLE_HTTPS").is_ok();
    let ssl_cert_file = std::env::var("SSL_CERT_FILE")?;
    let ignore_cert_check = std::env::var("IGNORE_CERT_CHECK").is_ok();
    let region = std::env::var("SERVER_REGION").ok();

    let mut base_url = BaseUrl::from_string(host).unwrap();
    base_url.https = secure;
    if let Some(v) = region {
        base_url.region = v;
    }

    let static_provider = StaticProvider::new(&access_key, &secret_key, None);
    let ctest = ClientTest::new(
        base_url,
        access_key,
        secret_key,
        &static_provider,
        ignore_cert_check,
        ssl_cert_file,
    );
    ctest.init().await;

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

    println!("listen_bucket_notification()");
    ctest.listen_bucket_notification().await;

    ctest.drop().await;

    Ok(())
}

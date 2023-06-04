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
use chrono::Duration;
use hyper::http::Method;
use minio::s3::types::NotificationRecords;
use rand::distributions::{Alphanumeric, DistString};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::BufReader;
use std::{fs, io};
use tokio::sync::mpsc;

use minio::s3::args::*;
use minio::s3::client::Client;
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;
use minio::s3::types::{
    CsvInputSerialization, CsvOutputSerialization, DeleteObject, FileHeaderInfo,
    NotificationConfig, ObjectLockConfig, PrefixFilterRule, QueueConfig, QuoteFields,
    RetentionMode, SelectRequest, SuffixFilterRule,
};
use minio::s3::utils::{to_iso8601utc, utc_now};

struct RandReader {
    size: usize,
}

impl RandReader {
    fn new(size: usize) -> RandReader {
        RandReader { size }
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
    const SQS_ARN: &str = "arn:minio:sqs::miniojavatest:webhook";

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
            base_url,
            access_key,
            secret_key,
            ignore_cert_check,
            ssl_cert_file,
            client,
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
                .make_bucket(&MakeBucketArgs::new(b).unwrap())
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
                .remove_bucket(&RemoveBucketArgs::new(b).unwrap())
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

    fn get_hash(filename: &String) -> String {
        let mut hasher = Sha256::new();
        let mut file = fs::File::open(filename).unwrap();
        io::copy(&mut file, &mut hasher).unwrap();
        format!("{:x}", hasher.finalize())
    }

    async fn upload_download_object(&self) {
        let object_name = rand_object_name();
        let size = 16_usize;
        let mut file = fs::File::create(&object_name).unwrap();
        io::copy(&mut RandReader::new(size), &mut file).unwrap();
        file.sync_all().unwrap();
        self.client
            .upload_object(
                &mut UploadObjectArgs::new(&self.test_bucket, &object_name, &object_name).unwrap(),
            )
            .await
            .unwrap();

        let filename = rand_object_name();
        self.client
            .download_object(
                &DownloadObjectArgs::new(&self.test_bucket, &object_name, &filename).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            ClientTest::get_hash(&object_name) == ClientTest::get_hash(&filename),
            true
        );

        fs::remove_file(&object_name).unwrap();
        fs::remove_file(&filename).unwrap();

        self.client
            .remove_object(&RemoveObjectArgs::new(&self.test_bucket, &object_name).unwrap())
            .await
            .unwrap();

        self.client
            .remove_object(&RemoveObjectArgs::new(&self.test_bucket, &object_name).unwrap())
            .await
            .unwrap();

        let object_name = rand_object_name();
        let size: usize = 16 + 5 * 1024 * 1024;
        let mut file = fs::File::create(&object_name).unwrap();
        io::copy(&mut RandReader::new(size), &mut file).unwrap();
        file.sync_all().unwrap();
        self.client
            .upload_object(
                &mut UploadObjectArgs::new(&self.test_bucket, &object_name, &object_name).unwrap(),
            )
            .await
            .unwrap();

        let filename = rand_object_name();
        self.client
            .download_object(
                &DownloadObjectArgs::new(&self.test_bucket, &object_name, &filename).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            ClientTest::get_hash(&object_name) == ClientTest::get_hash(&filename),
            true
        );

        fs::remove_file(&object_name).unwrap();
        fs::remove_file(&filename).unwrap();

        self.client
            .remove_object(&RemoveObjectArgs::new(&self.test_bucket, &object_name).unwrap())
            .await
            .unwrap();

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
                name,
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
                name,
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
                false
            };

            let args = &ListenBucketNotificationArgs::new(&test_bucket, &event_fn).unwrap();
            client.listen_bucket_notification(args).await.unwrap();
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

    async fn copy_object(&self) {
        let src_object_name = rand_object_name();

        let size = 16_usize;
        self.client
            .put_object(
                &mut PutObjectArgs::new(
                    &self.test_bucket,
                    &src_object_name,
                    &mut RandReader::new(size),
                    Some(size),
                    None,
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let object_name = rand_object_name();
        self.client
            .copy_object(
                &CopyObjectArgs::new(
                    &self.test_bucket,
                    &object_name,
                    CopySource::new(&self.test_bucket, &src_object_name).unwrap(),
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
        assert_eq!(resp.size, size);

        self.client
            .remove_object(&RemoveObjectArgs::new(&self.test_bucket, &object_name).unwrap())
            .await
            .unwrap();

        self.client
            .remove_object(&RemoveObjectArgs::new(&self.test_bucket, &src_object_name).unwrap())
            .await
            .unwrap();
    }

    async fn compose_object(&self) {
        let src_object_name = rand_object_name();

        let size = 16_usize;
        self.client
            .put_object(
                &mut PutObjectArgs::new(
                    &self.test_bucket,
                    &src_object_name,
                    &mut RandReader::new(size),
                    Some(size),
                    None,
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let mut s1 = ComposeSource::new(&self.test_bucket, &src_object_name).unwrap();
        s1.offset = Some(3);
        s1.length = Some(5);
        let mut sources: Vec<ComposeSource> = Vec::new();
        sources.push(s1);

        let object_name = rand_object_name();

        self.client
            .compose_object(
                &mut ComposeObjectArgs::new(&self.test_bucket, &object_name, &mut sources).unwrap(),
            )
            .await
            .unwrap();

        let resp = self
            .client
            .stat_object(&StatObjectArgs::new(&self.test_bucket, &object_name).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.size, 5);

        self.client
            .remove_object(&RemoveObjectArgs::new(&self.test_bucket, &object_name).unwrap())
            .await
            .unwrap();

        self.client
            .remove_object(&RemoveObjectArgs::new(&self.test_bucket, &src_object_name).unwrap())
            .await
            .unwrap();
    }

    async fn set_get_delete_bucket_notification(&self) {
        let bucket_name = rand_bucket_name();
        self.client
            .make_bucket(&MakeBucketArgs::new(&bucket_name).unwrap())
            .await
            .unwrap();

        println!("BN: {}", bucket_name);
        self.client
            .set_bucket_notification(
                &SetBucketNotificationArgs::new(
                    &bucket_name,
                    &NotificationConfig {
                        cloud_func_config_list: None,
                        queue_config_list: Some(vec![QueueConfig {
                            events: vec![
                                String::from("s3:ObjectCreated:Put"),
                                String::from("s3:ObjectCreated:Copy"),
                            ],
                            id: None,
                            prefix_filter_rule: Some(PrefixFilterRule {
                                value: String::from("images"),
                            }),
                            suffix_filter_rule: Some(SuffixFilterRule {
                                value: String::from("pg"),
                            }),
                            queue: String::from(ClientTest::SQS_ARN),
                        }]),
                        topic_config_list: None,
                    },
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let resp = self
            .client
            .get_bucket_notification(&GetBucketNotificationArgs::new(&bucket_name).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.config.queue_config_list.as_ref().unwrap().len(), 1);
        assert_eq!(
            resp.config.queue_config_list.as_ref().unwrap()[0]
                .events
                .contains(&String::from("s3:ObjectCreated:Put")),
            true
        );
        assert_eq!(
            resp.config.queue_config_list.as_ref().unwrap()[0]
                .events
                .contains(&String::from("s3:ObjectCreated:Copy")),
            true
        );
        assert_eq!(
            resp.config.queue_config_list.as_ref().unwrap()[0]
                .prefix_filter_rule
                .as_ref()
                .unwrap()
                .value,
            "images"
        );
        assert_eq!(
            resp.config.queue_config_list.as_ref().unwrap()[0]
                .suffix_filter_rule
                .as_ref()
                .unwrap()
                .value,
            "pg"
        );
        assert_eq!(
            resp.config.queue_config_list.as_ref().unwrap()[0].queue,
            ClientTest::SQS_ARN
        );

        self.client
            .delete_bucket_notification(&DeleteBucketNotificationArgs::new(&bucket_name).unwrap())
            .await
            .unwrap();

        let resp = self
            .client
            .get_bucket_notification(&GetBucketNotificationArgs::new(&bucket_name).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.config.queue_config_list.is_none(), true);

        self.client
            .remove_bucket(&RemoveBucketArgs::new(&bucket_name).unwrap())
            .await
            .unwrap();
    }

    async fn set_get_delete_bucket_policy(&self) {
        let bucket_name = rand_bucket_name();
        self.client
            .make_bucket(&MakeBucketArgs::new(&bucket_name).unwrap())
            .await
            .unwrap();

        let config = r#"
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": [
                "s3:GetObject"
            ],
            "Effect": "Allow",
            "Principal": {
                "AWS": [
                    "*"
                ]
            },
            "Resource": [
                "arn:aws:s3:::<BUCKET>/myobject*"
            ],
            "Sid": ""
        }
    ]
}
"#
        .replace("<BUCKET>", &bucket_name);

        self.client
            .set_bucket_policy(&SetBucketPolicyArgs::new(&bucket_name, &config).unwrap())
            .await
            .unwrap();

        let resp = self
            .client
            .get_bucket_policy(&GetBucketPolicyArgs::new(&bucket_name).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.config.is_empty(), false);

        self.client
            .delete_bucket_policy(&DeleteBucketPolicyArgs::new(&bucket_name).unwrap())
            .await
            .unwrap();

        let resp = self
            .client
            .get_bucket_policy(&GetBucketPolicyArgs::new(&bucket_name).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.config, "{}");

        self.client
            .remove_bucket(&RemoveBucketArgs::new(&bucket_name).unwrap())
            .await
            .unwrap();
    }

    async fn set_get_delete_bucket_tags(&self) {
        let bucket_name = rand_bucket_name();
        self.client
            .make_bucket(&MakeBucketArgs::new(&bucket_name).unwrap())
            .await
            .unwrap();

        let tags = HashMap::from([
            (String::from("Project"), String::from("Project One")),
            (String::from("User"), String::from("jsmith")),
        ]);

        self.client
            .set_bucket_tags(&SetBucketTagsArgs::new(&bucket_name, &tags).unwrap())
            .await
            .unwrap();

        let resp = self
            .client
            .get_bucket_tags(&GetBucketTagsArgs::new(&bucket_name).unwrap())
            .await
            .unwrap();
        assert_eq!(
            resp.tags.len() == tags.len() && resp.tags.keys().all(|k| tags.contains_key(k)),
            true
        );

        self.client
            .delete_bucket_tags(&DeleteBucketTagsArgs::new(&bucket_name).unwrap())
            .await
            .unwrap();

        let resp = self
            .client
            .get_bucket_tags(&GetBucketTagsArgs::new(&bucket_name).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.tags.is_empty(), true);

        self.client
            .remove_bucket(&RemoveBucketArgs::new(&bucket_name).unwrap())
            .await
            .unwrap();
    }

    async fn set_get_delete_object_lock_config(&self) {
        let bucket_name = rand_bucket_name();

        let mut args = MakeBucketArgs::new(&bucket_name).unwrap();
        args.object_lock = true;
        self.client.make_bucket(&args).await.unwrap();

        self.client
            .set_object_lock_config(
                &SetObjectLockConfigArgs::new(
                    &bucket_name,
                    &ObjectLockConfig::new(RetentionMode::GOVERNANCE, Some(7), None).unwrap(),
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let resp = self
            .client
            .get_object_lock_config(&GetObjectLockConfigArgs::new(&bucket_name).unwrap())
            .await
            .unwrap();
        assert_eq!(
            match resp.config.retention_mode {
                Some(r) => match r {
                    RetentionMode::GOVERNANCE => true,
                    _ => false,
                },
                _ => false,
            },
            true
        );

        assert_eq!(resp.config.retention_duration_days == Some(7), true);
        assert_eq!(resp.config.retention_duration_years.is_none(), true);

        self.client
            .delete_object_lock_config(&DeleteObjectLockConfigArgs::new(&bucket_name).unwrap())
            .await
            .unwrap();

        let resp = self
            .client
            .get_object_lock_config(&GetObjectLockConfigArgs::new(&bucket_name).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.config.retention_mode.is_none(), true);

        self.client
            .remove_bucket(&RemoveBucketArgs::new(&bucket_name).unwrap())
            .await
            .unwrap();
    }

    async fn set_get_delete_object_tags(&self) {
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

        let tags = HashMap::from([
            (String::from("Project"), String::from("Project One")),
            (String::from("User"), String::from("jsmith")),
        ]);

        self.client
            .set_object_tags(
                &SetObjectTagsArgs::new(&self.test_bucket, &object_name, &tags).unwrap(),
            )
            .await
            .unwrap();

        let resp = self
            .client
            .get_object_tags(&GetObjectTagsArgs::new(&self.test_bucket, &object_name).unwrap())
            .await
            .unwrap();
        assert_eq!(
            resp.tags.len() == tags.len() && resp.tags.keys().all(|k| tags.contains_key(k)),
            true
        );

        self.client
            .delete_object_tags(
                &DeleteObjectTagsArgs::new(&self.test_bucket, &object_name).unwrap(),
            )
            .await
            .unwrap();

        let resp = self
            .client
            .get_object_tags(&GetObjectTagsArgs::new(&self.test_bucket, &object_name).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.tags.is_empty(), true);

        self.client
            .remove_object(&RemoveObjectArgs::new(&self.test_bucket, &object_name).unwrap())
            .await
            .unwrap();
    }

    async fn set_get_bucket_versioning(&self) {
        let bucket_name = rand_bucket_name();

        self.client
            .make_bucket(&MakeBucketArgs::new(&bucket_name).unwrap())
            .await
            .unwrap();

        self.client
            .set_bucket_versioning(&SetBucketVersioningArgs::new(&bucket_name, true).unwrap())
            .await
            .unwrap();

        let resp = self
            .client
            .get_bucket_versioning(&GetBucketVersioningArgs::new(&bucket_name).unwrap())
            .await
            .unwrap();
        assert_eq!(
            match resp.status {
                Some(v) => v,
                _ => false,
            },
            true
        );

        self.client
            .set_bucket_versioning(&SetBucketVersioningArgs::new(&bucket_name, false).unwrap())
            .await
            .unwrap();

        let resp = self
            .client
            .get_bucket_versioning(&GetBucketVersioningArgs::new(&bucket_name).unwrap())
            .await
            .unwrap();
        assert_eq!(
            match resp.status {
                Some(v) => v,
                _ => false,
            },
            false
        );

        self.client
            .remove_bucket(&RemoveBucketArgs::new(&bucket_name).unwrap())
            .await
            .unwrap();
    }

    async fn set_get_object_retention(&self) {
        let bucket_name = rand_bucket_name();

        let mut args = MakeBucketArgs::new(&bucket_name).unwrap();
        args.object_lock = true;
        self.client.make_bucket(&args).await.unwrap();

        let object_name = rand_object_name();

        let size = 16_usize;
        let obj_resp = self
            .client
            .put_object(
                &mut PutObjectArgs::new(
                    &bucket_name,
                    &object_name,
                    &mut RandReader::new(size),
                    Some(size),
                    None,
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let mut args = SetObjectRetentionArgs::new(&bucket_name, &object_name).unwrap();
        args.retention_mode = Some(RetentionMode::GOVERNANCE);
        let retain_until_date = utc_now() + Duration::days(1);
        args.retain_until_date = Some(retain_until_date);

        self.client.set_object_retention(&args).await.unwrap();

        let resp = self
            .client
            .get_object_retention(&GetObjectRetentionArgs::new(&bucket_name, &object_name).unwrap())
            .await
            .unwrap();
        assert_eq!(
            match resp.retention_mode {
                Some(v) => match v {
                    RetentionMode::GOVERNANCE => true,
                    _ => false,
                },
                _ => false,
            },
            true
        );
        assert_eq!(
            match resp.retain_until_date {
                Some(v) => to_iso8601utc(v) == to_iso8601utc(retain_until_date),
                _ => false,
            },
            true,
        );

        let mut args = SetObjectRetentionArgs::new(&bucket_name, &object_name).unwrap();
        args.bypass_governance_mode = true;
        self.client.set_object_retention(&args).await.unwrap();

        let resp = self
            .client
            .get_object_retention(&GetObjectRetentionArgs::new(&bucket_name, &object_name).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.retention_mode.is_none(), true);
        assert_eq!(resp.retain_until_date.is_none(), true);

        let mut args = RemoveObjectArgs::new(&bucket_name, &object_name).unwrap();
        let version_id = obj_resp.version_id.unwrap().clone();
        args.version_id = Some(version_id.as_str());
        self.client.remove_object(&args).await.unwrap();

        self.client
            .remove_bucket(&RemoveBucketArgs::new(&bucket_name).unwrap())
            .await
            .unwrap();
    }

    async fn get_presigned_object_url(&self) {
        let object_name = rand_object_name();
        let resp = self
            .client
            .get_presigned_object_url(
                &GetPresignedObjectUrlArgs::new(&self.test_bucket, &object_name, Method::GET)
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.url.contains("X-Amz-Signature="), true);
    }

    async fn get_presigned_post_form_data(&self) {
        let object_name = rand_object_name();
        let expiration = utc_now() + Duration::days(5);

        let mut policy = PostPolicy::new(&self.test_bucket, &expiration).unwrap();
        policy.add_equals_condition("key", &object_name).unwrap();
        policy
            .add_content_length_range_condition(1024 * 1024, 4 * 1024 * 1024)
            .unwrap();

        let form_data = self
            .client
            .get_presigned_post_form_data(&policy)
            .await
            .unwrap();
        assert_eq!(form_data.contains_key("x-amz-signature"), true);
        assert_eq!(form_data.contains_key("policy"), true);
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

    println!("{{upload,download}}_object()");
    ctest.upload_download_object().await;

    println!("remove_objects()");
    ctest.remove_objects().await;

    println!("list_objects()");
    ctest.list_objects().await;

    println!("select_object_content()");
    ctest.select_object_content().await;

    println!("listen_bucket_notification()");
    ctest.listen_bucket_notification().await;

    println!("copy_object()");
    ctest.copy_object().await;

    println!("compose_object()");
    ctest.compose_object().await;

    println!("{{set,get,delete}}_bucket_notification()");
    ctest.set_get_delete_bucket_notification().await;

    println!("{{set,get,delete}}_bucket_policy()");
    ctest.set_get_delete_bucket_policy().await;

    println!("{{set,get,delete}}_bucket_tags()");
    ctest.set_get_delete_bucket_tags().await;

    println!("{{set,get,delete}}_object_lock_config()");
    ctest.set_get_delete_object_lock_config().await;

    println!("{{set,get,delete}}_object_tags()");
    ctest.set_get_delete_object_tags().await;

    println!("{{set,get}}_bucket_versioning()");
    ctest.set_get_bucket_versioning().await;

    println!("{{set,get}}_object_retention()");
    ctest.set_get_object_retention().await;

    println!("get_presigned_object_url()");
    ctest.get_presigned_object_url().await;

    println!("get_presigned_post_form_data()");
    ctest.get_presigned_post_form_data().await;

    ctest.drop().await;

    Ok(())
}

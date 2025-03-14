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

use crate::common::{TestContext, create_bucket_helper, rand_object_name};
use minio::s3::args::SelectObjectContentArgs;
use minio::s3::types::{
    CsvInputSerialization, CsvOutputSerialization, FileHeaderInfo, QuoteFields, S3Api,
    SelectRequest,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn select_object_content() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = create_bucket_helper(&ctx).await;
    let object_name = rand_object_name();

    let mut data = String::new();
    data.push_str("1997,Ford,E350,\"ac, abs, moon\",3000.00\n");
    data.push_str("1999,Chevy,\"Venture \"\"Extended Edition\"\"\",,4900.00\n");
    data.push_str("1999,Chevy,\"Venture \"\"Extended Edition, Very Large\"\"\",,5000.00\n");
    data.push_str("1996,Jeep,Grand Cherokee,\"MUST SELL!\n");
    data.push_str("air, moon roof, loaded\",4799.00\n");
    let body = String::from("Year,Make,Model,Description,Price\n") + &data;

    ctx.client
        .put_object_content(&bucket_name, &object_name, body)
        .send()
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
    let mut resp = ctx
        .client
        .select_object_content(
            &SelectObjectContentArgs::new(&bucket_name, &object_name, &request).unwrap(),
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
        got += core::str::from_utf8(&buf[..size]).unwrap();
    }
    assert_eq!(got, data);
    ctx.client
        .remove_object(&bucket_name, object_name.as_str())
        .send()
        .await
        .unwrap();
}

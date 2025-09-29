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

use crate::s3::builders::{SelectObjectContent, SelectObjectContentBldr};
use crate::s3::client::MinioClient;
use crate::s3::types::SelectRequest;

impl MinioClient {
    /// Creates a [`SelectObjectContent`] request builder.
    ///
    /// To execute the request, call [`SelectObjectContent::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`SelectObjectContentResponse`](crate::s3::response::SelectObjectContentResponse).
    ///
    /// 🛈 This operation is not supported for express buckets.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::response::SelectObjectContentResponse;
    /// use minio::s3::types::S3Api;
    /// use minio::s3::types::{SelectRequest, CsvInputSerialization, CsvOutputSerialization, FileHeaderInfo, QuoteFields};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = MinioClient::create_client_on_localhost().unwrap(); // configure your client here
    ///     let request = SelectRequest::new_csv_input_output(
    ///         "select * from S3Object",
    ///         CsvInputSerialization {
    ///             compression_type: None,
    ///             allow_quoted_record_delimiter: false,
    ///             comments: None,
    ///             field_delimiter: None,
    ///             file_header_info: Some(FileHeaderInfo::USE),
    ///             quote_character: None,
    ///             quote_escape_character: None,
    ///             record_delimiter: None,
    ///         },
    ///         CsvOutputSerialization {
    ///             field_delimiter: None,
    ///             quote_character: None,
    ///             quote_escape_character: None,
    ///             quote_fields: Some(QuoteFields::ASNEEDED),
    ///             record_delimiter: None,
    ///         },
    ///     ).unwrap();
    ///
    ///     let resp: SelectObjectContentResponse = client
    ///         .select_object_content("bucket-name", "object-name", request)
    ///         .build().send().await.unwrap();
    ///     println!("the progress: '{:?}'", resp.progress);
    /// }
    /// ```
    pub fn select_object_content<S1: Into<String>, S2: Into<String>>(
        &self,
        bucket: S1,
        object: S2,
        request: SelectRequest,
    ) -> SelectObjectContentBldr {
        SelectObjectContent::builder()
            .client(self.clone())
            .bucket(bucket)
            .object(object)
            .request(request)
    }
}

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

//! S3 APIs for bucket objects.

use super::Client;
use crate::s3::builders::SelectObjectContent;
use crate::s3::types::SelectRequest;

impl Client {
    /// Create a SelectObjectContent request builder.
    ///
    /// Returns argument for [select_object_content()](crate::s3::client::Client::select_object_content) API with given bucket name, object name and callback function for results.
    ///
    /// # Examples
    ///
    /// ```ignore TODO
    /// use minio::s3::args::*;
    /// use minio::s3::types::*;
    /// let request = SelectRequest::new_csv_input_output(
    ///     "select * from S3Object",
    ///     CsvInputSerialization {
    ///         compression_type: None,
    ///         allow_quoted_record_delimiter: false,
    ///         comments: None,
    ///         field_delimiter: None,
    ///         file_header_info: Some(FileHeaderInfo::USE),
    ///         quote_character: None,
    ///         quote_escape_character: None,
    ///         record_delimiter: None,
    ///     },
    ///     CsvOutputSerialization {
    ///         field_delimiter: None,
    ///         quote_character: None,
    ///         quote_escape_character: None,
    ///         quote_fields: Some(QuoteFields::ASNEEDED),
    ///         record_delimiter: None,
    ///     },
    /// ).unwrap();
    /// let args = SelectObjectContentArgs::new("my-bucket", "my-object", &request).unwrap();
    /// ```
    pub fn select_object_content(
        &self,
        bucket_name: &str,
        object_name: &str,
        request: SelectRequest,
    ) -> SelectObjectContent {
        SelectObjectContent::new(bucket_name)
            .client(self)
            .object(object_name.to_owned())
            .request(request)
    }
}

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

//! Select request types for S3 Select operations

use super::csv_input_serialization::CsvInputSerialization;
use super::csv_output_serialization::CsvOutputSerialization;
use super::json_input_serialization::JsonInputSerialization;
use super::json_output_serialization::JsonOutputSerialization;
use super::parquet_input_serialization::ParquetInputSerialization;
use crate::s3::error::ValidationErr;

#[derive(Clone, Debug, Default)]
pub struct SelectRequest {
    pub expr: String,
    pub csv_input: Option<CsvInputSerialization>,
    pub json_input: Option<JsonInputSerialization>,
    pub parquet_input: Option<ParquetInputSerialization>,
    pub csv_output: Option<CsvOutputSerialization>,
    pub json_output: Option<JsonOutputSerialization>,
    pub request_progress: bool,
    pub scan_start_range: Option<usize>,
    pub scan_end_range: Option<usize>,
}

impl SelectRequest {
    pub fn new_csv_input_output(
        expr: &str,
        csv_input: CsvInputSerialization,
        csv_output: CsvOutputSerialization,
    ) -> Result<SelectRequest, ValidationErr> {
        if expr.is_empty() {
            return Err(ValidationErr::InvalidSelectExpression(
                "select expression cannot be empty".into(),
            ));
        }

        Ok(SelectRequest {
            expr: expr.to_string(),
            csv_input: Some(csv_input),
            json_input: None,
            parquet_input: None,
            csv_output: Some(csv_output),
            json_output: None,
            request_progress: false,
            scan_start_range: None,
            scan_end_range: None,
        })
    }

    pub fn new_csv_input_json_output(
        expr: String,
        csv_input: CsvInputSerialization,
        json_output: JsonOutputSerialization,
    ) -> Result<SelectRequest, ValidationErr> {
        if expr.is_empty() {
            return Err(ValidationErr::InvalidSelectExpression(
                "select expression cannot be empty".into(),
            ));
        }

        Ok(SelectRequest {
            expr,
            csv_input: Some(csv_input),
            json_input: None,
            parquet_input: None,
            csv_output: None,
            json_output: Some(json_output),
            request_progress: false,
            scan_start_range: None,
            scan_end_range: None,
        })
    }

    pub fn new_json_input_output(
        expr: String,
        json_input: JsonInputSerialization,
        json_output: JsonOutputSerialization,
    ) -> Result<SelectRequest, ValidationErr> {
        if expr.is_empty() {
            return Err(ValidationErr::InvalidSelectExpression(
                "select expression cannot be empty".into(),
            ));
        }

        Ok(SelectRequest {
            expr,
            csv_input: None,
            json_input: Some(json_input),
            parquet_input: None,
            csv_output: None,
            json_output: Some(json_output),
            request_progress: false,
            scan_start_range: None,
            scan_end_range: None,
        })
    }

    pub fn new_parquet_input_csv_output(
        expr: String,
        parquet_input: ParquetInputSerialization,
        csv_output: CsvOutputSerialization,
    ) -> Result<SelectRequest, ValidationErr> {
        if expr.is_empty() {
            return Err(ValidationErr::InvalidSelectExpression(
                "select expression cannot be empty".into(),
            ));
        }

        Ok(SelectRequest {
            expr,
            csv_input: None,
            json_input: None,
            parquet_input: Some(parquet_input),
            csv_output: Some(csv_output),
            json_output: None,
            request_progress: false,
            scan_start_range: None,
            scan_end_range: None,
        })
    }

    pub fn new_parquet_input_json_output(
        expr: String,
        parquet_input: ParquetInputSerialization,
        json_output: JsonOutputSerialization,
    ) -> Result<SelectRequest, ValidationErr> {
        if expr.is_empty() {
            return Err(ValidationErr::InvalidSelectExpression(
                "select expression cannot be empty".into(),
            ));
        }

        Ok(SelectRequest {
            expr,
            csv_input: None,
            json_input: None,
            parquet_input: Some(parquet_input),
            csv_output: None,
            json_output: Some(json_output),
            request_progress: false,
            scan_start_range: None,
            scan_end_range: None,
        })
    }

    pub fn to_xml(&self) -> String {
        let mut data = String::from("<SelectObjectContentRequest>");

        data.push_str("<Expression>");
        data.push_str(&self.expr);
        data.push_str("</Expression>");
        data.push_str("<ExpressionType>SQL</ExpressionType>");

        data.push_str("<InputSerialization>");
        if let Some(c) = &self.csv_input {
            if let Some(v) = &c.compression_type {
                data.push_str("<CompressionType>");
                data.push_str(&v.to_string());
                data.push_str("</CompressionType>");
            }

            data.push_str("<CSV>");
            if c.allow_quoted_record_delimiter {
                data.push_str("<AllowQuotedRecordDelimiter>true</AllowQuotedRecordDelimiter>");
            }
            if let Some(v) = c.comments {
                data.push_str("<Comments>");
                data.push(v);
                data.push_str("</Comments>");
            }
            if let Some(v) = c.field_delimiter {
                data.push_str("<FieldDelimiter>");
                data.push(v);
                data.push_str("</FieldDelimiter>");
            }
            if let Some(v) = &c.file_header_info {
                data.push_str("<FileHeaderInfo>");
                data.push_str(&v.to_string());
                data.push_str("</FileHeaderInfo>");
            }
            if let Some(v) = c.quote_character {
                data.push_str("<QuoteCharacter>");
                data.push(v);
                data.push_str("</QuoteCharacter>");
            }
            if let Some(v) = c.record_delimiter {
                data.push_str("<RecordDelimiter>");
                data.push(v);
                data.push_str("</RecordDelimiter>");
            }
            data.push_str("</CSV>");
        } else if let Some(j) = &self.json_input {
            if let Some(v) = &j.compression_type {
                data.push_str("<CompressionType>");
                data.push_str(&v.to_string());
                data.push_str("</CompressionType>");
            }
            data.push_str("<JSON>");
            if let Some(v) = &j.json_type {
                data.push_str("<Type>");
                data.push_str(&v.to_string());
                data.push_str("</Type>");
            }
            data.push_str("</JSON>");
        } else if self.parquet_input.is_some() {
            data.push_str("<Parquet></Parquet>");
        }
        data.push_str("</InputSerialization>");

        data.push_str("<OutputSerialization>");
        if let Some(c) = &self.csv_output {
            data.push_str("<CSV>");
            if let Some(v) = c.field_delimiter {
                data.push_str("<FieldDelimiter>");
                data.push(v);
                data.push_str("</FieldDelimiter>");
            }
            if let Some(v) = c.quote_character {
                data.push_str("<QuoteCharacter>");
                data.push(v);
                data.push_str("</QuoteCharacter>");
            }
            if let Some(v) = c.quote_escape_character {
                data.push_str("<QuoteEscapeCharacter>");
                data.push(v);
                data.push_str("</QuoteEscapeCharacter>");
            }
            if let Some(v) = &c.quote_fields {
                data.push_str("<QuoteFields>");
                data.push_str(&v.to_string());
                data.push_str("</QuoteFields>");
            }
            if let Some(v) = c.record_delimiter {
                data.push_str("<RecordDelimiter>");
                data.push(v);
                data.push_str("</RecordDelimiter>");
            }
            data.push_str("</CSV>");
        } else if let Some(j) = &self.json_output {
            data.push_str("<JSON>");
            if let Some(v) = j.record_delimiter {
                data.push_str("<RecordDelimiter>");
                data.push(v);
                data.push_str("</RecordDelimiter>");
            }
            data.push_str("</JSON>");
        }
        data.push_str("</OutputSerialization>");

        if self.request_progress {
            data.push_str("<RequestProgress><Enabled>true</Enabled></RequestProgress>");
        }

        if let Some(s) = self.scan_start_range
            && let Some(e) = self.scan_end_range
        {
            data.push_str("<ScanRange>");
            data.push_str("<Start>");
            data.push_str(&s.to_string());
            data.push_str("</Start>");
            data.push_str("<End>");
            data.push_str(&e.to_string());
            data.push_str("</End>");
            data.push_str("</ScanRange>");
        }

        data.push_str("</SelectObjectContentRequest>");
        data
    }
}

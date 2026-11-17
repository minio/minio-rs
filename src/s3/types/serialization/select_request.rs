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
use crate::s3::xml_body::XmlBody;

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

    pub fn to_xml(&self) -> Result<String, ValidationErr> {
        let text = |v: Option<char>| v.map(String::from);
        let mut body = XmlBody::new();
        body.start("SelectObjectContentRequest")
            .element("Expression", &self.expr)
            .element("ExpressionType", "SQL")
            .start("InputSerialization");
        if let Some(c) = &self.csv_input {
            body.optional(
                "CompressionType",
                c.compression_type
                    .as_ref()
                    .map(|v| v.to_string())
                    .as_deref(),
            )
            .start("CSV");
            if c.allow_quoted_record_delimiter {
                body.element("AllowQuotedRecordDelimiter", "true");
            }
            body.optional("Comments", text(c.comments).as_deref())
                .optional("FieldDelimiter", text(c.field_delimiter).as_deref())
                .optional(
                    "FileHeaderInfo",
                    c.file_header_info
                        .as_ref()
                        .map(|v| v.to_string())
                        .as_deref(),
                )
                .optional("QuoteCharacter", text(c.quote_character).as_deref())
                .optional("RecordDelimiter", text(c.record_delimiter).as_deref())
                .end();
        } else if let Some(j) = &self.json_input {
            body.optional(
                "CompressionType",
                j.compression_type
                    .as_ref()
                    .map(|v| v.to_string())
                    .as_deref(),
            )
            .start("JSON")
            .optional(
                "Type",
                j.json_type.as_ref().map(|v| v.to_string()).as_deref(),
            )
            .end();
        } else if self.parquet_input.is_some() {
            body.start("Parquet").end();
        }
        body.end().start("OutputSerialization");
        if let Some(c) = &self.csv_output {
            body.start("CSV")
                .optional("FieldDelimiter", text(c.field_delimiter).as_deref())
                .optional("QuoteCharacter", text(c.quote_character).as_deref())
                .optional(
                    "QuoteEscapeCharacter",
                    text(c.quote_escape_character).as_deref(),
                )
                .optional(
                    "QuoteFields",
                    c.quote_fields.as_ref().map(|v| v.to_string()).as_deref(),
                )
                .optional("RecordDelimiter", text(c.record_delimiter).as_deref())
                .end();
        } else if let Some(j) = &self.json_output {
            body.start("JSON")
                .optional("RecordDelimiter", text(j.record_delimiter).as_deref())
                .end();
        }
        body.end();
        if self.request_progress {
            body.start("RequestProgress")
                .element("Enabled", "true")
                .end();
        }
        if let Some(s) = self.scan_start_range
            && let Some(e) = self.scan_end_range
        {
            body.start("ScanRange")
                .element("Start", &s.to_string())
                .element("End", &e.to_string())
                .end();
        }
        body.end();
        body.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_expression_with_xml_syntax_reaches_the_server_unchanged() {
        let expr = "SELECT * FROM S3Object s WHERE s.size < 5 AND s.team = 'R&D'";
        let request = SelectRequest::new_csv_input_output(
            expr,
            CsvInputSerialization {
                compression_type: None,
                allow_quoted_record_delimiter: false,
                comments: None,
                field_delimiter: None,
                file_header_info: None,
                quote_character: None,
                quote_escape_character: None,
                record_delimiter: None,
            },
            CsvOutputSerialization {
                field_delimiter: None,
                quote_character: None,
                quote_escape_character: None,
                quote_fields: None,
                record_delimiter: None,
            },
        )
        .unwrap();
        let xml = request.to_xml().unwrap();
        let root = xmltree::Element::parse(xml.as_bytes()).unwrap();
        assert_eq!(
            root.get_child("Expression").unwrap().get_text().unwrap(),
            expr
        );
    }
}

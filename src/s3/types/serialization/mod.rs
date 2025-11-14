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

//! S3 Select serialization types for input and output formats

pub mod compression_type;
pub mod csv_input_serialization;
pub mod csv_output_serialization;
pub mod file_header_info;
pub mod json_input_serialization;
pub mod json_output_serialization;
pub mod json_type;
pub mod parquet_input_serialization;
pub mod quote_fields;
pub mod select_progress;
pub mod select_request;

pub use compression_type::CompressionType;
pub use csv_input_serialization::CsvInputSerialization;
pub use csv_output_serialization::CsvOutputSerialization;
pub use file_header_info::FileHeaderInfo;
pub use json_input_serialization::JsonInputSerialization;
pub use json_output_serialization::JsonOutputSerialization;
pub use json_type::JsonType;
pub use parquet_input_serialization::ParquetInputSerialization;
pub use quote_fields::QuoteFields;
pub use select_progress::SelectProgress;
pub use select_request::SelectRequest;

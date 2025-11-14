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

//! Client method for GetConfig operation

use crate::s3tables::builders::{GetConfig, GetConfigBldr};
use crate::s3tables::client::TablesClient;

impl TablesClient {
    /// Retrieves catalog configuration
    ///
    /// Returns configuration settings for the warehouse.
    ///
    /// # Arguments
    ///
    /// * `warehouse_name` - Name of the warehouse
    pub fn get_config<S: Into<String>>(&self, warehouse_name: S) -> GetConfigBldr {
        GetConfig::builder()
            .client(self.clone())
            .warehouse_name(warehouse_name)
    }
}

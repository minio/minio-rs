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

//! Client method for ViewExists operation

use crate::s3tables::builders::{ViewExists, ViewExistsBldr};
use crate::s3tables::client::TablesClient;
use crate::s3tables::utils::{Namespace, ViewName, WarehouseName};

impl TablesClient {
    /// Checks if a view exists in a namespace
    ///
    /// # Arguments
    ///
    /// * `warehouse_name` - Name of the warehouse
    /// * `namespace` - Namespace identifier
    /// * `view_name` - Name of the view
    pub fn view_exists(
        &self,
        warehouse_name: WarehouseName,
        namespace: Namespace,
        view_name: ViewName,
    ) -> ViewExistsBldr {
        ViewExists::builder()
            .client(self.clone())
            .warehouse_name(warehouse_name)
            .namespace(namespace)
            .view_name(view_name)
    }
}

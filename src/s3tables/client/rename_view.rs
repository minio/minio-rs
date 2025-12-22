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

//! Client method for RenameView operation

use crate::s3tables::builders::{RenameView, RenameViewBldr};
use crate::s3tables::client::TablesClient;
use crate::s3tables::utils::{Namespace, ViewName, WarehouseName};

impl TablesClient {
    /// Renames or moves a view to a different namespace
    ///
    /// # Arguments
    ///
    /// * `warehouse_name` - Name of the warehouse
    /// * `source_namespace` - Source namespace identifier
    /// * `source_view_name` - Current name of the view
    /// * `dest_namespace` - Destination namespace identifier
    /// * `dest_view_name` - New name of the view
    pub fn rename_view(
        &self,
        warehouse_name: WarehouseName,
        source_namespace: Namespace,
        source_view_name: ViewName,
        dest_namespace: Namespace,
        dest_view_name: ViewName,
    ) -> RenameViewBldr {
        RenameView::builder()
            .client(self.clone())
            .warehouse_name(warehouse_name)
            .source_namespace(source_namespace)
            .source_view_name(source_view_name)
            .dest_namespace(dest_namespace)
            .dest_view_name(dest_view_name)
    }
}

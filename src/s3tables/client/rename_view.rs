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

use crate::s3::error::ValidationErr;
use crate::s3tables::builders::{RenameView, RenameViewBldr};
use crate::s3tables::client::TablesClient;
use crate::s3tables::utils::{Namespace, ViewName, WarehouseName};

impl TablesClient {
    /// Renames or moves a view to a different namespace
    ///
    /// # Arguments
    ///
    /// * `warehouse` - Name of the warehouse (or string to validate)
    /// * `source_namespace` - Source namespace identifier
    /// * `source_view_name` - Current name of the view (or string to validate)
    /// * `dest_namespace` - Destination namespace identifier
    /// * `dest_view_name` - New name of the view (or string to validate)
    pub fn rename_view<W, SN, SV, DN, DV>(
        &self,
        warehouse: W,
        source_namespace: SN,
        source_view_name: SV,
        dest_namespace: DN,
        dest_view_name: DV,
    ) -> Result<RenameViewBldr, ValidationErr>
    where
        W: TryInto<WarehouseName>,
        W::Error: Into<ValidationErr>,
        SN: TryInto<Namespace>,
        SN::Error: Into<ValidationErr>,
        SV: TryInto<ViewName>,
        SV::Error: Into<ValidationErr>,
        DN: TryInto<Namespace>,
        DN::Error: Into<ValidationErr>,
        DV: TryInto<ViewName>,
        DV::Error: Into<ValidationErr>,
    {
        Ok(RenameView::builder()
            .client(self.clone())
            .warehouse(warehouse.try_into().map_err(Into::into)?)
            .source_namespace(source_namespace.try_into().map_err(Into::into)?)
            .source_view_name(source_view_name.try_into().map_err(Into::into)?)
            .dest_namespace(dest_namespace.try_into().map_err(Into::into)?)
            .dest_view_name(dest_view_name.try_into().map_err(Into::into)?))
    }
}

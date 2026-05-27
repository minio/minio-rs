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

//! Client method for CreateView operation

use crate::s3::error::ValidationErr;
use crate::s3tables::builders::{CreateView, CreateViewBldr};
use crate::s3tables::client::TablesClient;
use crate::s3tables::iceberg::Schema;
use crate::s3tables::utils::{Namespace, ViewName, ViewSql, WarehouseName};

impl TablesClient {
    /// Creates a new view in the catalog
    ///
    /// # Arguments
    ///
    /// * `warehouse` - Name of the warehouse (or string to validate)
    /// * `namespace` - Namespace identifier
    /// * `view` - Name of the view (or string to validate)
    /// * `schema` - Schema for the view
    /// * `sql` - SQL query defining the view
    pub fn create_view<W, N, V>(
        &self,
        warehouse: W,
        namespace: N,
        view: V,
        schema: Schema,
        sql: ViewSql,
    ) -> Result<CreateViewBldr, ValidationErr>
    where
        W: TryInto<WarehouseName>,
        W::Error: Into<ValidationErr>,
        N: TryInto<Namespace>,
        N::Error: Into<ValidationErr>,
        V: TryInto<ViewName>,
        V::Error: Into<ValidationErr>,
    {
        Ok(CreateView::builder()
            .client(self.clone())
            .warehouse(warehouse.try_into().map_err(Into::into)?)
            .namespace(namespace.try_into().map_err(Into::into)?)
            .view(view.try_into().map_err(Into::into)?)
            .schema(schema)
            .sql(sql))
    }
}

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

//! Client method for RegisterView operation

use crate::s3::error::ValidationErr;
use crate::s3tables::builders::{RegisterView, RegisterViewBldr};
use crate::s3tables::client::TablesClient;
use crate::s3tables::utils::{MetadataLocation, Namespace, ViewName, WarehouseName};

impl TablesClient {
    /// Registers an existing Iceberg view (MinIO AIStor extension)
    ///
    /// Registers a view by pointing to its existing metadata location.
    /// This is a MinIO AIStor extension endpoint (v0 API).
    ///
    /// # Arguments
    ///
    /// * `warehouse` - Name of the warehouse (or string to validate)
    /// * `namespace` - Namespace to register the view in
    /// * `view` - Name for the registered view (or string to validate)
    /// * `metadata_location` - S3 URI of the view's metadata file (or string to validate)
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::{MinioClient, creds::StaticProvider, http::BaseUrl};
    /// use minio::s3tables::{TablesClient, TablesApi};
    /// use minio::s3::types::S3Api;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let base_url = "http://localhost:9000/".parse::<BaseUrl>()?;
    /// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    /// let client = MinioClient::new(base_url, Some(provider), None, None)?;
    /// let tables = TablesClient::new(client);
    ///
    /// let response = tables
    ///     .register_view(
    ///         "warehouse",
    ///         vec!["analytics".to_string()],
    ///         "sales_summary",
    ///         "s3://bucket/path/to/view/metadata.json",
    ///     )?
    ///     .build()
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn register_view<W, N, V, M>(
        &self,
        warehouse: W,
        namespace: N,
        view: V,
        metadata_location: M,
    ) -> Result<RegisterViewBldr, ValidationErr>
    where
        W: TryInto<WarehouseName>,
        W::Error: Into<ValidationErr>,
        N: TryInto<Namespace>,
        N::Error: Into<ValidationErr>,
        V: TryInto<ViewName>,
        V::Error: Into<ValidationErr>,
        M: TryInto<MetadataLocation>,
        M::Error: Into<ValidationErr>,
    {
        Ok(RegisterView::builder()
            .client(self.clone())
            .warehouse(warehouse.try_into().map_err(Into::into)?)
            .namespace(namespace.try_into().map_err(Into::into)?)
            .view(view.try_into().map_err(Into::into)?)
            .metadata_location(metadata_location.try_into().map_err(Into::into)?))
    }
}

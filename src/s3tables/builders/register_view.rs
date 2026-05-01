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

//! Builder for RegisterView operation
//!
//! MinIO AIStor Extension API: `POST /v0/{warehouse}/namespaces/{namespace}/views/register`
//!
//! This is a MinIO AIStor extension endpoint (v0 API) for registering existing Iceberg views.

use crate::s3::error::ValidationErr;
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::RegisterViewResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use crate::s3tables::utils::{
    MetadataLocation, Namespace, ViewName, WarehouseName, encode_namespace,
};
use http::Method;
use serde::Serialize;
use typed_builder::TypedBuilder;

/// Argument builder for RegisterView operation
///
/// Registers an existing Iceberg view by referencing its metadata location.
/// This is a MinIO AIStor extension endpoint.
///
/// # Example
///
/// ```no_run
/// use minio::s3::{MinioClient, creds::StaticProvider, http::BaseUrl};
/// use minio::s3tables::{TablesClient, TablesApi};
/// use minio::s3tables::utils::{MetadataLocation, Namespace, ViewName, WarehouseName};
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
///         WarehouseName::try_from("warehouse")?,
///         Namespace::single("analytics")?,
///         ViewName::new("sales_summary")?,
///         MetadataLocation::new("s3://bucket/path/to/view/metadata.json")?,
///     )?
///     .build()
///     .send()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, TypedBuilder)]
pub struct RegisterView {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    warehouse: WarehouseName,
    #[builder(!default)]
    namespace: Namespace,
    #[builder(!default)]
    view: ViewName,
    #[builder(!default)]
    metadata_location: MetadataLocation,
    /// Whether to overwrite an existing view with the same name
    #[builder(default = false)]
    overwrite: bool,
}

/// Request body for RegisterView
#[derive(Serialize)]
struct RegisterViewRequest {
    name: String,
    #[serde(rename = "metadata-location")]
    metadata_location: String,
    #[serde(skip_serializing_if = "is_false")]
    overwrite: bool,
}

fn is_false(b: &bool) -> bool {
    !*b
}

impl TablesApi for RegisterView {
    type TablesResponse = RegisterViewResponse;
}

/// Builder type for RegisterView
pub type RegisterViewBldr = RegisterViewBuilder<(
    (TablesClient,),
    (WarehouseName,),
    (Namespace,),
    (ViewName,),
    (MetadataLocation,),
    (),
)>;

impl ToTablesRequest for RegisterView {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        let request_body = RegisterViewRequest {
            name: self.view.into_inner(),
            metadata_location: self.metadata_location.into_inner(),
            overwrite: self.overwrite,
        };

        // Use absolute path for v0 extension API
        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::POST)
            .path(format!(
                "/_iceberg/v0/{}/namespaces/{}/views/register",
                self.warehouse,
                encode_namespace(&self.namespace)
            ))
            .body(Some(serde_json::to_vec(&request_body)?))
            .build())
    }
}

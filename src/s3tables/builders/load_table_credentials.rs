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

//! Builder for LoadTableCredentials operation
//!
//! Iceberg REST API: `GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials`
//! Spec: <https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml#L800>

use crate::s3::error::ValidationErr;
use crate::s3::header_constants::PLAN_ID;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::LoadTableCredentialsResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use crate::s3tables::utils::{Namespace, PlanId, TableName, WarehouseName, table_path};
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for LoadTableCredentials operation
///
/// Loads vended credentials for accessing a table's data files.
///
/// # Example
///
/// ```no_run
/// use minio::s3::{MinioClient, creds::StaticProvider, http::BaseUrl};
/// use minio::s3tables::{TablesClient, TablesApi};
/// use minio::s3tables::utils::{Namespace, TableName, WarehouseName};
/// use minio::s3::types::S3Api;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let base_url = "http://localhost:9000/".parse::<BaseUrl>()?;
/// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
/// let client = MinioClient::new(base_url, Some(provider), None, None)?;
/// let tables = TablesClient::new(client);
///
/// let response = tables
///     .load_table_credentials(
///         WarehouseName::try_from("my-warehouse")?,
///         Namespace::single("my-namespace")?,
///         TableName::new("my-table")?,
///     )
///     .build()
///     .send()
///     .await?;
///
/// for cred in response.storage_credentials()? {
///     println!("Credential prefix: {}", cred.prefix);
///     println!("Access key: {}", cred.access_key_id);
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, TypedBuilder)]
pub struct LoadTableCredentials {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    warehouse_name: WarehouseName,
    #[builder(!default)]
    namespace: Namespace,
    #[builder(!default)]
    table_name: TableName,
    #[builder(default, setter(strip_option))]
    plan_id: Option<PlanId>,
}

impl TablesApi for LoadTableCredentials {
    type TablesResponse = LoadTableCredentialsResponse;
}

/// Builder type for LoadTableCredentials
pub type LoadTableCredentialsBldr = LoadTableCredentialsBuilder<(
    (TablesClient,),
    (WarehouseName,),
    (Namespace,),
    (TableName,),
    (),
)>;

impl ToTablesRequest for LoadTableCredentials {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        let mut query_params = Multimap::new();

        if let Some(plan_id) = &self.plan_id {
            query_params.add(PLAN_ID, plan_id.as_str());
        }

        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::GET)
            .path(format!(
                "{}/credentials",
                table_path(&self.warehouse_name, &self.namespace, &self.table_name)
            ))
            .query_params(query_params)
            .body(None)
            .build())
    }
}

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

//! Builder for FetchPlanningResult operation
//!
//! Iceberg REST API: `GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}`
//! Spec: <https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml>

use crate::s3::error::ValidationErr;
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::FetchPlanningResultResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use crate::s3tables::utils::{Namespace, PlanId, TableName, WarehouseName, plan_result_path};
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for FetchPlanningResult operation
///
/// Retrieves the result of a previously submitted scan plan
#[derive(Clone, Debug, TypedBuilder)]
pub struct FetchPlanningResult {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    warehouse_name: WarehouseName,
    #[builder(!default)]
    namespace: Namespace,
    #[builder(!default)]
    table_name: TableName,
    #[builder(!default)]
    plan_id: PlanId,
}

impl TablesApi for FetchPlanningResult {
    type TablesResponse = FetchPlanningResultResponse;
}

/// Builder type for FetchPlanningResult
pub type FetchPlanningResultBldr = FetchPlanningResultBuilder<(
    (TablesClient,),
    (WarehouseName,),
    (Namespace,),
    (TableName,),
    (PlanId,),
)>;

impl ToTablesRequest for FetchPlanningResult {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::GET)
            .path(plan_result_path(
                &self.warehouse_name,
                &self.namespace,
                &self.table_name,
                &self.plan_id,
            ))
            .body(None)
            .build())
    }
}

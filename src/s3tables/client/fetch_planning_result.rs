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

//! Client method for FetchPlanningResult operation

use crate::s3::error::ValidationErr;
use crate::s3tables::builders::{FetchPlanningResult, FetchPlanningResultBldr};
use crate::s3tables::client::TablesClient;
use crate::s3tables::utils::{Namespace, PlanId, TableName, WarehouseName};

impl TablesClient {
    /// Retrieves the result of a previously submitted scan plan
    ///
    /// # Arguments
    ///
    /// * `warehouse` - Name of the warehouse (or string to validate)
    /// * `namespace` - Namespace identifier
    /// * `table` - Name of the table (or string to validate)
    /// * `plan_id` - ID of the plan to fetch results for (or string to validate)
    pub fn fetch_planning_result<W, N, T, P>(
        &self,
        warehouse: W,
        namespace: N,
        table: T,
        plan_id: P,
    ) -> Result<FetchPlanningResultBldr, ValidationErr>
    where
        W: TryInto<WarehouseName>,
        W::Error: Into<ValidationErr>,
        N: TryInto<Namespace>,
        N::Error: Into<ValidationErr>,
        T: TryInto<TableName>,
        T::Error: Into<ValidationErr>,
        P: TryInto<PlanId>,
        P::Error: Into<ValidationErr>,
    {
        Ok(FetchPlanningResult::builder()
            .client(self.clone())
            .warehouse(warehouse.try_into().map_err(Into::into)?)
            .namespace(namespace.try_into().map_err(Into::into)?)
            .table(table.try_into().map_err(Into::into)?)
            .plan_id(plan_id.try_into().map_err(Into::into)?))
    }
}

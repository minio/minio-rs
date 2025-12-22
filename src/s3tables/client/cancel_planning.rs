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

//! Client method for CancelPlanning operation

use crate::s3tables::builders::{CancelPlanning, CancelPlanningBldr};
use crate::s3tables::client::TablesClient;
use crate::s3tables::utils::{Namespace, PlanId, TableName, WarehouseName};

impl TablesClient {
    /// Cancels a previously submitted scan plan
    ///
    /// # Arguments
    ///
    /// * `warehouse_name` - Name of the warehouse
    /// * `namespace` - Namespace identifier
    /// * `table_name` - Name of the table
    /// * `plan_id` - ID of the plan to cancel
    pub fn cancel_planning(
        &self,
        warehouse_name: WarehouseName,
        namespace: Namespace,
        table_name: TableName,
        plan_id: PlanId,
    ) -> CancelPlanningBldr {
        CancelPlanning::builder()
            .client(self.clone())
            .warehouse_name(warehouse_name)
            .namespace(namespace)
            .table_name(table_name)
            .plan_id(plan_id)
    }
}

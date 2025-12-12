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

use crate::s3::client::MinioClient;
use crate::s3::multimap_ext::Multimap;
use crate::s3::types::{BucketName, Region};
use std::marker::PhantomData;
use typed_builder::TypedBuilder;

/// Common parameters for bucket operations.
#[derive(Clone, Debug, TypedBuilder)]
pub struct BucketCommon<T> {
    #[builder(!default)] // force required
    pub(crate) client: MinioClient,

    #[builder(default, setter(into))]
    pub(crate) extra_headers: Option<Multimap>,
    #[builder(default, setter(into))]
    pub(crate) extra_query_params: Option<Multimap>,
    #[builder(default)]
    pub(crate) region: Option<Region>,
    #[builder(!default)] // force required
    pub(crate) bucket: BucketName,

    #[builder(default)]
    _operation: PhantomData<T>,
}

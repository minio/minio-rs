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

//! Shared helpers for credential-provider tests.

use std::sync::LazyLock;
use tokio::sync::Mutex;

/// Process-wide lock serializing every test that mutates environment variables.
///
/// `std::env::set_var`/`remove_var` affect the whole process, and tests run in
/// parallel, so all credential-provider tests touching the environment (env,
/// file, IAM/ECS, web-identity) must hold this single lock. Synchronous `#[test]`
/// tests acquire it with `ENV_LOCK.blocking_lock()`; async `#[tokio::test]` tests
/// use `ENV_LOCK.lock().await`.
pub(crate) static ENV_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

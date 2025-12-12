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
mod test_attr;

use darling::ast::NestedMeta;
use darling::{Error, FromMeta};
use syn::ItemFn;
extern crate proc_macro;

/// A proc macro attribute for writing MinIO tests.
///
/// This macro extends the `#[tokio::test]` attribute to provide additional functionality for
/// testing MinIO operations. The macro takes care of setting up and tearing down the test
/// environment, it automatically creates a bucket for the test if needed and cleans it up after
/// the test is done.
///
/// By default, it requires the test function to have two parameters:
///
/// - `ctx: TestContext` - The test context which will give you access to a minio-client.
/// - `bucket_name: BucketName` - The typed bucket name to be used in the test.
///
/// ```no_run
/// use minio_common::test_context::TestContext;
/// use minio::s3::types::BucketName;
/// #[minio_macros::test]
/// async fn my_test(ctx: TestContext, bucket_name: BucketName) {
///    // Your test code here
/// }
/// ```
///
/// If the `no_bucket` argument is provided, the test function must have only one parameter:
///
/// - `ctx: TestContext` - The test context which will give you access to a minio-client.
///
/// ```no_run
/// use minio_common::test_context::TestContext;
/// #[minio_macros::test(no_bucket)]
/// async fn my_test(ctx: TestContext) {
///    // Your test code here
/// }
///```
/// The macro also supports additional arguments:
///
/// - `flavor`: Specifies the flavor of the Tokio test (e.g., "multi_thread").
/// - `worker_threads`: Specifies the number of worker threads for the Tokio test.
/// - `bucket_name`: Specifies the name of the bucket to be used in the test. If not provided, a random bucket name will be generated.
/// - `skip_if_express`: If set, the test will be skipped if the MinIO server is running in Express mode.
/// - `object_lock`: If set, the test bucket is created with `.object_lock(true)`
/// - `no_cleanup`: If set, the test bucket is not deleted after the test is run.
///
/// ```no_run
/// use minio_common::test_context::TestContext;
/// #[minio_macros::test(skip_if_express)]
/// async fn my_test(ctx: TestContext) {
///    // this test will not run if the MinIO server is running in Express mode
/// }
/// ```
/// - `skip_if_not_express`: If set, the test will be skipped if the MinIO server is NOT running in Express mode.
/// ```no_run
/// use minio_common::test_context::TestContext;
/// #[minio_macros::test(skip_if_not_express)]
/// async fn my_test(ctx: TestContext) {
///    // this test will not run if the MinIO server is NOT running in Express mode
/// }
/// ```
/// - `ignore`: Mark the test as ignored (skipped by default). Run with `cargo test -- --ignored` to include.
/// ```no_run
/// use minio_common::test_context::TestContext;
/// use minio::s3::types::BucketName;
/// #[minio_macros::test(ignore = "Requires newer server version")]
/// async fn my_test(ctx: TestContext, bucket_name: BucketName) {
///    // this test is skipped by default
/// }
/// ```
#[proc_macro_attribute]
pub fn test(
    args: proc_macro::TokenStream,
    input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    // Parse the function
    let input_fn = match syn::parse::<ItemFn>(input.clone()) {
        Ok(input_fn) => input_fn,
        Err(err) => return err.to_compile_error().into(),
    };

    // Parse the macro arguments
    let attr_args = match NestedMeta::parse_meta_list(args.into()) {
        Ok(v) => v,
        Err(e) => return Error::from(e).write_errors().into(),
    };

    let args = match test_attr::MacroArgs::from_list(&attr_args) {
        Ok(v) => v,
        Err(e) => return e.write_errors().into(),
    };

    // Validate the function arguments
    if let Err(err) = args.validate(&input_fn) {
        return err;
    }

    // Expand the macro
    match test_attr::expand_test_macro(args, input_fn) {
        Ok(expanded) => expanded.into(),
        Err(err) => err,
    }
}

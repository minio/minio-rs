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

use darling::FromMeta;
use darling_core::Error;
use proc_macro2::TokenStream;
use quote::{ToTokens, quote, quote_spanned};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{FnArg, ItemFn, ReturnType};
use uuid::Uuid;

#[derive(Debug, FromMeta)]
pub(crate) struct MacroArgs {
    flavor: Option<String>,
    worker_threads: Option<usize>,
    bucket_name: Option<String>,
    skip_if_express: darling::util::Flag,
    skip_if_not_express: darling::util::Flag,
    no_bucket: darling::util::Flag,
    object_lock: darling::util::Flag,
    no_cleanup: darling::util::Flag,
}

impl MacroArgs {
    pub(crate) fn validate(&self, func: &ItemFn) -> Result<(), proc_macro::TokenStream> {
        if self.no_bucket.is_present() && self.bucket_name.is_some() {
            let error_msg = "The `no_bucket` argument cannot be used with `bucket_name`";
            return Err(proc_macro::TokenStream::from(
                Error::custom(error_msg)
                    .with_span(&func.sig.span())
                    .write_errors(),
            ));
        }

        if self.no_bucket.is_present() && func.sig.inputs.len() != 1 {
            let error_msg = "When using `no_bucket`, the test function must have exactly one argument: (ctx: TestContext)";
            return Err(proc_macro::TokenStream::from(
                Error::custom(error_msg)
                    .with_span(&func.sig.inputs.span())
                    .write_errors(),
            ));
        }

        // Validate that the function has exactly two arguments: ctx and bucket_name
        if (func.sig.inputs.len() != 2) && !self.no_bucket.is_present() {
            let error_msg = "Minio test function must have exactly two arguments: (ctx: TestContext, bucket_name: String)";
            return Err(proc_macro::TokenStream::from(
                Error::custom(error_msg)
                    .with_span(&func.sig.inputs.span())
                    .write_errors(),
            ));
        }

        // Check the argument types
        let mut iter = func.sig.inputs.iter();

        // Check first argument (ctx: &mut TestContext)
        if let Some(FnArg::Typed(pat_type)) = iter.next() {
            let type_str = pat_type.ty.to_token_stream().to_string();
            if !type_str.contains("TestContext") {
                let error_msg = "The first argument must be of type TestContext";
                return Err(proc_macro::TokenStream::from(
                    Error::custom(error_msg)
                        .with_span(&pat_type.span())
                        .write_errors(),
                ));
            }
        }

        // Check the second argument (bucket_name: String)
        if !self.no_bucket.is_present()
            && let Some(FnArg::Typed(pat_type)) = iter.next()
        {
            let type_str = pat_type.ty.to_token_stream().to_string();
            if !type_str.contains("String") {
                let error_msg = "The second argument must be of type String";
                return Err(proc_macro::TokenStream::from(
                    Error::custom(error_msg)
                        .with_span(&pat_type.span())
                        .write_errors(),
                ));
            }
        }

        Ok(())
    }
}

/// Expands the test macro into the final TokenStream
pub(crate) fn expand_test_macro(
    args: MacroArgs,
    mut func: ItemFn,
) -> Result<TokenStream, proc_macro::TokenStream> {
    let input_span = func.sig.paren_token.span.span();
    func.sig.output = ReturnType::Default;
    let old_inps = func.sig.inputs.clone();
    func.sig.inputs = Punctuated::default();
    let sig = func.sig.clone().into_token_stream();

    // Generate the tokio test attribute based on the provided arguments
    let header = generate_tokio_test_header(&args, sig);

    let test_function_block = func.block.clone().into_token_stream();

    let inner_inputs = quote_spanned!(input_span=> #old_inps);
    let inner_fn_name = create_inner_func_name(&func);
    let inner_header = quote_spanned!(func.sig.span()=> async fn #inner_fn_name(#inner_inputs));

    // Generate the skip logic for express mode if required
    let maybe_skip_if_express = generate_express_skip_logic(&args, func.sig.span());

    // Setup common prelude
    let prelude = quote!(
            use ::futures_util::FutureExt;
            use ::std::panic::AssertUnwindSafe;
            use ::minio::s3::types::S3Api;
            use ::minio::s3::response::a_response_traits::HasBucket;

            let ctx = ::minio_common::test_context::TestContext::new_from_env();
    );

    // Generate the outer function body based on whether a bucket is needed
    let outer_body = if args.no_bucket.is_present() {
        generate_no_bucket_body(
            prelude,
            maybe_skip_if_express,
            inner_fn_name,
            func.block.span(),
        )
    } else {
        generate_with_bucket_body(
            prelude,
            maybe_skip_if_express,
            inner_fn_name,
            &args,
            func.block.span(),
        )
    };

    // Generate the inner function implementation
    let inner_impl = quote_spanned!(func.span()=>
        #inner_header
        #test_function_block
    );

    // Combine all parts into the final output
    let mut out = TokenStream::new();
    out.extend(header);
    out.extend(outer_body);
    out.extend(inner_impl);

    Ok(out)
}

fn generate_tokio_test_header(args: &MacroArgs, sig: TokenStream) -> TokenStream {
    let flavor = args
        .flavor
        .as_ref()
        .map(ToString::to_string)
        .or(std::env::var("MINIO_TEST_TOKIO_RUNTIME_FLAVOR").ok());
    match (flavor, args.worker_threads) {
        (Some(flavor), None) => {
            quote!(#[::tokio::test(flavor = #flavor)]
            #sig
                )
        }
        (None, Some(worker_threads)) => {
            quote!(#[::tokio::test(worker_threads = #worker_threads)]
            #sig
                )
        }
        (None, None) => {
            quote!(#[::tokio::test]
            #sig
                )
        }
        (Some(flavor), Some(worker_threads)) => {
            quote!(#[::tokio::test(flavor = #flavor, worker_threads = #worker_threads)]
            #sig
                )
        }
    }
}

fn generate_express_skip_logic(args: &MacroArgs, span: proc_macro2::Span) -> TokenStream {
    if args.skip_if_express.is_present() {
        quote_spanned!(span=>
        if ctx.client.is_minio_express().await {
            println!("Skipping test because it is running in MinIO Express mode");
            return;
        })
    } else if args.skip_if_not_express.is_present() {
        quote_spanned!(span=>
        if !ctx.client.is_minio_express().await {
            println!("Skipping test because it is NOT running in MinIO Express mode");
            return;
        })
    } else {
        TokenStream::new()
    }
}

fn generate_no_bucket_body(
    prelude: TokenStream,
    maybe_skip_if_express: TokenStream,
    inner_fn_name: TokenStream,
    span: proc_macro2::Span,
) -> TokenStream {
    quote_spanned!(span=> {
        #prelude
        #maybe_skip_if_express
        #inner_fn_name(ctx).await;
    })
}

fn generate_with_bucket_body(
    prelude: TokenStream,
    maybe_skip_if_express: TokenStream,
    inner_fn_name: TokenStream,
    args: &MacroArgs,
    span: proc_macro2::Span,
) -> TokenStream {
    let bucket_name = args
        .bucket_name
        .as_ref()
        .map(|b| b.to_token_stream())
        .unwrap_or_else(|| {
            let random_name = format!("test-bucket-{}", Uuid::new_v4());
            proc_macro2::Literal::string(&random_name).into_token_stream()
        });
    let maybe_lock = if args.object_lock.is_present() {
        quote! {
            .object_lock(true)
        }
    } else {
        TokenStream::new()
    };
    let maybe_cleanup = if args.no_cleanup.is_present() {
        quote! {}
    } else {
        quote! {
            ::minio_common::cleanup_guard::cleanup(client_clone, resp.bucket()).await;
        }
    };
    quote_spanned!(span=> {
        #prelude
        #maybe_skip_if_express

        let client_clone = ctx.client.clone();
        let bucket_name = #bucket_name;
        let resp = client_clone.create_bucket(bucket_name)#maybe_lock.build().send().await.expect("Failed to create bucket");
        assert_eq!(resp.bucket(), bucket_name);
        let res = AssertUnwindSafe(#inner_fn_name(ctx, resp.bucket().to_string())).catch_unwind().await;
        #maybe_cleanup
        if let Err(e) = res {
            ::std::panic::resume_unwind(e);
        }
    })
}

fn create_inner_func_name(func: &ItemFn) -> TokenStream {
    let inner_name = format!("{}_test_impl", func.sig.ident);
    let ident = proc_macro2::Ident::new(&inner_name, func.sig.span());
    quote! { #ident }
}

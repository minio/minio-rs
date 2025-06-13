pub use http::Extensions;

use http::Method;
use reqwest::Response;
use std::fmt::Debug;

use crate::s3::error::Error;
use crate::s3::http::Url;
use crate::s3::multimap::Multimap;
use crate::s3::segmented_bytes::SegmentedBytes;

#[async_trait::async_trait]
pub trait ClientHooks: Debug {
    fn name(&self) -> &'static str;

    async fn before_signing_mut(
        &self,
        _method: &Method,
        _url: &mut Url,
        _region: &str,
        _headers: &mut Multimap,
        _query_params: &Multimap,
        _bucket_name: Option<&str>,
        _object_name: Option<&str>,
        _body: Option<&SegmentedBytes>,
        _extensions: &mut Extensions,
    ) -> Result<(), Error> {
        Ok(())
    }
    async fn after_execute(
        &self,
        _method: &Method,
        _url: &Url,
        _region: &str,
        _headers: &Multimap,
        _query_params: &Multimap,
        _bucket_name: Option<&str>,
        _object_name: Option<&str>,
        _resp: &Result<Response, reqwest::Error>,
        _extensions: &mut Extensions,
    ) {
    }
}

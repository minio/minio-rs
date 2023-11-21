use async_trait::async_trait;
use bytes::Buf;
use http::HeaderMap;
use xmltree::Element;

use crate::s3::{
    error::Error,
    types::{Bucket, FromS3Response, S3Request},
    utils::{from_iso8601utc, get_option_text, get_text},
};

/// Response of [list_buckets()](crate::s3::client::Client::list_buckets) API
#[derive(Debug, Clone)]
pub struct ListBucketsResponse {
    pub headers: HeaderMap,
    pub buckets: Vec<Bucket>,
}

#[async_trait]
impl FromS3Response for ListBucketsResponse {
    async fn from_s3response<'a>(
        _req: S3Request<'a>,
        resp: reqwest::Response,
    ) -> Result<Self, Error> {
        let header_map = resp.headers().clone();
        let body = resp.bytes().await?;
        let mut root = Element::parse(body.reader())?;
        let buckets = root
            .get_mut_child("Buckets")
            .ok_or(Error::XmlError(String::from("<Buckets> tag not found")))?;

        let mut bucket_list: Vec<Bucket> = Vec::new();
        while let Some(b) = buckets.take_child("Bucket") {
            let bucket = b;
            bucket_list.push(Bucket {
                name: get_text(&bucket, "Name")?,
                creation_date: from_iso8601utc(&get_text(&bucket, "CreationDate")?)?,
            })
        }

        Ok(ListBucketsResponse {
            headers: header_map.clone(),
            buckets: bucket_list,
        })
    }
}

/// Response of
/// [get_bucket_versioning()](crate::s3::client::Client::get_bucket_versioning)
/// API
#[derive(Clone, Debug)]
pub struct GetBucketVersioningResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket: String,
    pub status: Option<bool>,
    pub mfa_delete: Option<bool>,
}

#[async_trait]
impl FromS3Response for GetBucketVersioningResponse {
    async fn from_s3response<'a>(
        req: S3Request<'a>,
        resp: reqwest::Response,
    ) -> Result<Self, Error> {
        let headers = resp.headers().clone();
        let body = resp.bytes().await?;
        let root = Element::parse(body.reader())?;

        Ok(GetBucketVersioningResponse {
            headers,
            region: req.get_computed_region(),
            bucket: req.bucket.unwrap().to_string(),
            status: get_option_text(&root, "Status").map(|v| v == "Enabled"),
            mfa_delete: get_option_text(&root, "MFADelete").map(|v| v == "Enabled"),
        })
    }
}

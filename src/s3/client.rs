// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2022 MinIO, Inc.
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

use crate::s3::args::*;
use crate::s3::creds::Provider;
use crate::s3::error::{Error, ErrorResponse};
use crate::s3::http::{BaseUrl, Url};
use crate::s3::response::*;
use crate::s3::signer::sign_v4_s3;
use crate::s3::sse::SseCustomerKey;
use crate::s3::types::{Bucket, DeleteObject, Item, Part};
use crate::s3::utils::{
    from_iso8601utc, get_default_text, get_option_text, get_text, md5sum_hash, merge, sha256_hash,
    to_amz_date, urldecode, utc_now, Multimap,
};
use bytes::{Buf, Bytes};
use dashmap::DashMap;
use hyper::http::Method;
use reqwest::header::HeaderMap;
use std::collections::HashMap;
use xmltree::Element;

fn url_decode(
    encoding_type: &Option<String>,
    prefix: Option<String>,
) -> Result<Option<String>, Error> {
    if let Some(v) = encoding_type.as_ref() {
        if v == "url" {
            if let Some(v) = prefix {
                return Ok(Some(urldecode(&v)?.to_string()));
            }
        }
    }

    if let Some(v) = prefix.as_ref() {
        return Ok(Some(v.to_string()));
    }

    return Ok(None);
}

fn add_common_list_objects_query_params(
    query_params: &mut Multimap,
    delimiter: Option<&str>,
    encoding_type: Option<&str>,
    max_keys: Option<u16>,
    prefix: Option<&str>,
) {
    query_params.insert(
        String::from("delimiter"),
        delimiter.unwrap_or("").to_string(),
    );
    query_params.insert(
        String::from("max-keys"),
        max_keys.unwrap_or(1000).to_string(),
    );
    query_params.insert(String::from("prefix"), prefix.unwrap_or("").to_string());
    if let Some(v) = encoding_type {
        query_params.insert(String::from("encoding-type"), v.to_string());
    }
}

fn parse_common_list_objects_response(
    root: &Element,
) -> Result<
    (
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        bool,
        Option<u16>,
    ),
    Error,
> {
    let encoding_type = get_option_text(&root, "EncodingType")?;
    let prefix = url_decode(&encoding_type, Some(get_default_text(&root, "Prefix")))?;
    Ok((
        get_text(&root, "Name")?,
        encoding_type,
        prefix,
        get_option_text(&root, "Delimiter")?,
        match get_option_text(&root, "IsTruncated")? {
            Some(v) => v.to_lowercase() == "true",
            None => false,
        },
        match get_option_text(&root, "MaxKeys")? {
            Some(v) => Some(v.parse::<u16>()?),
            None => None,
        },
    ))
}

fn parse_list_objects_contents(
    contents: &mut Vec<Item>,
    root: &mut xmltree::Element,
    tag: &str,
    encoding_type: &Option<String>,
    is_delete_marker: bool,
) -> Result<(), Error> {
    loop {
        let content = match root.take_child(tag) {
            Some(v) => v,
            None => break,
        };

        let etype = encoding_type.as_ref().map(|v| v.clone());
        let key = url_decode(&etype, Some(get_text(&content, "Key")?))?.unwrap();
        let last_modified = Some(from_iso8601utc(&get_text(&content, "LastModified")?)?);
        let etag = get_option_text(&content, "ETag")?;
        let v = get_default_text(&content, "Size");
        let size = match v.is_empty() {
            true => None,
            false => Some(v.parse::<usize>()?),
        };
        let storage_class = get_option_text(&content, "StorageClass")?;
        let is_latest = get_default_text(&content, "IsLatest").to_lowercase() == "true";
        let version_id = get_option_text(&content, "VersionId")?;
        let (owner_id, owner_name) = match content.get_child("Owner") {
            Some(v) => (
                get_option_text(&v, "ID")?,
                get_option_text(&v, "DisplayName")?,
            ),
            None => (None, None),
        };
        let user_metadata = match content.get_child("UserMetadata") {
            Some(v) => {
                let mut map: HashMap<String, String> = HashMap::new();
                for node in v.children.iter() {
                    let e = node.as_element().unwrap();
                    map.insert(e.name.clone(), e.get_text().unwrap_or_default().to_string());
                }
                Some(map)
            }
            None => None,
        };

        contents.push(Item {
            name: key,
            last_modified: last_modified,
            etag: etag,
            owner_id: owner_id,
            owner_name: owner_name,
            size: size,
            storage_class: storage_class,
            is_latest: is_latest,
            version_id: version_id,
            user_metadata: user_metadata,
            is_prefix: false,
            is_delete_marker: is_delete_marker,
            encoding_type: etype,
        });
    }

    Ok(())
}

fn parse_list_objects_common_prefixes(
    contents: &mut Vec<Item>,
    root: &mut Element,
    encoding_type: &Option<String>,
) -> Result<(), Error> {
    loop {
        let common_prefix = match root.take_child("CommonPrefixes") {
            Some(v) => v,
            None => break,
        };

        contents.push(Item {
            name: url_decode(&encoding_type, Some(get_text(&common_prefix, "Prefix")?))?.unwrap(),
            last_modified: None,
            etag: None,
            owner_id: None,
            owner_name: None,
            size: None,
            storage_class: None,
            is_latest: false,
            version_id: None,
            user_metadata: None,
            is_prefix: true,
            is_delete_marker: false,
            encoding_type: encoding_type.as_ref().map(|v| v.clone()),
        });
    }

    Ok(())
}

#[derive(Clone, Debug, Default)]
pub struct Client<'a> {
    base_url: BaseUrl,
    provider: Option<&'a dyn Provider>,
    user_agent: String,
    debug: bool,
    ignore_cert_check: bool,
    ssl_cert_file: String,
    region_map: DashMap<String, String>,
}

impl<'a> Client<'a> {
    pub fn new(base_url: BaseUrl, provider: Option<&dyn Provider>) -> Client {
        Client {
            base_url: base_url,
            provider: provider,
            user_agent: String::new(),
            debug: false,
            ignore_cert_check: false,
            ssl_cert_file: String::new(),
            region_map: DashMap::new(),
        }
    }

    fn build_headers(
        &self,
        headers: &mut Multimap,
        query_params: &Multimap,
        region: &String,
        url: &Url,
        method: &Method,
        data: &[u8],
    ) {
        headers.insert(String::from("Host"), url.host_header_value());
        headers.insert(
            String::from("User-Agent"),
            String::from("MinIO (Linux; x86_64) minio-rs/0.1.0"),
        );

        let mut md5sum = String::new();
        let mut sha256 = String::new();
        match *method {
            Method::PUT | Method::POST => {
                headers.insert(String::from("Content-Length"), data.len().to_string());
                if !headers.contains_key("Content-Type") {
                    headers.insert(
                        String::from("Content-Type"),
                        String::from("application/octet-stream"),
                    );
                }
                if self.provider.is_some() {
                    sha256 = sha256_hash(data);
                } else if !headers.contains_key("Content-MD5") {
                    md5sum = md5sum_hash(data);
                }
            }
            _ => {
                if self.provider.is_some() {
                    sha256 = String::from(
                        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                    );
                }
            }
        };
        if !md5sum.is_empty() {
            headers.insert(String::from("Content-MD5"), md5sum);
        }
        if !sha256.is_empty() {
            headers.insert(String::from("x-amz-content-sha256"), sha256.clone());
        }
        let date = utc_now();
        headers.insert(String::from("x-amz-date"), to_amz_date(date));

        match self.provider {
            Some(p) => {
                let creds = p.fetch();
                if creds.session_token.is_some() {
                    headers.insert(
                        String::from("X-Amz-Security-Token"),
                        creds.session_token.unwrap(),
                    );
                }
                sign_v4_s3(
                    &method,
                    &url.path,
                    region,
                    headers,
                    query_params,
                    &creds.access_key,
                    &creds.secret_key,
                    &sha256,
                    date,
                );
            }
            _ => todo!(), // Nothing to do for anonymous request
        };
    }

    fn handle_redirect_response(
        &self,
        status_code: u16,
        method: &Method,
        header_map: &reqwest::header::HeaderMap,
        bucket_name: Option<&str>,
        retry: bool,
    ) -> Result<(String, String), Error> {
        let (mut code, mut message) = match status_code {
            301 => (
                String::from("PermanentRedirect"),
                String::from("Moved Permanently"),
            ),
            307 => (String::from("Redirect"), String::from("Temporary redirect")),
            400 => (String::from("BadRequest"), String::from("Bad request")),
            _ => (String::new(), String::new()),
        };

        let region = match header_map.get("x-amz-bucket-region") {
            Some(v) => v.to_str()?,
            _ => "",
        };

        if !message.is_empty() && !region.is_empty() {
            message.push_str("; use region ");
            message.push_str(region);
        }

        if retry && !region.is_empty() && method == Method::HEAD {
            if let Some(v) = bucket_name {
                if self.region_map.contains_key(v) {
                    code = String::from("RetryHead");
                    message = String::new();
                }
            }
        }

        return Ok((code, message));
    }

    fn get_error_response(
        &self,
        body: &mut Bytes,
        status_code: u16,
        header_map: &reqwest::header::HeaderMap,
        method: &Method,
        resource: &str,
        bucket_name: Option<&str>,
        object_name: Option<&str>,
        retry: bool,
    ) -> Error {
        if body.len() > 0 {
            return match header_map.get("Content-Type") {
                Some(v) => match v.to_str() {
                    Ok(s) => match s.to_lowercase().contains("application/xml") {
                        true => match ErrorResponse::parse(body) {
                            Ok(v) => Error::S3Error(v),
                            Err(e) => e,
                        },
                        false => Error::InvalidResponse(status_code, s.to_string()),
                    },
                    Err(e) => return Error::StrError(e),
                },
                _ => Error::InvalidResponse(status_code, String::new()),
            };
        }

        let (code, message) = match status_code {
            301 | 307 | 400 => match self.handle_redirect_response(
                status_code,
                method,
                header_map,
                bucket_name,
                retry,
            ) {
                Ok(v) => v,
                Err(e) => return e,
            },
            403 => (String::from("AccessDenied"), String::from("Access denied")),
            404 => match object_name {
                Some(_) => (
                    String::from("NoSuchKey"),
                    String::from("Object does not exist"),
                ),
                _ => match bucket_name {
                    Some(_) => (
                        String::from("NoSuchBucket"),
                        String::from("Bucket does not exist"),
                    ),
                    _ => (
                        String::from("ResourceNotFound"),
                        String::from("Request resource not found"),
                    ),
                },
            },
            405 => (
                String::from("MethodNotAllowed"),
                String::from("The specified method is not allowed against this resource"),
            ),
            409 => match bucket_name {
                Some(_) => (
                    String::from("NoSuchBucket"),
                    String::from("Bucket does not exist"),
                ),
                _ => (
                    String::from("ResourceConflict"),
                    String::from("Request resource conflicts"),
                ),
            },
            501 => (
                String::from("MethodNotAllowed"),
                String::from("The specified method is not allowed against this resource"),
            ),
            _ => return Error::ServerError(status_code),
        };

        let request_id = match header_map.get("x-amz-request-id") {
            Some(v) => match v.to_str() {
                Ok(s) => s.to_string(),
                Err(e) => return Error::StrError(e),
            },
            _ => String::new(),
        };

        let host_id = match header_map.get("x-amz-id-2") {
            Some(v) => match v.to_str() {
                Ok(s) => s.to_string(),
                Err(e) => return Error::StrError(e),
            },
            _ => String::new(),
        };

        Error::S3Error(ErrorResponse {
            code: code,
            message: message,
            resource: resource.to_string(),
            request_id: request_id,
            host_id: host_id,
            bucket_name: bucket_name.unwrap_or_default().to_string(),
            object_name: object_name.unwrap_or_default().to_string(),
        })
    }

    pub async fn do_execute(
        &self,
        method: Method,
        region: &String,
        headers: &mut Multimap,
        query_params: &Multimap,
        bucket_name: Option<&str>,
        object_name: Option<&str>,
        data: Option<&[u8]>,
        retry: bool,
    ) -> Result<reqwest::Response, Error> {
        let body = data.unwrap_or_default();
        let url =
            self.base_url
                .build_url(&method, region, query_params, bucket_name, object_name)?;
        self.build_headers(headers, query_params, region, &url, &method, body);

        let client = reqwest::Client::new();
        let mut req = client.request(method.clone(), url.to_string());

        for (key, values) in headers.iter_all() {
            for value in values {
                req = req.header(key, value);
            }
        }

        if method == Method::PUT || method == Method::POST {
            req = req.body(body.to_vec());
        }

        let resp = req.send().await?;
        if resp.status().is_success() {
            return Ok(resp);
        }

        let status_code = resp.status().as_u16();
        let header_map = resp.headers().clone();
        let mut body = resp.bytes().await?;
        let e = self.get_error_response(
            &mut body,
            status_code,
            &header_map,
            &method,
            &url.path,
            bucket_name,
            object_name,
            retry,
        );

        match e {
            Error::S3Error(ref er) => {
                if er.code == "NoSuchBucket" || er.code == "RetryHead" {
                    if let Some(v) = bucket_name {
                        self.region_map.remove(v);
                    }
                }
            }
            _ => todo!(), // Nothing to do.
        };

        return Err(e);
    }

    pub async fn execute(
        &self,
        method: Method,
        region: &String,
        headers: &mut Multimap,
        query_params: &Multimap,
        bucket_name: Option<&str>,
        object_name: Option<&str>,
        data: Option<&[u8]>,
    ) -> Result<reqwest::Response, Error> {
        let res = self
            .do_execute(
                method.clone(),
                region,
                headers,
                query_params,
                bucket_name,
                object_name,
                data,
                true,
            )
            .await;
        match res {
            Ok(r) => return Ok(r),
            Err(e) => match e {
                Error::S3Error(ref er) => {
                    if er.code != "RetryHead" {
                        return Err(e);
                    }
                }
                _ => return Err(e),
            },
        };

        // Retry only once on RetryHead error.
        self.do_execute(
            method.clone(),
            region,
            headers,
            query_params,
            bucket_name,
            object_name,
            data,
            false,
        )
        .await
    }

    pub async fn get_region(
        &self,
        bucket_name: &str,
        region: Option<&str>,
    ) -> Result<String, Error> {
        if !region.map_or(true, |v| v.is_empty()) {
            if !self.base_url.region.is_empty() && self.base_url.region != *region.unwrap() {
                return Err(Error::RegionMismatch(
                    self.base_url.region.clone(),
                    region.unwrap().to_string(),
                ));
            }

            return Ok(region.unwrap().to_string());
        }

        if !self.base_url.region.is_empty() {
            return Ok(self.base_url.region.clone());
        }

        if bucket_name.is_empty() || self.provider.is_none() {
            return Ok(String::from("us-east-1"));
        }

        if let Some(v) = self.region_map.get(bucket_name) {
            return Ok((*v).to_string());
        }

        let mut headers = Multimap::new();
        let mut query_params = Multimap::new();
        query_params.insert(String::from("location"), String::new());

        let resp = self
            .execute(
                Method::GET,
                &String::from("us-east-1"),
                &mut headers,
                &query_params,
                Some(bucket_name),
                None,
                None,
            )
            .await?;
        let body = resp.bytes().await?;
        let root = Element::parse(body.reader())?;

        let location = root.get_text().unwrap_or_default().to_string();
        self.region_map
            .insert(bucket_name.to_string(), location.clone());
        Ok(location)
    }

    pub async fn abort_multipart_upload(
        &self,
        args: &AbortMultipartUploadArgs<'_>,
    ) -> Result<AbortMultipartUploadResponse, Error> {
        let region = self.get_region(&args.bucket, args.region).await?;

        let mut headers = Multimap::new();
        if let Some(v) = &args.extra_headers {
            merge(&mut headers, v);
        }
        let mut query_params = Multimap::new();
        if let Some(v) = &args.extra_query_params {
            merge(&mut query_params, v);
        }
        query_params.insert(String::from("uploadId"), args.upload_id.to_string());

        let resp = self
            .execute(
                Method::DELETE,
                &region,
                &mut headers,
                &query_params,
                Some(&args.bucket),
                Some(&args.object),
                None,
            )
            .await?;

        Ok(AbortMultipartUploadResponse {
            headers: resp.headers().clone(),
            region: region.clone(),
            bucket_name: args.bucket.to_string(),
            object_name: args.object.to_string(),
            upload_id: args.upload_id.to_string(),
        })
    }

    pub async fn bucket_exists(&self, args: &BucketExistsArgs<'_>) -> Result<bool, Error> {
        let region;
        match self.get_region(&args.bucket, args.region).await {
            Ok(r) => region = r,
            Err(e) => match e {
                Error::S3Error(ref er) => {
                    if er.code == "NoSuchBucket" {
                        return Ok(false);
                    }
                    return Err(e);
                }
                _ => return Err(e),
            },
        };

        let mut headers = Multimap::new();
        if let Some(v) = &args.extra_headers {
            merge(&mut headers, v);
        }
        let mut query_params = &Multimap::new();
        if let Some(v) = &args.extra_query_params {
            query_params = v;
        }

        match self
            .execute(
                Method::HEAD,
                &region,
                &mut headers,
                &query_params,
                Some(&args.bucket),
                None,
                None,
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(e) => match e {
                Error::S3Error(ref er) => {
                    if er.code == "NoSuchBucket" {
                        return Ok(false);
                    }
                    return Err(e);
                }
                _ => return Err(e),
            },
        }
    }

    pub async fn complete_multipart_upload(
        &self,
        args: &CompleteMultipartUploadArgs<'_>,
    ) -> Result<CompleteMultipartUploadResponse, Error> {
        let region = self.get_region(&args.bucket, args.region).await?;

        let mut data = String::from("<CompleteMultipartUpload>");
        for part in args.parts.iter() {
            let s = format!(
                "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>",
                part.number, part.etag
            );
            data.push_str(&s);
        }
        data.push_str("</CompleteMultipartUpload>");
        let b = data.as_bytes();

        let mut headers = Multimap::new();
        if let Some(v) = &args.extra_headers {
            merge(&mut headers, v);
        }
        headers.insert(
            String::from("Content-Type"),
            String::from("application/xml"),
        );
        headers.insert(String::from("Content-MD5"), md5sum_hash(b));

        let mut query_params = Multimap::new();
        if let Some(v) = &args.extra_query_params {
            merge(&mut query_params, v);
        }
        query_params.insert(String::from("uploadId"), args.upload_id.to_string());

        let resp = self
            .execute(
                Method::POST,
                &region,
                &mut headers,
                &query_params,
                Some(&args.bucket),
                Some(&args.object),
                Some(&b),
            )
            .await?;
        let header_map = resp.headers().clone();
        let body = resp.bytes().await?;
        let root = Element::parse(body.reader())?;

        Ok(CompleteMultipartUploadResponse {
            headers: header_map.clone(),
            bucket_name: get_text(&root, "Bucket")?,
            object_name: get_text(&root, "Key")?,
            location: get_text(&root, "Location")?,
            etag: get_text(&root, "ETag")?.trim_matches('"').to_string(),
            version_id: match header_map.get("x-amz-version-id") {
                Some(v) => Some(v.to_str()?.to_string()),
                None => None,
            },
        })
    }

    pub async fn create_multipart_upload(
        &self,
        args: &CreateMultipartUploadArgs<'_>,
    ) -> Result<CreateMultipartUploadResponse, Error> {
        let region = self.get_region(&args.bucket, args.region).await?;

        let mut headers = Multimap::new();
        if let Some(v) = &args.extra_headers {
            merge(&mut headers, v);
        }
        if !headers.contains_key("Content-Type") {
            headers.insert(
                String::from("Content-Type"),
                String::from("application/octet-stream"),
            );
        }

        let mut query_params = Multimap::new();
        if let Some(v) = &args.extra_query_params {
            merge(&mut query_params, v);
        }
        query_params.insert(String::from("uploads"), String::new());

        let resp = self
            .execute(
                Method::POST,
                &region,
                &mut headers,
                &query_params,
                Some(&args.bucket),
                Some(&args.object),
                None,
            )
            .await?;
        let header_map = resp.headers().clone();
        let body = resp.bytes().await?;
        let root = Element::parse(body.reader())?;

        Ok(CreateMultipartUploadResponse {
            headers: header_map.clone(),
            region: region.clone(),
            bucket_name: args.bucket.to_string(),
            object_name: args.object.to_string(),
            upload_id: get_text(&root, "UploadId")?,
        })
    }

    // DeleteBucketEncryptionResponse DeleteBucketEncryption(
    //     DeleteBucketEncryptionArgs args);
    // DisableObjectLegalHoldResponse DisableObjectLegalHold(
    //     DisableObjectLegalHoldArgs args);
    // DeleteBucketLifecycleResponse DeleteBucketLifecycle(
    //     DeleteBucketLifecycleArgs args);
    // DeleteBucketNotificationResponse DeleteBucketNotification(
    //     DeleteBucketNotificationArgs args);
    // DeleteBucketPolicyResponse DeleteBucketPolicy(DeleteBucketPolicyArgs args);
    // DeleteBucketReplicationResponse DeleteBucketReplication(
    //     DeleteBucketReplicationArgs args);
    // DeleteBucketTagsResponse DeleteBucketTags(DeleteBucketTagsArgs args);
    // DeleteObjectLockConfigResponse DeleteObjectLockConfig(
    //     DeleteObjectLockConfigArgs args);
    // DeleteObjectTagsResponse DeleteObjectTags(DeleteObjectTagsArgs args);
    // EnableObjectLegalHoldResponse EnableObjectLegalHold(
    //     EnableObjectLegalHoldArgs args);
    // GetBucketEncryptionResponse GetBucketEncryption(GetBucketEncryptionArgs args);
    // GetBucketLifecycleResponse GetBucketLifecycle(GetBucketLifecycleArgs args);
    // GetBucketNotificationResponse GetBucketNotification(
    //     GetBucketNotificationArgs args);
    // GetBucketPolicyResponse GetBucketPolicy(GetBucketPolicyArgs args);
    // GetBucketReplicationResponse GetBucketReplication(
    //     GetBucketReplicationArgs args);
    // GetBucketTagsResponse GetBucketTags(GetBucketTagsArgs args);
    // GetBucketVersioningResponse GetBucketVersioning(GetBucketVersioningArgs args);

    pub async fn get_object(&self, args: &GetObjectArgs<'_>) -> Result<reqwest::Response, Error> {
        if args.ssec.is_some() && !self.base_url.https {
            return Err(Error::SseTlsRequired);
        }

        let region = self.get_region(&args.bucket, args.region).await?;

        let mut headers = Multimap::new();
        if let Some(v) = &args.extra_headers {
            merge(&mut headers, v);
        }
        merge(&mut headers, &args.get_headers());

        let mut query_params = Multimap::new();
        if let Some(v) = &args.extra_query_params {
            merge(&mut query_params, v);
        }
        if let Some(v) = args.version_id {
            query_params.insert(String::from("versionId"), v.to_string());
        }

        self.execute(
            Method::GET,
            &region,
            &mut headers,
            &query_params,
            Some(&args.bucket),
            Some(&args.object),
            None,
        )
        .await
    }

    // GetObjectLockConfigResponse GetObjectLockConfig(GetObjectLockConfigArgs args);
    // GetObjectRetentionResponse GetObjectRetention(GetObjectRetentionArgs args);
    // GetObjectTagsResponse GetObjectTags(GetObjectTagsArgs args);
    // GetPresignedObjectUrlResponse GetPresignedObjectUrl(
    //     GetPresignedObjectUrlArgs args);
    // GetPresignedPostFormDataResponse GetPresignedPostFormData(PostPolicy policy);
    // IsObjectLegalHoldEnabledResponse IsObjectLegalHoldEnabled(
    //     IsObjectLegalHoldEnabledArgs args);

    pub async fn list_buckets(
        &self,
        args: &ListBucketsArgs<'_>,
    ) -> Result<ListBucketsResponse, Error> {
        let mut headers = Multimap::new();
        if let Some(v) = &args.extra_headers {
            merge(&mut headers, v);
        }
        let mut query_params = &Multimap::new();
        if let Some(v) = &args.extra_query_params {
            query_params = v;
        }
        let resp = self
            .execute(
                Method::GET,
                &String::from("us-east-1"),
                &mut headers,
                &query_params,
                None,
                None,
                None,
            )
            .await?;
        let header_map = resp.headers().clone();
        let body = resp.bytes().await?;
        let mut root = Element::parse(body.reader())?;
        let buckets = root
            .get_mut_child("Buckets")
            .ok_or(Error::XmlError(String::from("<Buckets> tag not found")))?;

        let mut bucket_list: Vec<Bucket> = Vec::new();
        loop {
            let bucket = match buckets.take_child("Bucket") {
                Some(b) => b,
                None => break,
            };

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

    // ListenBucketNotificationResponse ListenBucketNotification(
    //     ListenBucketNotificationArgs args);
    pub async fn list_objects_v1(
        &self,
        args: &ListObjectsV1Args<'_>,
    ) -> Result<ListObjectsV1Response, Error> {
        let region = self.get_region(&args.bucket, args.region).await?;

        let mut headers = Multimap::new();
        if let Some(v) = &args.extra_headers {
            merge(&mut headers, v);
        }

        let mut query_params = Multimap::new();
        if let Some(v) = &args.extra_query_params {
            merge(&mut query_params, v);
        }
        add_common_list_objects_query_params(
            &mut query_params,
            args.delimiter,
            args.encoding_type,
            args.max_keys,
            args.prefix,
        );
        if let Some(v) = &args.marker {
            query_params.insert(String::from("marker"), v.to_string());
        }

        let resp = self
            .execute(
                Method::GET,
                &region,
                &mut headers,
                &query_params,
                Some(&args.bucket),
                None,
                None,
            )
            .await?;
        let header_map = resp.headers().clone();
        let body = resp.bytes().await?;
        let mut root = Element::parse(body.reader())?;

        let (name, encoding_type, prefix, delimiter, is_truncated, max_keys) =
            parse_common_list_objects_response(&root)?;
        let marker = url_decode(&encoding_type, get_option_text(&root, "Marker")?)?;
        let mut next_marker = url_decode(&encoding_type, get_option_text(&root, "NextMarker")?)?;
        let mut contents: Vec<Item> = Vec::new();
        parse_list_objects_contents(&mut contents, &mut root, "Contents", &encoding_type, false)?;
        if is_truncated && next_marker.is_none() {
            next_marker = match contents.last() {
                Some(v) => Some(v.name.clone()),
                None => None,
            }
        }
        parse_list_objects_common_prefixes(&mut contents, &mut root, &encoding_type)?;

        Ok(ListObjectsV1Response {
            headers: header_map,
            name: name,
            encoding_type: encoding_type,
            prefix: prefix,
            delimiter: delimiter,
            is_truncated: is_truncated,
            max_keys: max_keys,
            contents: contents,
            marker: marker,
            next_marker: next_marker,
        })
    }

    pub async fn list_objects_v2(
        &self,
        args: &ListObjectsV2Args<'_>,
    ) -> Result<ListObjectsV2Response, Error> {
        let region = self.get_region(&args.bucket, args.region).await?;

        let mut headers = Multimap::new();
        if let Some(v) = &args.extra_headers {
            merge(&mut headers, v);
        }

        let mut query_params = Multimap::new();
        if let Some(v) = &args.extra_query_params {
            merge(&mut query_params, v);
        }
        query_params.insert(String::from("list-type"), String::from("2"));
        add_common_list_objects_query_params(
            &mut query_params,
            args.delimiter,
            args.encoding_type,
            args.max_keys,
            args.prefix,
        );
        if let Some(v) = &args.continuation_token {
            query_params.insert(String::from("continuation-token"), v.to_string());
        }
        if args.fetch_owner {
            query_params.insert(String::from("fetch-owner"), String::from("true"));
        }
        if let Some(v) = &args.start_after {
            query_params.insert(String::from("start-after"), v.to_string());
        }
        if args.include_user_metadata {
            query_params.insert(String::from("metadata"), String::from("true"));
        }

        let resp = self
            .execute(
                Method::GET,
                &region,
                &mut headers,
                &query_params,
                Some(&args.bucket),
                None,
                None,
            )
            .await?;
        let header_map = resp.headers().clone();
        let body = resp.bytes().await?;
        let mut root = Element::parse(body.reader())?;

        let (name, encoding_type, prefix, delimiter, is_truncated, max_keys) =
            parse_common_list_objects_response(&root)?;
        let text = get_option_text(&root, "KeyCount")?;
        let key_count = match text {
            Some(v) => match v.is_empty() {
                true => None,
                false => Some(v.parse::<u16>()?),
            },
            None => None,
        };
        let start_after = url_decode(&encoding_type, get_option_text(&root, "StartAfter")?)?;
        let continuation_token = get_option_text(&root, "ContinuationToken")?;
        let next_continuation_token = get_option_text(&root, "NextContinuationToken")?;
        let mut contents: Vec<Item> = Vec::new();
        parse_list_objects_contents(&mut contents, &mut root, "Contents", &encoding_type, false)?;
        parse_list_objects_common_prefixes(&mut contents, &mut root, &encoding_type)?;

        Ok(ListObjectsV2Response {
            headers: header_map,
            name: name,
            encoding_type: encoding_type,
            prefix: prefix,
            delimiter: delimiter,
            is_truncated: is_truncated,
            max_keys: max_keys,
            contents: contents,
            key_count: key_count,
            start_after: start_after,
            continuation_token: continuation_token,
            next_continuation_token: next_continuation_token,
        })
    }

    pub async fn list_object_versions(
        &self,
        args: &ListObjectVersionsArgs<'_>,
    ) -> Result<ListObjectVersionsResponse, Error> {
        let region = self.get_region(&args.bucket, args.region).await?;

        let mut headers = Multimap::new();
        if let Some(v) = &args.extra_headers {
            merge(&mut headers, v);
        }

        let mut query_params = Multimap::new();
        if let Some(v) = &args.extra_query_params {
            merge(&mut query_params, v);
        }
        query_params.insert(String::from("versions"), String::new());
        add_common_list_objects_query_params(
            &mut query_params,
            args.delimiter,
            args.encoding_type,
            args.max_keys,
            args.prefix,
        );
        if let Some(v) = &args.key_marker {
            query_params.insert(String::from("key-marker"), v.to_string());
        }
        if let Some(v) = &args.version_id_marker {
            query_params.insert(String::from("version-id-marker"), v.to_string());
        }

        let resp = self
            .execute(
                Method::GET,
                &region,
                &mut headers,
                &query_params,
                Some(&args.bucket),
                None,
                None,
            )
            .await?;
        let header_map = resp.headers().clone();
        let body = resp.bytes().await?;
        let mut root = Element::parse(body.reader())?;

        let (name, encoding_type, prefix, delimiter, is_truncated, max_keys) =
            parse_common_list_objects_response(&root)?;
        let key_marker = url_decode(&encoding_type, get_option_text(&root, "KeyMarker")?)?;
        let next_key_marker = url_decode(&encoding_type, get_option_text(&root, "NextKeyMarker")?)?;
        let version_id_marker = get_option_text(&root, "VersionIdMarker")?;
        let next_version_id_marker = get_option_text(&root, "NextVersionIdMarker")?;
        let mut contents: Vec<Item> = Vec::new();
        parse_list_objects_contents(&mut contents, &mut root, "Version", &encoding_type, false)?;
        parse_list_objects_common_prefixes(&mut contents, &mut root, &encoding_type)?;
        parse_list_objects_contents(
            &mut contents,
            &mut root,
            "DeleteMarker",
            &encoding_type,
            true,
        )?;

        Ok(ListObjectVersionsResponse {
            headers: header_map,
            name: name,
            encoding_type: encoding_type,
            prefix: prefix,
            delimiter: delimiter,
            is_truncated: is_truncated,
            max_keys: max_keys,
            contents: contents,
            key_marker: key_marker,
            next_key_marker: next_key_marker,
            version_id_marker: version_id_marker,
            next_version_id_marker: next_version_id_marker,
        })
    }

    pub async fn list_objects(&self, args: &ListObjectsArgs<'_>) -> Result<(), Error> {
        let mut lov1_args = ListObjectsV1Args::new(&args.bucket)?;
        lov1_args.extra_headers = args.extra_headers;
        lov1_args.extra_query_params = args.extra_query_params;
        lov1_args.region = args.region;
        if args.recursive {
            lov1_args.delimiter = None;
        } else {
            lov1_args.delimiter = Some(args.delimiter.unwrap_or("/"));
        }
        lov1_args.encoding_type = match args.use_url_encoding_type {
            true => Some("url"),
            false => None,
        };
        lov1_args.max_keys = args.max_keys;
        lov1_args.prefix = args.prefix;
        lov1_args.marker = args.marker.map(|x| x.to_string());

        let mut lov2_args = ListObjectsV2Args::new(&args.bucket)?;
        lov2_args.extra_headers = args.extra_headers;
        lov2_args.extra_query_params = args.extra_query_params;
        lov2_args.region = args.region;
        if args.recursive {
            lov2_args.delimiter = None;
        } else {
            lov2_args.delimiter = Some(args.delimiter.unwrap_or("/"));
        }
        lov2_args.encoding_type = match args.use_url_encoding_type {
            true => Some("url"),
            false => None,
        };
        lov2_args.max_keys = args.max_keys;
        lov2_args.prefix = args.prefix;
        lov2_args.start_after = args.start_after.map(|x| x.to_string());
        lov2_args.continuation_token = args.continuation_token.map(|x| x.to_string());
        lov2_args.fetch_owner = args.fetch_owner;
        lov2_args.include_user_metadata = args.include_user_metadata;

        let mut lov_args = ListObjectVersionsArgs::new(&args.bucket)?;
        lov_args.extra_headers = args.extra_headers;
        lov_args.extra_query_params = args.extra_query_params;
        lov_args.region = args.region;
        if args.recursive {
            lov_args.delimiter = None;
        } else {
            lov_args.delimiter = Some(args.delimiter.unwrap_or("/"));
        }
        lov_args.encoding_type = match args.use_url_encoding_type {
            true => Some("url"),
            false => None,
        };
        lov_args.max_keys = args.max_keys;
        lov_args.prefix = args.prefix;
        lov_args.key_marker = args.key_marker.map(|x| x.to_string());
        lov_args.version_id_marker = args.version_id_marker.map(|x| x.to_string());

        let mut stop = false;
        while !stop {
            if args.include_versions {
                let resp = self.list_object_versions(&lov_args).await;
                match resp {
                    Ok(v) => {
                        if v.is_truncated {
                            lov_args.key_marker = v.next_key_marker;
                            lov_args.version_id_marker = v.next_version_id_marker;
                        } else {
                            stop = true;
                        }
                        for item in v.contents.iter() {
                            if !(args.result_fn)(Ok(item)) {
                                stop = true;
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        (args.result_fn)(Err(e));
                        return Ok(());
                    }
                };
            } else if args.use_api_v1 {
                let resp = self.list_objects_v1(&lov1_args).await;
                match resp {
                    Ok(v) => {
                        if v.is_truncated {
                            lov1_args.marker = v.next_marker;
                        } else {
                            stop = true;
                        }
                        for item in v.contents.iter() {
                            if !(args.result_fn)(Ok(item)) {
                                stop = true;
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        (args.result_fn)(Err(e));
                        return Ok(());
                    }
                };
            } else {
                let resp = self.list_objects_v2(&lov2_args).await;
                match resp {
                    Ok(v) => {
                        if v.is_truncated {
                            lov2_args.start_after = v.start_after;
                            lov2_args.continuation_token = v.next_continuation_token;
                        } else {
                            stop = true;
                        }
                        for item in v.contents.iter() {
                            if !(args.result_fn)(Ok(item)) {
                                stop = true;
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        (args.result_fn)(Err(e));
                        return Ok(());
                    }
                };
            }
        }

        Ok(())
    }

    pub async fn make_bucket(
        &self,
        args: &MakeBucketArgs<'_>,
    ) -> Result<MakeBucketResponse, Error> {
        let mut region = "us-east-1";
        if let Some(r) = &args.region {
            if !self.base_url.region.is_empty() {
                if self.base_url.region != *r {
                    return Err(Error::RegionMismatch(
                        self.base_url.region.clone(),
                        r.to_string(),
                    ));
                }
                region = r;
            }
        }

        let mut headers = Multimap::new();
        if let Some(v) = &args.extra_headers {
            merge(&mut headers, v);
        };

        if args.object_lock {
            headers.insert(
                String::from("x-amz-bucket-object-lock-enabled"),
                String::from("true"),
            );
        }

        let mut query_params = &Multimap::new();
        if let Some(v) = &args.extra_query_params {
            query_params = v;
        }

        let data = match region {
	    "us-east-1" => String::new(),
	    _ => format!("<CreateBucketConfiguration><LocationConstraint>{}</LocationConstraint></CreateBucketConfiguration>", region),
	};

        let body = match data.is_empty() {
            true => None,
            false => Some(data.as_bytes()),
        };

        let resp = self
            .execute(
                Method::PUT,
                &region.to_string(),
                &mut headers,
                &query_params,
                Some(&args.bucket),
                None,
                body,
            )
            .await?;
        self.region_map
            .insert(args.bucket.to_string(), region.to_string());

        Ok(MakeBucketResponse {
            headers: resp.headers().clone(),
            region: region.to_string(),
            bucket_name: args.bucket.to_string(),
        })
    }

    fn read_part(
        reader: &mut dyn std::io::Read,
        buf: &mut [u8],
        size: usize,
    ) -> Result<usize, Error> {
        let mut bytes_read = 0_usize;
        let mut i = 0_usize;
        let mut stop = false;
        while !stop {
            let br = reader.read(&mut buf[i..size])?;
            bytes_read += br;
            stop = (br == 0) || (br == size - i);
            i += br;
        }

        Ok(bytes_read)
    }

    async fn do_put_object(
        &self,
        args: &mut PutObjectArgs<'_>,
        buf: &mut [u8],
        upload_id: &mut String,
    ) -> Result<PutObjectResponse, Error> {
        let mut headers = args.get_headers();
        if !headers.contains_key("Content-Type") {
            if args.content_type.is_empty() {
                headers.insert(
                    String::from("Content-Type"),
                    String::from("application/octet-stream"),
                );
            } else {
                headers.insert(String::from("Content-Type"), args.content_type.to_string());
            }
        }

        let mut uploaded_size = 0_usize;
        let mut part_number = 0_i16;
        let mut stop = false;
        let mut one_byte: Vec<u8> = Vec::new();
        let mut parts: Vec<Part> = Vec::new();
        let object_size = &args.object_size.unwrap();
        let mut part_size = args.part_size;
        let mut part_count = args.part_count;

        while !stop {
            part_number += 1;
            let mut bytes_read = 0_usize;
            if args.part_count > 0 {
                if part_number == args.part_count {
                    part_size = object_size - uploaded_size;
                    stop = true;
                }

                bytes_read = Client::read_part(&mut args.stream, buf, part_size)?;
                if bytes_read != part_size {
                    return Err(Error::InsufficientData(part_size, bytes_read));
                }
            } else {
                let mut size = part_size + 1;
                let mut newbuf = match one_byte.len() == 1 {
                    true => {
                        buf[0] = one_byte.pop().unwrap();
                        size -= 1;
                        bytes_read = 1;
                        &mut buf[1..]
                    }
                    false => buf,
                };

                let n = Client::read_part(&mut args.stream, &mut newbuf, size)?;
                bytes_read += n;

                // If bytes read is less than or equals to part size, then we have reached last part.
                if bytes_read <= part_size {
                    part_count = part_number;
                    part_size = bytes_read;
                    stop = true;
                } else {
                    one_byte.push(buf[part_size + 1]);
                }
            }

            let data = &buf[0..part_size];
            uploaded_size += part_size;

            if part_count == 1_i16 {
                let mut poaargs = PutObjectApiArgs::new(&args.bucket, &args.object, &data)?;
                poaargs.extra_query_params = args.extra_query_params;
                poaargs.region = args.region;
                poaargs.headers = Some(&headers);

                return self.put_object_api(&poaargs).await;
            }

            if upload_id.is_empty() {
                let mut cmuargs = CreateMultipartUploadArgs::new(&args.bucket, &args.object)?;
                cmuargs.extra_query_params = args.extra_query_params;
                cmuargs.region = args.region;
                cmuargs.headers = Some(&headers);

                let resp = self.create_multipart_upload(&cmuargs).await?;
                upload_id.push_str(&resp.upload_id);
            }

            let mut upargs = UploadPartArgs::new(
                &args.bucket,
                &args.object,
                &upload_id,
                part_number as u16,
                &data,
            )?;
            upargs.region = args.region;

            let ssec_headers = match args.sse {
                Some(v) => match v.as_any().downcast_ref::<SseCustomerKey>() {
                    Some(_) => v.headers(),
                    _ => Multimap::new(),
                },
                _ => Multimap::new(),
            };
            upargs.headers = Some(&ssec_headers);

            let resp = self.upload_part(&upargs).await?;
            parts.push(Part {
                number: part_number as u16,
                etag: resp.etag.clone(),
            });
        }

        let mut cmuargs =
            CompleteMultipartUploadArgs::new(&args.bucket, &args.object, &upload_id, &parts)?;
        cmuargs.region = args.region;

        return self.complete_multipart_upload(&cmuargs).await;
    }

    pub async fn put_object(
        &self,
        args: &mut PutObjectArgs<'_>,
    ) -> Result<PutObjectResponse, Error> {
        if let Some(v) = &args.sse {
            if v.tls_required() && !self.base_url.https {
                return Err(Error::SseTlsRequired);
            }
        }

        let bufsize = match args.part_count > 0 {
            true => args.part_size as usize,
            false => (args.part_size as usize) + 1,
        };
        let mut buf = vec![0_u8; bufsize];

        let mut upload_id = String::new();
        let res = self.do_put_object(args, &mut buf, &mut upload_id).await;

        std::mem::drop(buf);

        if res.is_err() && !upload_id.is_empty() {
            let amuargs = &AbortMultipartUploadArgs::new(&args.bucket, &args.object, &upload_id)?;
            self.abort_multipart_upload(&amuargs).await?;
        }

        return res;
    }

    pub async fn put_object_api(
        &self,
        args: &PutObjectApiArgs<'_>,
    ) -> Result<PutObjectApiResponse, Error> {
        let region = self.get_region(&args.bucket, args.region).await?;

        let mut headers = args.get_headers();

        let mut query_params = Multimap::new();
        if let Some(v) = &args.extra_query_params {
            merge(&mut query_params, v);
        }
        if let Some(v) = &args.query_params {
            merge(&mut query_params, v);
        }

        let resp = self
            .execute(
                Method::PUT,
                &region,
                &mut headers,
                &query_params,
                Some(&args.bucket),
                Some(&args.object),
                Some(&args.data),
            )
            .await?;
        let header_map = resp.headers();

        Ok(PutObjectBaseResponse {
            headers: header_map.clone(),
            bucket_name: args.bucket.to_string(),
            object_name: args.object.to_string(),
            location: region.clone(),
            etag: match header_map.get("etag") {
                Some(v) => v.to_str()?.to_string().trim_matches('"').to_string(),
                _ => String::new(),
            },
            version_id: match header_map.get("x-amz-version-id") {
                Some(v) => Some(v.to_str()?.to_string()),
                None => None,
            },
        })
    }

    pub async fn remove_bucket(
        &self,
        args: &RemoveBucketArgs<'_>,
    ) -> Result<RemoveBucketResponse, Error> {
        let region = self.get_region(&args.bucket, args.region).await?;

        let mut headers = Multimap::new();
        if let Some(v) = &args.extra_headers {
            merge(&mut headers, v);
        }
        let mut query_params = &Multimap::new();
        if let Some(v) = &args.extra_query_params {
            query_params = v;
        }

        let resp = self
            .execute(
                Method::DELETE,
                &region,
                &mut headers,
                &query_params,
                Some(&args.bucket),
                None,
                None,
            )
            .await?;
        self.region_map.remove(&args.bucket.to_string());

        Ok(RemoveBucketResponse {
            headers: resp.headers().clone(),
            region: region.to_string(),
            bucket_name: args.bucket.to_string(),
        })
    }

    pub async fn remove_object(
        &self,
        args: &RemoveObjectArgs<'_>,
    ) -> Result<RemoveObjectResponse, Error> {
        let region = self.get_region(&args.bucket, args.region).await?;

        let mut headers = Multimap::new();
        if let Some(v) = &args.extra_headers {
            merge(&mut headers, v);
        }
        let mut query_params = Multimap::new();
        if let Some(v) = &args.extra_query_params {
            merge(&mut query_params, v);
        }
        if let Some(v) = args.version_id {
            query_params.insert(String::from("versionId"), v.to_string());
        }

        let resp = self
            .execute(
                Method::DELETE,
                &region,
                &mut headers,
                &query_params,
                Some(&args.bucket),
                Some(&args.object),
                None,
            )
            .await?;

        Ok(RemoveObjectResponse {
            headers: resp.headers().clone(),
            region: region.to_string(),
            bucket_name: args.bucket.to_string(),
            object_name: args.object.to_string(),
            version_id: match args.version_id {
                Some(v) => Some(v.to_string()),
                None => None,
            },
        })
    }

    pub async fn remove_objects_api(
        &self,
        args: &RemoveObjectsApiArgs<'_>,
    ) -> Result<RemoveObjectsApiResponse, Error> {
        let region = self.get_region(&args.bucket, args.region).await?;

        let mut data = String::from("<Delete>");
        if args.quiet {
            data.push_str("<Quiet>true</Quiet>");
        }
        for object in args.objects.iter() {
            data.push_str("<Object>");
            data.push_str("<Key>");
            data.push_str(&object.name);
            data.push_str("</Key>");
            if let Some(v) = object.version_id {
                data.push_str("<VersionId>");
                data.push_str(&v);
                data.push_str("</VersionId>");
            }
            data.push_str("</Object>");
        }
        data.push_str("</Delete>");
        let b = data.as_bytes();

        let mut headers = Multimap::new();
        if let Some(v) = &args.extra_headers {
            merge(&mut headers, v);
        }
        if args.bypass_governance_mode {
            headers.insert(
                String::from("x-amz-bypass-governance-retention"),
                String::from("true"),
            );
        }
        headers.insert(
            String::from("Content-Type"),
            String::from("application/xml"),
        );
        headers.insert(String::from("Content-MD5"), md5sum_hash(b));

        let mut query_params = Multimap::new();
        if let Some(v) = &args.extra_query_params {
            merge(&mut query_params, v);
        }
        query_params.insert(String::from("delete"), String::new());

        let resp = self
            .execute(
                Method::POST,
                &region,
                &mut headers,
                &query_params,
                Some(&args.bucket),
                None,
                Some(&b),
            )
            .await?;
        let header_map = resp.headers().clone();
        let body = resp.bytes().await?;
        let mut root = Element::parse(body.reader())?;

        let mut objects: Vec<DeletedObject> = Vec::new();
        loop {
            let deleted = match root.take_child("Deleted") {
                Some(v) => v,
                None => break,
            };

            objects.push(DeletedObject {
                name: get_text(&deleted, "Key")?,
                version_id: get_option_text(&deleted, "VersionId")?,
                delete_marker: get_text(&deleted, "DeleteMarker")?.to_lowercase() == "true",
                delete_marker_version_id: get_option_text(&deleted, "DeleteMarkerVersionId")?,
            })
        }

        let mut errors: Vec<DeleteError> = Vec::new();
        loop {
            let error = match root.take_child("Error") {
                Some(v) => v,
                None => break,
            };

            errors.push(DeleteError {
                code: get_text(&error, "Code")?,
                message: get_text(&error, "Message")?,
                object_name: get_text(&error, "Key")?,
                version_id: get_option_text(&error, "VersionId")?,
            })
        }

        Ok(RemoveObjectsApiResponse {
            headers: header_map.clone(),
            region: region.clone(),
            bucket_name: args.bucket.to_string(),
            objects: objects,
            errors: errors,
        })
    }

    pub async fn remove_objects(
        &self,
        args: &mut RemoveObjectsArgs<'_>,
    ) -> Result<RemoveObjectsResponse, Error> {
        let region = self.get_region(&args.bucket, args.region).await?;

        loop {
            let mut objects: Vec<DeleteObject> = Vec::new();
            for object in args.objects.take(1000) {
                objects.push(*object);
            }
            if objects.len() == 0 {
                break;
            }

            let mut roa_args = RemoveObjectsApiArgs::new(&args.bucket, &objects)?;
            roa_args.extra_headers = args.extra_headers;
            roa_args.extra_query_params = args.extra_query_params;
            roa_args.region = args.region;
            roa_args.bypass_governance_mode = args.bypass_governance_mode;
            roa_args.quiet = true;
            let resp = self.remove_objects_api(&roa_args).await?;
            if resp.errors.len() > 0 {
                return Ok(resp);
            }
        }

        Ok(RemoveObjectsResponse {
            headers: HeaderMap::new(),
            region: region.to_string(),
            bucket_name: args.bucket.to_string(),
            objects: vec![],
            errors: vec![],
        })
    }

    // SetBucketEncryptionResponse SetBucketEncryption(SetBucketEncryptionArgs args);
    // SetBucketLifecycleResponse SetBucketLifecycle(SetBucketLifecycleArgs args);
    // SetBucketNotificationResponse SetBucketNotification(
    //     SetBucketNotificationArgs args);
    // SetBucketPolicyResponse SetBucketPolicy(SetBucketPolicyArgs args);
    // SetBucketReplicationResponse SetBucketReplication(
    //     SetBucketReplicationArgs args);
    // SetBucketTagsResponse SetBucketTags(SetBucketTagsArgs args);
    // SetBucketVersioningResponse SetBucketVersioning(SetBucketVersioningArgs args);
    // SetObjectLockConfigResponse SetObjectLockConfig(SetObjectLockConfigArgs args);
    // SetObjectRetentionResponse SetObjectRetention(SetObjectRetentionArgs args);
    // SetObjectTagsResponse SetObjectTags(SetObjectTagsArgs args);
    pub async fn select_object_content(
        &self,
        args: &SelectObjectContentArgs<'_>,
    ) -> Result<SelectObjectContentResponse, Error> {
        if args.ssec.is_some() && !self.base_url.https {
            return Err(Error::SseTlsRequired);
        }

        let region = self.get_region(&args.bucket, args.region).await?;

        let data = args.request.to_xml();
        let b = data.as_bytes();

        let mut headers = Multimap::new();
        if let Some(v) = &args.extra_headers {
            merge(&mut headers, v);
        }
        headers.insert(String::from("Content-MD5"), md5sum_hash(&b));

        let mut query_params = Multimap::new();
        if let Some(v) = &args.extra_query_params {
            merge(&mut query_params, v);
        }
        query_params.insert(String::from("select"), String::new());
        query_params.insert(String::from("select-type"), String::from("2"));

        Ok(SelectObjectContentResponse::new(
            self.execute(
                Method::POST,
                &region,
                &mut headers,
                &query_params,
                Some(&args.bucket),
                Some(&args.object),
                Some(&b),
            )
            .await?,
            &region,
            &args.bucket,
            &args.object,
        ))
    }

    pub async fn stat_object(
        &self,
        args: &StatObjectArgs<'_>,
    ) -> Result<StatObjectResponse, Error> {
        if args.ssec.is_some() && !self.base_url.https {
            return Err(Error::SseTlsRequired);
        }

        let region = self.get_region(&args.bucket, args.region).await?;

        let mut headers = Multimap::new();
        if let Some(v) = &args.extra_headers {
            merge(&mut headers, v);
        }
        merge(&mut headers, &args.get_headers());

        let mut query_params = Multimap::new();
        if let Some(v) = &args.extra_query_params {
            merge(&mut query_params, v);
        }
        if let Some(v) = args.version_id {
            query_params.insert(String::from("versionId"), v.to_string());
        }

        let resp = self
            .execute(
                Method::HEAD,
                &region,
                &mut headers,
                &query_params,
                Some(&args.bucket),
                Some(&args.object),
                None,
            )
            .await?;

        StatObjectResponse::new(&resp.headers(), &region, &args.bucket, &args.object)
    }

    pub async fn upload_part(
        &self,
        args: &UploadPartArgs<'_>,
    ) -> Result<UploadPartResponse, Error> {
        let mut query_params = Multimap::new();
        query_params.insert(String::from("partNumber"), args.part_number.to_string());
        query_params.insert(String::from("uploadId"), args.upload_id.to_string());

        let mut poa_args = PutObjectApiArgs::new(&args.bucket, &args.object, &args.data)?;
        poa_args.query_params = Some(&query_params);

        poa_args.extra_headers = args.extra_headers;
        poa_args.extra_query_params = args.extra_query_params;
        poa_args.region = args.region;
        poa_args.headers = args.headers;
        poa_args.user_metadata = args.user_metadata;
        poa_args.sse = args.sse;
        poa_args.tags = args.tags;
        poa_args.retention = args.retention;
        poa_args.legal_hold = args.legal_hold;

        self.put_object_api(&poa_args).await
    }

    // UploadPartCopyResponse UploadPartCopy(UploadPartCopyArgs args);
}

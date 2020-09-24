/*
 * MinIO Rust Library for Amazon S3 Compatible Cloud Storage
 * Copyright 2019 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use crate::minio;
use hyper::{header::HeaderName, header::HeaderValue, Body, Request};
use log::debug;

pub fn mk_request(
    r: minio::S3Req,
    svr_str: &str,
    sign_hdrs: &Vec<(HeaderName, HeaderValue)>,
) -> Result<Request<Body>, minio::Err> {
    let mut request = Request::builder();
    let uri_str = svr_str.trim_end_matches('/');
    debug!("uri_str: {}", uri_str);
    let upd_uri = format!("{}{}?{}", uri_str, &r.mk_path(), &r.mk_query());
    debug!("upd_uri: {}", upd_uri);

    request.uri(&upd_uri).method(&r.method);
    for hdr in r
        .headers
        .iter()
        .map(|(x, y)| (x.clone(), y.clone()))
        .chain(sign_hdrs.iter().map(|x| x.clone()))
    {
        request.header(hdr.0, hdr.1);
    }
    request
        .body(r.body)
        .map_err(|err| minio::Err::HttpErr(err))
}

use crate::minio;
use http;
use hyper::{header::HeaderName, header::HeaderValue, Body, Request};

pub fn mk_request(
    r: &minio::S3Req,
    c: &minio::Client,
    sign_hdrs: &Vec<(HeaderName, HeaderValue)>,
) -> http::Result<Request<Body>> {
    let mut request = Request::builder();
    let svr_str = &c.server.to_string();
    let uri_str = svr_str.trim_end_matches('/');
    println!("uri_str: {}", uri_str);
    let upd_uri = format!("{}{}?{}", uri_str, r.mk_path(), r.mk_query());
    println!("upd_uri: {}", upd_uri);

    request.uri(&upd_uri).method(&r.method);
    for hdr in r
        .headers
        .iter()
        .map(|(x, y)| (x.clone(), y.clone()))
        .chain(sign_hdrs.iter().map(|x| x.clone()))
    {
        request.header(hdr.0, hdr.1);
    }
    request.body(Body::empty())
}

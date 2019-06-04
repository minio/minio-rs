use crate::minio::types::{Err, Region};
use crate::minio::woxml;
use hyper::body::Body;
use roxmltree;

pub fn parse_bucket_location(s: String) -> Result<Region, Err> {
    let res = roxmltree::Document::parse(&s);
    match res {
        Ok(doc) => {
            let region_res = doc.root_element().text();
            if let Some(region) = region_res {
                Ok(Region::new(region))
            } else {
                Ok(Region::empty())
            }
        }
        Err(e) => Err(Err::XmlParseErr(e)),
    }
}

pub fn get_mk_bucket_body() -> Result<Body, Err> {
    let lc_node = woxml::XmlNode::new("LocationConstraint").text("us-east-1");
    let mk_bucket_xml = woxml::XmlNode::new("CreateBucketConfiguration")
        .namespace("http://s3.amazonaws.com/doc/2006-03-01/")
        .children(vec![lc_node]);
    let mut xml_bytes = Vec::new();

    mk_bucket_xml
        .serialize(&mut xml_bytes)
        .or_else(|err| Err(Err::XmlWriteErr(err.to_string())))?;
    Ok(Body::from(xml_bytes))
}

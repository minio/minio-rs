use crate::minio::types::{BucketInfo, Err, Region};
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

pub fn parse_bucket_list(s: String) -> Result<Vec<BucketInfo>, Err> {
    let res = roxmltree::Document::parse(&s);
    match res {
        Ok(doc) => {
            let mut bucket_infos: Vec<BucketInfo> = Vec::new();
            let bucket_nodes = doc
                .root_element()
                .descendants()
                .filter(|node| node.has_tag_name("Bucket"));
            for bucket in bucket_nodes {
                let bucket_names = bucket.children().filter(|node| node.has_tag_name("Name"));
                let bucket_ctimes = bucket
                    .children()
                    .filter(|node| node.has_tag_name("CreationDate"));
                for (name_node, ctime_node) in bucket_names.zip(bucket_ctimes) {
                    let name = name_node.text().ok_or(Err::InvalidXmlResponseErr(
                        "Missing name in list buckets XML response ".to_string(),
                    ))?;
                    let ctime = ctime_node.text().ok_or(Err::InvalidXmlResponseErr(
                        "Missing creation date in list buckets XML response".to_string(),
                    ))?;
                    match BucketInfo::new(name, ctime) {
                        Ok(bucket_info) => bucket_infos.push(bucket_info),
                        Err(err) => return Err(Err::InvalidTmFmt(format!("{:?}", err))),
                    }
                }
            }
            Ok(bucket_infos)
        }
        Err(err) => Err(Err::XmlParseErr(err)),
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

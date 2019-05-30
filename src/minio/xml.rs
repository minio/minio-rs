use crate::minio::types::{Err, Region};
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

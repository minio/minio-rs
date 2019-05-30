use crate::minio::types::{Err, Region};
use bytes::Bytes;
use xml::reader::{EventReader, XmlEvent};

pub fn parse_bucket_location(b: Bytes) -> Result<Region, Err> {
    let mut reader = EventReader::new(b.as_ref());
    loop {
        let event = reader.next();
        match event {
            Err(err) => return Err(Err::XmlParseErr(err)),
            Ok(XmlEvent::EndDocument) => return Err(Err::UnexpectedEOF("xml parsing".to_string())),
            Ok(XmlEvent::Characters(s)) => return Ok(Region::new(&s)),
            _ => continue,
        };
    }
}

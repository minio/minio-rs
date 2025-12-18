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

//! Well-Known Binary (WKB) parser for Iceberg V3 Geometry/Geography types
//!
//! This module provides parsing for the ISO 13249-3 WKB format used by
//! Iceberg V3 for geometry and geography columns. WKB is a binary encoding
//! for geometric objects defined by the Open Geospatial Consortium (OGC).
//!
//! # Geometry vs Geography
//!
//! - **Geometry**: Planar/Cartesian coordinates for projected coordinate systems
//! - **Geography**: Spherical coordinates (lat/lon) on Earth's surface
//!
//! Both use WKB encoding but differ in how distance/area calculations are performed.
//!
//! # Supported Geometry Types
//!
//! | Type Code | Type | Description |
//! |-----------|------|-------------|
//! | 1 / 1001 | Point | Single coordinate |
//! | 2 / 1002 | LineString | Sequence of points |
//! | 3 / 1003 | Polygon | Closed ring(s) |
//! | 4 / 1004 | MultiPoint | Collection of points |
//! | 5 / 1005 | MultiLineString | Collection of line strings |
//! | 6 / 1006 | MultiPolygon | Collection of polygons |
//! | 7 / 1007 | GeometryCollection | Heterogeneous collection |
//!
//! Type codes 1001-1007 indicate 3D (XYZ) variants.
//!
//! # Example
//!
//! ```
//! use minio::s3tables::wkb::{parse_wkb, WkbGeometry};
//!
//! // WKB for POINT(1.0 2.0) in little-endian
//! let wkb = vec![
//!     0x01,                               // little-endian
//!     0x01, 0x00, 0x00, 0x00,             // type = Point (1)
//!     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // x = 1.0
//!     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
//! ];
//!
//! let geom = parse_wkb(&wkb).unwrap();
//! if let WkbGeometry::Point { x, y, z } = geom {
//!     assert_eq!(x, 1.0);
//!     assert_eq!(y, 2.0);
//!     assert!(z.is_none());
//! }
//! ```
//!
//! # References
//!
//! - [Iceberg V3 Spec - Geospatial Types](https://iceberg.apache.org/spec/#geospatial-types)
//! - [OGC Simple Features - WKB](https://www.ogc.org/standard/sfa/)
//! - [ISO 13249-3 SQL/MM Spatial](https://www.iso.org/standard/60343.html)

use std::io::{Cursor, Read};

use crate::s3tables::types::iceberg::BoundingBox;

/// WKB parsing error
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WkbError {
    /// Unexpected end of input
    UnexpectedEof,
    /// Invalid byte order indicator
    InvalidByteOrder(u8),
    /// Unsupported geometry type
    UnsupportedGeometryType(u32),
    /// Invalid geometry structure
    InvalidGeometry(String),
}

impl std::fmt::Display for WkbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WkbError::UnexpectedEof => write!(f, "Unexpected end of WKB data"),
            WkbError::InvalidByteOrder(b) => write!(f, "Invalid WKB byte order: {b}"),
            WkbError::UnsupportedGeometryType(t) => write!(f, "Unsupported WKB geometry type: {t}"),
            WkbError::InvalidGeometry(msg) => write!(f, "Invalid WKB geometry: {msg}"),
        }
    }
}

impl std::error::Error for WkbError {}

/// WKB geometry type codes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum WkbType {
    Point = 1,
    LineString = 2,
    Polygon = 3,
    MultiPoint = 4,
    MultiLineString = 5,
    MultiPolygon = 6,
    GeometryCollection = 7,
    // 3D variants (add 1000)
    PointZ = 1001,
    LineStringZ = 1002,
    PolygonZ = 1003,
    MultiPointZ = 1004,
    MultiLineStringZ = 1005,
    MultiPolygonZ = 1006,
    GeometryCollectionZ = 1007,
}

impl TryFrom<u32> for WkbType {
    type Error = WkbError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(WkbType::Point),
            2 => Ok(WkbType::LineString),
            3 => Ok(WkbType::Polygon),
            4 => Ok(WkbType::MultiPoint),
            5 => Ok(WkbType::MultiLineString),
            6 => Ok(WkbType::MultiPolygon),
            7 => Ok(WkbType::GeometryCollection),
            1001 => Ok(WkbType::PointZ),
            1002 => Ok(WkbType::LineStringZ),
            1003 => Ok(WkbType::PolygonZ),
            1004 => Ok(WkbType::MultiPointZ),
            1005 => Ok(WkbType::MultiLineStringZ),
            1006 => Ok(WkbType::MultiPolygonZ),
            1007 => Ok(WkbType::GeometryCollectionZ),
            _ => Err(WkbError::UnsupportedGeometryType(value)),
        }
    }
}

impl WkbType {
    /// Check if this is a 3D (Z) geometry type
    pub fn is_3d(&self) -> bool {
        matches!(
            self,
            WkbType::PointZ
                | WkbType::LineStringZ
                | WkbType::PolygonZ
                | WkbType::MultiPointZ
                | WkbType::MultiLineStringZ
                | WkbType::MultiPolygonZ
                | WkbType::GeometryCollectionZ
        )
    }
}

/// A coordinate point with optional Z value
pub type Coordinate = (f64, f64, Option<f64>);

/// A ring (sequence of coordinates) forming part of a polygon
pub type Ring = Vec<Coordinate>;

/// A polygon represented as a collection of rings
pub type PolygonRings = Vec<Ring>;

/// Parsed WKB geometry
#[derive(Debug, Clone, PartialEq)]
pub enum WkbGeometry {
    /// Point geometry
    Point { x: f64, y: f64, z: Option<f64> },
    /// LineString geometry (sequence of points)
    LineString { points: Vec<Coordinate> },
    /// Polygon geometry (exterior ring + optional interior rings)
    Polygon { rings: PolygonRings },
    /// MultiPoint geometry
    MultiPoint { points: Vec<Coordinate> },
    /// MultiLineString geometry
    MultiLineString { line_strings: Vec<Ring> },
    /// MultiPolygon geometry
    MultiPolygon { polygons: Vec<PolygonRings> },
    /// GeometryCollection
    GeometryCollection { geometries: Vec<WkbGeometry> },
}

impl WkbGeometry {
    /// Compute the bounding box for this geometry
    pub fn bounding_box(&self) -> Option<BoundingBox> {
        let mut x_min = f64::MAX;
        let mut x_max = f64::MIN;
        let mut y_min = f64::MAX;
        let mut y_max = f64::MIN;
        let mut z_min = f64::MAX;
        let mut z_max = f64::MIN;
        let mut has_z = false;
        let mut has_points = false;

        self.visit_coords(&mut |x, y, z| {
            has_points = true;
            x_min = x_min.min(x);
            x_max = x_max.max(x);
            y_min = y_min.min(y);
            y_max = y_max.max(y);
            if let Some(z_val) = z {
                has_z = true;
                z_min = z_min.min(z_val);
                z_max = z_max.max(z_val);
            }
        });

        if !has_points {
            return None;
        }

        Some(if has_z {
            BoundingBox::new_3d(x_min, x_max, y_min, y_max, z_min, z_max)
        } else {
            BoundingBox::new_2d(x_min, x_max, y_min, y_max)
        })
    }

    /// Visit all coordinates in this geometry
    fn visit_coords<F: FnMut(f64, f64, Option<f64>)>(&self, visitor: &mut F) {
        match self {
            WkbGeometry::Point { x, y, z } => visitor(*x, *y, *z),
            WkbGeometry::LineString { points } => {
                for (x, y, z) in points {
                    visitor(*x, *y, *z);
                }
            }
            WkbGeometry::Polygon { rings } => {
                for ring in rings {
                    for (x, y, z) in ring {
                        visitor(*x, *y, *z);
                    }
                }
            }
            WkbGeometry::MultiPoint { points } => {
                for (x, y, z) in points {
                    visitor(*x, *y, *z);
                }
            }
            WkbGeometry::MultiLineString { line_strings } => {
                for ls in line_strings {
                    for (x, y, z) in ls {
                        visitor(*x, *y, *z);
                    }
                }
            }
            WkbGeometry::MultiPolygon { polygons } => {
                for polygon in polygons {
                    for ring in polygon {
                        for (x, y, z) in ring {
                            visitor(*x, *y, *z);
                        }
                    }
                }
            }
            WkbGeometry::GeometryCollection { geometries } => {
                for geom in geometries {
                    geom.visit_coords(visitor);
                }
            }
        }
    }

    /// Check if this geometry has Z coordinates
    pub fn is_3d(&self) -> bool {
        match self {
            WkbGeometry::Point { z, .. } => z.is_some(),
            WkbGeometry::LineString { points } => {
                points.first().is_some_and(|(_, _, z)| z.is_some())
            }
            WkbGeometry::Polygon { rings } => rings
                .first()
                .and_then(|r| r.first())
                .is_some_and(|(_, _, z)| z.is_some()),
            WkbGeometry::MultiPoint { points } => {
                points.first().is_some_and(|(_, _, z)| z.is_some())
            }
            WkbGeometry::MultiLineString { line_strings } => line_strings
                .first()
                .and_then(|ls| ls.first())
                .is_some_and(|(_, _, z)| z.is_some()),
            WkbGeometry::MultiPolygon { polygons } => polygons
                .first()
                .and_then(|p| p.first())
                .and_then(|r| r.first())
                .is_some_and(|(_, _, z)| z.is_some()),
            WkbGeometry::GeometryCollection { geometries } => {
                geometries.first().is_some_and(|g| g.is_3d())
            }
        }
    }
}

/// Internal reader that handles byte order
struct WkbReader<'a> {
    cursor: Cursor<&'a [u8]>,
    little_endian: bool,
}

impl<'a> WkbReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self {
            cursor: Cursor::new(data),
            little_endian: true,
        }
    }

    fn read_u8(&mut self) -> Result<u8, WkbError> {
        let mut buf = [0u8; 1];
        self.cursor
            .read_exact(&mut buf)
            .map_err(|_| WkbError::UnexpectedEof)?;
        Ok(buf[0])
    }

    fn read_u32(&mut self) -> Result<u32, WkbError> {
        let mut buf = [0u8; 4];
        self.cursor
            .read_exact(&mut buf)
            .map_err(|_| WkbError::UnexpectedEof)?;
        Ok(if self.little_endian {
            u32::from_le_bytes(buf)
        } else {
            u32::from_be_bytes(buf)
        })
    }

    fn read_f64(&mut self) -> Result<f64, WkbError> {
        let mut buf = [0u8; 8];
        self.cursor
            .read_exact(&mut buf)
            .map_err(|_| WkbError::UnexpectedEof)?;
        Ok(if self.little_endian {
            f64::from_le_bytes(buf)
        } else {
            f64::from_be_bytes(buf)
        })
    }

    fn read_byte_order(&mut self) -> Result<(), WkbError> {
        let bo = self.read_u8()?;
        match bo {
            0 => self.little_endian = false,
            1 => self.little_endian = true,
            _ => return Err(WkbError::InvalidByteOrder(bo)),
        }
        Ok(())
    }

    fn read_point(&mut self, has_z: bool) -> Result<(f64, f64, Option<f64>), WkbError> {
        let x = self.read_f64()?;
        let y = self.read_f64()?;
        let z = if has_z { Some(self.read_f64()?) } else { None };
        Ok((x, y, z))
    }

    fn read_points(&mut self, has_z: bool) -> Result<Vec<Coordinate>, WkbError> {
        let num_points = self.read_u32()? as usize;
        let mut points = Vec::with_capacity(num_points);
        for _ in 0..num_points {
            points.push(self.read_point(has_z)?);
        }
        Ok(points)
    }

    fn read_ring(&mut self, has_z: bool) -> Result<Ring, WkbError> {
        self.read_points(has_z)
    }

    fn read_polygon_rings(&mut self, has_z: bool) -> Result<PolygonRings, WkbError> {
        let num_rings = self.read_u32()? as usize;
        let mut rings = Vec::with_capacity(num_rings);
        for _ in 0..num_rings {
            rings.push(self.read_ring(has_z)?);
        }
        Ok(rings)
    }

    fn read_geometry(&mut self) -> Result<WkbGeometry, WkbError> {
        self.read_byte_order()?;
        let type_code = self.read_u32()?;
        let wkb_type = WkbType::try_from(type_code)?;
        let has_z = wkb_type.is_3d();

        match wkb_type {
            WkbType::Point | WkbType::PointZ => {
                let (x, y, z) = self.read_point(has_z)?;
                Ok(WkbGeometry::Point { x, y, z })
            }
            WkbType::LineString | WkbType::LineStringZ => {
                let points = self.read_points(has_z)?;
                Ok(WkbGeometry::LineString { points })
            }
            WkbType::Polygon | WkbType::PolygonZ => {
                let rings = self.read_polygon_rings(has_z)?;
                Ok(WkbGeometry::Polygon { rings })
            }
            WkbType::MultiPoint | WkbType::MultiPointZ => {
                let num_points = self.read_u32()? as usize;
                let mut points = Vec::with_capacity(num_points);
                for _ in 0..num_points {
                    // Each point in a MultiPoint has its own header
                    let geom = self.read_geometry()?;
                    if let WkbGeometry::Point { x, y, z } = geom {
                        points.push((x, y, z));
                    } else {
                        return Err(WkbError::InvalidGeometry(
                            "Expected Point in MultiPoint".to_string(),
                        ));
                    }
                }
                Ok(WkbGeometry::MultiPoint { points })
            }
            WkbType::MultiLineString | WkbType::MultiLineStringZ => {
                let num_line_strings = self.read_u32()? as usize;
                let mut line_strings = Vec::with_capacity(num_line_strings);
                for _ in 0..num_line_strings {
                    let geom = self.read_geometry()?;
                    if let WkbGeometry::LineString { points } = geom {
                        line_strings.push(points);
                    } else {
                        return Err(WkbError::InvalidGeometry(
                            "Expected LineString in MultiLineString".to_string(),
                        ));
                    }
                }
                Ok(WkbGeometry::MultiLineString { line_strings })
            }
            WkbType::MultiPolygon | WkbType::MultiPolygonZ => {
                let num_polygons = self.read_u32()? as usize;
                let mut polygons = Vec::with_capacity(num_polygons);
                for _ in 0..num_polygons {
                    let geom = self.read_geometry()?;
                    if let WkbGeometry::Polygon { rings } = geom {
                        polygons.push(rings);
                    } else {
                        return Err(WkbError::InvalidGeometry(
                            "Expected Polygon in MultiPolygon".to_string(),
                        ));
                    }
                }
                Ok(WkbGeometry::MultiPolygon { polygons })
            }
            WkbType::GeometryCollection | WkbType::GeometryCollectionZ => {
                let num_geometries = self.read_u32()? as usize;
                let mut geometries = Vec::with_capacity(num_geometries);
                for _ in 0..num_geometries {
                    geometries.push(self.read_geometry()?);
                }
                Ok(WkbGeometry::GeometryCollection { geometries })
            }
        }
    }
}

/// Parse a WKB byte array into a geometry
///
/// # Arguments
///
/// * `data` - WKB encoded geometry bytes
///
/// # Returns
///
/// Parsed geometry or error
///
/// # Example
///
/// ```
/// use minio::s3tables::wkb::{parse_wkb, WkbGeometry};
///
/// // POINT(1.0 2.0) in little-endian WKB
/// let wkb = vec![
///     0x01,                               // little-endian
///     0x01, 0x00, 0x00, 0x00,             // type = Point
///     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // x = 1.0
///     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
/// ];
/// let geom = parse_wkb(&wkb).unwrap();
/// ```
pub fn parse_wkb(data: &[u8]) -> Result<WkbGeometry, WkbError> {
    let mut reader = WkbReader::new(data);
    reader.read_geometry()
}

/// Compute bounding box from WKB data
///
/// Convenience function that parses WKB and extracts the bounding box.
///
/// # Arguments
///
/// * `data` - WKB encoded geometry bytes
///
/// # Returns
///
/// Bounding box or None if the geometry is empty
pub fn bounding_box_from_wkb(data: &[u8]) -> Result<Option<BoundingBox>, WkbError> {
    let geom = parse_wkb(data)?;
    Ok(geom.bounding_box())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper to create little-endian f64 bytes
    fn f64_le_bytes(v: f64) -> [u8; 8] {
        v.to_le_bytes()
    }

    #[test]
    fn test_parse_point_2d() {
        let mut wkb = vec![
            0x01, // little-endian
            0x01, 0x00, 0x00, 0x00, // type = Point (1)
        ];
        wkb.extend_from_slice(&f64_le_bytes(1.5));
        wkb.extend_from_slice(&f64_le_bytes(2.5));

        let geom = parse_wkb(&wkb).unwrap();
        match geom {
            WkbGeometry::Point { x, y, z } => {
                assert_eq!(x, 1.5);
                assert_eq!(y, 2.5);
                assert!(z.is_none());
            }
            _ => panic!("Expected Point"),
        }
    }

    #[test]
    fn test_parse_point_3d() {
        let mut wkb = vec![
            0x01, // little-endian
            0xE9, 0x03, 0x00, 0x00, // type = PointZ (1001)
        ];
        wkb.extend_from_slice(&f64_le_bytes(1.0));
        wkb.extend_from_slice(&f64_le_bytes(2.0));
        wkb.extend_from_slice(&f64_le_bytes(3.0));

        let geom = parse_wkb(&wkb).unwrap();
        match geom {
            WkbGeometry::Point { x, y, z } => {
                assert_eq!(x, 1.0);
                assert_eq!(y, 2.0);
                assert_eq!(z, Some(3.0));
            }
            _ => panic!("Expected Point"),
        }
    }

    #[test]
    fn test_parse_point_big_endian() {
        let mut wkb = vec![
            0x00, // big-endian
            0x00, 0x00, 0x00, 0x01, // type = Point (1)
        ];
        wkb.extend_from_slice(&10.0_f64.to_be_bytes());
        wkb.extend_from_slice(&20.0_f64.to_be_bytes());

        let geom = parse_wkb(&wkb).unwrap();
        match geom {
            WkbGeometry::Point { x, y, z } => {
                assert_eq!(x, 10.0);
                assert_eq!(y, 20.0);
                assert!(z.is_none());
            }
            _ => panic!("Expected Point"),
        }
    }

    #[test]
    fn test_parse_linestring() {
        let mut wkb = vec![
            0x01, // little-endian
            0x02, 0x00, 0x00, 0x00, // type = LineString (2)
            0x03, 0x00, 0x00, 0x00, // num_points = 3
        ];
        // Point 1
        wkb.extend_from_slice(&f64_le_bytes(0.0));
        wkb.extend_from_slice(&f64_le_bytes(0.0));
        // Point 2
        wkb.extend_from_slice(&f64_le_bytes(1.0));
        wkb.extend_from_slice(&f64_le_bytes(1.0));
        // Point 3
        wkb.extend_from_slice(&f64_le_bytes(2.0));
        wkb.extend_from_slice(&f64_le_bytes(0.0));

        let geom = parse_wkb(&wkb).unwrap();
        match geom {
            WkbGeometry::LineString { points } => {
                assert_eq!(points.len(), 3);
                assert_eq!(points[0], (0.0, 0.0, None));
                assert_eq!(points[1], (1.0, 1.0, None));
                assert_eq!(points[2], (2.0, 0.0, None));
            }
            _ => panic!("Expected LineString"),
        }
    }

    #[test]
    fn test_parse_polygon() {
        let mut wkb = vec![
            0x01, // little-endian
            0x03, 0x00, 0x00, 0x00, // type = Polygon (3)
            0x01, 0x00, 0x00, 0x00, // num_rings = 1
            0x04, 0x00, 0x00, 0x00, // ring has 4 points
        ];
        // Triangle: (0,0), (1,0), (0,1), (0,0)
        wkb.extend_from_slice(&f64_le_bytes(0.0));
        wkb.extend_from_slice(&f64_le_bytes(0.0));
        wkb.extend_from_slice(&f64_le_bytes(1.0));
        wkb.extend_from_slice(&f64_le_bytes(0.0));
        wkb.extend_from_slice(&f64_le_bytes(0.0));
        wkb.extend_from_slice(&f64_le_bytes(1.0));
        wkb.extend_from_slice(&f64_le_bytes(0.0));
        wkb.extend_from_slice(&f64_le_bytes(0.0));

        let geom = parse_wkb(&wkb).unwrap();
        match geom {
            WkbGeometry::Polygon { rings } => {
                assert_eq!(rings.len(), 1);
                assert_eq!(rings[0].len(), 4);
            }
            _ => panic!("Expected Polygon"),
        }
    }

    #[test]
    fn test_bounding_box_point() {
        let mut wkb = vec![
            0x01, // little-endian
            0x01, 0x00, 0x00, 0x00, // type = Point
        ];
        wkb.extend_from_slice(&f64_le_bytes(5.0));
        wkb.extend_from_slice(&f64_le_bytes(10.0));

        let bbox = bounding_box_from_wkb(&wkb).unwrap().unwrap();
        assert_eq!(bbox.x_min, 5.0);
        assert_eq!(bbox.x_max, 5.0);
        assert_eq!(bbox.y_min, 10.0);
        assert_eq!(bbox.y_max, 10.0);
        assert!(!bbox.is_3d());
    }

    #[test]
    fn test_bounding_box_linestring() {
        let mut wkb = vec![
            0x01, // little-endian
            0x02, 0x00, 0x00, 0x00, // type = LineString
            0x03, 0x00, 0x00, 0x00, // 3 points
        ];
        wkb.extend_from_slice(&f64_le_bytes(-10.0));
        wkb.extend_from_slice(&f64_le_bytes(-5.0));
        wkb.extend_from_slice(&f64_le_bytes(0.0));
        wkb.extend_from_slice(&f64_le_bytes(0.0));
        wkb.extend_from_slice(&f64_le_bytes(10.0));
        wkb.extend_from_slice(&f64_le_bytes(5.0));

        let bbox = bounding_box_from_wkb(&wkb).unwrap().unwrap();
        assert_eq!(bbox.x_min, -10.0);
        assert_eq!(bbox.x_max, 10.0);
        assert_eq!(bbox.y_min, -5.0);
        assert_eq!(bbox.y_max, 5.0);
    }

    #[test]
    fn test_bounding_box_3d() {
        let mut wkb = vec![
            0x01, // little-endian
            0xE9, 0x03, 0x00, 0x00, // type = PointZ (1001)
        ];
        wkb.extend_from_slice(&f64_le_bytes(1.0));
        wkb.extend_from_slice(&f64_le_bytes(2.0));
        wkb.extend_from_slice(&f64_le_bytes(3.0));

        let bbox = bounding_box_from_wkb(&wkb).unwrap().unwrap();
        assert!(bbox.is_3d());
        assert_eq!(bbox.z_min, Some(3.0));
        assert_eq!(bbox.z_max, Some(3.0));
    }

    #[test]
    fn test_invalid_byte_order() {
        let wkb = vec![0x02, 0x01, 0x00, 0x00, 0x00];
        let result = parse_wkb(&wkb);
        assert!(matches!(result, Err(WkbError::InvalidByteOrder(2))));
    }

    #[test]
    fn test_unsupported_type() {
        let wkb = vec![
            0x01, // little-endian
            0xFF, 0xFF, 0x00, 0x00, // invalid type
        ];
        let result = parse_wkb(&wkb);
        assert!(matches!(result, Err(WkbError::UnsupportedGeometryType(_))));
    }

    #[test]
    fn test_unexpected_eof() {
        let wkb = vec![0x01, 0x01, 0x00]; // truncated
        let result = parse_wkb(&wkb);
        assert!(matches!(result, Err(WkbError::UnexpectedEof)));
    }

    #[test]
    fn test_geometry_is_3d() {
        let geom_2d = WkbGeometry::Point {
            x: 1.0,
            y: 2.0,
            z: None,
        };
        assert!(!geom_2d.is_3d());

        let geom_3d = WkbGeometry::Point {
            x: 1.0,
            y: 2.0,
            z: Some(3.0),
        };
        assert!(geom_3d.is_3d());
    }

    #[test]
    fn test_wkb_type_is_3d() {
        assert!(!WkbType::Point.is_3d());
        assert!(!WkbType::LineString.is_3d());
        assert!(WkbType::PointZ.is_3d());
        assert!(WkbType::LineStringZ.is_3d());
    }
}

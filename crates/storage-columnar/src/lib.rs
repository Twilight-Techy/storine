//! Column segments for analytic queries over documents (`TASK-125`, `F-OLAP-01`).
//!
//! A columnar collection keeps, besides its row-oriented documents, immutable memory-mapped
//! segment files holding the same documents column by column: one column per field path,
//! encoded per row-group chunk with the smallest of several encodings, with a zone map per
//! chunk so filters skip chunks that cannot match. Values keep their exact kind (decimals stay
//! exact `i128` mantissas, a field holding several kinds becomes a variant chunk), so reading a
//! segment back yields exactly the documents it was built from.

#![forbid(unsafe_code)]

pub mod bits;
pub mod column;
pub mod int;
pub mod segment;

pub use column::{flatten, flatten_owned, Body, Chunk, ChunkMeta, Kind, Val, Zone};
pub use segment::{Segment, SegmentRow, SegmentSummary, SegmentWriter, ROW_GROUP_ROWS};

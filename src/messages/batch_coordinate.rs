use super::produce_request::ProduceRequest;

/// A BatchCoordinate represents the metadata of a record inside of a file.
#[derive(Debug, Clone)]
pub struct BatchCoordinate {
    /// The topic for this record.
    pub topic: String,
    /// The partition for this record.
    pub partition: Vec<u8>,
    /// The base_offset inside of the file at which this record exists.
    pub base_offset: u64,
    /// The offset at which this record exists.
    pub offset: u64,
    /// The size in bytes of this record.
    pub size: u32,
    /// The original request for retrieving the BatchCoordinate.
    pub request: ProduceRequest,
}

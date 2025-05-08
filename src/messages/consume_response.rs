use bytes::Bytes;

use crate::{
    batch_coordinator::{BatchInfo, FindBatchResponse},
    error::RisklessError,
};

/// Represents the data of a given record.
#[derive(Debug, Clone)]
pub struct ConsumeBatch {
    /// The topic of the record.
    pub topic: String,
    /// Partition of a record.
    pub partition: u64,
    /// The offset of this record.
        pub offset: u64,
    /// The maximum amount of bytes to retrieve from each partition.
    pub max_partition_fetch_bytes: u32,
    /// The bytes retrieved.
    pub data: Bytes,
}

/// The wrapping consume response.
#[derive(Debug, Clone)]
pub struct ConsumeResponse {
    /// The batches retrieved from the request.
    pub batches: Vec<ConsumeBatch>,
}

/// Very specific implementation for converting these data values.
impl TryFrom<(FindBatchResponse, &BatchInfo, &Bytes)> for ConsumeBatch {
    type Error = RisklessError;

    fn try_from(
        (find_batch_response, batch_info, bytes): (FindBatchResponse, &BatchInfo, &bytes::Bytes),
    ) -> Result<Self, Self::Error> {
        // index into the bytes.
        let start: usize =
            (batch_info.metadata.base_offset + batch_info.metadata.byte_offset).try_into()?;
        let end: usize = (batch_info.metadata.base_offset
            + batch_info.metadata.byte_offset
            + Into::<u64>::into(batch_info.metadata.byte_size))
        .try_into()?;

        tracing::info!("START: {} END: {} ", start, end);

        let data = bytes.slice(start..end);

        let batch = ConsumeBatch {
            topic: batch_info.metadata.topic_id_partition.0.clone(),
            partition: batch_info.metadata.topic_id_partition.1,
            offset: find_batch_response.log_start_offset,
            max_partition_fetch_bytes: 0,
            data,
        };

        Ok(batch)
    }
}

use crate::{
    batch_coordinator::{FindBatchRequest, TopicIdPartition},
    error::RisklessError,
};

/// A request to consume a record at a specified offset.
#[derive(Debug)]
pub struct ConsumeRequest {
    /// The topic that this request consumes from.
    pub topic: String,
    /// The partition that this request consumes from.
    pub partition: Vec<u8>,
    /// The offset from which this request consumes from.
    pub offset: u64,
    /// The maximum amount of bytes to retrieve from a partition.
    pub max_partition_fetch_bytes: u32,
}

impl TryInto<FindBatchRequest> for ConsumeRequest {
    type Error = RisklessError;

    fn try_into(self) -> Result<FindBatchRequest, Self::Error> {
        Ok(FindBatchRequest {
            topic_id_partition: TopicIdPartition(self.topic, self.partition),
            offset: self.offset,
            max_partition_fetch_bytes: 0,
        })
    }
}

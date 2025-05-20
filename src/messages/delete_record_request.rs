use crate::{batch_coordinator::TopicIdPartition, error::RisklessError};

/// A request that represents the intention to delete a record at a specified offset.
#[derive(Debug)]
pub struct DeleteRecordsRequest {
    /// The topic of the record.
    pub topic: String,
    /// Partition of a record.
    pub partition: Vec<u8>,
    /// The offset of this record.
    pub offset: u64,
}

impl TryInto<crate::batch_coordinator::DeleteRecordsRequest> for DeleteRecordsRequest {
    type Error = RisklessError;

    fn try_into(self) -> Result<crate::batch_coordinator::DeleteRecordsRequest, Self::Error> {
        Ok(crate::batch_coordinator::DeleteRecordsRequest {
            topic_id_partition: TopicIdPartition(self.topic, self.partition),
            offset: self.offset,
        })
    }
}

use crate::{coordinator::{FindBatchRequest, TopicIdPartition}, error::RisklessError};

#[derive(Debug)]
pub struct ConsumeRequest {
    pub topic: String,
    pub partition: u64,
    pub offset: u64,
    pub max_partition_fetch_bytes: u32,
}

impl TryInto<FindBatchRequest> for ConsumeRequest {
    type Error = RisklessError;

    fn try_into(self) -> Result<FindBatchRequest, Self::Error> {

        Ok(FindBatchRequest{
            topic_id_partition: TopicIdPartition(self.topic, self.partition),
            offset: self.offset,
            max_partition_fetch_bytes: 0,
        })

    }
}
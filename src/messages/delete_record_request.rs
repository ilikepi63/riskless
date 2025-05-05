use crate::{batch_coordinator::TopicIdPartition, error::RisklessError};

#[derive(Debug)]
pub struct DeleteRecordsRequest {
    pub topic: String,
    pub partition: u64,
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

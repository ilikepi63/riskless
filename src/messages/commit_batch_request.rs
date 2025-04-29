#![allow(dead_code)]

use crate::batch_coordinator::{TimestampType, TopicIdPartition};

use super::batch_coordinate::BatchCoordinate;

#[derive(Debug)]
pub struct CommitBatchRequest {
    pub request_id: u32,
    pub topic_id_partition: TopicIdPartition,
    pub byte_offset: u64,
    pub size: u32,
    pub base_offset: u64,
    pub last_offset: u64,
    pub batch_max_timestamp: u64,
    pub message_timestamp_type: TimestampType,
    pub producer_id: u64,
    pub producer_epoch: u16,
    pub base_sequence: u32,
    pub last_sequence: u32,
}

impl From<&BatchCoordinate> for CommitBatchRequest {
    fn from(value: &BatchCoordinate) -> Self {
        // Everything that is defaulted is unknown for now.
        CommitBatchRequest {
            request_id: 1, // TODO: have a generator for this.
            topic_id_partition: TopicIdPartition(value.topic.clone(), value.partition),
            byte_offset: value.offset,
            size: value.size,
            base_offset: value.base_offset,
            last_offset: Default::default(),
            batch_max_timestamp: Default::default(),
            message_timestamp_type: TimestampType::Dummy,
            producer_id: Default::default(),
            producer_epoch: Default::default(),
            base_sequence: Default::default(),
            last_sequence: Default::default(),
        }
    }
}

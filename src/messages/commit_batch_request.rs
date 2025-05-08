#![allow(dead_code)]

use crate::batch_coordinator::{TimestampType, TopicIdPartition};

use super::batch_coordinate::BatchCoordinate;

/// A request representing committing a batch to the BatchCoordinator for
/// indexing.
#[derive(Debug, Clone)]
pub struct CommitBatchRequest {
    /// The unique request ID.
    pub request_id: u32,
    /// The topic/partition combination.
    pub topic_id_partition: TopicIdPartition,
    /// The byte offset for this record.
    pub byte_offset: u64,
    /// The size retrieved for this record.
    pub size: u32,
    /// The base offset of the file.
    pub base_offset: u64,
    /// Unknown.
    pub last_offset: u64,
    /// Unknown.
    pub batch_max_timestamp: u64,
    /// Unknown.
    pub message_timestamp_type: TimestampType,
    /// The unique ID of the producer producing this batch.
    pub producer_id: u64,
    /// The producer's epoch.
    pub producer_epoch: u16,
    /// Unknown.
    pub base_sequence: u32,
    /// Unknown.
    pub last_sequence: u32,
}

impl From<&BatchCoordinate> for CommitBatchRequest {
    fn from(value: &BatchCoordinate) -> Self {
        // Everything that is defaulted is unknown for now.
        CommitBatchRequest {
            request_id: value.request.request_id,
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

//! This entire interface is generated directly from the underlying KIP-1164 interface found here:
//! <https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=350783984#KIP1164:TopicBasedBatchCoordinator-BatchCoordinatorpluggableinterface>

#![allow(dead_code)]

pub mod simple;

use std::collections::HashSet;
use std::vec::Vec;

use std::time::SystemTime;

use crate::messages::CommitBatchRequest;

/// Merged Topic/Partition identification struc .
#[derive(Debug, Hash, PartialEq, Eq, Clone, Default)]
pub struct TopicIdPartition(pub String, pub u64);

/// The type of the timestamp given.
#[derive(Debug, Default, Clone)]
pub enum TimestampType {
    /// Default implementation.
    #[default]
    Dummy,
}

/// Request parameter to create a topic/partition combination.
#[derive(Debug)]
pub struct CreateTopicAndPartitionsRequest {
    /// The ID of the topic.
    pub topic_id: uuid::Uuid,
    /// The name of the topic.
    pub topic_name: String,
    /// How many partitions this topic should have.
    pub num_partitions: u32,
}

/// Response for CommitBatchRequest in order to commit a batch.
#[derive(Debug)]
pub struct CommitBatchResponse {
    /// The errors from this request.
    pub errors: Vec<String>, // TODO: fix this. This needs to be an Errors object.
    /// Unknown.
    pub assigned_base_offset: u64,
    /// Unknown.
    pub log_append_time: u64,
    /// Unknown.
    pub log_start_offset: u64,
    /// Unknown.
    pub is_duplicate: bool,
    /// Unknown.
    pub request: CommitBatchRequest,
}

/// Request parameter for finding a specific offset in a topic/partiton.
#[derive(Debug)]
pub struct FindBatchRequest {
    /// ID of the topic/partition combination.
    pub topic_id_partition: TopicIdPartition,
    /// The offset of the requested record.
    pub offset: u64,
    /// How many bytes is the max that this response can hold.
    pub max_partition_fetch_bytes: u32,
}

/// The Response struct for the FindBatchRequest.
#[derive(Debug, Clone)]
pub struct FindBatchResponse {
    /// The errors from this request.
    pub errors: Vec<String>, // TODO: fix this. This needs to be an Errors object.
    /// The batches that were fetched using this response.
    pub batches: Vec<BatchInfo>,
    /// Unknown.
    pub log_start_offset: u64,
    /// Unknown.
    pub high_watermark: u64,
}

/// Information regarding where a batch is, specifically
/// which object_key and which offset the batch is at.
#[derive(Debug, Clone)]
pub struct BatchInfo {
    /// ID of this batch.
    pub batch_id: u64,
    /// The object key for where this resides.
    pub object_key: String,
    /// The metadata for this batch.
    pub metadata: BatchMetadata,
}

/// Metadata Information for a BatchInfo struct.
#[derive(Debug, Default, Clone)]
pub struct BatchMetadata {
    /// The topic/partition this batch belongs to.
    pub topic_id_partition: TopicIdPartition,
    /// The byte offset within the segment file for this batch.
    pub byte_offset: u64,
    /// The size in bytes of the record batch.
    pub byte_size: u32,
    /// The base offset of the offset for this batch.
    pub base_offset: u64,
    /// The last offset for this specific batch.
    pub last_offset: u64,
    /// The timestamp of when this batch was appended to the log.
    pub log_append_timestamp: u64,
    /// The max timestamp for this batch.
    pub batch_max_timestamp: u64,
    /// The timestamp type that the preceding timestamps adhere to.
    pub timestamp_type: TimestampType,
    /// The ID identifying the producer that produced this batch.
    pub producer_id: u64,
    /// The epoch of the producer.
    pub producer_epoch: i16,
    /// Unknown.
    pub base_sequence: u32,
    /// Unknown.
    pub last_sequence: u32,
}

/// The request to list the offsets of a topic/partition combination.
#[derive(Debug)]
pub struct ListOffsetsRequest {
    /// The topic/partition combination.
    pub topic_id_partition: TopicIdPartition,
    /// The given timestamp of the request.
    pub timestamp: u64,
}

// impl ListOffsetsRequest {
//     pub const EARLIEST_TIMESTAMP: u64 = org
//         .apache
//         .kafka
//         .common
//         .requests
//         .ListOffsetsRequest
//         .EARLIEST_TIMESTAMP;
//     pub const LATEST_TIMESTAMP: u64 = org
//         .apache
//         .kafka
//         .common
//         .requests
//         .ListOffsetsRequest
//         .LATEST_TIMESTAMP;
//     pub const MAX_TIMESTAMP: u64 = org
//         .apache
//         .kafka
//         .common
//         .requests
//         .ListOffsetsRequest
//         .MAX_TIMESTAMP;
//     pub const EARLIEST_LOCAL_TIMESTAMP: u64 = org
//         .apache
//         .kafka
//         .common
//         .requests
//         .ListOffsetsRequest
//         .EARLIEST_LOCAL_TIMESTAMP;
//     pub const LATEST_TIERED_TIMESTAMP: u64 = org
//         .apache
//         .kafka
//         .common
//         .requests
//         .ListOffsetsRequest
//         .LATEST_TIERED_TIMESTAMP;
// }

/// The response for a ListOffsetsRequest.
#[derive(Debug)]
pub struct ListOffsetsResponse {
    /// The errors from this request.
    pub errors: Vec<String>, // TODO: fix this. This needs to be an Errors object.
    /// The topic/partition combination.
    pub topic_id_partition: TopicIdPartition,
    /// The given timestamp of the request.
    pub timestamp: u64,
    /// The given offset of the topic/partition combination.
    pub offset: u64,
}

/// Request to delete a record.
#[derive(Debug)]
pub struct DeleteRecordsRequest {
    /// The topic/partition combination.
    pub topic_id_partition: TopicIdPartition,
    /// The offset for the record that will be deleted.
    pub offset: u64,
}

/// A response for a DeleteRecordsRequest.
#[derive(Debug)]
pub struct DeleteRecordsResponse {
    /// The errors from this request.
    pub errors: Vec<String>, // TODO: fix this. This needs to be an Errors object.
    /// Unknown.
    pub low_watermark: u64,
}

/// A File that is able to be deleted as it has been 
/// soft deleted by the BatchCoordinator.
#[derive(Debug)]
pub struct FileToDelete {
    /// The object key for the file.
    pub object_key: String,
    /// The system timestamp for when the file was marked for deletion.
    pub marked_for_deletion_at: SystemTime,
}

/// A request that tells the BatchCoordinator that the set of files
/// have been deleted and therefore can be marked as permanently deleted.
#[derive(Debug)]
pub struct DeleteFilesRequest {
    /// Set of files to be deleted.
    pub object_key_paths: HashSet<String>,
}


/// The BatchCoordinator trait. 
/// 
/// This structure is responsible for handling the indexing of offsets within topic's partitions.
/// It is designed to be reimplementable for custom usecases depending on what the specific need is.
#[async_trait::async_trait]
pub trait BatchCoordinator
where
    Self: Send + Sync + std::fmt::Debug,
{
    /// This operation is called when a Diskless partition
    /// (or a topic with one or more partitions) is created in the cluster.
    /// The Batch Coordinator initializes the corresponding logs.
    ///
    /// # Errors
    /// Returns an error if an unexpected error occurs.
    async fn create_topic_and_partitions(&self, requests: HashSet<CreateTopicAndPartitionsRequest>);

    /// This operation is called by a broker after uploading the
    /// shared log segment object to the object storage.
    ///
    /// The Batch Coordinator:
    /// 1. Performs the necessary checks for idempotent produce.
    /// 2. Accordingly increases the high watermark of the affected logs.
    /// 3. Assigns offsets to the batches.
    /// 4. Saves the batch and object metadata.
    /// 5. Returns the result to the broker.
    ///
    /// # Errors
    /// Returns an error if an unexpected error occurs.
    async fn commit_file(
        &self,
        object_key: [u8; 16],
        uploader_broker_id: u32,
        file_size: u64,
        batches: Vec<CommitBatchRequest>,
    ) -> Vec<CommitBatchResponse>;

    /// This operation is called by a broker when it needs to serve a Fetch request.
    /// The Batch Coordinator collects the batch coordinates to satisfy
    /// this request and sends the response back to the broker.
    ///
    /// # Errors
    /// Returns an error if an unexpected error occurs.
    async fn find_batches(
        &self,
        find_batch_requests: Vec<FindBatchRequest>,
        fetch_max_bytes: u32,
    ) -> Vec<FindBatchResponse>;

    /// This operation allows the broker to get the information about log offsets:
    /// earliest, latest, etc. The operation is a read-only operation.
    ///
    /// # Errors
    /// Returns an error if an unexpected error occurs.
    async fn list_offsets(&self, requests: Vec<ListOffsetsRequest>) -> Vec<ListOffsetsResponse>;

    /// This operation is called when a partition needs to be truncated by the user.
    /// The Batch Coordinator:
    /// 1. Modifies the log start offset for the affected partitions (logs).
    /// 2. Deletes the batches that are no longer needed due to this truncation.
    /// 3. If some objects become empty after deleting these batches,
    ///    they are marked for deletion as well.
    ///
    /// # Errors
    /// Returns an error if an unexpected error occurs.
    async fn delete_records(
        &self,
        requests: Vec<DeleteRecordsRequest>,
    ) -> Vec<DeleteRecordsResponse>;

    /// This operation is called when topics are deleted.
    /// It’s similar to deleting records, but all the associated batches
    /// are deleted and the log metadata are deleted as well.
    ///
    /// # Errors
    /// Returns an error if an unexpected error occurs.
    async fn delete_topics(&self, topic_ids: HashSet<String>);

    /// This operation allows a broker to get a list of soft deleted objects
    /// for asynchronous physical deletion from the object storage.
    ///
    /// # Errors
    /// Returns an error if an unexpected error occurs.
    async fn get_files_to_delete(&self) -> Vec<FileToDelete>;

    /// This operation informs the Batch Coordinator that certain soft deleted
    /// objects were also deleted physically from the object storage.
    /// The Batch Coordinator removes all metadata about these objects.
    ///
    /// # Errors
    /// Returns an error if an unexpected error occurs.
    async fn delete_files(&self, request: DeleteFilesRequest);

    /// Determines whether or not the given file with a specific object key
    /// is safe for permanent deletion.
    async fn is_safe_to_delete_file(&self, object_key: String) -> bool;
}

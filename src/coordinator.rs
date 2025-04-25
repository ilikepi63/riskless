use std::collections::HashSet;
use std::vec::Vec;

use std::time::SystemTime;

#[derive(Debug)]
pub struct TopicIdPartition(u64, u64);

#[derive(Debug)]
enum TimestampType {}

#[derive(Debug)]
pub struct CreateTopicAndPartitionsRequest {
    topic_id: uuid::Uuid,
    topic_name: String,
    num_partitions: i32,
}

#[derive(Debug)]
pub struct CommitBatchRequest {
    request_id: i32,
    topic_id_partition: TopicIdPartition,
    byte_offset: i64,
    size: i32,
    base_offset: i64,
    last_offset: i64,
    batch_max_timestamp: i64,
    message_timestamp_type: TimestampType,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    last_sequence: i32,
}

#[derive(Debug)]
pub struct CommitBatchResponse {
    errors: Vec<String>, // TODO: fix this. This needs to be an Errors object.
    assigned_base_offset: i64,
    log_append_time: i64,
    log_start_offset: i64,
    is_duplicate: bool,
    request: CommitBatchRequest,
}

#[derive(Debug)]
pub struct FindBatchRequest {
    topic_id_partition: TopicIdPartition,
    offset: i64,
    max_partition_fetch_bytes: i32,
}

#[derive(Debug)]
pub struct FindBatchResponse {
    errors: Vec<String>, // TODO: fix this. This needs to be an Errors object.
    batches: Vec<BatchInfo>,
    log_start_offset: i64,
    high_watermark: i64,
}

#[derive(Debug)]
pub struct BatchInfo {
    batch_id: i64,
    object_key: String,
    metadata: BatchMetadata,
}

#[derive(Debug)]
pub struct BatchMetadata {
    topic_id_partition: TopicIdPartition,
    byte_offset: i64,
    byte_size: i64,
    base_offset: i64,
    last_offset: i64,
    log_append_timestamp: i64,
    batch_max_timestamp: i64,
    timestamp_type: TimestampType,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    last_sequence: i32,
}

#[derive(Debug)]
pub struct ListOffsetsRequest {
    topic_id_partition: TopicIdPartition,
    timestamp: i64,
}

// impl ListOffsetsRequest {
//     pub const EARLIEST_TIMESTAMP: i64 = org
//         .apache
//         .kafka
//         .common
//         .requests
//         .ListOffsetsRequest
//         .EARLIEST_TIMESTAMP;
//     pub const LATEST_TIMESTAMP: i64 = org
//         .apache
//         .kafka
//         .common
//         .requests
//         .ListOffsetsRequest
//         .LATEST_TIMESTAMP;
//     pub const MAX_TIMESTAMP: i64 = org
//         .apache
//         .kafka
//         .common
//         .requests
//         .ListOffsetsRequest
//         .MAX_TIMESTAMP;
//     pub const EARLIEST_LOCAL_TIMESTAMP: i64 = org
//         .apache
//         .kafka
//         .common
//         .requests
//         .ListOffsetsRequest
//         .EARLIEST_LOCAL_TIMESTAMP;
//     pub const LATEST_TIERED_TIMESTAMP: i64 = org
//         .apache
//         .kafka
//         .common
//         .requests
//         .ListOffsetsRequest
//         .LATEST_TIERED_TIMESTAMP;
// }

#[derive(Debug)]
pub struct ListOffsetsResponse {
    errors: Vec<String>, // TODO: fix this. This needs to be an Errors object.
    topic_id_partition: TopicIdPartition,
    timestamp: i64,
    offset: i64,
}

#[derive(Debug)]
pub struct DeleteRecordsRequest {
    topic_id_partition: TopicIdPartition,
    offset: i64,
}

#[derive(Debug)]
pub struct DeleteRecordsResponse {
    errors: Vec<String>, // TODO: fix this. This needs to be an Errors object.
    low_watermark: i64,
}

#[derive(Debug)]
pub struct FileToDelete {
    object_key: String,
    marked_for_deletion_at: SystemTime,
}

#[derive(Debug)]
pub struct DeleteFilesRequest {
    object_key_paths: HashSet<String>,
}
pub trait BatchCoordinator where Self: Send + Sync {
    /// This operation is called when a Diskless partition
    /// (or a topic with one or more partitions) is created in the cluster.
    /// The Batch Coordinator initializes the corresponding logs.
    ///
    /// # Errors
    /// Returns an error if an unexpected error occurs.
    fn create_topic_and_partitions(&self, requests: HashSet<CreateTopicAndPartitionsRequest>);

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
    fn commit_file(
        &self,
        object_key: String,
        uploader_broker_id: i32,
        file_size: i64,
        batches: Vec<CommitBatchRequest>,
    ) -> Vec<CommitBatchResponse>;

    /// This operation is called by a broker when it needs to serve a Fetch request.
    /// The Batch Coordinator collects the batch coordinates to satisfy
    /// this request and sends the response back to the broker.
    ///
    /// # Errors
    /// Returns an error if an unexpected error occurs.
    fn find_batches(
        &self,
        find_batch_requests: Vec<FindBatchRequest>,
        fetch_max_bytes: i32,
    ) -> Vec<FindBatchResponse>;

    /// This operation allows the broker to get the information about log offsets:
    /// earliest, latest, etc. The operation is a read-only operation.
    ///
    /// # Errors
    /// Returns an error if an unexpected error occurs.
    fn list_offsets(&self, requests: Vec<ListOffsetsRequest>) -> Vec<ListOffsetsResponse>;

    /// This operation is called when a partition needs to be truncated by the user.
    /// The Batch Coordinator:
    /// 1. Modifies the log start offset for the affected partitions (logs).
    /// 2. Deletes the batches that are no longer needed due to this truncation.
    /// 3. If some objects become empty after deleting these batches,
    ///    they are marked for deletion as well.
    ///
    /// # Errors
    /// Returns an error if an unexpected error occurs.
    fn delete_records(&self, requests: Vec<DeleteRecordsRequest>) -> Vec<DeleteRecordsResponse>;

    /// This operation is called when topics are deleted.
    /// It’s similar to deleting records, but all the associated batches
    /// are deleted and the log metadata are deleted as well.
    ///
    /// # Errors
    /// Returns an error if an unexpected error occurs.
    fn delete_topics(&self, topic_ids: HashSet<uuid::Uuid>);

    /// This operation allows a broker to get a list of soft deleted objects
    /// for asynchronous physical deletion from the object storage.
    ///
    /// # Errors
    /// Returns an error if an unexpected error occurs.
    fn get_files_to_delete(&self) -> Vec<FileToDelete>;

    /// This operation informs the Batch Coordinator that certain soft deleted
    /// objects were also deleted physically from the object storage.
    /// The Batch Coordinator removes all metadata about these objects.
    ///
    /// # Errors
    /// Returns an error if an unexpected error occurs.
    fn delete_files(&self, request: DeleteFilesRequest);

    fn is_safe_to_delete_file(&self, object_key: String) -> bool;
}
pub struct DefaultBatchCoordinator {}

impl DefaultBatchCoordinator {
    pub fn new() -> Self {
        Self {}
    }
}

impl BatchCoordinator for DefaultBatchCoordinator {
    fn create_topic_and_partitions(&self, requests: HashSet<CreateTopicAndPartitionsRequest>) {
        todo!()
    }

    fn commit_file(
        &self,
        object_key: String,
        uploader_broker_id: i32,
        file_size: i64,
        batches: Vec<CommitBatchRequest>,
    ) -> Vec<CommitBatchResponse> {
        todo!()
    }

    fn find_batches(
        &self,
        find_batch_requests: Vec<FindBatchRequest>,
        fetch_max_bytes: i32,
    ) -> Vec<FindBatchResponse> {
        todo!()
    }

    fn list_offsets(&self, requests: Vec<ListOffsetsRequest>) -> Vec<ListOffsetsResponse> {
        todo!()
    }

    fn delete_records(&self, requests: Vec<DeleteRecordsRequest>) -> Vec<DeleteRecordsResponse> {
        todo!()
    }

    fn delete_topics(&self, topic_ids: HashSet<uuid::Uuid>) {
        todo!()
    }

    fn get_files_to_delete(&self) -> Vec<FileToDelete> {
        todo!()
    }

    fn delete_files(&self, request: DeleteFilesRequest) {
        todo!()
    }

    fn is_safe_to_delete_file(&self, object_key: String) -> bool {
        todo!()
    }
}

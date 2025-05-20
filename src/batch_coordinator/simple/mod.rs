//! Simple Implementation of a BatchCoordinator.
mod index;

use index::Index;
use std::{
    collections::HashSet,
    fs::{File, OpenOptions},
    io::{Read, Seek, Write},
    path::PathBuf,
};

use bytes::BytesMut;
use uuid::Uuid;

use crate::{batch_coordinator::BatchInfo, error::RisklessResult, messages::CommitBatchRequest};

use crate::batch_coordinator::{
    BatchMetadata, CommitBatchResponse, DeleteFilesRequest, DeleteRecordsRequest,
    DeleteRecordsResponse, FileToDelete, FindBatchRequest, FindBatchResponse,
};

use super::{CommitFile, DeleteFiles, FindBatches};

/// The SimpleBatchCoordinator is a default implementation that is
///
/// - Single Node
/// - Based to a very less extent on Kafka's segment file implementation.
#[derive(Debug)]
pub struct SimpleBatchCoordinator {
    directory: PathBuf,
}

impl SimpleBatchCoordinator {
    /// Creates a new instance of this struct.
    pub fn new(directory: String) -> Self {
        Self {
            directory: PathBuf::from(directory),
        }
    }

    /// Retrieves the current topic directory of this Coordinator.
    ///
    /// Side effect: if the topic directory does not exist, this simply creates it.
    fn topic_dir(&self, topic: String) -> PathBuf {
        let mut current_topic_dir = self.directory.clone();
        current_topic_dir.push(topic);

        match current_topic_dir.exists() {
            true => current_topic_dir,
            false => {
                let _ = std::fs::create_dir(current_topic_dir.clone()); // TODO: handle this error.
                current_topic_dir
            }
        }
    }

    /// Retrieves the directory of the given topic/partition.
    fn partition_index_file_from_topic_dir<'a>(
        topic_dir: &'a mut PathBuf,
        partition: &'a Vec<u8>,
    ) -> &'a mut PathBuf {
        topic_dir.push(format!(
            "{:0>20}.index",
            partition.iter().map(|b| b.to_string()).collect::<String>()
        ));

        (topic_dir) as _
    }

    /// Opens or creates the underlying file depending on it's existence.
    fn open_or_create_file(current_partition_file: &PathBuf) -> RisklessResult<File> {
        let file = match current_partition_file.exists() {
            true => {
                let mut open_opts = OpenOptions::new();

                open_opts.append(true);

                open_opts.open(current_partition_file)
            }
            false => std::fs::File::create(current_partition_file),
        };
        Ok(file?)
    }

    /// Simply opens a given file.
    fn open_file(current_partition_file: &PathBuf) -> RisklessResult<File> {
        let file = std::fs::File::open(current_partition_file)?;

        Ok(file)
    }
}

#[async_trait::async_trait]
impl CommitFile for SimpleBatchCoordinator {
    async fn commit_file(
        &self,
        object_key: [u8; 16],
        _uploader_broker_id: u32,
        _file_size: u64,
        batches: Vec<CommitBatchRequest>,
    ) -> Vec<CommitBatchResponse> {
        // TODO: this needs to return CommitBatchResponses.

        let mut commit_batch_responses = Vec::with_capacity(batches.len());

        for batch in batches {
            let mut current_topic_dir = self.topic_dir(batch.topic_id_partition.0.clone());

            let current_partition_file = Self::partition_index_file_from_topic_dir(
                &mut current_topic_dir,
                &batch.topic_id_partition.1,
            );

            let file = Self::open_or_create_file(current_partition_file);

            match file {
                Ok(mut file) => {
                    let offset = batch.byte_offset;
                    let size = batch.size;

                    let index = Index::new(Uuid::from_bytes(object_key), offset, size);

                    let buf: BytesMut = index.into();

                    let _ = file.write_all(&buf); // TODO: handle this error.
                    commit_batch_responses.push(CommitBatchResponse {
                        errors: vec![],
                        assigned_base_offset: 0,
                        log_append_time: 0,
                        log_start_offset: 0,
                        is_duplicate: false,
                        request: batch,
                    });
                }
                Err(err) => {
                    commit_batch_responses.push(CommitBatchResponse {
                        errors: vec![err.to_string()],
                        assigned_base_offset: 0,
                        log_append_time: 0,
                        log_start_offset: 0,
                        is_duplicate: false,
                        request: batch,
                    });
                }
            }
        }

        commit_batch_responses
    }
}

#[async_trait::async_trait]
impl FindBatches for SimpleBatchCoordinator {
    async fn find_batches(
        &self,
        find_batch_requests: Vec<FindBatchRequest>,
        _fetch_max_bytes: u32,
    ) -> Vec<FindBatchResponse> {
        let mut results = vec![];

        for request in find_batch_requests {
            let topic_id_partition = request.topic_id_partition.clone();

            let mut current_topic_dir = self.topic_dir(request.topic_id_partition.0);

            let current_partition_file = Self::partition_index_file_from_topic_dir(
                &mut current_topic_dir,
                &request.topic_id_partition.1,
            );

            let file = Self::open_file(current_partition_file);

            match file {
                Ok(mut file) => {
                    let size_in_u64: u64 = match Index::packed_size().try_into() {
                        Ok(s) => s,
                        Err(err) => {
                            let message = format!(
                                "Failed to convert Index size into u64 with error: {:#?}",
                                err
                            );

                            results.push(FindBatchResponse {
                                errors: vec![err.to_string()],
                                batches: vec![],
                                log_start_offset: request.offset,
                                high_watermark: 0,
                            });

                            tracing::error!(message);
                            continue;
                        }
                    };

                    // Seek to the desired offset in the file.
                    match file.seek(std::io::SeekFrom::Start(request.offset * size_in_u64)) {
                        Ok(_) => {}
                        Err(err) => {
                            let message =
                                format!("Failed to Seek in file with resultant error: {:#?}", err);

                            results.push(FindBatchResponse {
                                errors: vec![err.to_string()],
                                batches: vec![],
                                log_start_offset: request.offset,
                                high_watermark: 0,
                            });

                            tracing::error!(message);
                            continue;
                        }
                    };

                    let mut buf: [u8; 28] = [0; Index::packed_size()];

                    match file.read_exact(&mut buf) {
                        Ok(_) => {}
                        Err(err) => {
                            let message =
                                format!("Failed to read bytes from file with error: {:#?}", err);

                            results.push(FindBatchResponse {
                                errors: vec![err.to_string()],
                                batches: vec![],
                                log_start_offset: request.offset,
                                high_watermark: 0,
                            });

                            tracing::error!(message);
                            continue;
                        }
                    };

                    let index = Index::try_from(buf.as_ref());

                    match index {
                        Ok(index) => {
                            results.push(FindBatchResponse {
                                errors: vec![],
                                batches: vec![BatchInfo {
                                    batch_id: 0,
                                    object_key: index.object_key.to_string(),
                                    metadata: BatchMetadata {
                                        topic_id_partition,
                                        byte_offset: index.offset,
                                        byte_size: index.size,
                                        ..Default::default() // base_offset: todo!(),
                                                             // last_offset: todo!(),
                                                             // log_append_timestamp: todo!(),
                                                             // batch_max_timestamp: todo!(),
                                                             // timestamp_type: crate::coordinator::TimestampType::Dummy,
                                                             // producer_id: todo!(),
                                                             // producer_epoch: todo!(),
                                                             // base_sequence: todo!(),
                                                             // last_sequence: todo!(),
                                    },
                                }],
                                log_start_offset: request.offset,
                                high_watermark: 0,
                            });
                        }
                        Err(err) => {
                            results.push(FindBatchResponse {
                                errors: vec![err.to_string()],
                                batches: vec![],
                                log_start_offset: request.offset,
                                high_watermark: 0,
                            });
                        }
                    }
                }
                Err(_err) => {}
            }
        }

        results
    }
}

#[async_trait::async_trait]
impl DeleteFiles for SimpleBatchCoordinator {
    /// Simply returns errors as this implementation does not support this operation.
    async fn delete_records(
        &self,
        requests: Vec<DeleteRecordsRequest>,
    ) -> Vec<DeleteRecordsResponse> {
        requests
            .iter()
            .map(|_req| DeleteRecordsResponse {
                errors: vec!["Coordinator does not support deleting records.".to_string()],
                low_watermark: 1,
            })
            .collect::<Vec<_>>()
    }
    /// No-op as this operation is not supported in the SimpleBatchCoordinator.
    async fn delete_topics(&self, _topic_ids: HashSet<String>) {}

    /// Returns an empty vec as this operation is not supported in SimpleBatchCoordinator.
    async fn get_files_to_delete(&self) -> Vec<FileToDelete> {
        vec![]
    }

    /// No-op as this operation is not supported in the SimpleBatchCoordinator.
    async fn delete_files(&self, _request: DeleteFilesRequest) {}

    /// Always returns false.
    async fn is_safe_to_delete_file(&self, _object_key: String) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use crate::batch_coordinator::TopicIdPartition;

    use super::*;
    use std::fs::{self, File};
    use std::io::Read;
    use tracing_test::traced_test;
    use uuid::Uuid;

    fn set_up_dirs() -> PathBuf {
        let mut batch_coord_path = std::env::temp_dir();
        batch_coord_path.push(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir(&batch_coord_path).expect("");

        batch_coord_path
    }

    fn tear_down_dirs(batch_coord: PathBuf) {
        std::fs::remove_dir_all(&batch_coord).expect("");
    }

    #[test]
    fn test_new() {
        let temp_dir = set_up_dirs();
        let path = temp_dir.to_string_lossy().to_string();
        let coordinator = SimpleBatchCoordinator::new(path.clone());

        assert_eq!(coordinator.directory, PathBuf::from(path));
        tear_down_dirs(temp_dir);
    }

    #[test]
    fn test_topic_dir_creates_directory_if_not_exists() {
        let temp_dir = set_up_dirs();

        let coordinator = SimpleBatchCoordinator::new(temp_dir.to_str().expect("").to_string());

        let topic = "test_topic".to_string();

        // Directory shouldn't exist yet
        let expected_path = temp_dir.join(&topic);
        assert!(!expected_path.exists());

        // Call topic_dir which should create it
        let result = coordinator.topic_dir(topic.clone());

        // Verify the directory was created
        assert_eq!(result, expected_path);
        assert!(expected_path.exists());
        assert!(expected_path.is_dir());
        tear_down_dirs(temp_dir);
    }

    #[test]
    fn test_topic_dir_uses_existing_directory() {
        let temp_dir = set_up_dirs();

        let coordinator = SimpleBatchCoordinator::new(temp_dir.to_str().expect("").to_string());

        let topic = "existing_topic".to_string();

        // Create the directory manually first
        let expected_path = temp_dir.join(&topic);
        fs::create_dir(&expected_path).expect("");

        // Call topic_dir
        let result = coordinator.topic_dir(topic.clone());

        // Should return the existing directory
        assert_eq!(result, expected_path);
        tear_down_dirs(temp_dir);
    }

    #[test]
    fn test_partition_index_file_from_topic_dir() {
        let mut topic_dir = PathBuf::from("test_topic");
        let partition = Vec::from(&42_u8.to_be_bytes());

        let result =
            SimpleBatchCoordinator::partition_index_file_from_topic_dir(&mut topic_dir, &partition);

        assert_eq!(
            result.to_str().expect(""),
            "test_topic/00000000000000000042.index"
        );
    }

    #[test]
    fn test_open_or_create_file_creates_new_file() {
        let temp_dir = set_up_dirs();

        let file_path = temp_dir.join("test_file.index");

        // File shouldn't exist yet
        assert!(!file_path.exists());

        // Try to open/create
        let result = SimpleBatchCoordinator::open_or_create_file(&file_path);
        assert!(result.is_ok());

        // File should now exist
        assert!(file_path.exists());
        tear_down_dirs(temp_dir);
    }

    #[test]
    fn test_open_or_create_file_opens_existing_file() {
        let temp_dir = set_up_dirs();
        let file_path = temp_dir.join("existing_file.index");

        // Create the file first
        File::create(&file_path).expect("");

        // Try to open
        let result = SimpleBatchCoordinator::open_or_create_file(&file_path);
        assert!(result.is_ok());
        tear_down_dirs(temp_dir);
    }

    #[test]
    fn test_open_file_success() {
        let temp_dir = set_up_dirs();
        let file_path = temp_dir.join("test_open_file.index");

        // Create the file first
        File::create(&file_path).expect("");

        // Try to open
        let result = SimpleBatchCoordinator::open_file(&file_path);
        assert!(result.is_ok());
        tear_down_dirs(temp_dir);
    }

    #[test]
    fn test_open_file_fails_when_not_exists() {
        let temp_dir = set_up_dirs();
        let file_path = temp_dir.join("nonexistent_file.index");

        // Try to open
        let result = SimpleBatchCoordinator::open_file(&file_path);
        assert!(result.is_err());
        tear_down_dirs(temp_dir);
    }

    #[tokio::test]
    #[traced_test]
    async fn test_commit_file_creates_index_file() {
        let temp_dir = set_up_dirs();

        let coordinator = SimpleBatchCoordinator::new(temp_dir.to_str().expect("").to_string());

        let whole_dir = temp_dir.clone();

        let topic = "test_topic".to_string();
        let partition = Vec::from(&1_u8.to_be_bytes());

        let object_key = Uuid::new_v4().into_bytes();
        let batches = vec![CommitBatchRequest {
            topic_id_partition: TopicIdPartition(topic.clone(), partition.clone()),
            byte_offset: 0,
            size: 100,
            request_id: 1,
            base_offset: 0,
            last_offset: 0,
            batch_max_timestamp: 0,
            message_timestamp_type: crate::batch_coordinator::TimestampType::Dummy,
            producer_id: 0,
            producer_epoch: 0,
            base_sequence: 0,
            last_sequence: 0,
        }];

        let expected_file_path = temp_dir.join(&topic).join(format!(
            "{:0>20}.index",
            partition.iter().map(|b| b.to_string()).collect::<String>()
        ));

        // File shouldn't exist yet
        assert!(!expected_file_path.exists());

        // Commit the file
        coordinator.commit_file(object_key, 1, 100, batches).await;

        // File should now exist
        assert!(expected_file_path.exists());

        // Verify file contents
        let mut file = File::open(&expected_file_path).expect("");
        let mut buf = [0u8; 28]; // Assuming Index is 28 bytes
        file.read_exact(&mut buf).expect("");

        // You might want to add more specific assertions about the contents
        assert_ne!(buf, [0u8; 28]);
        tear_down_dirs(whole_dir);
    }

    #[tokio::test]
    async fn test_find_batches_reads_correct_data() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = set_up_dirs();

        let coordinator = SimpleBatchCoordinator::new(temp_dir.to_str().expect("").to_string());

        // TODO: This behaviour likely needs to be implemented in the SimpleBatchCoordinator.
        let data_path = temp_dir.clone();
        data_path.to_path_buf().push("data");
        let index_path = temp_dir.clone();
        index_path.to_path_buf().push("index");

        let _ = std::fs::create_dir(data_path);
        let _ = std::fs::create_dir(index_path);

        let topic = "test_topic".to_string();
        let partition = Vec::from(&1_u8.to_be_bytes());

        // First, create an index file with some data
        let object_key = Uuid::new_v4().into_bytes();
        let offset = 0;
        let size = 100;

        let batches = vec![CommitBatchRequest {
            topic_id_partition: TopicIdPartition(topic.clone(), partition.clone()),
            byte_offset: offset,
            size,
            request_id: 1,
            base_offset: 0,
            last_offset: 0,
            batch_max_timestamp: 0,
            message_timestamp_type: crate::batch_coordinator::TimestampType::Dummy,
            producer_id: 0,
            producer_epoch: 0,
            base_sequence: 0,
            last_sequence: 0,
        }];

        coordinator.commit_file(object_key, 1, 100, batches).await;

        // Now try to find the batch
        let find_requests = vec![FindBatchRequest {
            topic_id_partition: TopicIdPartition(topic.clone(), partition),
            offset,
            max_partition_fetch_bytes: 1024,
        }];

        let results = coordinator.find_batches(find_requests, 1024).await;

        assert_eq!(results.len(), 1);
        let response = &results[0];

        assert!(response.errors.is_empty());
        assert_eq!(response.batches.len(), 1);

        let batch = &response.batches[0];
        assert_eq!(batch.metadata.byte_offset, offset);
        assert_eq!(batch.metadata.byte_size, size);

        tear_down_dirs(temp_dir);
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_multiple_writes() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = set_up_dirs();

        let coordinator = SimpleBatchCoordinator::new(temp_dir.to_str().expect("").to_string());

        // TODO: This behaviour likely needs to be implemented in the SimpleBatchCoordinator.
        let data_path = temp_dir.clone();
        data_path.to_path_buf().push("data");
        let index_path = temp_dir.clone();
        index_path.to_path_buf().push("index");

        let _ = std::fs::create_dir(&data_path);
        let _ = std::fs::create_dir(&index_path);

        let topic = "test_topic".to_string();
        let partition = Vec::from(&1_u8.to_be_bytes());

        // First, create an index file with some data
        let object_key = Uuid::new_v4().into_bytes();
        let object_key_two = Uuid::new_v4().into_bytes();
        let offset = 0;
        let size = 100;

        let batches = vec![CommitBatchRequest {
            topic_id_partition: TopicIdPartition(topic.clone(), partition.clone()),
            byte_offset: offset,
            size,
            request_id: 1,
            base_offset: 0,
            last_offset: 0,
            batch_max_timestamp: 0,
            message_timestamp_type: crate::batch_coordinator::TimestampType::Dummy,
            producer_id: 0,
            producer_epoch: 0,
            base_sequence: 0,
            last_sequence: 0,
        }];

        coordinator.commit_file(object_key, 1, 100, batches).await;

        let batches = vec![CommitBatchRequest {
            topic_id_partition: TopicIdPartition(topic.clone(), partition.clone()),
            byte_offset: 1,
            size,
            request_id: 1,
            base_offset: 0,
            last_offset: 0,
            batch_max_timestamp: 0,
            message_timestamp_type: crate::batch_coordinator::TimestampType::Dummy,
            producer_id: 0,
            producer_epoch: 0,
            base_sequence: 0,
            last_sequence: 0,
        }];

        coordinator
            .commit_file(object_key_two, 1, 100, batches)
            .await;

        let batches = vec![CommitBatchRequest {
            topic_id_partition: TopicIdPartition(topic.clone(), partition.clone()),
            byte_offset: 1,
            size,
            request_id: 2,
            base_offset: 0,
            last_offset: 0,
            batch_max_timestamp: 0,
            message_timestamp_type: crate::batch_coordinator::TimestampType::Dummy,
            producer_id: 0,
            producer_epoch: 0,
            base_sequence: 0,
            last_sequence: 0,
        }];

        coordinator
            .commit_file(object_key_two, 1, 100, batches)
            .await;

        let mut index_path = index_path.to_path_buf();

        index_path.push(&topic);

        index_path.push(format!("{:0>20}.index", partition.iter().map(|b| b.to_string()).collect::<String>()));

        let data = std::fs::read(index_path)?;

        assert_eq!(data.len(), Index::packed_size() * 3);

        // Now try to find the batch
        let find_requests = vec![FindBatchRequest {
            topic_id_partition: TopicIdPartition(topic.clone(), partition.clone()),
            offset,
            max_partition_fetch_bytes: 1024,
        }];

        let results = coordinator.find_batches(find_requests, 1024).await;

        assert_eq!(results.len(), 1);
        let response = &results[0];

        assert!(response.errors.is_empty());
        assert_eq!(response.batches.len(), 1);

        let batch = &response.batches[0];
        assert_eq!(batch.metadata.byte_offset, offset);
        assert_eq!(batch.metadata.byte_size, size);

        let find_requests = vec![FindBatchRequest {
            topic_id_partition: TopicIdPartition(topic.clone(), partition.clone()),
            offset: 1,
            max_partition_fetch_bytes: 1024,
        }];

        let results = coordinator.find_batches(find_requests, 1024).await;

        assert_eq!(results.len(), 1);
        let response = &results[0];

        assert!(response.errors.is_empty());
        assert_eq!(response.batches.len(), 1);

        let batch = &response.batches[0];
        assert_eq!(batch.metadata.byte_offset, 1);
        assert_eq!(batch.metadata.byte_size, size);

        tear_down_dirs(temp_dir);

        Ok(())
    }

    // #[tokio::test]
    // Not Implemented yet.
    #[allow(dead_code)]
    async fn test_find_batches_handles_missing_file() {
        let temp_dir = set_up_dirs();

        let coordinator = SimpleBatchCoordinator::new(temp_dir.to_str().expect("").to_string());

        let find_requests = vec![FindBatchRequest {
            topic_id_partition: TopicIdPartition("nonexistent_topic".to_string(), Vec::from(&1_u8.to_be_bytes())),
            offset: 0,
            max_partition_fetch_bytes: 1024,
        }];

        let results = coordinator.find_batches(find_requests, 1024).await;

        assert_eq!(results.len(), 1);
        let response = &results[0];

        // Should have an error about the missing file
        assert!(!response.errors.is_empty());
        assert!(response.batches.is_empty());
        tear_down_dirs(temp_dir);
    }
}

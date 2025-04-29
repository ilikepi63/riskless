use std::{
    collections::HashSet,
    fs::{File, OpenOptions},
    io::{Read, Seek, Write},
    path::PathBuf,
};

use bytes::BytesMut;
use uuid::Uuid;

use crate::{
    batch_coordinator::BatchInfo, error::RisklessResult,
    messages::commit_batch_request::CommitBatchRequest, simple_batch_coordinator::index::Index,
};

use crate::batch_coordinator::{
    BatchCoordinator, BatchMetadata, CommitBatchResponse, CreateTopicAndPartitionsRequest,
    DeleteFilesRequest, DeleteRecordsRequest, DeleteRecordsResponse, FileToDelete,
    FindBatchRequest, FindBatchResponse, ListOffsetsRequest, ListOffsetsResponse,
};

pub struct SimpleBatchCoordinator {
    directory: PathBuf,
}

impl SimpleBatchCoordinator {
    pub fn new(directory: String) -> Self {
        Self {
            directory: PathBuf::from(directory),
        }
    }

    /// Has the necessary side effect making certain this is a directory that exists.
    fn topic_dir(&self, topic: String) -> PathBuf {
        let mut current_topic_dir = self.directory.clone();
        current_topic_dir.push(topic);

        let current_topic_dir = match current_topic_dir.exists() {
            true => current_topic_dir,
            false => {
                let _ = std::fs::create_dir(current_topic_dir.clone()); // TODO: handle this error.
                current_topic_dir
            }
        };

        current_topic_dir
    }

    fn partition_index_file_from_topic_dir(
        topic_dir: &mut PathBuf,
        partition: u64,
    ) -> &mut PathBuf {
        topic_dir.push(format!("{:0>20}.index", partition.to_string()));

        let current_partition_file = topic_dir;

        current_partition_file
    }

    // I think you might be able to do this with the File API?
    fn open_or_create_file(current_partition_file: &PathBuf) -> RisklessResult<File> {
        let file = match current_partition_file.exists() {
            true => {
                let mut open_opts = OpenOptions::new();

                open_opts.append(true).write(true);

                open_opts.open(current_partition_file)
            }
            false => std::fs::File::create(current_partition_file),
        }?;

        Ok(file)
    }

    fn open_file(current_partition_file: &PathBuf) -> RisklessResult<File> {
        let file = std::fs::File::open(current_partition_file)?;

        Ok(file)
    }
}

impl BatchCoordinator for SimpleBatchCoordinator {
    fn create_topic_and_partitions(&self, _requests: HashSet<CreateTopicAndPartitionsRequest>) {
        // This is not implemented for SimpleBatchCoordinator as the topics + partitions get 
        // created as they have data produced to them.
    }

    fn commit_file(
        &self,
        object_key: [u8; 16],
        _uploader_broker_id: u32,
        _file_size: u64,
        batches: Vec<CommitBatchRequest>,
    ) -> Vec<CommitBatchResponse> {
        // TODO: this needs to return CommitBatchResponses.

        for batch in batches {
            let mut current_topic_dir = self.topic_dir(batch.topic_id_partition.0);

            let current_partition_file = Self::partition_index_file_from_topic_dir(
                &mut current_topic_dir,
                batch.topic_id_partition.1,
            );

            let file = Self::open_or_create_file(current_partition_file);

            match file {
                Ok(mut file) => {
                    let offset = batch.byte_offset;
                    let size = batch.size;

                    let index = Index::new(Uuid::from_bytes(object_key), offset, size);

                    let buf: BytesMut = index.into();

                    let _ = file.write_all(&buf); // TODO: handle this error.
                }
                Err(err) => {
                    println!("Error when creating index file: {:#?}", err);
                    // TODO: File error and return to result.
                }
            }
        }

        vec![]
    }

    fn find_batches(
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
                request.topic_id_partition.1,
            );

            let file = Self::open_file(current_partition_file);

            match file {
                Ok(mut file) => {
                    println!("Reading from position: {:#?}", request.offset);

                    let _result = file.seek(std::io::SeekFrom::Start(request.offset)).unwrap();

                    let mut buf: [u8; 28] = [0; Index::packed_size()];

                    file.read_exact(&mut buf).unwrap();

                    let index = Index::try_from(buf.as_ref());

                    match index {
                        Ok(index) => {
                            println!("Received Index: {:#?}", index);

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
                Err(err) => {
                    println!("Error when creating index file: {:#?}", err);
                    // TODO: File error and return to result.
                }
            }
        }

        results
    }

    fn list_offsets(&self, _requests: Vec<ListOffsetsRequest>) -> Vec<ListOffsetsResponse> {
        todo!()
    }

    fn delete_records(&self, _requests: Vec<DeleteRecordsRequest>) -> Vec<DeleteRecordsResponse> {
        todo!()
    }

    fn delete_topics(&self, _topic_ids: HashSet<uuid::Uuid>) {
        todo!()
    }

    fn get_files_to_delete(&self) -> Vec<FileToDelete> {
        todo!()
    }

    fn delete_files(&self, _request: DeleteFilesRequest) {
        todo!()
    }

    fn is_safe_to_delete_file(&self, _object_key: String) -> bool {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::batch_coordinator::TopicIdPartition;

    use super::*;
    use std::fs::{self, File};
    use std::io::Read;
    use tempdir::TempDir;
    use uuid::Uuid;

    fn create_test_dir() -> tempdir::TempDir {
        let temp_dir = TempDir::new("test_dir_prefix").unwrap();
        temp_dir
    }

    // Helper function to create a test coordinator with a temp directory
    fn create_test_coordinator() -> (SimpleBatchCoordinator, tempdir::TempDir) {
        let temp_dir = TempDir::new("test_dir_prefix").unwrap();
        let coordinator =
            SimpleBatchCoordinator::new(temp_dir.path().to_str().unwrap().to_string());
        (coordinator, temp_dir)
    }

    #[test]
    fn test_new() {
        let temp_dir = TempDir::new("test_dir_prefix").unwrap();
        let path = temp_dir.path().to_str().unwrap().to_string();
        let coordinator = SimpleBatchCoordinator::new(path.clone());

        assert_eq!(coordinator.directory, PathBuf::from(path));
    }

    #[test]
    fn test_topic_dir_creates_directory_if_not_exists() {
        let (coordinator, temp_dir) = create_test_coordinator();
        let topic = "test_topic".to_string();

        // Directory shouldn't exist yet
        let expected_path = temp_dir.path().join(&topic);
        assert!(!expected_path.exists());

        // Call topic_dir which should create it
        let result = coordinator.topic_dir(topic.clone());

        // Verify the directory was created
        assert_eq!(result, expected_path);
        assert!(expected_path.exists());
        assert!(expected_path.is_dir());
    }

    #[test]
    fn test_topic_dir_uses_existing_directory() {
        let (coordinator, temp_dir) = create_test_coordinator();
        let topic = "existing_topic".to_string();

        // Create the directory manually first
        let expected_path = temp_dir.path().join(&topic);
        fs::create_dir(&expected_path).unwrap();

        // Call topic_dir
        let result = coordinator.topic_dir(topic.clone());

        // Should return the existing directory
        assert_eq!(result, expected_path);
    }

    #[test]
    fn test_partition_index_file_from_topic_dir() {
        let mut topic_dir = PathBuf::from("test_topic");
        let partition = 42;

        let result =
            SimpleBatchCoordinator::partition_index_file_from_topic_dir(&mut topic_dir, partition);

        assert_eq!(
            result.to_str().unwrap(),
            "test_topic/00000000000000000042.index"
        );
    }

    #[test]
    fn test_open_or_create_file_creates_new_file() {
        let temp_dir = create_test_dir();
        let file_path = temp_dir.path().join("test_file.index");

        // File shouldn't exist yet
        assert!(!file_path.exists());

        // Try to open/create
        let result = SimpleBatchCoordinator::open_or_create_file(&file_path);
        assert!(result.is_ok());

        // File should now exist
        assert!(file_path.exists());
    }

    #[test]
    fn test_open_or_create_file_opens_existing_file() {
        let temp_dir = create_test_dir();
        let file_path = temp_dir.path().join("existing_file.index");

        // Create the file first
        File::create(&file_path).unwrap();

        // Try to open
        let result = SimpleBatchCoordinator::open_or_create_file(&file_path);
        assert!(result.is_ok());
    }

    #[test]
    fn test_open_file_success() {
        let temp_dir = create_test_dir();
        let file_path = temp_dir.path().join("test_open_file.index");

        // Create the file first
        File::create(&file_path).unwrap();

        // Try to open
        let result = SimpleBatchCoordinator::open_file(&file_path);
        assert!(result.is_ok());
    }

    #[test]
    fn test_open_file_fails_when_not_exists() {
        let temp_dir = create_test_dir();
        let file_path = temp_dir.path().join("nonexistent_file.index");

        // Try to open
        let result = SimpleBatchCoordinator::open_file(&file_path);
        assert!(result.is_err());
    }

    #[test]
    fn test_commit_file_creates_index_file() {
        let (coordinator, temp_dir) = create_test_coordinator();
        let topic = "test_topic".to_string();
        let partition = 1;

        let object_key = Uuid::new_v4().into_bytes();
        let batches = vec![CommitBatchRequest {
            topic_id_partition: TopicIdPartition(topic.clone(), partition),
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

        let expected_file_path = temp_dir
            .path()
            .join(&topic)
            .join(format!("{:0>20}.index", partition));

        // File shouldn't exist yet
        assert!(!expected_file_path.exists());

        // Commit the file
        coordinator.commit_file(object_key, 1, 100, batches);

        // File should now exist
        assert!(expected_file_path.exists());

        // Verify file contents
        let mut file = File::open(&expected_file_path).unwrap();
        let mut buf = [0u8; 28]; // Assuming Index is 28 bytes
        file.read_exact(&mut buf).unwrap();

        // You might want to add more specific assertions about the contents
        assert_ne!(buf, [0u8; 28]);
    }

    #[test]
    fn test_find_batches_reads_correct_data() {
        let (coordinator, _) = create_test_coordinator();
        let topic = "test_topic".to_string();
        let partition = 1;

        // First, create an index file with some data
        let object_key = Uuid::new_v4().into_bytes();
        let offset = 0;
        let size = 100;

        let batches = vec![CommitBatchRequest {
            topic_id_partition: TopicIdPartition(topic.clone(), partition),
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

        coordinator.commit_file(object_key, 1, 100, batches);

        // Now try to find the batch
        let find_requests = vec![FindBatchRequest {
            topic_id_partition: TopicIdPartition(topic.clone(), partition),
            offset,
            max_partition_fetch_bytes: 1024,
        }];

        let results = coordinator.find_batches(find_requests, 1024);

        assert_eq!(results.len(), 1);
        let response = &results[0];

        assert!(response.errors.is_empty());
        assert_eq!(response.batches.len(), 1);

        let batch = &response.batches[0];
        assert_eq!(batch.metadata.byte_offset, offset);
        assert_eq!(batch.metadata.byte_size, size);
    }

    // #[test]
    // Not Implemented yet.
    #[allow(dead_code)]
    fn test_find_batches_handles_missing_file() {
        let (coordinator, _) = create_test_coordinator();

        let find_requests = vec![FindBatchRequest {
            topic_id_partition: TopicIdPartition("nonexistent_topic".to_string(), 1),
            offset: 0,
            max_partition_fetch_bytes: 1024,
        }];

        let results = coordinator.find_batches(find_requests, 1024);

        assert_eq!(results.len(), 1);
        let response = &results[0];

        // Should have an error about the missing file
        assert!(!response.errors.is_empty());
        assert!(response.batches.is_empty());
    }
}

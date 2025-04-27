use std::{
    collections::HashSet,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    thread::current,
};

use bytes::{BufMut, BytesMut};

use super::{
    BatchCoordinator, CommitBatchRequest, CommitBatchResponse, CreateTopicAndPartitionsRequest,
    DeleteFilesRequest, DeleteRecordsRequest, DeleteRecordsResponse, FileToDelete,
    FindBatchRequest, FindBatchResponse, ListOffsetsRequest, ListOffsetsResponse,
};

pub struct DefaultBatchCoordinator {
    directory: PathBuf,
}

impl DefaultBatchCoordinator {
    pub fn new(directory: String) -> Self {
        Self {
            directory: PathBuf::from(directory),
        }
    }
}

impl BatchCoordinator for DefaultBatchCoordinator {
    fn create_topic_and_partitions(&self, requests: HashSet<CreateTopicAndPartitionsRequest>) {
        todo!()
    }

    fn commit_file(
        &self,
        object_key: [u8; 16],
        uploader_broker_id: i32,
        file_size: i64,
        batches: Vec<CommitBatchRequest>,
    ) -> Vec<CommitBatchResponse> {

        let mut results: Vec<CommitBatchResponse> = vec![];

        for batch in batches {
            let mut current_topic_dir = self.directory.clone();
            current_topic_dir.push(batch.topic_id_partition.0);

            let current_topic_dir = match current_topic_dir.exists() {
                true => current_topic_dir,
                false => {
                    std::fs::create_dir(current_topic_dir.clone());
                    current_topic_dir
                }
            };


            if current_topic_dir.exists() {
                let mut current_partition_file = current_topic_dir.clone();
                current_partition_file.push(batch.topic_id_partition.1.to_string()); // TODO: probably pad this right?

                let file = match current_partition_file.exists() {
                    true => {
                        let mut open_opts = OpenOptions::new();

                        open_opts.append(true).write(true);

                        open_opts.open(current_partition_file)
                    }
                    false => std::fs::File::create(current_partition_file),
                };

                match file {
                    Ok(mut file) => {
                        let offset = batch.byte_offset;
                        let size = batch.size;

                        let mut buf = BytesMut::with_capacity(16 + 8 + 4);

                        buf.put_slice(&object_key);

                        buf.put_u64(offset);
                        buf.put_u32(size);

                        file.write_all(&buf);
                    }
                    Err(err) => {
                        println!("Error when creating index file: {:#?}", err);
                        // TODO: File error and return to result.
                    }
                }
            }
        }

        vec![]
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

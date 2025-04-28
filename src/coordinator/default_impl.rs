use std::{
    collections::HashSet,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    thread::current,
};

use bytes::{BufMut, BytesMut};

use crate::error::RisklessResult;

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

    /// Has the necessary side effect making certain this is a directory that exists.
    fn topic_dir(&self, topic: String) -> PathBuf {
        let mut current_topic_dir = self.directory.clone();
        current_topic_dir.push(topic);

        let current_topic_dir = match current_topic_dir.exists() {
            true => current_topic_dir,
            false => {
                std::fs::create_dir(current_topic_dir.clone());
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

        vec![]
    }

    fn find_batches(
        &self,
        find_batch_requests: Vec<FindBatchRequest>,
        fetch_max_bytes: i32,
    ) -> Vec<FindBatchResponse> {
        for request in find_batch_requests {
            let mut current_topic_dir = self.topic_dir(request.topic_id_partition.0);

            let current_partition_file = Self::partition_index_file_from_topic_dir(
                &mut current_topic_dir,
                request.topic_id_partition.1,
            );

            let file = Self::open_or_create_file(current_partition_file);

            // match file {
            //     Ok(mut file) => {
            //         let offset = request.byte_offset;
            //         let size = request.size;

            //         let mut buf = BytesMut::with_capacity(16 + 8 + 4);

            //         buf.put_slice(&object_key);

            //         buf.put_u64(offset);
            //         buf.put_u32(size);

            //         file.write_all(&buf);
            //     }
            //     Err(err) => {
            //         println!("Error when creating index file: {:#?}", err);
            //         // TODO: File error and return to result.
            //     }
            // }
        }

        vec![]
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

use std::{
    collections::HashSet,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    thread::current,
};

use bytes::{BufMut, Bytes, BytesMut};
use uuid::Uuid;

use crate::error::{RisklessError, RisklessResult};

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

                    let index = Index::new(Uuid::from_bytes(object_key), offset, size);

                    let mut buf: BytesMut = index.into();

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

            match file {
                Ok(mut file) => {
                    // TODO: probably logic to determine where in the file things exist?

                    // let offset = request.byte_offset;
                    // let size = request.size;

                    // let mut buf = BytesMut::with_capacity(16 + 8 + 4);

                    // buf.put_slice(&object_key);

                    // buf.put_u64(offset);
                    // buf.put_u32(size);

                    // file.write_all(&buf);
                }
                Err(err) => {
                    println!("Error when creating index file: {:#?}", err);
                    // TODO: File error and return to result.
                }
            }
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

#[derive(Debug, Clone)]
pub struct Index {
    object_key: Uuid,
    offset: u64,
    size: u32,
}

impl Index {
    pub fn new(object_key: uuid::Uuid, offset: u64, size: u32) -> Self {
        Self {
            object_key,
            offset,
            size,
        }
    }

    #[inline]
    pub fn packed_size() -> usize {
        16 + std::mem::size_of::<u64>() + std::mem::size_of::<u32>()
    }

    pub fn write(self, buf: &mut BytesMut) {
        buf.put_slice(self.object_key.as_bytes());

        buf.put_u64(self.offset);
        buf.put_u32(self.size);
    }
}

impl TryFrom<&[u8]> for Index {
    type Error = RisklessError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() < Self::packed_size() {
            return Err(RisklessError::Unknown); // TODO make an error for this.; 
        }

        println!("{:#?}", &value[0..16]);

        let object_key = Uuid::from_slice(&value[0..16])?;

        println!("{:#?}", &value[17..24]);

        let offset = u64::from_be_bytes(value[16..24].try_into()?);

        let size = u32::from_be_bytes(value[24..28].try_into()?);

        Ok(Self {
            object_key,
            offset,
            size,
        })
    }
}

impl Into<BytesMut> for Index {
    fn into(self) -> BytesMut {
        let mut bytes = BytesMut::with_capacity(Self::packed_size());

        self.write(&mut bytes);

        bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};
    use uuid::Uuid;

    #[test]
    fn test_index_new() {
        let uuid = Uuid::new_v4();
        let offset = 12345;
        let size = 67890;

        let index = Index::new(uuid, offset, size);

        assert_eq!(index.object_key, uuid);
        assert_eq!(index.offset, offset);
        assert_eq!(index.size, size);
    }

    #[test]
    fn test_index_write() {
        let uuid = Uuid::new_v4();
        let offset = 12345;
        let size = 67890;

        let index = Index::new(uuid, offset, size);
        let mut buf = BytesMut::new();
        index.write(&mut buf);

        // Check the written bytes
        assert_eq!(buf.len(), 16 + 8 + 4); // UUID + u64 + u32
        assert_eq!(&buf[0..16], uuid.as_bytes());
        assert_eq!(u64::from_be_bytes(buf[16..24].try_into().unwrap()), offset);
        assert_eq!(u32::from_be_bytes(buf[24..28].try_into().unwrap()), size);
    }

    #[test]
    fn test_try_from_slice_success() {
        let uuid: Uuid = Uuid::new_v4();
        let offset: u64 = 12345;
        let size: u32 = 67890;

        let mut bytes = Vec::new();
        bytes.extend_from_slice(uuid.as_bytes());
        bytes.extend_from_slice(&offset.to_be_bytes());
        bytes.extend_from_slice(&size.to_be_bytes());

        println!("{:#?}", bytes);

        let index = Index::try_from(bytes.as_slice()).unwrap();

        assert_eq!(index.object_key, uuid);
        assert_eq!(index.offset, offset);
        assert_eq!(index.size, size);
    }

    #[test]
    fn test_try_from_slice_too_short() {
        let bytes = [0u8; 15]; // Less than needed (16 + 8 + 4 = 28 bytes)

        let result = Index::try_from(bytes.as_slice());

        println!("{:#?}", result);

        assert!(result.is_err());
    }

    #[test]
    fn test_round_trip() {
        let uuid: Uuid = Uuid::new_v4();
        let offset: u64 = 12345;
        let size: u32 = 67890;

        let original = Index::new(uuid, offset, size);

        let original_clone = original.clone();

        let mut buf = BytesMut::new();
        original_clone.write(&mut buf);

        let reconstructed = Index::try_from(buf.as_ref()).unwrap();

        assert_eq!(reconstructed.object_key, original.object_key);
        assert_eq!(reconstructed.offset, original.offset);
        assert_eq!(reconstructed.size, original.size);
    }
}

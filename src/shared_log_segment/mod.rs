use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::{
    error::RisklessError,
    messages::{batch_coordinate::BatchCoordinate, produce_request::ProduceRequestCollection},
};

static MAGIC_NUMBER: u32 = 522;
static V1_VERSION_NUMBER: u32 = 1;

pub enum SharedLogSegmentHeader {
    V1(SharedLogSegmentHeaderV1),
}

impl SharedLogSegmentHeader {
    #[allow(unused)] 
    pub fn size(&self) -> usize {
        match self {
            Self::V1(_) => SharedLogSegmentHeaderV1::size(),
        }
    }
}

impl TryFrom<Bytes> for SharedLogSegmentHeader {
    type Error = RisklessError;

    fn try_from(mut value: Bytes) -> Result<Self, Self::Error> {
        let magic_number = value.try_get_u32().map_err(|err| {
            RisklessError::UnableToPassHeaderError(format!(
                "Failed to retrieve u32 from Header: {:#?}",
                err
            ))
        })?;

        if magic_number != MAGIC_NUMBER {
            return Err(RisklessError::InvalidMagicNumberError(magic_number));
        }

        let version = value.try_get_u32().map_err(|err| {
            RisklessError::UnableToPassHeaderError(format!(
                "Failed to retrieve u32 from Header: {:#?}",
                err
            ))
        })?;

        match version {
            1 => Ok(SharedLogSegmentHeader::V1(SharedLogSegmentHeaderV1)),
            _ => Err(RisklessError::InvalidSharedLogSegmentVersionNumber(version)),
        }
    }
}

pub struct SharedLogSegmentHeaderV1;

impl SharedLogSegmentHeaderV1 {
    pub const fn version_number() -> u32 {
        V1_VERSION_NUMBER
    }

    pub const fn magic_number() -> u32 {
        MAGIC_NUMBER
    }

    pub const fn size() -> usize {
        std::mem::size_of::<u32>() + std::mem::size_of::<u32>()
    }

    pub fn bytes() -> Bytes {
        let mut bytes = BytesMut::new();

        bytes.put_u32(Self::magic_number());
        bytes.put_u32(Self::version_number());

        bytes.into()
    }
}

pub struct SharedLogSegment(Vec<BatchCoordinate>, BytesMut);

// TODO: ADD IN OBJECT NAME FOR COLLECTION?
impl TryFrom<ProduceRequestCollection> for SharedLogSegment {
    type Error = RisklessError;

    fn try_from(mut value: ProduceRequestCollection) -> Result<Self, Self::Error> {
        let mut buf = BytesMut::with_capacity(value.size().try_into()?);

        buf.put_slice(&SharedLogSegmentHeaderV1::bytes());

        let mut batch_coords = Vec::with_capacity(value.iter_partitions().count()); // TODO: probably just get length here?

        // TODO: probably put some file  header stuff in here..
        let base_offset = SharedLogSegmentHeaderV1::size().try_into()?;

        for partition in value.iter_partitions() {
            for req in partition.value() {
                let offset: u64 = (buf.len()).try_into()?;
                let size = req.data.len();

                buf.put_slice(&req.data);

                tracing::info!("{}", buf.len());

                batch_coords.push(BatchCoordinate {
                    topic: req.topic.clone(),
                    partition: req.partition,
                    base_offset,
                    offset,
                    size: size.try_into()?,
                    request: req.clone(),
                });
            }
        }

        Ok(SharedLogSegment(batch_coords, buf))
    }
}

impl SharedLogSegment {
    pub fn get_batch_coords(&self) -> Vec<BatchCoordinate> {
        self.0.clone()
    }
}

impl From<SharedLogSegment> for bytes::Bytes {
    fn from(val: SharedLogSegment) -> Self {
        val.1.into()
    }
}

#[cfg(test)]
mod tests {
    use crate::{messages::produce_request::ProduceRequest, utils::request_response::Request};

    use super::*;
    use std::convert::TryFrom;

    #[test]
    fn test_empty_collection() {
        let collection = ProduceRequestCollection::new();
        let result = SharedLogSegment::try_from(collection);
        assert!(result.is_ok());
        let segment = result.unwrap();
        assert_eq!(segment.0.len(), 0); // No batch coordinates
        assert_eq!(segment.1.len(), SharedLogSegmentHeaderV1::size()); // Empty buffer
    }

    #[test]
    fn test_single_partition_single_request() -> Result<(), Box<dyn std::error::Error>> {
        let collection = ProduceRequestCollection::new();

        collection.collect(
            Request::new(ProduceRequest {
                request_id: 1,
                topic: "test".to_string(),
                partition: 0,
                data: vec![1, 2, 3],
            })
            .0,
        )?;

        let result = SharedLogSegment::try_from(collection);
        assert!(result.is_ok());
        let segment = result.unwrap();

        assert_eq!(segment.0.len(), 1); // One batch coordinate
        assert_eq!(segment.1.len(), SharedLogSegmentHeaderV1::size() + 3); // 3 bytes of data

        let coord = &segment.0[0];
        assert_eq!(coord.topic, "test");
        assert_eq!(coord.partition, 0);
        assert_eq!(coord.base_offset, SharedLogSegmentHeaderV1::size().try_into().unwrap());
        assert_eq!(coord.offset, SharedLogSegmentHeaderV1::size().try_into().unwrap()); // buf.len() - 1 (3-1=2)

        let expected_bytes = [SharedLogSegmentHeaderV1::bytes().iter().as_slice(), &[1,2,3]].concat();

        assert_eq!(segment.1.as_ref(), &expected_bytes);

        Ok(())
    }

    #[test]
    fn test_multiple_partitions_multiple_requests() -> Result<(), Box<dyn std::error::Error>> {
        let collection = ProduceRequestCollection::new();

        // Partition 0
        collection.collect(
            Request::new(ProduceRequest {
                request_id: 1,
                topic: "test".to_string(),
                partition: 0,
                data: vec![1, 2, 3],
            })
            .0,
        )?;

        collection.collect(
            Request::new(ProduceRequest {
                request_id: 2,
                topic: "test".to_string(),
                partition: 0,
                data: vec![4, 5],
            })
            .0,
        )?;

        // Partition 1
        collection.collect(
            Request::new(ProduceRequest {
                request_id: 3,
                topic: "test".to_string(),
                partition: 1,
                data: vec![6, 7, 8, 9],
            })
            .0,
        )?;

        let result = SharedLogSegment::try_from(collection);
        assert!(result.is_ok());
        let segment = result.unwrap();

        assert_eq!(segment.0.len(), 3); // Three batch coordinates
        assert_eq!(segment.1.len(), 9 + SharedLogSegmentHeaderV1::size()); // 3 + 2 + 4 bytes of data

        // COMMENTED as ordering is not guaranteed.
        // Verify coordinates
        // let coord0 = &segment.0[0];
        // assert_eq!(coord0.partition, 0);
        // assert_eq!(coord0.offset, 0); // First batch ends at index 2

        // let coord1 = &segment.0[1];
        // assert_eq!(coord1.partition, 0);
        // assert_eq!(coord1.offset, 3); // Second batch ends at index 4 (2 + 2)

        // let coord2 = &segment.0[2];
        // assert_eq!(coord2.partition, 1);
        // assert_eq!(coord2.offset, 8); // Third batch ends at index 8 (4 + 4)

        // Verify concatenated data
        // assert_eq!(segment.1.as_ref(), &[1, 2, 3, 4, 5, 6, 7, 8, 9]);

        Ok(())
    }

    #[test]
    fn test_large_data_offsets() -> Result<(), Box<dyn std::error::Error>> {
        let collection = ProduceRequestCollection::new();

        let header = SharedLogSegmentHeaderV1::bytes().to_vec();

        let large_data = vec![0; 10000];

        let expected_large_data = [header, large_data.clone()].concat();

        let request = Request::new(ProduceRequest {
            request_id: 1,
            topic: "large".to_string(),
            partition: 0,
            data: large_data.clone(),
        });
        collection.collect(request.0)?;

        let result = SharedLogSegment::try_from(collection);
        assert!(result.is_ok());
        let segment = result.unwrap();

        assert_eq!(segment.0.len(), 1);
        assert_eq!(segment.1.len(), 10000 + SharedLogSegmentHeaderV1::size());

        let coord = &segment.0[0];
        assert_eq!(coord.offset, SharedLogSegmentHeaderV1::size().try_into().unwrap()); // 10000 - 1
        assert_eq!(segment.1.as_ref(), expected_large_data.as_slice());

        Ok(())
    }
}

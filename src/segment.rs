use bytes::{BufMut, BytesMut};

use crate::{
    error::RisklessError,
    messages::{batch_coordinate::BatchCoordinate, produce_request::ProduceRequestCollection},
};

pub struct SharedLogSegment(Vec<BatchCoordinate>, BytesMut);

// TODO: ADD IN OBJECT NAME FOR COLLECTION?
impl TryFrom<ProduceRequestCollection> for SharedLogSegment {
    type Error = RisklessError;

    fn try_from(mut value: ProduceRequestCollection) -> Result<Self, Self::Error> {
        let mut buf = BytesMut::with_capacity(value.size().try_into()?);

        let mut batch_coords = Vec::with_capacity(value.iter_partitions().count()); // TODO: probably just get length here?

        // TODO: probably put some file  header stuff in here..
        let base_offset = 0;

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
        assert_eq!(segment.1.len(), 0); // Empty buffer
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
        assert_eq!(segment.1.len(), 3); // 3 bytes of data

        let coord = &segment.0[0];
        assert_eq!(coord.topic, "test");
        assert_eq!(coord.partition, 0);
        assert_eq!(coord.base_offset, 0);
        assert_eq!(coord.offset, 0); // buf.len() - 1 (3-1=2)

        assert_eq!(segment.1.as_ref(), &[1, 2, 3]);

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
        assert_eq!(segment.1.len(), 9); // 3 + 2 + 4 bytes of data

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
        let large_data = vec![0; 10000];

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
        assert_eq!(segment.1.len(), 10000);

        let coord = &segment.0[0];
        assert_eq!(coord.offset, 0); // 10000 - 1
        assert_eq!(segment.1.as_ref(), large_data.as_slice());

        Ok(())
    }
}

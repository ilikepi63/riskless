#[cfg(test)]
mod tests {
    use riskless::messages::consume_request::ConsumeRequest;
    use riskless::messages::produce_request::ProduceRequest;
    use riskless::simple_batch_coordinator::SimpleBatchCoordinator;
    use riskless::{Broker, BrokerConfiguration};
    use std::sync::Arc;
    use tracing_test::traced_test;

    #[tokio::test]
    #[traced_test]
    async fn can_produce_without_failure() {
        let batch_coord_path = tempdir::TempDir::new("index").unwrap();
        let object_store_path = tempdir::TempDir::new("data").unwrap();

        let config = BrokerConfiguration {
            object_store: Arc::new(
                object_store::local::LocalFileSystem::new_with_prefix(object_store_path).unwrap(),
            ),
            batch_coordinator: Arc::new(SimpleBatchCoordinator::new(
                batch_coord_path.path().to_string_lossy().to_string(),
            )),
        };

        let mut broker = Broker::new(config);

        let result = broker
            .produce(ProduceRequest {
                request_id: 1,
                topic: "example-topic".to_string(),
                partition: 1,
                data: "hello".as_bytes().to_vec(),
            })
            .await
            .unwrap();

        assert_eq!(result.request_id, 1);
        assert_eq!(result.errors.len(), 0);

        let consume_response = broker
            .consume(ConsumeRequest {
                topic: "example-topic".to_string(),
                partition: 1,
                offset: 0,
                max_partition_fetch_bytes: 0,
            })
            .await;

        assert!(consume_response.is_ok());

        let resp = consume_response.unwrap();

        assert_eq!(resp.batches.len(), 1);
        assert_eq!(
            resp.batches.get(0).unwrap().data,
            bytes::Bytes::from_static(b"hello")
        )
    }

    #[tokio::test]
    #[traced_test]
    async fn can_produce_to_multiple_partitions() {
        let batch_coord_path = tempdir::TempDir::new("index").unwrap();
        let object_store_path = tempdir::TempDir::new("data").unwrap();

        let config = BrokerConfiguration {
            object_store: Arc::new(
                object_store::local::LocalFileSystem::new_with_prefix(object_store_path).unwrap(),
            ),
            batch_coordinator: Arc::new(SimpleBatchCoordinator::new(
                batch_coord_path.path().to_string_lossy().to_string(),
            )),
        };

        let mut broker = Broker::new(config);

        let result = broker
            .produce(ProduceRequest {
                request_id: 1,
                topic: "example-topic".to_string(),
                partition: 1,
                data: "hello".as_bytes().to_vec(),
            })
            .await;

        let result = result.unwrap();
        assert_eq!(result.request_id, 1);
        assert_eq!(result.errors.len(), 0);

        let result = broker
            .produce(ProduceRequest {
                request_id: 2,
                topic: "example-topic".to_string(),
                partition: 2,
                data: "partition-two".as_bytes().to_vec(),
            })
            .await
            .unwrap();

        assert_eq!(result.request_id, 2);
        assert_eq!(result.errors.len(), 0);

        let result = broker
            .produce(ProduceRequest {
                request_id: 3,
                topic: "example-topic".to_string(),
                partition: 3,
                data: "partition-three".as_bytes().to_vec(),
            })
            .await
            .unwrap();

        assert_eq!(result.request_id, 3);
        assert_eq!(result.errors.len(), 0);

        let consume_response = broker
            .consume(ConsumeRequest {
                topic: "example-topic".to_string(),
                partition: 1,
                offset: 0,
                max_partition_fetch_bytes: 0,
            })
            .await;

        assert_eq!(consume_response.unwrap().batches.len(), 1);

        let consume_response = broker
            .consume(ConsumeRequest {
                topic: "example-topic".to_string(),
                partition: 2,
                offset: 0,
                max_partition_fetch_bytes: 0,
            })
            .await
            .unwrap();

        assert_eq!(consume_response.batches.len(), 1);
        assert_eq!(
            consume_response.batches.get(0).unwrap().data,
            bytes::Bytes::from_static(b"partition-two")
        );

        let consume_response = broker
            .consume(ConsumeRequest {
                topic: "example-topic".to_string(),
                partition: 3,
                offset: 0,
                max_partition_fetch_bytes: 0,
            })
            .await
            .unwrap();

        assert_eq!(consume_response.batches.len(), 1);
        assert_eq!(
            consume_response.batches.get(0).unwrap().data,
            bytes::Bytes::from_static(b"partition-three")
        )
    }

    #[tokio::test]
    #[traced_test]
    async fn can_produce_to_the_same_partition_multiple_times() {
        let batch_coord_path = tempdir::TempDir::new("index").unwrap();
        let object_store_path = tempdir::TempDir::new("data").unwrap();

        let config = BrokerConfiguration {
            object_store: Arc::new(
                object_store::local::LocalFileSystem::new_with_prefix(object_store_path).unwrap(),
            ),
            batch_coordinator: Arc::new(SimpleBatchCoordinator::new(
                batch_coord_path.path().to_string_lossy().to_string(),
            )),
        };

        let mut broker = Broker::new(config);

        let result = broker
            .produce(ProduceRequest {
                request_id: 1,
                topic: "example-topic".to_string(),
                partition: 1,
                data: "hello".as_bytes().to_vec(),
            })
            .await;

        tracing::info!("{:#?}", std::fs::read_dir(batch_coord_path).unwrap().into_iter().collect::<Vec<_>>());

        let result = result.unwrap();
        assert_eq!(result.request_id, 1);
        assert_eq!(result.errors.len(), 0);

        let result = broker
            .produce(ProduceRequest {
                request_id: 2,
                topic: "example-topic".to_string(),
                partition: 1,
                data: "partition-two".as_bytes().to_vec(),
            })
            .await
            .unwrap();

        assert_eq!(result.request_id, 2);
        assert_eq!(result.errors.len(), 0);

        let result = broker
            .produce(ProduceRequest {
                request_id: 3,
                topic: "example-topic".to_string(),
                partition: 1,
                data: "partition-three".as_bytes().to_vec(),
            })
            .await
            .unwrap();

        assert_eq!(result.request_id, 3);
        assert_eq!(result.errors.len(), 0);

        let consume_response = broker
            .consume(ConsumeRequest {
                topic: "example-topic".to_string(),
                partition: 1,
                offset: 0,
                max_partition_fetch_bytes: 0,
            })
            .await;

        assert_eq!(consume_response.unwrap().batches.len(), 1);

        let consume_response = broker
            .consume(ConsumeRequest {
                topic: "example-topic".to_string(),
                partition: 1,
                offset: 1,
                max_partition_fetch_bytes: 0,
            })
            .await
            .unwrap();

        assert_eq!(consume_response.batches.len(), 1);
        assert_eq!(
            consume_response.batches.get(0).unwrap().data,
            bytes::Bytes::from_static(b"partition-two")
        );

        let consume_response = broker
            .consume(ConsumeRequest {
                topic: "example-topic".to_string(),
                partition: 1,
                offset: 2,
                max_partition_fetch_bytes: 0,
            })
            .await
            .unwrap();

        assert_eq!(consume_response.batches.len(), 1);
        assert_eq!(
            consume_response.batches.get(0).unwrap().data,
            bytes::Bytes::from_static(b"partition-three")
        )
    }
}

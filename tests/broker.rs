/// Awaits all the data that a receiver sends and
/// returns a vec with the buffered data.
async fn await_all_receiver<T>(mut recv: tokio::sync::mpsc::Receiver<T>) -> Vec<T> {
    let mut result = vec![];

    while let Some(res) = recv.recv().await {
        result.push(res);
    }

    result
}
#[cfg(test)]
mod tests {

    use riskless::messages::consume_request::ConsumeRequest;
    use riskless::messages::produce_request::ProduceRequest;
    use riskless::simple_batch_coordinator::SimpleBatchCoordinator;
    use riskless::{Broker, BrokerConfiguration};
    use std::path::PathBuf;
    use std::sync::Arc;
    use tracing_test::traced_test;

    use crate::await_all_receiver;

    fn set_up_dirs() -> (PathBuf, PathBuf) {
        // UUIDs are used to ensure uniqueness. Note: this may make test a little flaky on chance of collision.
        let mut batch_coord_path = std::env::temp_dir();
        batch_coord_path.push(uuid::Uuid::new_v4().to_string());
        let mut object_store_path = std::env::temp_dir();
        object_store_path.push(uuid::Uuid::new_v4().to_string());

        std::fs::create_dir(&batch_coord_path).unwrap();
        std::fs::create_dir(&object_store_path).unwrap();

        (batch_coord_path, object_store_path)
    }

    fn tear_down_dirs(batch_coord: PathBuf, object_store: PathBuf) {
        std::fs::remove_dir_all(&batch_coord).unwrap();
        std::fs::remove_dir_all(&object_store).unwrap();
    }

    #[tokio::test]
    #[traced_test]
    async fn can_produce_without_failure() {
        let (batch_coord_path, object_store_path) = set_up_dirs();

        let config = BrokerConfiguration {
            object_store: Arc::new(
                object_store::local::LocalFileSystem::new_with_prefix(&object_store_path).unwrap(),
            ),
            batch_coordinator: Arc::new(SimpleBatchCoordinator::new(
                batch_coord_path.to_string_lossy().to_string(),
            )),
            segment_size_in_bytes: 50_000,
            flush_interval_in_ms: 500,
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
        let batches = await_all_receiver(resp).await;

        assert_eq!(batches.len(), 1);
        assert_eq!(
            batches.first().unwrap().batches.first().unwrap().data,
            bytes::Bytes::from_static(b"hello")
        );

        tear_down_dirs(batch_coord_path, object_store_path);
    }

    #[tokio::test]
    #[traced_test]
    async fn can_produce_to_multiple_partitions() {
        let (batch_coord_path, object_store_path) = set_up_dirs();

        let config = BrokerConfiguration {
            object_store: Arc::new(
                object_store::local::LocalFileSystem::new_with_prefix(&object_store_path).unwrap(),
            ),
            batch_coordinator: Arc::new(SimpleBatchCoordinator::new(
                batch_coord_path.to_string_lossy().to_string(),
            )),
            segment_size_in_bytes: 50_000,
            flush_interval_in_ms: 500,
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
            .await
            .unwrap();

        let consume_response = await_all_receiver(consume_response).await;

        assert_eq!(consume_response.first().unwrap().batches.len(), 1);

        let consume_response = broker
            .consume(ConsumeRequest {
                topic: "example-topic".to_string(),
                partition: 2,
                offset: 0,
                max_partition_fetch_bytes: 0,
            })
            .await
            .unwrap();

        let consume_response = await_all_receiver(consume_response).await;

        assert_eq!(consume_response.first().unwrap().batches.len(), 1);
        assert_eq!(
            consume_response
                .first()
                .unwrap()
                .batches
                .first()
                .unwrap()
                .data,
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

        let consume_response = await_all_receiver(consume_response).await;

        assert_eq!(consume_response.first().unwrap().batches.len(), 1);
        assert_eq!(
            consume_response
                .first()
                .unwrap()
                .batches
                .first()
                .unwrap()
                .data,
            bytes::Bytes::from_static(b"partition-three")
        );

        tear_down_dirs(batch_coord_path, object_store_path);
    }

    #[tokio::test]
    #[traced_test]
    async fn can_produce_to_the_same_partition_multiple_times() {
        let (batch_coord_path, object_store_path) = set_up_dirs();

        let config = BrokerConfiguration {
            object_store: Arc::new(
                object_store::local::LocalFileSystem::new_with_prefix(&object_store_path).unwrap(),
            ),
            batch_coordinator: Arc::new(SimpleBatchCoordinator::new(
                batch_coord_path.to_string_lossy().to_string(),
            )),
            segment_size_in_bytes: 50_000,
            flush_interval_in_ms: 500,
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

        tracing::info!(
            "{:#?}",
            std::fs::read_dir(&batch_coord_path)
                .unwrap()
                .collect::<Vec<_>>()
        );

        let result = result.unwrap();
        assert_eq!(result.request_id, 1);

        tracing::info!("{:#?}", result.errors);

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

        tracing::info!("{:#?}", result.errors);

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
            .await
            .unwrap();

        let consume_response = await_all_receiver(consume_response).await;

        assert_eq!(consume_response.first().unwrap().batches.len(), 1);

        let consume_response = broker
            .consume(ConsumeRequest {
                topic: "example-topic".to_string(),
                partition: 1,
                offset: 1,
                max_partition_fetch_bytes: 0,
            })
            .await
            .unwrap();

        let consume_response = await_all_receiver(consume_response).await;

        assert_eq!(consume_response.first().unwrap().batches.len(), 1);
        assert_eq!(
            consume_response
                .first()
                .unwrap()
                .batches
                .first()
                .unwrap()
                .data,
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

        let consume_response = await_all_receiver(consume_response).await;

        assert_eq!(consume_response.first().unwrap().batches.len(), 1);
        assert_eq!(
            consume_response
                .first()
                .unwrap()
                .batches
                .first()
                .unwrap()
                .data,
            bytes::Bytes::from_static(b"partition-three")
        );

        tear_down_dirs(batch_coord_path, object_store_path);
    }

    #[tokio::test]
    #[traced_test]
    async fn can_produce_to_multiple_topics_and_partitions() {
        let (batch_coord_path, object_store_path) = set_up_dirs();

        let config = BrokerConfiguration {
            object_store: Arc::new(
                object_store::local::LocalFileSystem::new_with_prefix(&object_store_path).unwrap(),
            ),
            batch_coordinator: Arc::new(SimpleBatchCoordinator::new(
                batch_coord_path.to_string_lossy().to_string(),
            )),
            segment_size_in_bytes: 50_000,
            flush_interval_in_ms: 500,
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

        tracing::info!(
            "{:#?}",
            std::fs::read_dir(&batch_coord_path)
                .unwrap()
                .collect::<Vec<_>>()
        );

        let result = result.unwrap();
        assert_eq!(result.request_id, 1);

        tracing::info!("{:#?}", result.errors);

        assert_eq!(result.errors.len(), 0);

        let result = broker
            .produce(ProduceRequest {
                request_id: 2,
                topic: "example-topic".to_string(),
                partition: 1,
                data: "example-topic-partition-one-first".as_bytes().to_vec(),
            })
            .await
            .unwrap();

        assert_eq!(result.request_id, 2);

        tracing::info!("{:#?}", result.errors);

        assert_eq!(result.errors.len(), 0);

        let result = broker
            .produce(ProduceRequest {
                request_id: 3,
                topic: "example-topic".to_string(),
                partition: 1,
                data: "example-topic-partition-one-second".as_bytes().to_vec(),
            })
            .await
            .unwrap();

        assert_eq!(result.request_id, 3);
        assert_eq!(result.errors.len(), 0);

        // Different Topics
        let result = broker
            .produce(ProduceRequest {
                request_id: 4,
                topic: "example-topic-two".to_string(),
                partition: 1,
                data: "example-topic-two-partition-one-first".as_bytes().to_vec(),
            })
            .await;

        let result = result.unwrap();
        assert_eq!(result.request_id, 4);

        tracing::info!("{:#?}", result.errors);

        assert_eq!(result.errors.len(), 0);

        let result = broker
            .produce(ProduceRequest {
                request_id: 5,
                topic: "example-topic-two".to_string(),
                partition: 1,
                data: "example-topic-two-partition-one-second".as_bytes().to_vec(),
            })
            .await
            .unwrap();

        assert_eq!(result.request_id, 5);

        tracing::info!("{:#?}", result.errors);

        assert_eq!(result.errors.len(), 0);

        let result = broker
            .produce(ProduceRequest {
                request_id: 6,
                topic: "example-topic-two".to_string(),
                partition: 2,
                data: "example-topic-two-partition-two-first".as_bytes().to_vec(),
            })
            .await
            .unwrap();

        assert_eq!(result.request_id, 6);
        assert_eq!(result.errors.len(), 0);

        let consume_response = broker
            .consume(ConsumeRequest {
                topic: "example-topic".to_string(),
                partition: 1,
                offset: 0,
                max_partition_fetch_bytes: 0,
            })
            .await
            .unwrap();
        let consume_response = await_all_receiver(consume_response).await;

        assert_eq!(consume_response.first().unwrap().batches.len(), 1);

        let consume_response = broker
            .consume(ConsumeRequest {
                topic: "example-topic".to_string(),
                partition: 1,
                offset: 1,
                max_partition_fetch_bytes: 0,
            })
            .await
            .unwrap();

        let consume_response = await_all_receiver(consume_response).await;

        assert_eq!(consume_response.first().unwrap().batches.len(), 1);
        assert_eq!(
            consume_response
                .first()
                .unwrap()
                .batches
                .first()
                .unwrap()
                .data,
            bytes::Bytes::from_static(b"example-topic-partition-one-first")
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

        let consume_response = await_all_receiver(consume_response).await;

        assert_eq!(consume_response.first().unwrap().batches.len(), 1);
        assert_eq!(
            consume_response
                .first()
                .unwrap()
                .batches
                .first()
                .unwrap()
                .data,
            bytes::Bytes::from_static(b"example-topic-partition-one-second")
        );

        let consume_response = broker
            .consume(ConsumeRequest {
                topic: "example-topic-two".to_string(),
                partition: 1,
                offset: 0,
                max_partition_fetch_bytes: 0,
            })
            .await
            .unwrap();

        let consume_response = await_all_receiver(consume_response).await;

        assert_eq!(consume_response.first().unwrap().batches.len(), 1);

        let consume_response = broker
            .consume(ConsumeRequest {
                topic: "example-topic-two".to_string(),
                partition: 2,
                offset: 0,
                max_partition_fetch_bytes: 0,
            })
            .await
            .unwrap();

        let consume_response = await_all_receiver(consume_response).await;

        assert_eq!(consume_response.first().unwrap().batches.len(), 1);
        assert_eq!(
            consume_response
                .first()
                .unwrap()
                .batches
                .first()
                .unwrap()
                .data,
            bytes::Bytes::from_static(b"example-topic-two-partition-two-first")
        );

        let consume_response = broker
            .consume(ConsumeRequest {
                topic: "example-topic-two".to_string(),
                partition: 1,
                offset: 1,
                max_partition_fetch_bytes: 0,
            })
            .await
            .unwrap();

        let consume_response = await_all_receiver(consume_response).await;

        assert_eq!(consume_response.first().unwrap().batches.len(), 1);
        assert_eq!(
            consume_response
                .first()
                .unwrap()
                .batches
                .first()
                .unwrap()
                .data,
            bytes::Bytes::from_static(b"example-topic-two-partition-one-second")
        );

        tear_down_dirs(batch_coord_path, object_store_path);
    }

    #[tokio::test]
    #[traced_test]
    async fn can_request_deletion_of_records() {
        let (batch_coord_path, object_store_path) = set_up_dirs();

        let config = BrokerConfiguration {
            object_store: Arc::new(
                object_store::local::LocalFileSystem::new_with_prefix(&object_store_path).unwrap(),
            ),
            batch_coordinator: Arc::new(SimpleBatchCoordinator::new(
                batch_coord_path.to_string_lossy().to_string(),
            )),
            segment_size_in_bytes: 50_000,
            flush_interval_in_ms: 500,
        };

        let mut broker = Broker::new(config);

        let result = broker
            .delete_record(
                riskless::messages::delete_record_request::DeleteRecordsRequest {
                    topic: "".to_string(),
                    partition: 1,
                    offset: 0,
                },
            )
            .await;

        assert!(result.is_ok());

        let result = result.unwrap();

        // Not really ideal test scenarios.
        assert_eq!(result.errors.len(), 1);
        tear_down_dirs(batch_coord_path, object_store_path);
    }

    #[tokio::test]
    #[traced_test]
    async fn can_delete_files_effectively() {
        let (batch_coord_path, object_store_path) = set_up_dirs();

        let config = BrokerConfiguration {
            object_store: Arc::new(
                object_store::local::LocalFileSystem::new_with_prefix(&object_store_path).unwrap(),
            ),
            batch_coordinator: Arc::new(SimpleBatchCoordinator::new(
                batch_coord_path.to_string_lossy().to_string(),
            )),
            segment_size_in_bytes: 50_000,
            flush_interval_in_ms: 500,
        };

        let mut broker = Broker::new(config);

        let _result = broker.heartbeat_permanent_delete().await.unwrap();

        tear_down_dirs(batch_coord_path, object_store_path);
    }
}

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

    use riskless::batch_coordinator::simple::SimpleBatchCoordinator;
    use riskless::messages::ConsumeRequest;
    use riskless::get_refs;
    use riskless::dereference;
    use riskless::messages::{ProduceRequest, ProduceRequestCollection};
    use riskless::{consume, delete_record, flush, scan_and_permanently_delete_records};
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::RwLock;
    use tracing_test::traced_test;

    use crate::await_all_receiver;

    fn set_up_dirs() -> (PathBuf, PathBuf) {
        // UUIDs are used to ensure uniqueness. Note: this may make test a little flaky on chance of collision.
        let mut batch_coord_path = std::env::temp_dir();
        batch_coord_path.push(uuid::Uuid::new_v4().to_string());
        let mut object_store_path = std::env::temp_dir();
        object_store_path.push(uuid::Uuid::new_v4().to_string());

        std::fs::create_dir(&batch_coord_path).expect("");
        std::fs::create_dir(&object_store_path).expect("");

        (batch_coord_path, object_store_path)
    }

    fn tear_down_dirs(batch_coord: PathBuf, object_store: PathBuf) {
        std::fs::remove_dir_all(&batch_coord).expect("");
        std::fs::remove_dir_all(&object_store).expect("");
    }

    #[tokio::test]
    #[traced_test]
    async fn can_produce_without_failure_multitasked() {
        let (batch_coord_path, object_store_path) = set_up_dirs();

        let object_store = Arc::new(
            object_store::local::LocalFileSystem::new_with_prefix(&object_store_path).expect(""),
        );
        let batch_coordinator = Arc::new(SimpleBatchCoordinator::new(
            batch_coord_path.to_string_lossy().to_string(),
        ));

        let col = Arc::new(RwLock::new(ProduceRequestCollection::new()));

        let col_produce = col.clone();

        let handle_one = tokio::spawn(async move {
            let col_lock = col_produce.read().await;

            col_lock
                .collect(ProduceRequest {
                    request_id: 1,
                    topic: "example-topic".to_string(),
                    partition: Vec::from(&1_u8.to_be_bytes()),
                    data: "hello".as_bytes().to_vec(),
                })
                .expect("");
        });

        let col_flush = col.clone();
        let flush_object_store_ref = object_store.clone();
        let flush_batch_coord_ref = batch_coordinator.clone();

        let handle_two = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;

            let mut col_lock = col_flush.write().await;

            let new_ref = col_lock.take();

            drop(col_lock); // Explicitly drop the lock.

            let produce_response = flush(new_ref, flush_object_store_ref, flush_batch_coord_ref)
                .await
                .expect("");

            assert_eq!(produce_response.len(), 1);
        });

        let _ = tokio::join!(handle_one, handle_two);

        let consume_response = get_refs(
            vec![ConsumeRequest {
                topic: "example-topic".to_string(),
                partition: Vec::from(&1_u8.to_be_bytes()),
                offset: 0,
                max_partition_fetch_bytes: 0,
            }],
            batch_coordinator,
        )
        .await;

        assert!(consume_response.is_ok());

        let resp = consume_response.expect("").first().unwrap().clone();

        let mut batches = dereference(resp.clone(), object_store).await.expect("");

        assert_eq!(batches.len(), 1);

        let batch = batches.swap_remove(0).await;

        assert_eq!(
            batch.batches.first().expect("").data,
            bytes::Bytes::from_static(b"hello")
        );

        tear_down_dirs(batch_coord_path, object_store_path);
    }
}

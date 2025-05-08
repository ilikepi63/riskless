use std::{sync::Arc, time::Duration};

use riskless::{
    batch_coordinator::simple::SimpleBatchCoordinator,
    consume, flush,
    messages::{
        ConsumeRequest,
        ProduceRequest, ProduceRequestCollection,
    },
    produce,
};
use tokio::sync::RwLock;

#[tokio::main]
async fn main() {
    let object_store =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix("data").unwrap());
    let batch_coordinator = Arc::new(SimpleBatchCoordinator::new("index".to_string()));

    let col = Arc::new(RwLock::new(ProduceRequestCollection::new()));

    let col_produce = col.clone();

    let handle_one = tokio::spawn(async move {
        let col_lock = col_produce.read().await;

        produce(
            &col_lock,
            ProduceRequest {
                request_id: 1,
                topic: "example-topic".to_string(),
                partition: 1,
                data: "hello".as_bytes().to_vec(),
            },
        )
        .await
        .unwrap();
    });

    let col_flush = col.clone();
    let flush_object_store_ref = object_store.clone();
    let flush_batch_coord_ref = batch_coordinator.clone();

    let handle_two = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;

        let mut col_lock = col_flush.write().await;

        let mut new_ref = ProduceRequestCollection::new();

        std::mem::swap(&mut *col_lock, &mut new_ref);

        drop(col_lock);

        let produce_response = flush(new_ref, flush_object_store_ref, flush_batch_coord_ref)
            .await
            .unwrap();

        assert_eq!(produce_response.len(), 1);
    });

    let _ = tokio::join!(handle_one, handle_two);

    let consume_response = consume(
        ConsumeRequest {
            topic: "example-topic".to_string(),
            partition: 1,
            offset: 0,
            max_partition_fetch_bytes: 0,
        },
        object_store,
        batch_coordinator,
    )
    .await;

    assert!(consume_response.is_ok());

    let mut 
    
    resp = consume_response.unwrap();
    let batch = resp.recv().await;

    println!("Batch: {:#?}", batch);
}

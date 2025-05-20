use std::sync::Arc;

use riskless::{
    batch_coordinator::simple::SimpleBatchCoordinator,
    consume, flush,
    messages::{ConsumeRequest, ProduceRequest, ProduceRequestCollection},
};

#[tokio::main]
async fn main() {
    let object_store =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix("data").expect(""));
    let batch_coordinator = Arc::new(SimpleBatchCoordinator::new("index".to_string()));

    let col = ProduceRequestCollection::new();

    col.collect(ProduceRequest {
        request_id: 1,
        topic: "example-topic".to_string(),
        partition: Vec::from(&1_u8.to_be_bytes()),
        data: "hello".as_bytes().to_vec(),
    })
    .expect("");

    let produce_response = flush(col, object_store.clone(), batch_coordinator.clone())
        .await
        .expect("");

    assert_eq!(produce_response.len(), 1);

    let consume_response = consume(
        ConsumeRequest {
            topic: "example-topic".to_string(),
            partition: Vec::from(&1_u8.to_be_bytes()),
            offset: 0,
            max_partition_fetch_bytes: 0,
        },
        object_store,
        batch_coordinator,
    )
    .await;

    let mut resp = consume_response.expect("");
    let batch = resp.recv().await;

    println!("Batch: {:#?}", batch);
}

#[cfg(test)]
mod tests {
    use riskless::messages::consume_request::ConsumeRequest;
    use riskless::messages::produce_request::ProduceRequest;
    use riskless::simple_batch_coordinator::SimpleBatchCoordinator;
    use riskless::{Broker, BrokerConfiguration};
    use std::sync::Arc;

    #[tokio::test]
    // #[tracing_test::traced_test]
    async fn can_produce_without_failure() -> Result<(), Box<dyn std::error::Error>> {
        let mut batch_coord_path = std::env::current_dir()?;

        tracing::info!("Path: {:#?}", batch_coord_path);

        batch_coord_path.push("index");

        let mut object_store_path = std::env::current_dir()?;
        
        object_store_path.push("data");

        let config = BrokerConfiguration {
            object_store: Arc::new(object_store::local::LocalFileSystem::new_with_prefix(
                object_store_path,
            )?),
            batch_coordinator: Arc::new(SimpleBatchCoordinator::new(
                batch_coord_path.to_string_lossy().to_string(),
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
            .await?;


        tracing::info!("{:#?}", result);

        assert_eq!(result.request_id, 1);
        assert_eq!(result.errors.len(), 0);

        // TODO: Assert result has the correct data.

        let consume_response = broker
            .consume(ConsumeRequest {
                topic: "example-topic".to_string(),
                partition: 1,
                offset: 0,
                max_partition_fetch_bytes: 0,
            })
            .await;

        tracing::info!("{:#?}", consume_response);

        assert!(consume_response.is_ok());

        let resp = consume_response.unwrap();

        assert_eq!(resp.batches.len(), 1);

        Ok(())
    }
}

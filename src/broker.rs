use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use object_store::{ObjectStore, PutPayload, path::Path};
use tokio::sync::RwLock;

use crate::{
    coordinator::{BatchCoordinator, DefaultBatchCoordinator},
    error::RisklessResult,
    messages::{
        consume_request::ConsumeRequest,
        consume_response::ConsumeResponse,
        produce_request::{ProduceRequest, ProduceRequestCollection},
        produce_response::ProduceResponse,
    },
    segment::SharedLogSegment,
};

pub struct Broker {
    config: BrokerConfiguration,
    // produce_buffer: Vec<ProduceRequest>,
    produce_request_tx: tokio::sync::mpsc::Sender<ProduceRequest>,
}

pub struct BrokerConfiguration {
    object_store: Arc<dyn ObjectStore>,
    batch_coordinator: Arc<dyn BatchCoordinator>,
}

impl Default for BrokerConfiguration {
    fn default() -> Self {
        Self {
            object_store: Arc::new(object_store::local::LocalFileSystem::new()),
            batch_coordinator: Arc::new(DefaultBatchCoordinator::new()),
        }
    }
}

impl Broker {
    pub fn new(config: BrokerConfiguration) -> Self {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<ProduceRequest>(100);
        let batch_coordinator_ref = config.batch_coordinator.clone();
        let object_store_ref = config.object_store.clone();

        tokio::task::spawn(async move {
            let buffer: Arc<RwLock<ProduceRequestCollection>> =
                Arc::new(RwLock::new(ProduceRequestCollection::new()));
            let cloned_buffer_ref = buffer.clone();

            let (flush_tx, mut flush_rx) = tokio::sync::mpsc::channel::<()>(1);

            // Flusher task.
            tokio::task::spawn(async move {
                loop {
                    let timer = tokio::time::sleep(Duration::from_millis(500)); // TODO: retrieve this from the configuration.

                    // Await either a flush command or a timer expiry.
                    tokio::select! {
                        _timer = timer => {    },
                        _recv = flush_rx.recv() => {}
                    };

                    let mut buffer_lock = buffer.write().await;

                    if buffer_lock.size() > 0 {
                        // The
                        let buffer = buffer_lock.clone();

                        buffer_lock.clear();

                        drop(buffer_lock); // Explicitly drop the lock.

                        if let Err(err) = flush_buffer(
                            buffer,
                            object_store_ref.clone(),
                            batch_coordinator_ref.clone(),
                        )
                        .await
                        {
                            tracing::error!(
                                "Error occurred when trying to flush buffer: {:#?}",
                                err
                            );
                        }
                    }
                }
            });

            // Accumulator task.
            tokio::spawn(async move {
                while let Some(req) = rx.recv().await {
                    let mut buffer_lock = cloned_buffer_ref.write().await;

                    let _ = buffer_lock.collect(req);

                    // TODO: This is currently hardcoded to 50kb, but we possibly want to make
                    if buffer_lock.size() > 50_000 {
                        let _ = flush_tx.send(()).await;
                    }
                }

                // TODO: what here if there is None?
            });
        });

        Self {
            config,
            produce_request_tx: tx,
        }
    }

    pub async fn produce(&mut self, request: ProduceRequest) -> RisklessResult<ProduceResponse> {
        self.produce_request_tx.send(request).await?;

        Ok(ProduceResponse {})

        // The broker commits the batch coordinates with the Batch Coordinator (described in details in KIP-1164).
        // The Batch Coordinator assigns offsets to the written batches, persists the batch coordinates, and responds to the Broker.
        // The broker sends responses to all Produce requests that are associated with the committed object.
    }

    pub async fn consume(&self, request: ConsumeRequest) -> RisklessResult<ConsumeResponse> {
        // The consumer sends a Fetch request to the broker.
        // The broker queries the Batch Coordinator for the relevant batch coordinates.
        // The broker gets the data either from the object storage and/or from the cache.
        // The broker injects the computed offsets and timestamps into the batches.
        // The broker constructs and sends the Fetch response to the Consumer.

        todo!();
    }
}

async fn flush_buffer(
    reqs: ProduceRequestCollection,
    object_storage: Arc<dyn ObjectStore>,
    batch_coordinator: Arc<dyn BatchCoordinator>,
) -> RisklessResult<()> {
    let reqs: SharedLogSegment = reqs.try_into()?;

    let buf: Bytes = reqs.into();

    let buf_size = buf.len();

    let path = Path::from(uuid::Uuid::new_v4().to_string());

    let _put_result = object_storage
        .put(&path, PutPayload::from_bytes(buf))
        .await?;

    // TODO: The responses here?
    batch_coordinator.commit_file(path.to_string(), 1, buf_size.try_into()?, vec![]);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn can_produce_without_failure() -> Result<(), Box<dyn std::error::Error>> {
        let config = BrokerConfiguration::default();

        let mut broker = Broker::new(config);

        let result = broker
            .produce(ProduceRequest {
                topic: "example-topic".to_string(),
                partition: 1,
                data: "hello".as_bytes().to_vec(),
            })
            .await?;

        // TODO: assertions

        Ok(())
    }
}

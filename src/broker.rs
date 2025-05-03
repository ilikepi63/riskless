use std::{collections::HashSet, sync::Arc, time::Duration};

use bytes::Bytes;
use object_store::{ObjectStore, PutPayload, path::Path};
use tokio::sync::RwLock;

use crate::{
    batch_coordinator::{BatchCoordinator, FindBatchRequest, TopicIdPartition},
    error::RisklessResult,
    messages::{
        commit_batch_request::CommitBatchRequest,
        consume_request::ConsumeRequest,
        consume_response::{ConsumeBatch, ConsumeResponse},
        produce_request::{ProduceRequest, ProduceRequestCollection},
        produce_response::ProduceResponse,
    },
    segment::SharedLogSegment,
};

/// A Broker is the primary interface through riskless is implemented. Broker's are designed to
/// be embedded in nodes, have full access (both read and write) to the underlying object storage implementation and
/// communicate with the configured batch coordinator.
#[derive(Debug)]
pub struct Broker {
    config: BrokerConfiguration,
    produce_request_tx: tokio::sync::mpsc::Sender<(
        ProduceRequest,
        tokio::sync::oneshot::Sender<ProduceResponse>,
    )>,
}

/// Configuration for the broker containing required dependencies.
#[derive(Debug)]
pub struct BrokerConfiguration {
    /// The object store implementation used for persisting message batches
    pub object_store: Arc<dyn ObjectStore>,
    /// The batch coordinator responsible for assigning offsets to batches
    pub batch_coordinator: Arc<dyn BatchCoordinator>,
}

impl Broker {
    /// Creates a new broker instance with the given configuration.
    pub fn new(config: BrokerConfiguration) -> Self {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<(
            ProduceRequest,
            tokio::sync::oneshot::Sender<ProduceResponse>,
        )>(100);
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
                        let mut new_ref = ProduceRequestCollection::new();

                        std::mem::swap(&mut *buffer_lock, &mut new_ref);

                        drop(buffer_lock); // Explicitly drop the lock.

                        if let Err(err) = flush_buffer(
                            new_ref,
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

                    tracing::info!("Received request: {:#?}", req);
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

    /// Handles a produce request by buffering the message for later persistence.
    ///
    /// The message is added to an in-memory buffer which will be periodically
    /// flushed to object storage by a background task. The actual persistence
    /// happens asynchronously.
    #[tracing::instrument(skip_all, name = "produce")]
    pub async fn produce(&mut self, request: ProduceRequest) -> RisklessResult<ProduceResponse> {

        tracing::info!("Producing Request {:#?}.", request);

        let (produce_response_tx, produce_response_rx) = tokio::sync::oneshot::channel();

        self.produce_request_tx
            .send((request, produce_response_tx))
            .await?;

        let message = produce_response_rx.await?;

        Ok(message)
    }

    /// Handles a consume request by retrieving messages from object storage.
    #[tracing::instrument(skip_all, name = "consume")]
    pub async fn consume(&self, request: ConsumeRequest) -> RisklessResult<ConsumeResponse> {
        let batch_responses = self
            .config
            .batch_coordinator
            .find_batches(
                vec![FindBatchRequest {
                    topic_id_partition: TopicIdPartition(request.topic, request.partition),
                    offset: request.offset,
                    max_partition_fetch_bytes: 0,
                }],
                0,
            )
            .await;

        let objects_to_retrieve = batch_responses
            .iter()
            .flat_map(|resp| resp.batches.clone())
            .map(|batch_info| batch_info.object_key)
            .collect::<HashSet<_>>();

        let result = objects_to_retrieve
            .iter()
            .map(|object_name| async {
                let get_object_result = self
                    .config
                    .object_store
                    .get(&Path::from(object_name.as_str()))
                    .await;

                let result = match get_object_result {
                    Ok(get_result) => {
                        if let Ok(b) = get_result.bytes().await {
                            tracing::info!("Retrieved Bytes: {:#?}", b);

                            // Retrieve the current fetch Responses by name.
                            let batch_responses_for_object = batch_responses
                                .iter()
                                .flat_map(|res| {
                                    res.batches
                                        .iter()
                                        .filter(|batch| batch.object_key == *object_name)
                                        .map(|batch| (res.clone(), batch))
                                })
                                .map(|(res, batch)| {
                                    // index into the bytes.
                                    let start: usize = (batch.metadata.base_offset
                                        + batch.metadata.byte_offset)
                                        .try_into()
                                        .unwrap();
                                    let end: usize = (batch.metadata.base_offset
                                        + batch.metadata.byte_offset
                                        + Into::<u64>::into(batch.metadata.byte_size))
                                    .try_into()
                                    .unwrap();

                                    tracing::info!("START: {} END: {} ", start, end);

                                    let data = b.slice(start..end);

                                    let batch = ConsumeBatch {
                                        topic: batch.metadata.topic_id_partition.0.clone(),
                                        partition: batch.metadata.topic_id_partition.1,
                                        offset: res.log_start_offset,
                                        max_partition_fetch_bytes: 0,
                                        data,
                                    };

                                    Some(batch)
                                })
                                .collect::<Vec<_>>();

                            batch_responses_for_object
                        } else {
                            vec![]
                        }
                    }
                    Err(_err) => {
                        // TODO: How are we going to handle errors here?
                        vec![]
                    }
                };
                result
            })
            .collect::<Vec<_>>();

        let mut consume_response = ConsumeResponse { batches: vec![] };

        futures::future::join_all(result)
            .await
            .iter()
            .for_each(|res| {
                res.iter().for_each(|batch| if let Some(b) = batch {
                    consume_response.batches.push(b.clone());
                })
            });

        Ok(consume_response)
    }
}

async fn flush_buffer(
    mut reqs: ProduceRequestCollection,
    object_storage: Arc<dyn ObjectStore>,
    batch_coordinator: Arc<dyn BatchCoordinator>,
) -> RisklessResult<()> {
    let mut senders = reqs.extract_response_senders();

    tracing::info!("Senders: {:#?}", senders);

    tracing::info!("Produce Requests: {:#?}", reqs);

    let reqs: SharedLogSegment = reqs.try_into()?;

    let batch_coords = reqs.get_batch_coords().clone();

    let buf: Bytes = reqs.into();

    let buf_size = buf.len();

    let path = uuid::Uuid::new_v4();

    let path_string = Path::from(path.to_string());

    let _put_result = object_storage
        .put(&path_string, PutPayload::from_bytes(buf))
        .await?;

    // TODO: assert put_result has the correct response?

    // TODO: The responses here?
    let put_result = batch_coordinator
        .commit_file(
            path.into_bytes(),
            1,
            buf_size.try_into()?,
            batch_coords
                .iter()
                .map(CommitBatchRequest::from)
                .collect::<Vec<_>>(),
        )
        .await;

    tracing::info!("Put Result: {:#?}", put_result);

    // This logic might need to go somewhere else.
    for commit_batch_response in put_result.iter() {

        let produce_response = ProduceResponse::from(commit_batch_response);

        match senders.remove(&commit_batch_response.request.request_id) {
            Some(tx) => {
                match tx.send(produce_response) {
                    Ok(_) => {}
                    Err(err) => {
                        // TODO: perhaps retry here?
                        tracing::error!(
                            "Error occurred trying to send produce response {:#?}.",
                            err
                        );
                    }
                };
            }
            None => {
                tracing::error!(
                    "No Sender found for ID: {:#?}",
                    commit_batch_response.request.request_id
                );
            }
        };
    }

    Ok(())
}

use std::{collections::HashSet, sync::Arc, time::Duration};

use bytes::Bytes;
use object_store::{ObjectStore, PutPayload, path::Path};
use tokio::sync::RwLock;

use crate::{
    batch_coordinator::{BatchCoordinator, DeleteFilesRequest, FindBatchRequest, TopicIdPartition},
    error::{RisklessError, RisklessResult},
    messages::{
        commit_batch_request::CommitBatchRequest,
        consume_request::ConsumeRequest,
        consume_response::{ConsumeBatch, ConsumeResponse},
        produce_request::{ProduceRequest, ProduceRequestCollection},
        produce_response::ProduceResponse,
    },
    shared_log_segment::SharedLogSegment,
    utils::request_response::Request,
};

/// A Broker is the primary interface through riskless is implemented. Broker's are designed to
/// be embedded in nodes, have full access (both read and write) to the underlying object storage implementation and
/// communicate with the configured batch coordinator.
#[derive(Debug)]
pub struct Broker {
    config: BrokerConfiguration,
    request_collection: Arc<RwLock<ProduceRequestCollection>>,
    flusher: tokio::sync::mpsc::Sender<()>,
}

/// Configuration for the broker containing required dependencies.
#[derive(Debug)]
pub struct BrokerConfiguration {
    /// The object store implementation used for persisting message batches
    pub object_store: Arc<dyn ObjectStore>,
    /// The batch coordinator responsible for assigning offsets to batches
    pub batch_coordinator: Arc<dyn BatchCoordinator>,
    pub flush_interval_in_ms: u64,
    pub segment_size_in_bytes: u64,
}

impl Broker {
    /// Creates a new broker instance with the given configuration.
    pub fn new(config: BrokerConfiguration) -> Self {
        let batch_coordinator_ref = config.batch_coordinator.clone();
        let object_store_ref = config.object_store.clone();

        let buffer: Arc<RwLock<ProduceRequestCollection>> =
            Arc::new(RwLock::new(ProduceRequestCollection::new()));

        let cloned_buffer_ref = buffer.clone();

        let (flush_tx, mut flush_rx) = tokio::sync::mpsc::channel::<()>(1);

        // Flusher task.
        tokio::task::spawn(async move {
            loop {
                let timer = tokio::time::sleep(Duration::from_millis(config.flush_interval_in_ms));

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
                        tracing::error!("Error occurred when trying to flush buffer: {:#?}", err);
                    }
                }
            }
        });

        Self {
            config,
            request_collection: cloned_buffer_ref,
            flusher: flush_tx,
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

        let (req, res) = Request::new(request);

        let buffer_lock = self.request_collection.read().await;

        let _ = buffer_lock.collect(req);

        if buffer_lock.size() > self.config.segment_size_in_bytes {
            let _ = self.flusher.send(()).await;
        }

        // Explicitly drop the lock here as we await the response.
        drop(buffer_lock);

        let message = res.recv().await?;

        Ok(message)
    }

    /// Delete a specific record.
    ///
    /// Important to note that this undertakes a "soft" delete, which means that
    /// the record still persists in object storage, but does not persist in the BatchCoordinator.
    ///
    /// The process to permanently delete files and records from the underlying object storage is done by a separate function.
    #[tracing::instrument(skip_all, name = "delete_records")]
    pub async fn delete_record(
        &mut self,
        request: crate::messages::delete_record_request::DeleteRecordsRequest,
    ) -> RisklessResult<crate::messages::delete_record_response::DeleteRecordsResponse> {
        let result = self
            .config
            .batch_coordinator
            .delete_records(vec![request.try_into().map_err(|e| {
                RisklessError::Generic(format!(
                    "Failed to convert request into DeleteRecordsRequest with error {:#?}",
                    e
                ))
            })?])
            .await
            .pop()
            .ok_or(RisklessError::Unknown)?;

        result.try_into()
    }

    /// As Records become "soft" deleted over time, the underlying storage mechanism may
    /// have clusters that do not have any references to live records after a certain amount of time.
    ///
    /// This will make the storage mechanism's collection ready for delete. This function effectively
    /// queries the batch coordinator for objects that are ready for delete and then attempts to delete them.
    ///
    /// The interval at which this happens is delegated to the implementor.
    #[tracing::instrument(skip_all, name = "heartbeat_permanent_delete")]
    pub async fn heartbeat_permanent_delete(&mut self) -> RisklessResult<()> {
        let batch_coordinator = self.config.batch_coordinator.clone();
        let object_store = self.config.object_store.clone();

        let files_to_delete = batch_coordinator.get_files_to_delete().await;

        for file in files_to_delete {
            let batch_coordinator = batch_coordinator.clone();
            let object_store = object_store.clone();

            tokio::spawn(async move {
                let file_path = object_store::path::Path::from(file.object_key.as_ref());

                let result = object_store.delete(&file_path).await;

                match result {
                    Ok(_) => {
                        batch_coordinator
                            .delete_files(DeleteFilesRequest {
                                object_key_paths: HashSet::from([file.object_key]),
                            })
                            .await;
                    }
                    Err(err) => {
                        tracing::error!(
                            "Error occurred when trying to delete files in object store: {:#?}",
                            err
                        );
                    }
                }
            });
        }

        Ok(())
    }

    /// Handles a consume request by retrieving messages from object storage.
    #[tracing::instrument(skip_all, name = "consume")]
    pub async fn consume(
        &self,
        request: ConsumeRequest,
    ) -> RisklessResult<tokio::sync::mpsc::Receiver<ConsumeResponse>> {
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

        // We create a
        let (batch_response_tx, batch_reponse_rx) =
            tokio::sync::mpsc::channel(objects_to_retrieve.len());

        let batch_responses = Arc::new(batch_responses);

        for object_name in objects_to_retrieve {
            let batch_response_tx = batch_response_tx.clone();
            let object_name = object_name.clone();
            let object_store = self.config.object_store.clone();
            let batch_responses = batch_responses.clone();

            tokio::spawn(async move {
                let get_object_result = object_store.get(&Path::from(object_name.as_str())).await;

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
                                .filter_map(|(res, batch)| {
                                    ConsumeBatch::try_from((res, batch, &b)).ok()
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

                if !result.is_empty() {
                    if let Err(e) = batch_response_tx
                        .send(ConsumeResponse { batches: result })
                        .await
                    {
                        tracing::error!("Failed to send consume response: {:#?}", e);
                    };
                };
            });
        }

        Ok(batch_reponse_rx)
    }
}

async fn flush_buffer(
    mut reqs: ProduceRequestCollection,
    object_storage: Arc<dyn ObjectStore>,
    batch_coordinator: Arc<dyn BatchCoordinator>,
) -> RisklessResult<()> {
    let senders = reqs.extract_response_senders();

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
            Some((_, tx)) => {
                match tx.respond(produce_response) {
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

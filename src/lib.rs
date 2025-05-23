//! Riskless
//!
//! An implementation of KIP-1150 - Diskless Topics as a reuseable library for general implementation of distributed logs on object storage.
//!
//! example usage:
//!
//! ```ignore
//!
//! let object_store = Arc::new(object_store::local::LocalFileSystem::new_with_prefix("data").unwrap());
//! let batch_coordinator = Arc::new(SimpleBatchCoordinator::new("index".to_string()));
//!
//! let collection = ProduceRequestCollection::new();
//!
//! collection.collect(
//!     ProduceRequest {
//!         request_id: 1,
//!         topic: "example-topic".to_string(),
//!         partition: 1,
//!         data: "hello".as_bytes().to_vec(),
//!     },
//! )
//! .unwrap();
//!
//! let produce_response = flush(collection, object_store.clone(), batch_coordinator.clone())
//!     .await
//!     .unwrap();
//!
//! assert_eq!(produce_response.len(), 1);
//!
//! let consume_response = consume(
//!     ConsumeRequest {
//!         topic: "example-topic".to_string(),
//!         partition: 1,
//!         offset: 0,
//!         max_partition_fetch_bytes: 0,
//!     },
//!     object_store,
//!     batch_coordinator,
//! )
//! .await;
//!
//! let mut resp = consume_response.unwrap();
//! let batch = resp.recv().await;
//!
//! println!("Batch: {:#?}", batch);
//! ```
#![deny(missing_docs)]
#![deny(clippy::print_stdout)]
#![deny(clippy::print_stderr)]
#![deny(clippy::unwrap_used)]

pub mod batch_coordinator;
pub mod messages;
mod shared_log_segment;

use std::{collections::HashSet, sync::Arc};

use batch_coordinator::{
    CommitFile, DeleteFiles, DeleteFilesRequest, FindBatchRequest, FindBatches, TopicIdPartition,
};
use bytes::Bytes;
use messages::{
    CommitBatchRequest, ConsumeBatch, ConsumeRequest, ConsumeResponse, ProduceRequestCollection,
    ProduceResponse,
};
pub mod error;

use error::{RisklessError, RisklessResult};
pub use object_store;
use object_store::{ObjectStore, PutPayload, path::Path};
use shared_log_segment::SharedLogSegment;

/// Flush the ProduceRequestCollection to the ObjectStore/BatchCoordinator.
pub async fn flush(
    reqs: ProduceRequestCollection,
    object_storage: Arc<dyn ObjectStore>,
    batch_coordinator: Arc<dyn CommitFile>,
) -> RisklessResult<Vec<ProduceResponse>> {
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

    Ok(put_result
        .iter()
        .map(ProduceResponse::from)
        .collect::<Vec<_>>())
}

/// Handles a consume request by retrieving messages from object storage.
pub async fn consume(
    request: ConsumeRequest,
    object_storage: Arc<dyn ObjectStore>,
    batch_coordinator: Arc<dyn FindBatches>,
) -> RisklessResult<tokio::sync::mpsc::Receiver<ConsumeResponse>> {
    let batch_responses = batch_coordinator
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
        let object_store = object_storage.clone();
        let batch_responses = batch_responses.clone();

        tokio::spawn(async move {
            let get_object_result = object_store.get(&Path::from(object_name.as_str())).await;

            let result = match get_object_result {
                Ok(get_result) => {
                    if let Ok(b) = get_result.bytes().await {
                        // Retrieve the current fetch Responses by name.
                        let batch_responses_for_object = batch_responses
                            .iter()
                            .flat_map(|res| {
                                res.batches
                                    .iter()
                                    .filter(|batch| batch.object_key == *object_name)
                                    .map(|batch| (res.clone(), batch))
                            })
                            .inspect(|val| {
                                tracing::trace!("Result returned for query: {:#?}", val);
                            })
                            .filter_map(|(res, batch)| {
                                ConsumeBatch::try_from((res, batch, &b)).ok()
                            })
                            .collect::<Vec<_>>();

                        batch_responses_for_object
                    } else {
                        tracing::trace!(
                            "Could not retrieve bytes for given GetObject query: {}",
                            object_name
                        );
                        vec![]
                    }
                }
                Err(err) => {
                    tracing::error!(
                        "An error occurred trying to retrieve the object with key {}. Error: {:#?}",
                        object_name,
                        err
                    );
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
            } else {
                tracing::trace!("No ConsumeBatches found for query.");
            };
        });
    }

    Ok(batch_reponse_rx)
}

/// Delete a specific record.
///
/// Important to note that this undertakes a "soft" delete, which means that
/// the record still persists in object storage, but does not persist in the BatchCoordinator.
///
/// The process to permanently delete files and records from the underlying object storage is done by a separate function.
#[tracing::instrument(skip_all, name = "delete_records")]
pub async fn delete_record(
    request: crate::messages::DeleteRecordsRequest,
    batch_coordinator: Arc<dyn DeleteFiles>,
) -> RisklessResult<crate::messages::DeleteRecordsResponse> {
    let result = batch_coordinator
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
pub async fn scan_and_permanently_delete_records(
    batch_coordinator: Arc<dyn DeleteFiles>,
    object_store: Arc<dyn ObjectStore>,
) -> RisklessResult<()> {
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

// #![deny(missing_docs)]
#![deny(clippy::print_stdout)]
#![deny(clippy::print_stderr)]
#![deny(clippy::unwrap_used)]

pub mod batch_coordinator;
mod broker;
pub mod messages;
mod shared_log_segment;
mod utils;

use std::{
    collections::HashSet,
    sync::{Arc, atomic::Ordering},
};

use batch_coordinator::{BatchCoordinator, FindBatchRequest, TopicIdPartition};
pub use broker::{Broker, BrokerConfiguration};
use bytes::Bytes;
use dashmap::Entry;
use messages::{
    commit_batch_request::CommitBatchRequest,
    consume_request::ConsumeRequest,
    consume_response::{ConsumeBatch, ConsumeResponse},
    produce_request::{ProduceRequest, ProduceRequestCollection},
    produce_response::ProduceResponse,
};
pub mod error;

use error::RisklessResult;
use object_store::{ObjectStore, PutPayload, path::Path};
use shared_log_segment::SharedLogSegment;

/// Handles a produce request by buffering the message for later persistence.
///
/// The message is added to an in-memory buffer which will be periodically
/// flushed to object storage by a background task. The actual persistence
/// happens asynchronously.
#[tracing::instrument(skip_all, name = "produce")]
pub async fn produce(
    collection: &ProduceRequestCollection,
    request: ProduceRequest,
) -> RisklessResult<()> {
    tracing::info!("Producing Request {:#?}.", request);

    let topic_id_partition = TopicIdPartition(request.topic.clone(), request.partition);

    let entry = collection.inner.entry(topic_id_partition);

    match entry {
        Entry::Occupied(mut occupied_entry) => {
            collection.size.fetch_add(
                TryInto::<u64>::try_into(request.data.len())?,
                Ordering::Relaxed,
            );
            occupied_entry.get_mut().push(request.clone());
        }
        Entry::Vacant(vacant_entry) => {
            collection.size.fetch_add(
                TryInto::<u64>::try_into(request.data.len())?,
                Ordering::Relaxed,
            );
            vacant_entry.insert(vec![request.clone()]);
        }
    }

    Ok(())
}

pub async fn flush(
    reqs: ProduceRequestCollection,
    object_storage: Arc<dyn ObjectStore>,
    batch_coordinator: Arc<dyn BatchCoordinator>,
) -> RisklessResult<Vec<ProduceResponse>> {
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

    Ok(put_result
        .iter()
        .map(ProduceResponse::from)
        .collect::<Vec<_>>())
}

/// Handles a consume request by retrieving messages from object storage.
#[tracing::instrument(skip_all, name = "consume")]
pub async fn consume(
    request: ConsumeRequest,
    object_storage: Arc<dyn ObjectStore>,
    batch_coordinator: Arc<dyn BatchCoordinator>,
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

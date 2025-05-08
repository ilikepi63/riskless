# Riskless

An implementation of KIP-1150 - Diskless Topics as a reuseable library for general implementation of distributed logs on object storage. 

### Motivation

Considering separation between compute and storage for distributed logs is becoming more important than ever, primarily because of the costs related to traditional implementations, this library was written with the intention to be built upon. 

When most organisations set out to build a distributed log, it is built with the intention to be fairly "monolithic" in nature. For example, most distributed log implementations come with a specific network protocol, consensus protocol, storage implementation and are generally quite opinionated with most of their implementations. 

Riskless is hopefully the first in a number of libraries that try to make distributed logs composable, similar to what the Apache Arrow/Datafusion projects are doing for traditional databases.

## Usage

### Installation

To use `riskless`, first add this to your `Cargo.toml`:

```toml
[dependencies]
riskless = "x.x"
```

### How to use

```rust
// Custom implementation of BatchCoordinator.
let mut batch_coordinator = Arc::new(MyBatchCoordinator::new()); 
// Create an object store.
let mut object_store = Arc::new(MyObjectStore::new()); 

// Create the current produce request collection
let collection = ProduceRequestCollection::new();

collection.collect(    
    ProduceRequest {
        request_id: 1,
        topic: "example-topic".to_string(),
        partition: 1,
        data: "hello".as_bytes().to_vec(),
    },
)
.await
.unwrap();

let produce_response = flush(col, object_store.clone(), batch_coordinator.clone())
    .await
    .unwrap();

assert_eq!(produce_response.len(), 1);

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
```

#### Implementation of BatchCoordinator

The BatchCoordinator is a trait based on the writings from KIP-1164. This is primarily for developers to implement their own BatchCoordinator based off of their own needs. 

There is a SimpleBatchCoordinator implementation that ships with Riskless that is: 

- Single Node
- Based (to a very small extent) on Kafka's index file implementation.
- Very simplistic in nature
- an example for developers to build their own Batch Coordinator implementation off of. 

```rust 
let mut batch_coordinator = SimpleBatchhCoordinator::new("current_dir");
```

## License

This project is licensed under both an Apache 2.0 license and an MIT license.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in `riskless` by you, shall be licensed under Apache 2.0 and MIT, without any additional
terms or conditions.
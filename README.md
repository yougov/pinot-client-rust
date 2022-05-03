Pinot Client Rust
===============
<!--[![GoDoc](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white)](https://pkg.go.dev/github.com/xiangfu0/pinot-client-go)-->
<!--[![Build Status](https://travis-ci.org/xiangfu0/pinot-client-go.svg?branch=master)](https://travis-ci.org/xiangfu0/pinot-client-go)-->

---

Applications can use this rust client library to query Apache Pinot.

Installing Pinot
================

To install Pinot locally, please follow this [Pinot Quickstart](https://docs.pinot.apache.org/basics/getting-started/running-pinot-locally) link to install and start Pinot batch quickstart locally.

```
bin/quick-start-batch.sh
```

Alternatively, the docker contained Pinot database ochestrated by this repository's `docker-compose.yaml` file may be used.

```
make prepare-pinot
```

Examples
========

Check out Client library Github Repo

```
git clone git@github.com:yougov/pinot-client-rust.git
cd pinot-client-rust
```

Start up the docker contained pinot database

```
make prepare-pinot
```

Build and run the example application to query from Pinot Batch Quickstart

```
cargo run --example batch-quickstart
```

Usage
=====

Create a Pinot Connection
-------------------------

Pinot client could be initialized through:

1. Zookeeper Path.

```
let client = pinot_client_rust::connection::client_from_zookeeper(
    &pinot_client_rust::zookeeper::ZookeeperConfig::new(
        vec!["localhost:2181".to_string()],
        "/PinotCluster".to_string(),
    ),
    None
);
```


2. A list of broker addresses.

```
let client = pinot_client_rust::connection::client_from_broker_list(
    vec!["localhost:8099".to_string()], None);
```


Query Pinot
-----------

Please see this [example](https://github.com/yougov/pinot-client-rust/blob/master/examples/batch-quickstart.rs) for your reference.

Code snippet:
```rust
fn main() {
    let client = pinot_client_rust::connection::client_from_broker_list(
        vec!["localhost:8099".to_string()], None).unwrap();
    let broker_response = client.execute_sql::<pinot_client_rust::response::data::DataRow>(
        "baseballStats",
        "select count(*) as cnt, sum(homeRuns) as sum_homeRuns from baseballStats group by teamID limit 10"
    ).unwrap();
    log::info!(
        "Query Stats: response time - {} ms, scanned docs - {}, total docs - {}",
        broker_response.time_used_ms,
        broker_response.num_docs_scanned,
        broker_response.total_docs,
    );
}
```

Response Format
---------------

Query Responses are defined by one of two broker response structures.
SQL queries return `SqlBrokerResponse`, whose generic parameter is supported by all structs implementing the 
`FromRow` trait, whereas PQL queries return `PqlBrokerResponse`.
`SqlBrokerResponse` contains a `ResultTable`, the holder for SQL query data, whereas `PqlBrokerResponse` contains 
`AggregationResults` and `SelectionResults`, the holders for PQL query data.

### PQL

`PqlBrokerResponse` is defined as:

```rust
/// PqlBrokerResponse is the data structure for broker response to a PQL query.
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct PqlBrokerResponse {
    #[serde(default)]
    #[serde(rename(deserialize = "aggregationResults"))]
    pub aggregation_results: Vec<AggregationResult>,
    #[serde(default)]
    #[serde(rename(deserialize = "selectionResults"))]
    pub selection_results: Option<SelectionResults>,
    pub exceptions: Vec<Exception>,
    #[serde(default)]
    #[serde(rename(deserialize = "traceInfo"))]
    pub trace_info: HashMap<String, String>,
    #[serde(rename(deserialize = "numServersQueried"))]
    pub num_servers_queried: i32,
    #[serde(rename(deserialize = "numServersResponded"))]
    pub num_servers_responded: i32,
    #[serde(rename(deserialize = "numSegmentsQueried"))]
    pub num_segments_queried: i32,
    #[serde(rename(deserialize = "numSegmentsProcessed"))]
    pub num_segments_processed: i32,
    #[serde(rename(deserialize = "numSegmentsMatched"))]
    pub num_segments_matched: i32,
    #[serde(rename(deserialize = "numConsumingSegmentsQueried"))]
    pub num_consuming_segments_queried: i32,
    #[serde(rename(deserialize = "numDocsScanned"))]
    pub num_docs_scanned: i64,
    #[serde(rename(deserialize = "numEntriesScannedInFilter"))]
    pub num_entries_scanned_in_filter: i64,
    #[serde(rename(deserialize = "numEntriesScannedPostFilter"))]
    pub num_entries_scanned_post_filter: i64,
    #[serde(rename(deserialize = "numGroupsLimitReached"))]
    pub num_groups_limit_reached: bool,
    #[serde(rename(deserialize = "totalDocs"))]
    pub total_docs: i64,
    #[serde(rename(deserialize = "timeUsedMs"))]
    pub time_used_ms: i32,
    #[serde(rename(deserialize = "minConsumingFreshnessTimeMs"))]
    pub min_consuming_freshness_time_ms: i64,
}
```

### SQL

`SqlBrokerResponse` is defined as:

```rust
/// BrokerResponse is the data structure for a broker response to an SQL query.
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct SqlBrokerResponse<T: FromRow> {
    #[serde(default = "default_optional_result_table")]
    #[serde(rename(deserialize = "resultTable"))]
    #[serde(deserialize_with = "deserialize_optional_result_table")]
    pub result_table: Option<ResultTable<T>>,
    pub exceptions: Vec<Exception>,
    #[serde(default)]
    #[serde(rename(deserialize = "traceInfo"))]
    pub trace_info: HashMap<String, String>,
    #[serde(rename(deserialize = "numServersQueried"))]
    pub num_servers_queried: i32,
    #[serde(rename(deserialize = "numServersResponded"))]
    pub num_servers_responded: i32,
    #[serde(rename(deserialize = "numSegmentsQueried"))]
    pub num_segments_queried: i32,
    #[serde(rename(deserialize = "numSegmentsProcessed"))]
    pub num_segments_processed: i32,
    #[serde(rename(deserialize = "numSegmentsMatched"))]
    pub num_segments_matched: i32,
    #[serde(rename(deserialize = "numConsumingSegmentsQueried"))]
    pub num_consuming_segments_queried: i32,
    #[serde(rename(deserialize = "numDocsScanned"))]
    pub num_docs_scanned: i64,
    #[serde(rename(deserialize = "numEntriesScannedInFilter"))]
    pub num_entries_scanned_in_filter: i64,
    #[serde(rename(deserialize = "numEntriesScannedPostFilter"))]
    pub num_entries_scanned_post_filter: i64,
    #[serde(rename(deserialize = "numGroupsLimitReached"))]
    pub num_groups_limit_reached: bool,
    #[serde(rename(deserialize = "totalDocs"))]
    pub total_docs: i64,
    #[serde(rename(deserialize = "timeUsedMs"))]
    pub time_used_ms: i32,
    #[serde(rename(deserialize = "minConsumingFreshnessTimeMs"))]
    pub min_consuming_freshness_time_ms: i64,
}
```

`ResultTable` is defined as:

```rust
/// ResultTable is the holder for SQL queries.
#[derive(Clone, Debug, PartialEq)]
pub struct ResultTable<T: FromRow> {
    data_schema: RespSchema,
    rows: Vec<T>,
}
```

`RespSchema` is defined as:

```rust
/// RespSchema is response schema with a bimap to allow easy name <-> index retrieval
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RespSchema {
    column_data_types: Vec<DataType>,
    column_name_to_index: bimap::BiMap::<String, usize>,
}
```

There are multiple functions defined for `RespSchema`, like:

```
fn get_column_count(&self) -> usize;
fn get_column_name(&self, column_index: usize) -> Result<&str>;
fn get_column_index(&self, column_name: &str) -> Result<usize>;
fn get_column_data_type(&self, column_index: usize) -> Result<DataType>;
fn get_column_data_type_by_name(&self, column_name: &str) -> Result<DataType>;
```

`DataType` is defined as:

```rust
/// Pinot native types
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DataType {
    Int,
    Long,
    Float,
    Double,
    Boolean,
    Timestamp,
    String,
    Json,
    Bytes,
    IntArray,
    LongArray,
    FloatArray,
    DoubleArray,
    BooleanArray,
    TimestampArray,
    StringArray,
    BytesArray,
}
```

`FromRow` is defined as:

```rust

/// FromRow represents any structure which can deserialize
/// the ResultTable.rows json field provided a `RespSchema`
pub trait FromRow {
    fn from_row(
        data_schema: &RespSchema,
        row: Vec<Value>,
    ) -> std::result::Result<Self, serde_json::Error> where Self: Sized;
}
```

In addition to being implemented by TODO, `FromRow` is also implemented by `DataRow`, 
the default data container which is defined as:

```rust
/// A row of `Data`
#[derive(Clone, Debug, PartialEq)]
pub struct DataRow {
    row: Vec<Data>,
}
```

`Data` is defined as:

```rust
/// Typed Pinot data
#[derive(Clone, Debug, PartialEq)]
pub enum Data {
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Boolean(bool),
    Timestamp(DateTime<Utc>),
    String(String),
    Json(Value),
    Bytes(Vec<u8>),
    IntArray(Vec<i32>),
    LongArray(Vec<i64>),
    FloatArray(Vec<f32>),
    DoubleArray(Vec<f64>),
    BooleanArray(Vec<bool>),
    TimestampArray(Vec<DateTime<Utc>>),
    StringArray(Vec<String>),
    BytesArray(Vec<Vec<u8>>),
    Null(DataType),
}
```

There are multiple functions defined for `Data`, like:

```
fn data_type(&self) -> DataType;
fn get_int(&self) -> Result<i32>;
fn get_long(&self) -> Result<i64>;
fn get_float(&self) -> Result<f32>;
fn get_double(&self) -> Result<f64>;
fn get_boolean(&self) -> Result<bool>;
fn get_timestamp(&self) -> Result<DateTime<Utc>>;
fn get_string(&self) -> Result<&str>;
fn get_json(&self) -> Result<&Value>;
fn get_bytes(&self) -> Result<&Vec<u8>>;
fn is_null(&self) -> bool;
```

In addition to row count, `DataRow` also contains convenience counterparts to those above given a column index.

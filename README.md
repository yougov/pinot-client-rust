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

```bash
make prepare-pinot
```

Examples
========

Check out Client library Github Repo

```bash
git clone git@github.com:yougov/pinot-client-rust.git
cd pinot-client-rust
```

Start up the docker contained pinot database

```base
make prepare-pinot
```

Build and run an example application to query from Pinot

```bash
cargo run --example pql-query
cargo run --example sql-query-deserialize-to-data-row
cargo run --example sql-query-deserialize-to-struct
```

Usage
=====

Create a Pinot Connection
-------------------------

Pinot client could be initialized through:

1. Zookeeper Path.

```rust
let client = pinot_client_rust::connection::client_from_zookeeper(
    &pinot_client_rust::zookeeper::ZookeeperConfig::new(
        vec!["localhost:2181".to_string()],
        "/PinotCluster".to_string(),
    ),
    None
);
```


2. A list of broker addresses.

```rust
let client = pinot_client_rust::connection::client_from_broker_list(
    vec!["localhost:8099".to_string()], None);
```

### Asynchronous Queries

An asynchronous connection can be established with `pinot_client_rust::async_connection::AsyncConnection` for
which exist equivalents to the above described synchronous instantiation methods.

Query Pinot
-----------

Please see this [example](https://github.com/yougov/pinot-client-rust/blob/master/examples/sql-query-deserialize-to-data-row.rs) for your reference.

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
        broker_response.stats.time_used_ms,
        broker_response.stats.num_docs_scanned,
        broker_response.stats.total_docs,
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
Exceptions for a given request for both `SqlBrokerResponse` and `PqlBrokerResponse` are stored in the `Exception` array.
Stats for a given request for both `SqlBrokerResponse` and `PqlBrokerResponse` are stored in `ResponseStats`.

### Common

`Exception` is defined as:

```rust
/// Pinot exception.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct Exception {
    #[serde(rename(deserialize = "errorCode"))]
    pub error_code: i32,
    pub message: String,
}
```

`ResponseStats` is defined as:

```rust
/// ResponseStats carries all stats returned by a query.
#[derive(Clone, Debug, PartialEq)]
pub struct ResponseStats {
    pub trace_info: HashMap<String, String>,
    pub num_servers_queried: i32,
    pub num_servers_responded: i32,
    pub num_segments_queried: i32,
    pub num_segments_processed: i32,
    pub num_segments_matched: i32,
    pub num_consuming_segments_queried: i32,
    pub num_docs_scanned: i64,
    pub num_entries_scanned_in_filter: i64,
    pub num_entries_scanned_post_filter: i64,
    pub num_groups_limit_reached: bool,
    pub total_docs: i64,
    pub time_used_ms: i32,
    pub min_consuming_freshness_time_ms: i64,
}
```

### PQL

`PqlBrokerResponse` is defined as:

```rust
/// PqlBrokerResponse is the data structure for broker response to a PQL query.
#[derive(Clone, Debug, PartialEq)]
pub struct PqlBrokerResponse {
    pub aggregation_results: Vec<AggregationResult>,
    pub selection_results: Option<SelectionResults>,
    pub exceptions: Vec<Exception>,
    pub stats: ResponseStats,
}
```

### SQL

`SqlBrokerResponse` is defined as:

```rust
/// SqlBrokerResponse is the data structure for a broker response to an SQL query.
#[derive(Clone, Debug, PartialEq)]
pub struct SqlBrokerResponse<T: FromRow> {
    pub result_table: Option<ResultTable<T>>,
    pub exceptions: Vec<Exception>,
    pub stats: ResponseStats,
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
pub trait FromRow: Sized {
    fn from_row(
        data_schema: &RespSchema,
        row: Vec<Value>,
    ) -> std::result::Result<Self, serde_json::Error>;
}
```

In addition to being implemented by `DataRow`, `FromRow` is also implemented by all implementors
of `serde::de::Deserialize`, which is achieved by first deserializing the response to json and then 
before each row is deserialized into final form, a json map of column name to value is substituted.
Additionally, there are a number of serde deserializer functions provided to deserialize complex pinot types:

```
/// Converts Pinot timestamps into `Vec<DateTime<Utc>>` using `deserialize_timestamps_from_json()`.
fn deserialize_timestamps<'de, D>(deserializer: D) -> std::result::Result<Vec<DateTime<Utc>>, D::Error>...

/// Converts Pinot timestamps into `DateTime<Utc>` using `deserialize_timestamp_from_json()`.
pub fn deserialize_timestamp<'de, D>(deserializer: D) -> std::result::Result<DateTime<Utc>, D::Error>...

/// Converts Pinot hex strings into `Vec<Vec<u8>>` using `deserialize_bytes_array_from_json()`.
pub fn deserialize_bytes_array<'de, D>(deserializer: D) -> std::result::Result<Vec<Vec<u8>>, D::Error>...

/// Converts Pinot hex string into `Vec<u8>` using `deserialize_bytes_from_json()`.
pub fn deserialize_bytes<'de, D>(deserializer: D) -> std::result::Result<Vec<u8>, D::Error>...

/// Deserializes json potentially packaged into a string by calling `deserialize_json_from_json()`.
pub fn deserialize_json<'de, D>(deserializer: D) -> std::result::Result<Value, D::Error>
```

For example usage, please refer to this [example](https://github.com/yougov/pinot-client-rust/blob/master/examples/sql-query-deserialize-to-struct.rs) 

`DataRow` is defined as:

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

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


/*!
Applications can use this rust client library to query Apache Pinot.

# Usage

## Create a Pinot Connection

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

- For HTTP
  Default scheme is HTTP if not specified.

```rust
let client = pinot_client_rust::connection::client_from_broker_list(
    vec!["localhost:8099".to_string()], None);
```

- For HTTPS
  Scheme is required to be part of the URI.

```rust
let client = pinot_client_rust::connection::client_from_broker_list(
    vec!["https://localhost:8099".to_string()], None);
```

## Example Pinot Query

```rust
let client = pinot_client_rust::connection::client_from_broker_list(
    vec!["localhost:8099".to_string()], None).unwrap();
let broker_response = client.execute_sql(
    "baseballStats",
    "select count(*) as cnt, sum(homeRuns) as sum_homeRuns from baseballStats group by teamID limit 10"
).unwrap();
log::info!(
    "Query Stats: response time - {} ms, scanned docs - {}, total docs - {}",
    broker_response.time_used_ms,
    broker_response.num_docs_scanned,
    broker_response.total_docs,
);
```

## Response Format

Query Response is defined as the struct `pinot_client_rust::response::BrokerResponse`.
Note that `pinot_client_rust::response::AggregationResults` and
`pinot_client_rust::response::SelectionResults` are holders for PQL queries.
Meanwhile `pinot_client_rust::response::ResultTable` is the holder for SQL queries.
 */

pub mod broker_selector;
pub mod client_transport;
pub mod connection;
pub mod dynamic_broker_selector;
pub mod errors;
pub mod external_view;
pub mod json_http_client_transport;
pub mod request;
pub mod response;
pub mod simple_broker_selector;
pub mod zookeeper;

mod rand;

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::fmt::Debug;
    use std::hash::Hash;

    use chrono::{DateTime, NaiveDateTime, Utc};

    pub fn assert_iters_equal_anyorder<T: Eq + Hash + Debug>(
        actual: impl Iterator<Item=T> + Debug,
        expected: impl Iterator<Item=T> + Debug,
    ) {
        let actual_set: HashSet<T> = actual.collect();
        let expected_set: HashSet<T> = expected.collect();
        let missing: Vec<&T> = expected_set.difference(&actual_set).collect();
        let additional: Vec<&T> = actual_set.difference(&expected_set).collect();

        if !missing.is_empty() || !additional.is_empty() {
            let mut msg = String::from("Expected doesn't match actual. ");
            if !missing.is_empty() {
                msg.push_str(format!("Missing: {:?}", &missing).as_str())
            }
            if !additional.is_empty() {
                msg.push_str(format!("Has extra: {:?}", &additional).as_str())
            }
            panic!("{}", &msg)
        }
    }

    pub fn date_time_utc(
        year: u32, month: u32, day: u32, hour: u32, min: u32, sec: u32,
    ) -> DateTime<Utc> {
        DateTime::from_utc(
            NaiveDateTime::parse_from_str(
                &format!("{}-{}-{} {}:{}:{}", year, month, day, hour, min, sec),
                "%Y-%m-%d %H:%M:%S",
            ).unwrap(),
            Utc,
        )
    }

    pub fn to_string_vec(vec: Vec<&str>) -> Vec<String> {
        vec.into_iter().map(|e| e.to_string()).collect()
    }
}
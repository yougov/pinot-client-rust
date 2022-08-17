use std::collections::HashMap;

use serde::{Deserialize, Serialize};

pub use pql::PqlBrokerResponse;
pub use sql::SqlBrokerResponse;

pub mod data;
pub mod pql;
pub mod raw;
pub mod sql;
pub mod deserialise;

/// Pinot exception.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct Exception {
    #[serde(rename(deserialize = "errorCode"))]
    pub error_code: i32,
    pub message: String,
}

/// ResponseStats carries all stats returned by a query.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
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

/// Pinot native types
#[derive(Copy, Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
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

#[cfg(test)]
pub(crate) mod tests {
    use serde_json::{json, Value};

    use crate::response::{PqlBrokerResponse, ResponseStats};
    use crate::response::data::DataRow;
    use crate::response::raw::SelectionResults;
    use crate::response::sql::SqlBrokerResponse;
    use crate::response::sql::tests::test_result_table;
    use crate::tests::to_string_vec;

    use super::*;

    #[test]
    fn data_type_deserializes_correctly() {
        assert_eq!(DataType::deserialize(json!("INT")).unwrap(), DataType::Int);
        assert_eq!(DataType::deserialize(json!("LONG")).unwrap(), DataType::Long);
        assert_eq!(DataType::deserialize(json!("DOUBLE")).unwrap(), DataType::Double);
        assert_eq!(DataType::deserialize(json!("BOOLEAN")).unwrap(), DataType::Boolean);
        assert_eq!(DataType::deserialize(json!("TIMESTAMP")).unwrap(), DataType::Timestamp);
        assert_eq!(DataType::deserialize(json!("STRING")).unwrap(), DataType::String);
        assert_eq!(DataType::deserialize(json!("JSON")).unwrap(), DataType::Json);
        assert_eq!(DataType::deserialize(json!("BYTES")).unwrap(), DataType::Bytes);
        assert_eq!(DataType::deserialize(json!("INT_ARRAY")).unwrap(), DataType::IntArray);
        assert_eq!(DataType::deserialize(json!("LONG_ARRAY")).unwrap(), DataType::LongArray);
        assert_eq!(DataType::deserialize(json!("FLOAT_ARRAY")).unwrap(), DataType::FloatArray);
        assert_eq!(DataType::deserialize(json!("DOUBLE_ARRAY")).unwrap(), DataType::DoubleArray);
        assert_eq!(DataType::deserialize(json!("BOOLEAN_ARRAY")).unwrap(), DataType::BooleanArray);
        assert_eq!(DataType::deserialize(json!("TIMESTAMP_ARRAY")).unwrap(), DataType::TimestampArray);
        assert_eq!(DataType::deserialize(json!("STRING_ARRAY")).unwrap(), DataType::StringArray);
        assert_eq!(DataType::deserialize(json!("BYTES_ARRAY")).unwrap(), DataType::BytesArray);
    }

    #[test]
    fn data_type_serializes_correctly() {
        assert_eq!(serde_json::to_value(&DataType::Int).unwrap(), json!("INT"));
        assert_eq!(serde_json::to_value(&DataType::Long).unwrap(), json!("LONG"));
        assert_eq!(serde_json::to_value(&DataType::Double).unwrap(), json!("DOUBLE"));
        assert_eq!(serde_json::to_value(&DataType::Boolean).unwrap(), json!("BOOLEAN"));
        assert_eq!(serde_json::to_value(&DataType::Timestamp).unwrap(), json!("TIMESTAMP"));
        assert_eq!(serde_json::to_value(&DataType::String).unwrap(), json!("STRING"));
        assert_eq!(serde_json::to_value(&DataType::Json).unwrap(), json!("JSON"));
        assert_eq!(serde_json::to_value(&DataType::Bytes).unwrap(), json!("BYTES"));
        assert_eq!(serde_json::to_value(&DataType::IntArray).unwrap(), json!("INT_ARRAY"));
        assert_eq!(serde_json::to_value(&DataType::LongArray).unwrap(), json!("LONG_ARRAY"));
        assert_eq!(serde_json::to_value(&DataType::FloatArray).unwrap(), json!("FLOAT_ARRAY"));
        assert_eq!(serde_json::to_value(&DataType::DoubleArray).unwrap(), json!("DOUBLE_ARRAY"));
        assert_eq!(serde_json::to_value(&DataType::BooleanArray).unwrap(), json!("BOOLEAN_ARRAY"));
        assert_eq!(serde_json::to_value(&DataType::TimestampArray).unwrap(), json!("TIMESTAMP_ARRAY"));
        assert_eq!(serde_json::to_value(&DataType::StringArray).unwrap(), json!("STRING_ARRAY"));
        assert_eq!(serde_json::to_value(&DataType::BytesArray).unwrap(), json!("BYTES_ARRAY"));
    }

    pub fn test_broker_response_error_msg() -> String {
        let error_message: &str = concat!(
        "QueryExecutionError:\n",
        "java.lang.NumberFormatException: For input string: \"UA\"\n",
        "\tat sun.misc.FloatingDecimal.readJavaFormatString(FloatingDecimal.java:2043)\n",
        "\tat sun.misc.FloatingDecimal.parseDouble(FloatingDecimal.java:110)\n",
        "\tat java.lang.Double.parseDouble(Double.java:538)\n",
        "\tat org.apache.pinot.core.segment.index.readers.StringDictionary.getDoubleValue(StringDictionary.java:58)\n",
        "\tat org.apache.pinot.core.operator.query.DictionaryBasedAggregationOperator.getNextBlock(DictionaryBasedAggregationOperator.java:81)\n",
        "\tat org.apache.pinot.core.operator.query.DictionaryBasedAggregationOperator.getNextBlock(DictionaryBasedAggregationOperator.java:47)\n",
        "\tat org.apache.pinot.core.operator.BaseOperator.nextBlock(BaseOperator.java:48)\n",
        "\tat org.apache.pinot.core.operator.CombineOperator$1.runJob(CombineOperator.java:102)\n",
        "\tat org.apache.pinot.core.util.trace.TraceRunnable.run(TraceRunnable.java:40)\n",
        "\tat java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)\n",
        "\tat java.util.concurrent.FutureTask.run(FutureTask.java:266)\n",
        "\tat java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)\n",
        "\tat shaded.com.google.common.util.concurrent.TrustedListenableFutureTask$TrustedFutureInterruptibleTask.runInterruptibly(TrustedListenableFutureTask.java:111)\n",
        "\tat shaded.com.google.common.util.concurrent.InterruptibleTask.run(InterruptibleTask.java:58)",
        );
        error_message.to_string()
    }

    pub fn test_error_containing_broker_response(error_message: &str) -> Value {
        json!({
            "exceptions": [{
                "errorCode": 200,
                "message": error_message.clone(),
            }],
            "numServersQueried": 1,
            "numServersResponded": 1,
            "numSegmentsQueried": 12
            ,"numSegmentsProcessed": 0,
            "numSegmentsMatched": 0,
            "numConsumingSegmentsQueried": 0,
            "numDocsScanned": 0,
            "numEntriesScannedInFilter": 0,
            "numEntriesScannedPostFilter": 0,
            "numGroupsLimitReached": false,
            "totalDocs": 97889,
            "timeUsedMs": 5,
            "segmentStatistics": [],
            "traceInfo": {},
            "minConsumingFreshnessTimeMs": 0
        })
    }

    pub fn test_broker_response_json() -> Value {
        json!({
            "resultTable": {
                "dataSchema": {
                    "columnDataTypes": ["LONG", "INT"],
                    "columnNames": ["cnt", "cnt2"]
                },
                "rows": [[97889, 0]]
            },
            "exceptions": [],
            "numServersQueried": 1,
            "numServersResponded": 1,
            "numSegmentsQueried": 1,
            "numSegmentsProcessed": 1,
            "numSegmentsMatched": 1,
            "numConsumingSegmentsQueried": 0,
            "numDocsScanned": 97889,
            "numEntriesScannedInFilter": 0,
            "numEntriesScannedPostFilter": 0,
            "numGroupsLimitReached": false,
            "totalDocs": 97889,
            "timeUsedMs": 5,
            "segmentStatistics": [],
            "traceInfo": {},
            "minConsumingFreshnessTimeMs": 0
        })
    }

    pub fn test_sql_broker_response() -> SqlBrokerResponse<DataRow> {
        SqlBrokerResponse {
            result_table: Some(test_result_table()),
            stats: Some(ResponseStats {
                trace_info: Default::default(),
                num_servers_queried: 1,
                num_servers_responded: 1,
                num_segments_queried: 1,
                num_segments_processed: 1,
                num_segments_matched: 1,
                num_consuming_segments_queried: 0,
                num_docs_scanned: 97889,
                num_entries_scanned_in_filter: 0,
                num_entries_scanned_post_filter: 0,
                num_groups_limit_reached: false,
                total_docs: 97889,
                time_used_ms: 5,
                min_consuming_freshness_time_ms: 0,
            }),
        }
    }

    pub fn test_pql_broker_response() -> PqlBrokerResponse {
        PqlBrokerResponse {
            aggregation_results: vec![],
            selection_results: Some(SelectionResults::new(
                to_string_vec(vec!["cnt", "extra"]),
                vec![vec![
                    Value::String("1".to_string()),
                    Value::String("{\"a\": \"b\"}".to_string()),
                ]],
            )),
            stats: Some(ResponseStats {
                trace_info: Default::default(),
                num_servers_queried: 1,
                num_servers_responded: 1,
                num_segments_queried: 1,
                num_segments_processed: 1,
                num_segments_matched: 1,
                num_consuming_segments_queried: 0,
                num_docs_scanned: 97889,
                num_entries_scanned_in_filter: 0,
                num_entries_scanned_post_filter: 0,
                num_groups_limit_reached: false,
                total_docs: 97889,
                time_used_ms: 5,
                min_consuming_freshness_time_ms: 0,
            }),
        }
    }
}
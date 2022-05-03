use serde::Deserialize;

pub use pql::PqlBrokerResponse;
pub use sql::DataType;
pub use sql::SqlBrokerResponse;

pub mod pql;
pub mod sql;
pub mod data;

/// Exception is Pinot exceptions.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct Exception {
    #[serde(rename(deserialize = "errorCode"))]
    pub error_code: i32,
    pub message: String,
}

#[cfg(test)]
pub mod tests {
    use serde_json::{json, Value};

    use crate::response::data::DataRow;
    use crate::response::pql::SelectionResults;
    use crate::response::PqlBrokerResponse;
    use crate::response::sql::SqlBrokerResponse;
    use crate::response::sql::tests::test_result_table;
    use crate::tests::to_string_vec;

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
            exceptions: vec![],
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
            exceptions: vec![],
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
        }
    }
}
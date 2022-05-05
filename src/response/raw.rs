use std::collections::HashMap;

use serde::Deserialize;
use serde_json::Value;

use crate::errors::{Error, Result};
use crate::response::{DataType, Exception};

/// RawBrokerResponse is the data structure for a broker response to any query.
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct RawBrokerResponse {
    #[serde(default)]
    #[serde(rename(deserialize = "aggregationResults"))]
    pub aggregation_results: Vec<AggregationResult>,
    #[serde(default)]
    #[serde(rename(deserialize = "selectionResults"))]
    pub selection_results: Option<SelectionResults>,
    #[serde(default)]
    #[serde(rename(deserialize = "resultTable"))]
    pub result_table: Option<RawResultTable>,
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

/// AggregationResult is the data structure for PQL aggregation result
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct AggregationResult {
    pub function: String,
    #[serde(default)]
    pub value: String,
    #[serde(default)]
    #[serde(rename(deserialize = "traceInfo"))]
    pub group_by_columns: Vec<String>,
    #[serde(default)]
    #[serde(rename(deserialize = "traceInfo"))]
    pub group_by_result: Vec<GroupValue>,
}

/// GroupValue is the data structure for PQL aggregation GroupBy result
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct GroupValue {
    pub value: String,
    pub group: Vec<String>,
}

/// SelectionResults is the data structure for PQL selection result
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct SelectionResults {
    columns: Vec<String>,
    results: Vec<Vec<Value>>,
}

impl SelectionResults {
    pub fn new(columns: Vec<String>, results: Vec<Vec<Value>>) -> Self {
        Self { columns, results }
    }

    /// Returns how many rows in the ResultTable
    pub fn get_results_count(&self) -> usize {
        self.results.len()
    }

    /// Returns how many columns in the ResultTable
    pub fn get_column_count(&self) -> usize {
        self.columns.len()
    }

    /// Returns column name given column index
    pub fn get_column_name(&self, column_index: usize) -> Result<&str> {
        self.columns.get(column_index)
            .map(|column| column.as_str())
            .ok_or_else(|| Error::InvalidResultColumnIndex(column_index))
    }

    /// Returns a row given a row index
    pub fn get_row(&self, row_index: usize) -> Result<&Vec<Value>> {
        self.results.get(row_index)
            .ok_or_else(|| Error::InvalidResultRowIndex(row_index))
    }

    /// Returns a json `Value` entry given row index and column index
    pub fn get_data(&self, row_index: usize, column_index: usize) -> Result<&Value> {
        self.get_row(row_index)?.get(column_index)
            .ok_or_else(|| Error::InvalidResultColumnIndex(column_index))
    }
}

/// ResultTable is the holder for SQL queries.
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct RawResultTable {
    #[serde(rename(deserialize = "dataSchema"))]
    pub data_schema: RawRespSchema,
    pub rows: Vec<Vec<Value>>,
}

/// RespSchema is a response schema as returned by pinot
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct RawRespSchema {
    #[serde(rename(deserialize = "columnDataTypes"))]
    pub column_data_types: Vec<DataType>,
    #[serde(rename(deserialize = "columnNames"))]
    pub column_names: Vec<String>,
}

#[cfg(test)]
pub mod tests {
    use serde_json::json;
    use crate::response::{
        DataType::Double as DubT,
        DataType::Long as LngT,
        DataType::String as StrT,
    };
    use crate::response::tests::{test_broker_response_error_msg, test_error_containing_broker_response};
    use crate::tests::to_string_vec;

    use super::*;

    #[test]
    fn pql_broker_response_deserializes_pql_aggregation_query_correctly() {
        let json: Value = json!({
            "selectionResults": {
                "columns": ["cnt", "extra"],
                "results": [[97889, json!({"a": "b"})]]
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
        });
        let broker_response: RawBrokerResponse = serde_json::from_value(json).unwrap();

        assert_eq!(broker_response, RawBrokerResponse {
            aggregation_results: vec![],
            selection_results: Some(SelectionResults::new(
                to_string_vec(vec!["cnt", "extra"]),
                vec![vec![json!(97889), json!({"a": "b"})]],
            )),
            result_table: None,
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
        });
    }

    #[test]
    fn pql_broker_response_deserializes_exception_correctly() {
        let error_message = test_broker_response_error_msg();
        let json = test_error_containing_broker_response(&error_message);
        let broker_response: RawBrokerResponse = serde_json::from_value(json).unwrap();

        assert_eq!(broker_response, RawBrokerResponse {
            aggregation_results: vec![],
            selection_results: None,
            result_table: None,
            exceptions: vec![Exception {
                error_code: 200,
                message: error_message,
            }],
            trace_info: Default::default(),
            num_servers_queried: 1,
            num_servers_responded: 1,
            num_segments_queried: 12,
            num_segments_processed: 0,
            num_segments_matched: 0,
            num_consuming_segments_queried: 0,
            num_docs_scanned: 0,
            num_entries_scanned_in_filter: 0,
            num_entries_scanned_post_filter: 0,
            num_groups_limit_reached: false,
            total_docs: 97889,
            time_used_ms: 5,
            min_consuming_freshness_time_ms: 0,
        });
    }

    #[test]
    fn selection_results_get_row_count_provides_correct_number_of_rows() {
        assert_eq!(test_selection_results().get_results_count(), 1);
    }

    #[test]
    fn selection_results_get_column_count_provides_correct_number_of_columns() {
        assert_eq!(test_selection_results().get_column_count(), 2);
    }

    #[test]
    fn selection_results_get_column_name_provides_correct_name() {
        assert_eq!(test_selection_results().get_column_name(1).unwrap(), "extra");
    }

    #[test]
    fn selection_results_get_column_name_returns_error_for_out_of_bounds() {
        match test_selection_results().get_column_name(3).unwrap_err() {
            Error::InvalidResultColumnIndex(index) => assert_eq!(index, 3),
            _ => panic!("Incorrect error kind"),
        }
    }

    #[test]
    fn selection_results_get_row_provides_correct_row() {
        assert_eq!(
            test_selection_results().get_row(0).unwrap(),
            &vec![json!(48547), json!({"a": "b"})]
        );
    }

    #[test]
    fn selection_results_get_row_returns_error_for_out_of_bounds() {
        match test_selection_results().get_row(1).unwrap_err() {
            Error::InvalidResultRowIndex(index) => assert_eq!(index, 1),
            _ => panic!("Incorrect error kind"),
        }
    }

    #[test]
    fn selection_results_get_data_returns_error_for_out_of_bounds() {
        match test_selection_results().get_data(1, 0).unwrap_err() {
            Error::InvalidResultRowIndex(index) => assert_eq!(index, 1),
            _ => panic!("Incorrect error kind"),
        }
        match test_selection_results().get_data(0, 2).unwrap_err() {
            Error::InvalidResultColumnIndex(index) => assert_eq!(index, 2),
            _ => panic!("Incorrect error kind"),
        }
    }

    #[test]
    fn selection_results_get_data_provides_correct_data() {
        assert_eq!(test_selection_results().get_data(0, 0).unwrap(), &json!(48547));
    }

    #[test]
    fn sql_broker_response_deserializes_sql_aggregation_query_correctly() {
        let json: Value = json!({
            "resultTable": {
                "dataSchema": {
                    "columnDataTypes": ["LONG"],
                    "columnNames": ["cnt"]
                },
                "rows": [[97889]]
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
        });
        let broker_response: RawBrokerResponse = serde_json::from_value(json).unwrap();

        assert_eq!(broker_response, RawBrokerResponse {
            aggregation_results: vec![],
            selection_results: None,
            result_table: Some(RawResultTable {
                data_schema: RawRespSchema {
                    column_data_types: vec![LngT],
                    column_names: to_string_vec(vec!["cnt"]),
                },
                rows: vec![vec![json!(97889)]],
            }),
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
        });
    }

    #[test]
    fn sql_broker_response_deserializes_aggregation_group_by_response_correctly() {
        let json: Value = json!({
            "resultTable": {
                "dataSchema": {
                    "columnDataTypes": ["STRING","LONG","DOUBLE"],
                    "columnNames":["teamID","cnt","sum_homeRuns"]
                },
                "rows": [
                    ["ANA",337,1324.0],
                    ["BL2",197,136.0],
                    ["ARI",727,2715.0],
                    ["BL1",48,24.0],
                    ["ALT",17,2.0],
                    ["ATL",1951,7312.0],
                    ["BFN",122,105.0],
                    ["BL3",36,32.0],
                    ["BFP",26,20.0],
                    ["BAL",2380,9164.0]
                ]
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
            "numEntriesScannedPostFilter": 195778,
            "numGroupsLimitReached": true,
            "totalDocs": 97889,
            "timeUsedMs": 24,
            "segmentStatistics": [],
            "traceInfo": {},
            "minConsumingFreshnessTimeMs": 0
        });
        let broker_response: RawBrokerResponse = serde_json::from_value(json).unwrap();

        assert_eq!(broker_response, RawBrokerResponse {
            aggregation_results: vec![],
            selection_results: None,
            result_table: Some(RawResultTable {
                data_schema: RawRespSchema {
                    column_data_types: vec![StrT, LngT, DubT],
                    column_names: to_string_vec(vec!["teamID", "cnt", "sum_homeRuns"]),
                },
                rows: vec![
                    vec![json!("ANA"), json!(337), json!(1324.0)],
                    vec![json!("BL2"), json!(197), json!(136.0)],
                    vec![json!("ARI"), json!(727), json!(2715.0)],
                    vec![json!("BL1"), json!(48), json!(24.0)],
                    vec![json!("ALT"), json!(17), json!(2.0)],
                    vec![json!("ATL"), json!(1951), json!(7312.0)],
                    vec![json!("BFN"), json!(122), json!(105.0)],
                    vec![json!("BL3"), json!(36), json!(32.0)],
                    vec![json!("BFP"), json!(26), json!(20.0)],
                    vec![json!("BAL"), json!(2380), json!(9164.0)],
                ],
            }),
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
            num_entries_scanned_post_filter: 195778,
            num_groups_limit_reached: true,
            total_docs: 97889,
            time_used_ms: 24,
            min_consuming_freshness_time_ms: 0,
        });
    }

    #[test]
    fn sql_broker_response_deserializes_exception_correctly() {
        let error_message = test_broker_response_error_msg();
        let json = test_error_containing_broker_response(&error_message);
        let broker_response: RawBrokerResponse = serde_json::from_value(json).unwrap();

        assert_eq!(broker_response, RawBrokerResponse {
            aggregation_results: vec![],
            selection_results: None,
            result_table: None,
            exceptions: vec![Exception {
                error_code: 200,
                message: error_message,
            }],
            trace_info: Default::default(),
            num_servers_queried: 1,
            num_servers_responded: 1,
            num_segments_queried: 12,
            num_segments_processed: 0,
            num_segments_matched: 0,
            num_consuming_segments_queried: 0,
            num_docs_scanned: 0,
            num_entries_scanned_in_filter: 0,
            num_entries_scanned_post_filter: 0,
            num_groups_limit_reached: false,
            total_docs: 97889,
            time_used_ms: 5,
            min_consuming_freshness_time_ms: 0,
        });
    }

    pub fn test_selection_results() -> SelectionResults {
        SelectionResults::new(
            to_string_vec(vec!["cnt", "extra"]),
            vec![vec![json!(48547), json!({"a": "b"})]],
        )
    }
}

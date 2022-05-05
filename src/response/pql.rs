use crate::response::raw::{AggregationResult, RawBrokerResponse, SelectionResults};
use crate::response::ResponseStats;

use super::Exception;

/// PqlBrokerResponse is the data structure for broker response to a PQL query.
#[derive(Clone, Debug, PartialEq)]
pub struct PqlBrokerResponse {
    pub aggregation_results: Vec<AggregationResult>,
    pub selection_results: Option<SelectionResults>,
    pub exceptions: Vec<Exception>,
    pub stats: ResponseStats,
}

impl From<RawBrokerResponse> for PqlBrokerResponse {
    fn from(raw: RawBrokerResponse) -> Self {
        PqlBrokerResponse {
            aggregation_results: raw.aggregation_results,
            selection_results: raw.selection_results,
            exceptions: raw.exceptions,
            stats: ResponseStats {
                trace_info: raw.trace_info,
                num_servers_queried: raw.num_servers_queried,
                num_servers_responded: raw.num_servers_responded,
                num_segments_queried: raw.num_segments_queried,
                num_segments_processed: raw.num_segments_processed,
                num_segments_matched: raw.num_segments_matched,
                num_consuming_segments_queried: raw.num_consuming_segments_queried,
                num_docs_scanned: raw.num_docs_scanned,
                num_entries_scanned_in_filter: raw.num_entries_scanned_in_filter,
                num_entries_scanned_post_filter: raw.num_entries_scanned_post_filter,
                num_groups_limit_reached: raw.num_groups_limit_reached,
                total_docs: raw.total_docs,
                time_used_ms: raw.time_used_ms,
                min_consuming_freshness_time_ms: raw.min_consuming_freshness_time_ms,
            },
        }
    }
}

#[cfg(test)]
pub mod tests {
    use serde_json::json;

    use crate::response::pql::SelectionResults;
    use crate::tests::to_string_vec;

    use super::*;

    #[test]
    fn pql_broker_response_deserializes_pql_aggregation_query_correctly() {
        let raw_broker_response = RawBrokerResponse {
            aggregation_results: vec![AggregationResult {
                function: "sort".to_string(),
                value: "1".to_string(),
                group_by_columns: vec![],
                group_by_result: vec![],
            }],
            selection_results: Some(SelectionResults::new(
                to_string_vec(vec!["cnt", "extra"]),
                vec![vec![json!(97889), json!({"a": "b"})]],
            )),
            result_table: None,
            exceptions: vec![Exception { error_code: 0, message: "msg".to_string() }],
            trace_info: Default::default(),
            num_servers_queried: 1,
            num_servers_responded: 2,
            num_segments_queried: 3,
            num_segments_processed: 4,
            num_segments_matched: 5,
            num_consuming_segments_queried: 6,
            num_docs_scanned: 7,
            num_entries_scanned_in_filter: 8,
            num_entries_scanned_post_filter: 9,
            num_groups_limit_reached: true,
            total_docs: 10,
            time_used_ms: 11,
            min_consuming_freshness_time_ms: 12,
        };
        let broker_response: PqlBrokerResponse = raw_broker_response.into();

        assert_eq!(broker_response, PqlBrokerResponse {
            aggregation_results: vec![AggregationResult {
                function: "sort".to_string(),
                value: "1".to_string(),
                group_by_columns: vec![],
                group_by_result: vec![],
            }],
            selection_results: Some(SelectionResults::new(
                to_string_vec(vec!["cnt", "extra"]),
                vec![vec![json!(97889), json!({"a": "b"})]],
            )),
            exceptions: vec![Exception { error_code: 0, message: "msg".to_string() }],
            stats: ResponseStats {
                trace_info: Default::default(),
                num_servers_queried: 1,
                num_servers_responded: 2,
                num_segments_queried: 3,
                num_segments_processed: 4,
                num_segments_matched: 5,
                num_consuming_segments_queried: 6,
                num_docs_scanned: 7,
                num_entries_scanned_in_filter: 8,
                num_entries_scanned_post_filter: 9,
                num_groups_limit_reached: true,
                total_docs: 10,
                time_used_ms: 11,
                min_consuming_freshness_time_ms: 12,
            },
        });
    }
}
use serde::{Deserialize, Serialize};

use crate::{Error, Result};
use crate::response::raw::{AggregationResult, RawBrokerResponse, RawBrokerResponseWithoutStats, SelectionResults};
use crate::response::ResponseStats;

/// Data structure for broker response to a PQL query.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct PqlResponse {
    pub aggregation_results: Vec<AggregationResult>,
    pub selection_results: Option<SelectionResults>,
    pub stats: Option<ResponseStats>,
}

impl From<RawBrokerResponse> for Result<PqlResponse> {
    fn from(raw: RawBrokerResponse) -> Self {
        if !raw.exceptions.is_empty() {
            return Err(Error::PinotExceptions(raw.exceptions));
        };
        Ok(PqlResponse {
            aggregation_results: raw.aggregation_results,
            selection_results: raw.selection_results,
            stats: Some(ResponseStats {
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
            }),
        })
    }
}

impl From<RawBrokerResponseWithoutStats> for Result<PqlResponse> {
    fn from(raw: RawBrokerResponseWithoutStats) -> Self {
        if !raw.exceptions.is_empty() {
            return Err(Error::PinotExceptions(raw.exceptions));
        };
        Ok(PqlResponse {
            aggregation_results: raw.aggregation_results,
            selection_results: raw.selection_results,
            stats: None,
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use serde_json::json;
    use crate::response::PinotException;

    use crate::response::pql::SelectionResults;
    use crate::tests::to_string_vec;

    use super::*;

    #[test]
    fn pql_response_deserializes_pql_aggregation_query_correctly() {
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
            exceptions: vec![],
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
        let broker_response: Result<PqlResponse> = Result::from(raw_broker_response);
        assert_eq!(broker_response.unwrap(), PqlResponse {
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
            stats: Some(ResponseStats {
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
            }),
        });
    }

    #[test]
    fn pql_response_deserializes_exceptions_correctly() {
        let raw_broker_response = RawBrokerResponse {
            aggregation_results: vec![],
            selection_results: None,
            result_table: None,
            exceptions: vec![PinotException { error_code: 0, message: "msg".to_string() }],
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
        let broker_response: Result<PqlResponse> = Result::from(raw_broker_response);
        match broker_response.unwrap_err() {
            Error::PinotExceptions(exceptions) => assert_eq!(
                exceptions, vec![PinotException { error_code: 0, message: "msg".to_string() }]),
            _ => panic!("Wrong variant")
        };
    }

    #[test]
    fn pql_response_deserializes_pql_aggregation_query_without_stats_correctly() {
        let raw_broker_response = RawBrokerResponseWithoutStats {
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
            exceptions: vec![],
        };
        let broker_response: Result<PqlResponse> = Result::from(raw_broker_response);
        assert_eq!(broker_response.unwrap(), PqlResponse {
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
            stats: None,
        });
    }

    #[test]
    fn pql_response_deserializes_exceptions_without_stats_correctly() {
        let raw_broker_response = RawBrokerResponseWithoutStats {
            aggregation_results: vec![],
            selection_results: None,
            result_table: None,
            exceptions: vec![PinotException { error_code: 0, message: "msg".to_string() }],
        };
        let broker_response: Result<PqlResponse> = Result::from(raw_broker_response);
        match broker_response.unwrap_err() {
            Error::PinotExceptions(exceptions) => assert_eq!(
                exceptions, vec![PinotException { error_code: 0, message: "msg".to_string() }]),
            _ => panic!("Wrong variant")
        };
    }
}
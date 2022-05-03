use serde_json::json;

use crate::client_transport::ClientTransport;
use crate::errors::{Error, Result};
use crate::request::{QueryFormat, Request};
use crate::response::pql::PqlBrokerResponse;
use crate::response::sql::{FromRow, SqlBrokerResponse};

/// An asynchronous json implementation of clientTransport
pub struct JsonHttpClientTransport {
    client: reqwest::blocking::Client,
    header: http::HeaderMap,
}

impl JsonHttpClientTransport {
    pub fn new(client: reqwest::blocking::Client, header: http::HeaderMap) -> Self {
        Self { client, header }
    }

    #[cfg(test)]
    pub fn header(&self) -> &http::HeaderMap { &self.header }
}

impl ClientTransport for JsonHttpClientTransport {
    fn execute_sql<T: FromRow>(
        &self, broker_address: &str, query: &str,
    ) -> Result<SqlBrokerResponse<T>> {
        let query = Request::new(QueryFormat::SQL, query);
        let response = execute_blocking_http_request(
            broker_address, &query, &self.header, &self.client,
        )?;
        let broker_response: SqlBrokerResponse<T> = response.json()
            .map_err(|e| Error::FailedRequest(query, e))?;
        Ok(broker_response)
    }

    fn execute_pql(&self, broker_address: &str, query: &str) -> Result<PqlBrokerResponse> {
        let query = Request::new(QueryFormat::PQL, query);
        let response = execute_blocking_http_request(
            broker_address, &query, &self.header, &self.client,
        )?;
        let broker_response: PqlBrokerResponse = response.json()
            .map_err(|e| Error::FailedRequest(query, e))?;
        Ok(broker_response)
    }
}

fn execute_blocking_http_request(
    broker_address: &str,
    query: &Request,
    header: &http::HeaderMap,
    client: &reqwest::blocking::Client,
) -> Result<reqwest::blocking::Response> {
    let url = encode_query_address(broker_address, &query.query_format);
    let json_value = json!(&query);
    let extra_headers = header.clone();
    let request = create_post_json_http_request(url, json_value, extra_headers, client)
        .map_err(|e| Error::InvalidRequest(query.clone(), e))?;
    let response = client.execute(request)
        .map_err(|e| Error::FailedRequest(query.clone(), e))?;

    if response.status() == reqwest::StatusCode::OK {
        Ok(response)
    } else {
        Err(Error::InvalidResponse(response))
    }
}

fn encode_query_address(broker_address: &str, query_format: &QueryFormat) -> String {
    let query_address = match query_format {
        QueryFormat::PQL => format!("{}/query", broker_address),
        QueryFormat::SQL => format!("{}/query/sql", broker_address),
    };
    if !broker_address.starts_with("http://") && !broker_address.starts_with("https://") {
        format!("http://{}", query_address)
    } else {
        query_address
    }
}

fn create_post_json_http_request(
    url: String,
    json_value: serde_json::Value,
    extra_headers: http::HeaderMap,
    client: &reqwest::blocking::Client,
) -> reqwest::Result<reqwest::blocking::Request> {
    client
        .post(url)
        .json(&json_value)
        .headers(extra_headers)
        .build()
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::iter::FromIterator;

    use chrono::{DateTime, Utc};
    use http::HeaderValue;
    use reqwest::header::HeaderMap;
    use serde_json::Value;

    use crate::connection::tests::test_broker_localhost_8099;
    use crate::response::data::{Data, DataRow};
    use crate::response::pql::SelectionResults;
    use crate::response::tests::{test_broker_response_json, test_sql_broker_response};
    use crate::tests::{date_time_utc, to_string_vec};

    use super::*;

    #[test]
    fn encode_query_address_encodes_pql_without_http_prefix() {
        let broker_address = "localhost:8000";
        assert_eq!(
            encode_query_address(broker_address, &QueryFormat::PQL),
            "http://localhost:8000/query"
        );
    }

    #[test]
    fn encode_query_address_encodes_sql_without_http_prefix() {
        let broker_address = "localhost:8000";
        assert_eq!(
            encode_query_address(broker_address, &QueryFormat::SQL),
            "http://localhost:8000/query/sql"
        );
    }

    #[test]
    fn encode_query_address_encodes_pql_with_http_prefix() {
        let broker_address = "http://localhost:8000";
        assert_eq!(
            encode_query_address(broker_address, &QueryFormat::PQL),
            "http://localhost:8000/query"
        );
    }

    #[test]
    fn encode_query_address_encodes_sql_with_http_prefix() {
        let broker_address = "http://localhost:8000";
        assert_eq!(
            encode_query_address(broker_address, &QueryFormat::SQL),
            "http://localhost:8000/query/sql"
        );
    }

    #[test]
    fn encode_query_address_encodes_pql_with_https_prefix() {
        let broker_address = "https://localhost:8000";
        assert_eq!(
            encode_query_address(broker_address, &QueryFormat::PQL),
            "https://localhost:8000/query"
        );
    }

    #[test]
    fn encode_query_address_encodes_sql_with_https_prefix() {
        let broker_address = "https://localhost:8000";
        assert_eq!(
            encode_query_address(broker_address, &QueryFormat::SQL),
            "https://localhost:8000/query/sql"
        );
    }

    #[test]
    fn create_http_request_creates_for_valid_address() {
        let mut header_map: HeaderMap = HeaderMap::new();
        header_map.insert("a", HeaderValue::from_str("b").unwrap());
        let url = "https://localhost:8000/";
        let json_value = json!({"sql": "SELECT * FROM baseball_stats"});
        let request = create_post_json_http_request(
            url.to_string(),
            json_value.clone(),
            header_map,
            &reqwest::blocking::Client::new(),
        ).unwrap();
        assert_eq!(request.url().as_str(), url);
        let captured: Value = serde_json::from_slice(request.body().unwrap().as_bytes().unwrap()).unwrap();
        assert_eq!(
            captured,
            json_value
        );
        assert_eq!(request.headers().len(), 2);
        assert_eq!(request.headers().get("a"), Some(&HeaderValue::from_str("b").unwrap()));
        assert_eq!(
            request.headers().get(http::header::CONTENT_TYPE),
            Some(&HeaderValue::from_str("application/json").unwrap())
        );
    }

    #[test]
    fn execute_select_returns_sql_data() {
        let transport = JsonHttpClientTransport::new(
            reqwest::blocking::Client::new(),
            HeaderMap::new(),
        );
        let response: SqlBrokerResponse<DataRow> = transport.execute_sql(
            &test_broker_localhost_8099(), "SELECT * FROM scoreSheet",
        ).unwrap();
        let result_table = response.result_table.unwrap();

        let expected = test_database_score_sheet_data_rows_by_name();
        assert_eq!(result_table.get_row_count(), 2);
        assert_eq!(result_table.get_schema().get_column_count(), 10);
        for row_index in 0..2 {
            let name_column_index = result_table.get_schema().get_column_index("name").unwrap();
            let row_name = result_table.get_row(row_index).unwrap().get_string(name_column_index).unwrap();
            let expected_row = expected.get(row_name).unwrap();
            for column_index in 0..10 {
                let col_name = result_table.get_schema().get_column_name(column_index).unwrap();
                let col_type = result_table.get_schema().get_column_data_type(column_index).unwrap();
                let expected_data = expected_row.get(col_name).unwrap();
                let data = result_table.get_row(row_index).unwrap().get_data(column_index).unwrap();
                assert_eq!(data.data_type(), col_type);
                assert_eq!(data, expected_data);
            }
        }
    }

    #[test]
    fn execute_select_returns_pql_data() {
        let transport = JsonHttpClientTransport::new(
            reqwest::blocking::Client::new(),
            HeaderMap::new(),
        );
        let response = transport.execute_pql(
            &test_broker_localhost_8099(), "SELECT * FROM scoreSheet",
        ).unwrap();
        let results = response.selection_results.unwrap();
        assert_eq!(results, SelectionResults::new(
            to_string_vec(vec![
                "age", "avgScore", "avgScore_highPrecision", "dateOfBirth", "dateOfFirstGame",
                "extra", "hasPlayed", "name", "raw", "totalScore",
            ]),
            vec![
                vec![
                    json!("10"), json!("5.0"), json!("5.1"), json!("2011-01-01 00:00:00.0"),
                    json!("1356998400000"), json!("{\"a\":\"b\"}"), json!("true"), json!("A"),
                    json!("ab"), json!("11"),
                ],
                vec![
                    json!("30"), json!("0.0"), json!("0.0"), json!("1991-01-01 00:00:00.0"),
                    json!("1420070400000"), json!("{}"), json!("false"), json!("B"),
                    json!(""), json!("0"),
                ],
            ],
        ))
    }

    #[test]
    fn execute_sql_with_malformed_host_returns_error() {
        let transport = JsonHttpClientTransport::new(
            reqwest::blocking::Client::new(),
            HeaderMap::new(),
        );
        let query = "SELECT * FROM baseball_stats";
        let request = Request::new(QueryFormat::SQL, query);
        let error = transport.execute_sql::<DataRow>("localhost:abcd", query).unwrap_err();
        match error {
            Error::InvalidRequest(captured_request, _) => assert_eq!(captured_request, request),
            _ => panic!("Incorrect error kind"),
        }
    }

    #[test]
    fn execute_pql_with_malformed_host_returns_error() {
        let transport = JsonHttpClientTransport::new(
            reqwest::blocking::Client::new(),
            HeaderMap::new(),
        );
        let query = "SELECT * FROM baseball_stats";
        let request = Request::new(QueryFormat::PQL, query);
        let error = transport.execute_pql("localhost:abcd", query).unwrap_err();
        match error {
            Error::InvalidRequest(captured_request, _) => assert_eq!(captured_request, request),
            _ => panic!("Incorrect error kind"),
        }
    }

    #[test]
    fn execute_sql_with_unknown_host_returns_error() {
        let transport = JsonHttpClientTransport::new(
            reqwest::blocking::Client::new(),
            HeaderMap::new(),
        );
        let query = "SELECT * FROM baseball_stats";
        let request = Request::new(QueryFormat::SQL, query);
        let error = transport.execute_sql::<DataRow>("unknownhost:8000", query).unwrap_err();
        match error {
            Error::FailedRequest(captured_request, _) => assert_eq!(captured_request, request),
            _ => panic!("Incorrect error kind"),
        }
    }

    #[test]
    fn execute_pql_with_unknown_host_returns_error() {
        let transport = JsonHttpClientTransport::new(
            reqwest::blocking::Client::new(),
            HeaderMap::new(),
        );
        let query = "SELECT * FROM baseball_stats";
        let request = Request::new(QueryFormat::PQL, query);
        let error = transport.execute_pql("unknownhost:8000", query).unwrap_err();
        match error {
            Error::FailedRequest(captured_request, _) => assert_eq!(captured_request, request),
            _ => panic!("Incorrect error kind"),
        }
    }

    #[test]
    pub fn decode_response_decodes_valid_response() {
        let json: Value = test_broker_response_json();
        let response_bytes = serde_json::to_vec(&json).unwrap();
        let broker_response: SqlBrokerResponse<DataRow> = serde_json::from_slice(&response_bytes)
            .unwrap();
        assert_eq!(broker_response, test_sql_broker_response());
    }

    fn test_database_score_sheet_data_rows_by_name() -> HashMap<String, HashMap<String, Data>> {
        HashMap::from_iter(vec![
            ("A".to_string(), HashMap::from_iter(vec![
                ("name".to_string(), Data::String("A".to_string())),
                ("age".to_string(), Data::Int(10)),
                ("hasPlayed".to_string(), Data::Boolean(true)),
                ("dateOfBirth".to_string(), Data::Timestamp(date_time_2011_01_01t00_00_00z())),
                ("totalScore".to_string(), Data::Long(11)),
                ("avgScore".to_string(), Data::Float(5.0)),
                ("avgScore_highPrecision".to_string(), Data::Double(5.1)),
                ("dateOfFirstGame".to_string(), Data::Long(1356998400000)),
                ("extra".to_string(), Data::Json(json!({"a": "b"}))),
                ("raw".to_string(), Data::Bytes(vec![171])),
            ])),
            ("B".to_string(), HashMap::from_iter(vec![
                ("name".to_string(), Data::String("B".to_string())),
                ("age".to_string(), Data::Int(30)),
                ("hasPlayed".to_string(), Data::Boolean(false)),
                ("dateOfBirth".to_string(), Data::Timestamp(date_time_1991_01_01t00_00_00z())),
                ("totalScore".to_string(), Data::Long(0)),
                ("avgScore".to_string(), Data::Float(0.0)),
                ("avgScore_highPrecision".to_string(), Data::Double(0.0)),
                ("dateOfFirstGame".to_string(), Data::Long(1420070400000)),
                ("extra".to_string(), Data::Json(json!({}))),
                ("raw".to_string(), Data::Bytes(vec![])),
            ])),
        ])
    }

    fn date_time_2011_01_01t00_00_00z() -> DateTime<Utc> {
        date_time_utc(2011, 1, 1, 0, 0, 0)
    }

    fn date_time_1991_01_01t00_00_00z() -> DateTime<Utc> {
        date_time_utc(1991, 1, 1, 0, 0, 0)
    }
}
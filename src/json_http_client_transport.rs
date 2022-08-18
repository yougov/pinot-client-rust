use serde_json::json;

use crate::client_transport::ClientTransport;
use crate::errors::{Error, Result};
use crate::request::{encode_query_address, QueryFormat, Request};
use crate::response::pql::PqlResponse;
use crate::response::raw::{RawBrokerResponse, RawBrokerResponseWithoutStats};
use crate::response::sql::{FromRow, SqlResponse};

/// An asynchronous json implementation of clientTransport
#[derive(Clone, Debug)]
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
        &self, broker_address: &str, query: &str, include_stats: bool,
    ) -> Result<SqlResponse<T>> {
        let query = Request::new(QueryFormat::SQL, query);
        let response = execute_blocking_http_request(
            broker_address, &query, &self.header, &self.client,
        )?;
        if include_stats {
            let raw_broker_response: RawBrokerResponse = response.json()
                .map_err(|e| Error::FailedRequest(query, e))?;
            Result::from(raw_broker_response)
        } else {
            let raw_broker_response: RawBrokerResponseWithoutStats = response.json()
                .map_err(|e| Error::FailedRequest(query, e))?;
            Result::from(raw_broker_response)
        }
    }

    fn execute_pql(
        &self, broker_address: &str, query: &str, include_stats: bool,
    ) -> Result<PqlResponse> {
        let query = Request::new(QueryFormat::PQL, query);
        let response = execute_blocking_http_request(
            broker_address, &query, &self.header, &self.client,
        )?;
        if include_stats {
            let raw_broker_response: RawBrokerResponse = response.json()
                .map_err(|e| Error::FailedRequest(query, e))?;
            Result::from(raw_broker_response)
        } else {
            let raw_broker_response: RawBrokerResponseWithoutStats = response.json()
                .map_err(|e| Error::FailedRequest(query, e))?;
            Result::from(raw_broker_response)
        }
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
        Err(Error::InvalidBlockingResponse(response))
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
    use chrono::{DateTime, Utc};
    use http::HeaderValue;
    use reqwest::header::HeaderMap;
    use serde_json::Value;

    use crate::connection::tests::test_broker_localhost_8099;
    use crate::response::data::{Data, DataRow};
    use crate::response::DataType;
    use crate::response::raw::{RawSchema, SelectionResults};
    use crate::response::sql::{Schema, Table};
    use crate::tests::{date_time_utc, to_string_vec};

    use super::*;

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
        let response: SqlResponse<DataRow> = transport.execute_sql(
            &test_broker_localhost_8099(), "SELECT * FROM scoreSheet", true,
        ).unwrap();
        let table = response.table.unwrap();

        assert_eq!(table, Table::new(
            Schema::from(RawSchema {
                column_data_types: vec![
                    DataType::Int, DataType::Float, DataType::Double, DataType::Timestamp,
                    DataType::Long, DataType::Json, DataType::IntArray, DataType::FloatArray,
                    DataType::DoubleArray, DataType::String, DataType::Boolean, DataType::StringArray,
                    DataType::Bytes, DataType::LongArray, DataType::Long,
                ],
                column_names: to_string_vec(vec![
                    "age", "avgScore", "avgScore_highPrecision", "dateOfBirth", "dateOfFirstGame",
                    "extra", "gameIds", "handicapAdjustedScores", "handicapAdjustedScores_highPrecision",
                    "handle", "hasPlayed", "names", "raw", "scores", "totalScore",
                ]),
            }),
            vec![
                DataRow::new(vec![
                    Data::Int(10), Data::Float(3.6), Data::Double(3.66),
                    Data::Timestamp(date_time_2011_01_01t00_00_00z()),
                    Data::Long(1577875528000), Data::Json(json!({"a": "b"})),
                    Data::IntArray(vec![1, 2, 3]), Data::FloatArray(vec![2.1, 4.9, 3.2]),
                    Data::DoubleArray(vec![2.15, 4.99, 3.21]), Data::String("Gladiator".to_string()),
                    Data::Boolean(true), Data::StringArray(to_string_vec(vec!["James", "Smith"])),
                    Data::Bytes(vec![171]), Data::LongArray(vec![3, 6, 2]), Data::Long(11),
                ]),
                DataRow::new(vec![
                    Data::Int(30), Data::Float(f32::MIN), Data::Double(f64::MIN),
                    Data::Timestamp(date_time_1991_01_01t00_00_00z()),
                    Data::Long(1420070400001), Data::Null(DataType::Json),
                    Data::IntArray(vec![i32::MIN]), Data::FloatArray(vec![f32::MIN]),
                    Data::DoubleArray(vec![f64::MIN]), Data::String("Thrumbar".to_string()),
                    Data::Boolean(false), Data::StringArray(to_string_vec(vec!["Giles", "Richie"])),
                    Data::Bytes(vec![]), Data::LongArray(vec![i64::MIN]), Data::Long(0),
                ]),
            ],
        ))
    }

    #[test]
    fn execute_select_returns_pql_data() {
        let transport = JsonHttpClientTransport::new(
            reqwest::blocking::Client::new(),
            HeaderMap::new(),
        );
        let response = transport.execute_pql(
            &test_broker_localhost_8099(), "SELECT * FROM scoreSheet", true,
        ).unwrap();
        let results = response.selection_results.unwrap();
        assert_eq!(results, SelectionResults::new(
            to_string_vec(vec![
                "age", "avgScore", "avgScore_highPrecision", "dateOfBirth", "dateOfFirstGame",
                "extra", "gameIds", "handicapAdjustedScores", "handicapAdjustedScores_highPrecision",
                "handle", "hasPlayed", "names", "raw", "scores", "totalScore",
            ]),
            vec![
                vec![
                    json!("10"), json!("3.6"), json!("3.66"), json!("2011-01-01 00:00:00.0"),
                    json!("1577875528000"), json!("{\"a\":\"b\"}"), json!(["1", "2", "3"]),
                    json!(["2.1", "4.9", "3.2"]), json!(["2.15", "4.99", "3.21"]), json!("Gladiator"),
                    json!("true"), json!(["James", "Smith"]), json!("ab"), json!(["3", "6", "2"]),
                    json!("11"),
                ],
                vec![
                    json!("30"), json!("-∞"), json!("-∞"), json!("1991-01-01 00:00:00.0"),
                    json!("1420070400001"), json!("null"), json!(["-2147483648"]), json!(["-∞"]),
                    json!(["-∞"]), json!("Thrumbar"), json!("false"), json!(["Giles", "Richie"]),
                    json!(""), json!(["-9223372036854775808"]), json!("0"),
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
        let error = transport.execute_sql::<DataRow>("localhost:abcd", query, true).unwrap_err();
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
        let error = transport.execute_pql("localhost:abcd", query, true).unwrap_err();
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
        let error = transport.execute_sql::<DataRow>("unknownhost:8000", query, true).unwrap_err();
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
        let error = transport.execute_pql("unknownhost:8000", query, true).unwrap_err();
        match error {
            Error::FailedRequest(captured_request, _) => assert_eq!(captured_request, request),
            _ => panic!("Incorrect error kind"),
        }
    }

    fn date_time_2011_01_01t00_00_00z() -> DateTime<Utc> {
        date_time_utc(2011, 1, 1, 0, 0, 0)
    }

    fn date_time_1991_01_01t00_00_00z() -> DateTime<Utc> {
        date_time_utc(1991, 1, 1, 0, 0, 0)
    }
}
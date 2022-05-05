use serde_json::json;

use crate::client_transport::ClientTransport;
use crate::errors::{Error, Result};
use crate::request::{QueryFormat, Request};
use crate::response::BrokerResponse;

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
    fn execute(&self, broker_address: &str, query: Request) -> Result<BrokerResponse> {
        let url = encode_query_address(broker_address, &query.query_format);
        let json_value = json!(&query);
        let extra_headers = self.header.clone();
        let request = create_post_json_http_request(url, json_value, extra_headers, &self.client)
            .map_err(|e| Error::InvalidRequest(query.clone(), e))?;
        let response = self.client.execute(request)
            .map_err(|e| Error::FailedRequest(query.clone(), e))?;

        if response.status() == reqwest::StatusCode::OK {
            let broker_response: BrokerResponse = response.json()
                .map_err(|e| Error::FailedRequest(query, e))?;
            Ok(broker_response)
        } else {
            Err(Error::InvalidResponse(response))
        }
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
    use chrono::{DateTime, Utc};
    use http::HeaderValue;
    use reqwest::header::HeaderMap;
    use serde_json::Value;

    use crate::connection::tests::test_broker_localhost_8099;
    use crate::response::{Data, DataRows, DataType, RawRespSchema, RespSchema, ResultTable, SelectionResults};
    use crate::response::tests::{test_broker_response, test_broker_response_json};
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
        let response = transport.execute(
            &test_broker_localhost_8099(),
            Request::new(QueryFormat::SQL, "SELECT * FROM scoreSheet"),
        ).unwrap();
        let result_table = response.result_table.unwrap();

        assert_eq!(result_table, ResultTable::new(
            RespSchema::from(RawRespSchema {
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
            DataRows::new(vec![
                vec![
                    Data::Int(10), Data::Float(3.6), Data::Double(3.66),
                    Data::Timestamp(date_time_2011_01_01t00_00_00z()),
                    Data::Long(1577875528000), Data::Json(json!({"a": "b"})),
                    Data::IntArray(vec![1, 2, 3]), Data::FloatArray(vec![2.1, 4.9, 3.2]),
                    Data::DoubleArray(vec![2.15, 4.99, 3.21]), Data::String("Gladiator".to_string()),
                    Data::Boolean(true), Data::StringArray(to_string_vec(vec!["James", "Smith"])),
                    Data::Bytes(vec![171]), Data::LongArray(vec![3, 6, 2]), Data::Long(11),
                ],
                vec![
                    Data::Int(30), Data::Float(f32::MIN), Data::Double(f64::MIN),
                    Data::Timestamp(date_time_1991_01_01t00_00_00z()),
                    Data::Long(1420070400001), Data::Null(DataType::Json),
                    Data::IntArray(vec![i32::MIN]), Data::FloatArray(vec![f32::MIN]),
                    Data::DoubleArray(vec![f64::MIN]), Data::String("Thrumbar".to_string()),
                    Data::Boolean(false), Data::StringArray(to_string_vec(vec!["Giles", "Richie"])),
                    Data::Bytes(vec![]), Data::LongArray(vec![i64::MIN]), Data::Long(0),
                ],
            ]),
        ))
    }

    #[test]
    fn execute_select_returns_pql_data() {
        let transport = JsonHttpClientTransport::new(
            reqwest::blocking::Client::new(),
            HeaderMap::new(),
        );
        let response = transport.execute(
            &test_broker_localhost_8099(),
            Request::new(QueryFormat::PQL, "SELECT * FROM scoreSheet"),
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
    fn execute_with_malformed_host_returns_error() {
        let transport = JsonHttpClientTransport::new(
            reqwest::blocking::Client::new(),
            HeaderMap::new(),
        );
        let request = Request::new(QueryFormat::SQL, "SELECT * FROM baseball_stats");
        let error = transport.execute("localhost:abcd", request.clone()).unwrap_err();
        match error {
            Error::InvalidRequest(captured_request, _) => assert_eq!(captured_request, request),
            _ => panic!("Incorrect error kind"),
        }
    }

    #[test]
    fn execute_with_unknown_host_returns_error() {
        let transport = JsonHttpClientTransport::new(
            reqwest::blocking::Client::new(),
            HeaderMap::new(),
        );
        let request = Request::new(QueryFormat::SQL, "SELECT * FROM baseball_stats");
        let error = transport.execute("unknownhost:8000", request.clone()).unwrap_err();
        match error {
            Error::FailedRequest(captured_request, _) => assert_eq!(captured_request, request),
            _ => panic!("Incorrect error kind"),
        }
    }

    #[test]
    pub fn decode_response_decodes_valid_response() {
        let json: Value = test_broker_response_json();
        let response_bytes = serde_json::to_vec(&json).unwrap();
        let broker_response: BrokerResponse = serde_json::from_slice(&response_bytes).unwrap();
        assert_eq!(broker_response, test_broker_response());
    }

    fn date_time_2011_01_01t00_00_00z() -> DateTime<Utc> {
        date_time_utc(2011, 1, 1, 0, 0, 0)
    }

    fn date_time_1991_01_01t00_00_00z() -> DateTime<Utc> {
        date_time_utc(1991, 1, 1, 0, 0, 0)
    }
}
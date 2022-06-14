use std::fmt;

use serde::ser::SerializeMap;

const QUERY_OPTIONS: &str = "queryOptions";
const QUERY_OPTIONS_SQL_VALUE: &str = "groupByMode=sql;responseFormat=sql";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryFormat {
    PQL,
    SQL,
}

impl fmt::Display for QueryFormat {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            QueryFormat::PQL => write!(f, "pql"),
            QueryFormat::SQL => write!(f, "sql"),
        }
    }
}

/// Request is used in server request to host multiple pinot query types, like PQL, SQL.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Request {
    pub query_format: QueryFormat,
    pub query: String,
}

impl Request {
    pub fn new(query_format: QueryFormat, query: &str) -> Self {
        Self { query_format, query: query.to_string() }
    }
}

impl serde::Serialize for Request {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
    {
        let n = match self.query_format {
            QueryFormat::PQL => 1,
            QueryFormat::SQL => 2,
        };
        let mut map = serializer.serialize_map(Some(n))?;
        map.serialize_entry(
            self.query_format.to_string().as_str(),
            &self.query,
        )?;
        if self.query_format == QueryFormat::SQL {
            map.serialize_entry(
                QUERY_OPTIONS,
                QUERY_OPTIONS_SQL_VALUE,
            )?;
        }
        map.end()
    }
}

pub(crate) fn encode_query_address(broker_address: &str, query_format: &QueryFormat) -> String {
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

#[cfg(test)]
mod test {
    use serde_json::json;

    use super::*;

    #[test]
    fn query_format_displays_correctly() {
        assert_eq!(QueryFormat::PQL.to_string(), "pql".to_string());
        assert_eq!(QueryFormat::SQL.to_string(), "sql".to_string());
    }

    #[test]
    fn request_serialises_pql() {
        let request = Request::new(QueryFormat::PQL, "SELECT * FROM test");
        let result = json!(request);
        let expected = json!({
            "pql": "SELECT * FROM test",
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn request_serialises_sql() {
        let request = Request::new(QueryFormat::SQL, "SELECT * FROM test");
        let result = json!(request);
        let expected = json!({
            "sql": "SELECT * FROM test",
            QUERY_OPTIONS: QUERY_OPTIONS_SQL_VALUE,
        });
        assert_eq!(result, expected);
    }

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
}
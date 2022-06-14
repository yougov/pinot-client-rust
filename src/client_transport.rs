use crate::errors::Result;
use crate::response::PqlBrokerResponse;
use crate::response::sql::{FromRow, SqlBrokerResponse};

/// A client transport that communicates with a Pinot cluster
/// given a broker address
pub trait ClientTransport {
    /// Execute SQL
    fn execute_sql<T: FromRow>(
        &self, broker_address: &str, query: &str,
    ) -> Result<SqlBrokerResponse<T>>;

    /// Execute PQL
    fn execute_pql(
        &self, broker_address: &str, query: &str,
    ) -> Result<PqlBrokerResponse>;
}

#[cfg(test)]
pub mod tests {
    use serde_json::Value;
    use crate::response::raw::RawBrokerResponse;

    use super::*;

    pub struct TestClientTransport {
        sql_return_function: Box<dyn Fn(&str, &str) -> Result<Value>>,
        pql_return_function: Box<dyn Fn(&str, &str) -> Result<PqlBrokerResponse>>,
    }

    impl TestClientTransport {
        pub fn new<SqlFn, PqlFn>(sql_return_function: SqlFn, pql_return_function: PqlFn) -> Self
            where
                SqlFn: 'static + Fn(&str, &str) -> Result<Value>,
                PqlFn: 'static + Fn(&str, &str) -> Result<PqlBrokerResponse>,
        {
            Self {
                sql_return_function: Box::new(sql_return_function),
                pql_return_function: Box::new(pql_return_function),
            }
        }
    }

    impl ClientTransport for TestClientTransport {
        fn execute_sql<T: FromRow>(
            &self, broker_address: &str, query: &str,
        ) -> Result<SqlBrokerResponse<T>> {
            let json: Value = (self.sql_return_function)(broker_address, query)?;
            let raw_broker_response: RawBrokerResponse = serde_json::from_value(json)?;
            Result::from(raw_broker_response)
        }

        fn execute_pql(&self, broker_address: &str, query: &str) -> Result<PqlBrokerResponse> {
            (self.pql_return_function)(broker_address, query)
        }
    }
}
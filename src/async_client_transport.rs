#![cfg(feature = "async")]

use std::fmt::Debug;

use async_trait::async_trait;

use crate::errors::Result;
use crate::response::PqlResponse;
use crate::response::sql::{FromRow, SqlResponse};

/// An asynchronous client transport that communicates
/// with a Pinot cluster given a broker address
#[async_trait]
pub trait AsyncClientTransport: Clone + Debug {
    /// Execute SQL asynchronously
    async fn execute_sql<T: FromRow>(
        &self, broker_address: &str, query: &str, include_stats: bool,
    ) -> Result<SqlResponse<T>>;

    /// Execute PQL asynchronously
    async fn execute_pql(
        &self, broker_address: &str, query: &str, include_stats: bool,
    ) -> Result<PqlResponse>;
}

#[cfg(test)]
pub(crate) mod tests {
    use std::fmt::Formatter;
    use std::sync::{Arc, Mutex};

    use serde_json::Value;

    use crate::response::raw::RawBrokerResponse;

    use super::*;

    #[derive(Clone)]
    pub struct TestAsyncClientTransport {
        sql_return_function: Arc<Mutex<Box<dyn Fn(&str, &str) -> Result<Value> + Send>>>,
        pql_return_function: Arc<Mutex<Box<dyn Fn(&str, &str) -> Result<PqlResponse> + Send>>>,
    }

    impl Debug for TestAsyncClientTransport {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "TestAsyncClientTransport")
        }
    }

    impl TestAsyncClientTransport {
        pub fn new<SqlFn, PqlFn>(sql_return_function: SqlFn, pql_return_function: PqlFn) -> Self
            where
                SqlFn: 'static + Fn(&str, &str) -> Result<Value> + Send,
                PqlFn: 'static + Fn(&str, &str) -> Result<PqlResponse> + Send,
        {
            Self {
                sql_return_function: Arc::new(Mutex::new(Box::new(sql_return_function))),
                pql_return_function: Arc::new(Mutex::new(Box::new(pql_return_function))),
            }
        }
    }

    #[async_trait]
    impl AsyncClientTransport for TestAsyncClientTransport {
        async fn execute_sql<T: FromRow>(
            &self, broker_address: &str, query: &str, _include_stats: bool,
        ) -> Result<SqlResponse<T>> {
            let json: Value = (self.sql_return_function.lock().unwrap())(broker_address, query)?;
            let raw_broker_response: RawBrokerResponse = serde_json::from_value(json)?;
            Result::from(raw_broker_response)
        }

        async fn execute_pql(
            &self, broker_address: &str, query: &str, _include_stats: bool,
        ) -> Result<PqlResponse> {
            (self.pql_return_function.lock().unwrap())(broker_address, query)
        }
    }
}
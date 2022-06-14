#![cfg(feature = "async")]

use async_trait::async_trait;

use crate::errors::Result;
use crate::response::PqlBrokerResponse;
use crate::response::sql::{FromRow, SqlBrokerResponse};

#[async_trait]
pub trait AsyncClientTransport {
    async fn execute_sql<T: FromRow>(
        &self, broker_address: &str, query: &str,
    ) -> Result<SqlBrokerResponse<T>>;
    async fn execute_pql(
        &self, broker_address: &str, query: &str,
    ) -> Result<PqlBrokerResponse>;
}

#[cfg(test)]
pub mod tests {
    use std::sync::Mutex;

    use serde_json::Value;

    use crate::response::raw::RawBrokerResponse;

    use super::*;

    pub struct TestAsyncClientTransport {
        sql_return_function: Mutex<Box<dyn Fn(&str, &str) -> Result<Value> + Send>>,
        pql_return_function: Mutex<Box<dyn Fn(&str, &str) -> Result<PqlBrokerResponse> + Send>>,
    }

    impl TestAsyncClientTransport {
        pub fn new<SqlFn, PqlFn>(sql_return_function: SqlFn, pql_return_function: PqlFn) -> Self
            where
                SqlFn: 'static + Fn(&str, &str) -> Result<Value> + Send,
                PqlFn: 'static + Fn(&str, &str) -> Result<PqlBrokerResponse> + Send,
        {
            Self {
                sql_return_function: Mutex::new(Box::new(sql_return_function)),
                pql_return_function: Mutex::new(Box::new(pql_return_function)),
            }
        }
    }

    #[async_trait]
    impl AsyncClientTransport for TestAsyncClientTransport {
        async fn execute_sql<T: FromRow>(
            &self, broker_address: &str, query: &str,
        ) -> Result<SqlBrokerResponse<T>> {
            let json: Value = (self.sql_return_function.lock().unwrap())(broker_address, query)?;
            let raw_broker_response: RawBrokerResponse = serde_json::from_value(json)?;
            Result::from(raw_broker_response)
        }

        async fn execute_pql(&self, broker_address: &str, query: &str) -> Result<PqlBrokerResponse> {
            (self.pql_return_function.lock().unwrap())(broker_address, query)
        }
    }
}
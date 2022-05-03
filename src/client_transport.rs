use crate::errors::Result;
use crate::response::PqlBrokerResponse;
use crate::response::sql::{FromRow, SqlBrokerResponse};

pub trait ClientTransport {
    fn execute_sql<T: FromRow>(
        &self, broker_address: &str, query: &str,
    ) -> Result<SqlBrokerResponse<T>>;
    fn execute_pql(
        &self, broker_address: &str, query: &str,
    ) -> Result<PqlBrokerResponse>;
}

#[cfg(test)]
pub mod tests {
    use serde_json::Value;

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
            let result: SqlBrokerResponse<T> = serde_json::from_value(json)?;
            Ok(result)
        }

        fn execute_pql(&self, broker_address: &str, query: &str) -> Result<PqlBrokerResponse> {
            (self.pql_return_function)(broker_address, query)
        }
    }
}
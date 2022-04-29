use std::sync::Arc;
use std::time::Duration;

use http::HeaderMap;

use crate::broker_selector::BrokerSelector;
use crate::client_transport::ClientTransport;
use crate::dynamic_broker_selector::{auto_refreshing_dynamic_broker_selector, DynamicBrokerSelector};
use crate::errors::Result;
use crate::external_view::format_external_view_zk_path;
use crate::json_http_client_transport::JsonHttpClientTransport;
use crate::request::{QueryFormat, Request};
use crate::response::BrokerResponse;
use crate::simple_broker_selector::SimpleBrokerSelector;
use crate::zookeeper::{connect_to_zookeeper, ZookeeperConfig};

/// Connection to Pinot
pub struct Connection<CT: ClientTransport, BS: BrokerSelector> {
    transport: CT,
    broker_selector: BS,
}

impl<CT: ClientTransport, BS: BrokerSelector> Connection<CT, BS> {
    pub fn new(transport: CT, broker_selector: BS) -> Self {
        Self { transport, broker_selector }
    }
}

/// ExecuteSQL for a given table
impl<CT: ClientTransport, BS: BrokerSelector> Connection<CT, BS> {
    pub fn execute_sql(&self, table: &str, query: &str) -> Result<BrokerResponse> {
        let broker_address = self.broker_selector.select_broker(table)?;
        self.transport.execute(&broker_address, Request::new(QueryFormat::SQL, query))
    }

    pub fn execute_pql(&self, table: &str, query: &str) -> Result<BrokerResponse> {
        let broker_address = self.broker_selector.select_broker(table)?;
        self.transport.execute(&broker_address, Request::new(QueryFormat::PQL, query))
    }
}

/// Create a new Pinot connection with pre configured Pinot Broker list.
pub fn client_from_broker_list(
    broker_list: Vec<String>,
    extra_http_header: Option<HeaderMap>,
) -> Result<Connection<JsonHttpClientTransport, SimpleBrokerSelector>> {
    let tansport = JsonHttpClientTransport::new(
        reqwest::blocking::Client::new(),
        extra_http_header.unwrap_or_else(|| HeaderMap::new()),
    );
    let broker_selector = SimpleBrokerSelector::new(broker_list)?;
    Ok(Connection::new(tansport, broker_selector))
}

/// Create a new Pinot connection through Pinot Zookeeper.
pub fn client_from_zookeeper(
    zk_config: &ZookeeperConfig,
    extra_http_header: Option<HeaderMap>,
) -> Result<Connection<JsonHttpClientTransport, Arc<DynamicBrokerSelector>>> {
    let tansport = JsonHttpClientTransport::new(
        reqwest::blocking::Client::new(),
        extra_http_header.unwrap_or_else(|| HeaderMap::new()),
    );

    let zk_conn = connect_to_zookeeper(zk_config)?;
    let external_view_zk_path = format_external_view_zk_path(zk_config);
    let dynamic_broker_selector = auto_refreshing_dynamic_broker_selector(
        zk_conn, external_view_zk_path, Duration::from_millis(100))?;
    Ok(Connection::new(tansport, dynamic_broker_selector))
}

#[cfg(test)]
pub mod tests {
    use http::HeaderValue;

    use crate::broker_selector::tests::TestBrokerSelector;
    use crate::client_transport::tests::TestClientTransport;
    use crate::response::tests::test_broker_response;
    use crate::zookeeper::test::test_pinot_cluster_zookeeper_config;

    use super::*;

    #[test]
    fn execute_sql_calls_broker_selector_and_client_transport_correctly() {
        let conn = Connection::new(
            TestClientTransport::new(|broker_address, query| {
                assert_eq!(broker_address, "localhost:8099");
                assert_eq!(query, Request::new(QueryFormat::SQL, "SELECT * FROM table"));
                Ok(test_broker_response())
            }),
            TestBrokerSelector::new(|table| {
                assert_eq!(table, "table");
                Ok(test_broker_addresses().remove(0))
            }),
        );

        let broker_response = conn.execute_sql("table", "SELECT * FROM table").unwrap();
        assert_eq!(broker_response, test_broker_response());
    }

    #[test]
    fn execute_pql_calls_broker_selector_and_client_transport_correctly() {
        let conn = Connection::new(
            TestClientTransport::new(|broker_address, query| {
                assert_eq!(broker_address, "localhost:8099");
                assert_eq!(query, Request::new(QueryFormat::PQL, "SELECT * FROM table"));
                Ok(test_broker_response())
            }),
            TestBrokerSelector::new(|table| {
                assert_eq!(table, "table");
                Ok(test_broker_addresses().remove(0))
            }),
        );

        let broker_response = conn.execute_pql("table", "SELECT * FROM table").unwrap();
        assert_eq!(broker_response, test_broker_response());
    }

    #[test]
    fn clients_from_broker_list_provides_header_to_transport() {
        let mut header_map = HeaderMap::new();
        header_map.insert("a", HeaderValue::from_str("b").unwrap());
        let conn = client_from_broker_list(
            test_broker_addresses(),
            Some(header_map),
        ).unwrap();
        let headers = conn.transport.header();
        assert_eq!(headers.len(), 1);
        assert!(headers.contains_key("a"));
        assert_eq!(headers["a"], "b");
    }

    #[test]
    fn clients_from_broker_list_provides_empty_header_if_non_provided() {
        let conn = client_from_broker_list(
            test_broker_addresses(),
            None,
        ).unwrap();
        let headers = conn.transport.header();
        assert_eq!(headers.len(), 0);
    }

    #[test]
    fn clients_from_zookeeper_provides_header_to_transport() {
        let mut header_map = HeaderMap::new();
        header_map.insert("a", HeaderValue::from_str("b").unwrap());
        let conn = client_from_zookeeper(
            &test_pinot_cluster_zookeeper_config(),
            Some(header_map),
        ).unwrap();
        let headers = conn.transport.header();
        assert_eq!(headers.len(), 1);
        assert!(headers.contains_key("a"));
        assert_eq!(headers["a"], "b");
    }

    #[test]
    fn clients_from_zookeeper_provides_empty_header_if_non_provided() {
        let conn = client_from_zookeeper(
            &test_pinot_cluster_zookeeper_config(),
            None,
        ).unwrap();
        let headers = conn.transport.header();
        assert_eq!(headers.len(), 0);
    }

    pub fn test_broker_addresses() -> Vec<String> {
        vec![test_broker_localhost_8099()]
    }

    pub fn test_broker_localhost_8099() -> String {
        "localhost:8099".to_string()
    }
}
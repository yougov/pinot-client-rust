use std::collections::HashMap;

use serde::Deserialize;
use serde_json;
use zookeeper::ZooKeeper;

use crate::zookeeper::{read_zookeeper_node, ZookeeperConfig};

use super::errors::{Error, Result};

const BROKER_EXTERNAL_VIEW_PATH: &str = "EXTERNALVIEW/brokerResource";

/// A representation of the external view of the cluster
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct ExternalView {
    pub id: String,
    #[serde(rename(deserialize = "simpleFields"))]
    pub simple_fields: HashMap<String, String>,
    #[serde(rename(deserialize = "mapFields"))]
    pub map_fields: HashMap<String, HashMap<String, String>>,
    #[serde(rename(deserialize = "listFields"))]
    pub list_fields: HashMap<String, Vec<String>>,
}

/// Format the external view path given a zookeeper config
pub fn format_external_view_zk_path(zk_config: &ZookeeperConfig) -> String {
    format!("{}/{}", &zk_config.path_prefix, BROKER_EXTERNAL_VIEW_PATH)
}

/// Retrieve the external view given its address
pub fn get_external_view(
    zk_conn: &ZooKeeper, external_view_zk_path: &str,
) -> Result<ExternalView> {
    let node = read_zookeeper_node(zk_conn, external_view_zk_path)?;
    let external_view = decode_external_view(&node)?;
    Ok(external_view)
}

fn decode_external_view(external_view_bytes: &[u8]) -> Result<ExternalView> {
    let result: ExternalView = serde_json::from_slice(external_view_bytes)
        .map_err(Error::FailedJsonDeserialization)?;
    Ok(result)
}


#[cfg(test)]
mod test {
    use std::iter::FromIterator;

    use serde_json::json;
    use serde_json::Value;

    use crate::zookeeper::test::{test_pinot_cluster_zookeeper_config, test_zookeeper_connection};

    use super::*;

    #[test]
    fn get_external_view_fetches_specified_external_view() {
        let zk_conn = test_zookeeper_connection();
        let external_view_zk_path = format_external_view_zk_path(&test_pinot_cluster_zookeeper_config());
        let external_view = get_external_view(&zk_conn, &external_view_zk_path).unwrap();
        assert_eq!(external_view.id, "brokerResource".to_string());
    }

    #[test]
    fn decode_external_view_decodes_valid_external_view() {
        let json: Value = json!({
            "id": "brokerResource",
            "simpleFields": {
                "BATCH_MESSAGE_MODE": "false",
                "BUCKET_SIZE": "0",
                "IDEAL_STATE_MODE": "CUSTOMIZED",
                "NUM_PARTITIONS": "1",
                "REBALANCE_MODE": "CUSTOMIZED",
                "REPLICAS": "0",
                "STATE_MODEL_DEF_REF": "BrokerResourceOnlineOfflineStateModel",
                "STATE_MODEL_FACTORY_NAME": "DEFAULT"
            },
            "mapFields": {
                "baseballStats_OFFLINE": {
                    "Broker_127.0.0.1_8000": "ONLINE",
                    "Broker_127.0.0.1_9000": "ONLINE"
                }
            },
            "listFields": {}
        });
        let external_view_bytes = serde_json::to_vec(&json).unwrap();
        let external_view: ExternalView = decode_external_view(&external_view_bytes).unwrap();

        assert_eq!(external_view, ExternalView {
            id: "brokerResource".to_string(),
            simple_fields: HashMap::from_iter(vec![
                ("BATCH_MESSAGE_MODE".to_string(), "false".to_string()),
                ("BUCKET_SIZE".to_string(), "0".to_string()),
                ("IDEAL_STATE_MODE".to_string(), "CUSTOMIZED".to_string()),
                ("NUM_PARTITIONS".to_string(), "1".to_string()),
                ("REBALANCE_MODE".to_string(), "CUSTOMIZED".to_string()),
                ("REPLICAS".to_string(), "0".to_string()),
                ("STATE_MODEL_DEF_REF".to_string(), "BrokerResourceOnlineOfflineStateModel".to_string()),
                ("STATE_MODEL_FACTORY_NAME".to_string(), "DEFAULT".to_string()),
            ]),
            map_fields: HashMap::from_iter(vec![
                ("baseballStats_OFFLINE".to_string(), HashMap::from_iter(vec![
                    ("Broker_127.0.0.1_8000".to_string(), "ONLINE".to_string()),
                    ("Broker_127.0.0.1_9000".to_string(), "ONLINE".to_string()),
                ])),
            ]),
            list_fields: Default::default(),
        });
    }
}

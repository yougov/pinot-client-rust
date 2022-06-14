use std::time::Duration;

use log::error;
use zookeeper::{WatchedEvent, Watcher, ZooKeeper};

use crate::errors::Error;

use super::errors::Result;

const DEFAULT_ZK_SESSION_TIME_OUT_SEC: u64 = 60;

/// ZookeeperConfig describes how to config Pinot Zookeeper connection
pub struct ZookeeperConfig {
    /// List of host:port pairs, each corresponding to a zk server
    pub zookeeper_path: Vec<String>,
    pub path_prefix: String,
    pub session_timeout_sec: u64,
}

impl ZookeeperConfig {
    pub fn with_timeout(
        zookeeper_path: Vec<String>,
        path_prefix: String,
        session_timeout_sec: u64,
    ) -> Self {
        Self { zookeeper_path, path_prefix, session_timeout_sec }
    }

    pub fn new(zookeeper_path: Vec<String>, path_prefix: String) -> Self {
        Self { zookeeper_path, path_prefix, session_timeout_sec: DEFAULT_ZK_SESSION_TIME_OUT_SEC }
    }

    /// Provide comma separated host:port pairs string
    pub fn connect_string(&self) -> String {
        self.zookeeper_path.join(",")
    }
}

/// Establish a connection with zookeeper
pub fn connect_to_zookeeper(zk_config: &ZookeeperConfig) -> Result<ZooKeeper> {
    let zk_conn = ZooKeeper::connect(
        &zk_config.connect_string(),
        Duration::from_secs(zk_config.session_timeout_sec),
        |_| {},
    )?;
    Ok(zk_conn)
}

/// Read a zookeeper node
pub fn read_zookeeper_node(zk_conn: &ZooKeeper, path: &str) -> Result<Vec<u8>> {
    let (node, _) = zk_conn.get_data(path, false)?;
    Ok(node)
}

/// Set up a node watcher which reacts to changes on a node
pub fn set_up_node_watcher<W: 'static + Watcher>(
    zk_conn: &ZooKeeper,
    path: &str,
    watcher: W,
) -> Result<()> {
    zk_conn.get_data_w(path, watcher)
        .map(|_| {})
        .map_err(
            |e| Error::FailedExternalViewWatcher(path.to_string(), e)
        )
}

/// Create node watcher that logs errors and ignores them
pub fn log_and_discard_error_node_watcher(
    name: &str, on_event: impl Fn(WatchedEvent) -> Result<()>,
) -> impl Fn(WatchedEvent) {
    let name = name.to_string();
    move |event: WatchedEvent| {
        if let Err(e) = on_event(event) {
            error!("Error returned on zookeeper node watcher '{}': {:?}", name, e);
        }
    }
}

/// Call a provided function when the status of the node has changed
pub fn on_node_status_changed(
    on_event: impl Fn(WatchedEvent) -> Result<()>,
) -> impl Fn(WatchedEvent) -> Result<()> {
    move |event: WatchedEvent| {
        match event.event_type {
            zookeeper::WatchedEventType::NodeDataChanged => on_event(event),
            _ => Ok(())
        }
    }
}

#[cfg(test)]
pub mod test {
    use std::sync::{Arc, RwLock};

    use uuid::Uuid;

    use crate::dynamic_broker_selector::DynamicBrokerSelectorError;
    use crate::external_view::{ExternalView, format_external_view_zk_path};

    use super::*;

    #[test]
    fn connect_to_zookeeper_connects() {
        let result = connect_to_zookeeper(&test_pinot_cluster_zookeeper_config());
        assert!(result.is_ok());
    }

    #[test]
    fn read_zookeeper_node_can_read_external_view() {
        let zk_conn = test_zookeeper_connection();
        let external_view_zk_path = format_external_view_zk_path(&test_pinot_cluster_zookeeper_config());
        let external_view_bytes = read_zookeeper_node(&zk_conn, &external_view_zk_path).unwrap();
        let external_view: ExternalView = serde_json::from_slice(&external_view_bytes).unwrap();
        assert_eq!(external_view.id, "brokerResource".to_string());
    }

    #[test]
    fn set_up_node_watcher_reacts_to_node_change() {
        let zk_conn = test_zookeeper_connection();
        let root_path = ensure_root_level_unit_test_node_exists(&zk_conn);
        let path = format!("{}/{}", root_path, Uuid::new_v4());
        let state: Arc<RwLock<Vec<WatchedEvent>>> = Arc::new(RwLock::new(vec![]));
        let copy = state.clone();

        zk_conn.create(
            &path,
            vec![1, 2, 3],
            zookeeper::Acl::open_unsafe().to_vec(),
            zookeeper::CreateMode::Ephemeral,
        ).unwrap();
        set_up_node_watcher(&zk_conn, &path, move |event: WatchedEvent| {
            copy.write().unwrap().push(event)
        }).unwrap();
        zk_conn.set_data(&path, vec![4, 5, 6], None).unwrap();
        std::thread::sleep(Duration::from_secs(1));

        let events = state.read().unwrap();
        let event: &WatchedEvent = events.get(0).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(event.path, Some(path));
        assert_eq!(event.keeper_state, zookeeper::KeeperState::SyncConnected);
        match event.event_type {
            zookeeper::WatchedEventType::NodeDataChanged => {}
            _ => panic!("Incorrect event"),
        }
    }

    #[test]
    fn log_and_discard_error_node_watcher_ignores_error() {
        let func = log_and_discard_error_node_watcher(
            "test", |_| Err(Error::NoAvailableBroker));
        func(zookeeper::WatchedEvent {
            event_type: zookeeper::WatchedEventType::None,
            keeper_state: zookeeper::KeeperState::Disconnected,
            path: None,
        })
    }

    #[test]
    fn on_node_status_changed_calls_on_status_changed() {
        let state_changed = Arc::new(RwLock::new(false));
        let copy = state_changed.clone();
        let on_event = on_node_status_changed(|_| {
            *copy.write().unwrap() = true;
            Ok(())
        });
        assert!(on_event(WatchedEvent {
            event_type: zookeeper::WatchedEventType::NodeDataChanged,
            keeper_state: zookeeper::KeeperState::Disconnected,
            path: None,
        }).is_ok());
        assert!(*state_changed.read().unwrap());
    }

    #[test]
    fn on_node_status_changed_ignores_on_status_changed() {
        let state_changed = Arc::new(RwLock::new(false));
        let copy = state_changed.clone();
        let on_event = on_node_status_changed(|_| {
            *copy.write().unwrap() = true;
            Ok(())
        });
        assert!(on_event(WatchedEvent {
            event_type: zookeeper::WatchedEventType::None,
            keeper_state: zookeeper::KeeperState::Disconnected,
            path: None,
        }).is_ok());
        assert!(!*state_changed.read().unwrap());
    }

    #[test]
    fn on_node_status_changed_returns_error() {
        let on_event = on_node_status_changed(|_| Err(
            DynamicBrokerSelectorError::PoisonedConcurrentAllBrokerListWrite.into()
        ));
        assert!(on_event(WatchedEvent {
            event_type: zookeeper::WatchedEventType::NodeDataChanged,
            keeper_state: zookeeper::KeeperState::Disconnected,
            path: None,
        }).is_err());
    }

    pub fn root_level_unit_test_node_path() -> String {
        "/UnitTests".to_string()
    }

    pub fn ensure_root_level_unit_test_node_exists(zk_conn: &ZooKeeper) -> String {
        let path = root_level_unit_test_node_path();
        if zk_conn.exists(&path, false).unwrap().is_none() {
            match zk_conn.create(
                &path,
                vec![1, 2, 3],
                zookeeper::Acl::open_unsafe().to_vec(),
                zookeeper::CreateMode::Persistent,
            ) {
                Ok(_) => {}
                Err(zookeeper::ZkError::NodeExists) => {}
                Err(e) => panic!("{}", e),
            }
        }
        path
    }

    pub fn test_zookeeper_connection() -> ZooKeeper {
        connect_to_zookeeper(&test_pinot_cluster_zookeeper_config())
            .expect("Could not connect to test zookeeper instance")
    }

    pub fn test_pinot_cluster_zookeeper_config() -> ZookeeperConfig {
        ZookeeperConfig::new(
            vec!["localhost:2181".to_string()],
            "/PinotCluster".to_string(),
        )
    }
}

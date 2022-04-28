use std::collections::HashMap;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::sync::mpsc::{channel, Receiver, RecvError, Sender, SendError};
use std::thread::JoinHandle;

use zookeeper::ZooKeeper;

use crate::broker_selector::BrokerSelector;
use crate::errors::{Error, log_error, Result};
use crate::external_view::{ExternalView, get_external_view};
use crate::rand::clone_random_element;
use crate::zookeeper::{log_and_discard_error_node_watcher, on_node_status_changed, set_up_node_watcher};

const OFFLINE_SUFFIX: &str = "_OFFLINE";
const REALTIME_SUFFIX: &str = "_REALTIME";

pub type TableBrokerMap = HashMap<String, Vec<String>>;
pub type AllBrokerList = Vec<String>;

/// Dynamic broker error.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DynamicBrokerSelectorError {
    /// Failed send refresh event
    #[error("Failed to send refresh event: {0}")]
    FailedSendRefreshEvent(#[from] SendError<()>),

    /// Failed receive refresh event
    #[error("Failed to receive refresh event: {0}")]
    FailedReceiveRefreshEvent(#[from] RecvError),

    /// Poisoned concurrent read of all broker list.
    #[error("Poisoned read of all broker list")]
    PoisonedConcurrentAllBrokerListRead,

    /// Poisoned concurrent write of all broker list.
    #[error("Poisoned write of all broker list")]
    PoisonedConcurrentAllBrokerListWrite,

    /// Poisoned concurrent read of table broker map.
    #[error("Poisoned read of table broker map")]
    PoisonedConcurrentTableBrokerMapRead,

    /// Poisoned concurrent write of table broker map.
    #[error("Poisoned write of table broker map")]
    PoisonedConcurrentTableBrokerMapWrite,
}

pub fn auto_refreshing_dynamic_broker_selector(
    zk_conn: ZooKeeper,
    external_view_zk_path: String,
    receiver_timeout: std::time::Duration,
) -> Result<Arc<DynamicBrokerSelector>> {
    let external_view = get_external_view(&zk_conn, &external_view_zk_path)?;
    let dynamic_broker_selector = Arc::new(DynamicBrokerSelector::new(&external_view));
    let (tx, rx): (Sender<()>, Receiver<()>) = channel();
    set_up_dynamic_broker_selector_external_view_watcher(&zk_conn, &external_view_zk_path, tx)?;
    set_up_dynamic_broker_selector_external_view_refresher(
        zk_conn,
        external_view_zk_path,
        dynamic_broker_selector.clone(),
        receiver_timeout,
        rx,
    );
    Ok(dynamic_broker_selector)
}

fn set_up_dynamic_broker_selector_external_view_watcher(
    zk_conn: &ZooKeeper,
    external_view_zk_path: &str,
    tx: Sender<()>,
) -> Result<()> {
    set_up_node_watcher(
        &zk_conn,
        external_view_zk_path,
        log_and_discard_error_node_watcher(
            "Refresh dynamic broker external view",
            on_node_status_changed(
                move |_| tx.send(()).map_err(|e|
                    DynamicBrokerSelectorError::FailedSendRefreshEvent(e).into()
                ),
            ),
        ),
    )
}

fn set_up_dynamic_broker_selector_external_view_refresher(
    zk_conn: ZooKeeper,
    external_view_zk_path: String,
    dynamic_broker_selector: Arc<DynamicBrokerSelector>,
    timeout: std::time::Duration,
    rx: Receiver<()>,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        looping_receiver("Refresh dynamic broker selector", timeout, rx, |_| {
            let external_view = get_external_view(&zk_conn, &external_view_zk_path)?;
            dynamic_broker_selector.refresh_external_view(&external_view)?;
            Ok(())
        })
    })
}

fn looping_receiver<T>(
    name: &str,
    timeout: std::time::Duration,
    rx: Receiver<T>,
    func: impl Fn(T) -> Result<()>,
) {
    let msg = format!("Encountered error while handling received event from '{}'", name);
    loop {
        log_error(&msg, || {
            let event: T = rx.recv()
                .map_err(|e| DynamicBrokerSelectorError::FailedReceiveRefreshEvent(e))?;
            func(event)?;
            Ok(())
        });
        std::thread::sleep(timeout);
    }
}

pub struct DynamicBrokerSelector {
    table_broker_map: RwLock<TableBrokerMap>,
    all_broker_list: RwLock<AllBrokerList>,
}

impl DynamicBrokerSelector {
    pub fn new(
        external_view: &ExternalView,
    ) -> Self {
        let (table_broker_map, all_broker_list) =
            generate_new_broker_mapping_from_external_view(external_view);
        let table_broker_map: RwLock<TableBrokerMap> = RwLock::new(table_broker_map);
        let all_broker_list: RwLock<AllBrokerList> = RwLock::new(all_broker_list);
        Self { table_broker_map, all_broker_list }
    }

    pub fn refresh_external_view(
        &self,
        external_view: &ExternalView,
    ) -> Result<()> {
        let (new_table_broker_map, new_all_broker_list) =
            generate_new_broker_mapping_from_external_view(external_view);
        let mut table_broker_map = self.write_table_broker_map()?;
        let mut all_broker_list = self.write_all_broker_list()?;
        *table_broker_map = new_table_broker_map;
        *all_broker_list = new_all_broker_list;
        Ok(())
    }

    pub fn select_random_broker_by_table_name(&self, table_name: &str) -> Result<String> {
        match self.read_table_broker_map()?.get(table_name) {
            None => Err(Error::NoAvailableBrokerForTable(table_name.to_string())),
            Some(broker_list) => {
                let broker = clone_random_element(&broker_list);
                match broker {
                    None => Err(Error::NoAvailableBrokerForTable(table_name.to_string())),
                    Some(broker) => Ok(broker),
                }
            }
        }
    }

    pub fn select_random_broker(&self) -> Result<String> {
        let broker_list = self.read_all_broker_list()?;
        let broker = clone_random_element(&broker_list);
        match broker {
            None => Err(Error::NoAvailableBroker),
            Some(broker) => Ok(broker),
        }
    }

    fn read_all_broker_list(&self) -> Result<RwLockReadGuard<AllBrokerList>> {
        self.all_broker_list.read()
            .map_err(|_| DynamicBrokerSelectorError::PoisonedConcurrentAllBrokerListRead.into())
    }

    fn write_all_broker_list(&self) -> Result<RwLockWriteGuard<AllBrokerList>> {
        self.all_broker_list.write()
            .map_err(|_| DynamicBrokerSelectorError::PoisonedConcurrentAllBrokerListWrite.into())
    }

    fn read_table_broker_map(&self) -> Result<RwLockReadGuard<TableBrokerMap>> {
        self.table_broker_map.read()
            .map_err(|_| DynamicBrokerSelectorError::PoisonedConcurrentTableBrokerMapRead.into())
    }

    fn write_table_broker_map(&self) -> Result<RwLockWriteGuard<TableBrokerMap>> {
        self.table_broker_map.write()
            .map_err(|_| DynamicBrokerSelectorError::PoisonedConcurrentTableBrokerMapWrite.into())
    }
}

impl BrokerSelector for DynamicBrokerSelector {
    fn select_broker(&self, table: &str) -> Result<String> {
        let table_name = extract_table_name(table);
        if let Some(table_name) = table_name {
            self.select_random_broker_by_table_name(table_name.as_str())
        } else {
            self.select_random_broker()
        }
    }
}

impl BrokerSelector for Arc<DynamicBrokerSelector> {
    fn select_broker(&self, table: &str) -> Result<String> {
        let table_name = extract_table_name(table);
        if let Some(table_name) = table_name {
            self.select_random_broker_by_table_name(table_name.as_str())
        } else {
            self.select_random_broker()
        }
    }
}

fn generate_new_broker_mapping_from_external_view(
    external_view: &ExternalView
) -> (TableBrokerMap, AllBrokerList) {
    let n = external_view.map_fields.len();
    let mut table_broker_map: TableBrokerMap = TableBrokerMap::with_capacity(n);
    let mut all_broker_list: AllBrokerList = AllBrokerList::with_capacity(n);
    for (table, broker_map) in &external_view.map_fields {
        let table_name = extract_table_name(table);
        let brokers = extract_brokers(broker_map);
        all_broker_list.append(&mut brokers.clone());
        if let Some(table_name) = table_name {
            table_broker_map.insert(table_name, brokers);
        }
    }
    (table_broker_map, all_broker_list)
}

fn extract_table_name(table: &str) -> Option<String> {
    let raw_name = table
        .replacen(OFFLINE_SUFFIX, "", 1)
        .replacen(REALTIME_SUFFIX, "", 1);
    if raw_name.is_empty() {
        None
    } else {
        Some(raw_name)
    }
}

fn extract_brokers(broker_map: &HashMap<String, String>) -> Vec<String> {
    let mut brokers: Vec<String> = Vec::with_capacity(broker_map.len());
    for (broker_key, status) in broker_map {
        if status == "ONLINE" {
            let result = extract_broker_host_port(broker_key.as_str());
            if let Ok((host, port)) = result {
                brokers.push(format!("{}:{}", host, port));
            }
        }
    }
    brokers
}

fn extract_broker_host_port(broker_key: &str) -> Result<(String, u64)> {
    let splits: Vec<&str> = broker_key.split("_").collect();
    let last_index = splits.len() - 1;
    if last_index < 1 {
        return Err(Error::InvalidBrokerKey(broker_key.to_string()));
    }
    let host = splits[last_index - 1].to_string();
    let port_str = splits[last_index];
    let port = port_str
        .parse::<u64>()
        .map_err(|e| Error::InvalidBrokerPort(port_str.to_string(), e))?;
    Ok((host, port))
}

#[cfg(test)]
mod tests {
    use std::iter::FromIterator;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use crate::tests::{assert_iters_equal_anyorder, to_string_vec};

    use super::*;

    #[test]
    fn looping_receiver_calls_provided_function_upon_event() {
        let (tx, rx): (Sender<usize>, Receiver<usize>) = channel();
        let state = Arc::new(AtomicUsize::new(0));
        let copy = state.clone();
        std::thread::spawn(move || {
            looping_receiver(
                "test",
                Duration::from_millis(10),
                rx,
                |event| {
                    copy.fetch_add(event, Ordering::SeqCst);
                    Ok(())
                },
            )
        });
        tx.send(10).unwrap();
        std::thread::sleep(Duration::from_secs(1));
        assert_eq!(state.load(Ordering::SeqCst), 10);
    }

    #[test]
    fn looping_receiver_ignores_errors() {
        let (tx, rx): (Sender<usize>, Receiver<usize>) = channel();
        let state = Arc::new(AtomicUsize::new(0));
        let copy = state.clone();
        std::thread::spawn(move || {
            looping_receiver(
                "test",
                Duration::from_millis(10),
                rx,
                |v| {
                    if v > 5 {
                        Err(Error::NoAvailableBroker)
                    } else {
                        copy.fetch_add(v, Ordering::SeqCst);
                        Ok(())
                    }
                },
            )
        });
        tx.send(6).unwrap();
        std::thread::sleep(Duration::from_secs(1));
        tx.send(5).unwrap();
        std::thread::sleep(Duration::from_secs(1));
        assert_eq!(state.load(Ordering::SeqCst), 5);
    }

    #[test]
    fn extract_table_name_from_offline_table() {
        assert_eq!(Some("table".to_string()), extract_table_name("table_OFFLINE"));
    }

    #[test]
    fn extract_table_name_from_realtime_table() {
        assert_eq!(Some("table".to_string()), extract_table_name("table_REALTIME"));
    }

    #[test]
    fn extract_table_name_from_schema() {
        assert_eq!(Some("table".to_string()), extract_table_name("table"));
    }

    #[test]
    fn extract_table_name_returns_none_when_offline_table_has_no_name() {
        assert_eq!(None, extract_table_name("_OFFLINE"));
    }

    #[test]
    fn extract_table_name_returns_none_when_realtime_table_has_no_name() {
        assert_eq!(None, extract_table_name("_REALTIME"));
    }

    #[test]
    fn extract_table_name_returns_none_for_empty_str() {
        assert_eq!(None, extract_table_name(""));
    }

    #[test]
    fn extract_brokers_extracts_broker_addresses() {
        let broker_map: HashMap<String, String> = HashMap::from_iter(vec![
            ("BROKER_broker-1_1000".to_string(), "ONLINE".to_string()),
            ("BROKER_broker-2_1000".to_string(), "ONLINE".to_string()),
        ]);
        let brokers = extract_brokers(&broker_map);
        assert_eq!(brokers.len(), 2);
        assert!(brokers.contains(&"broker-1:1000".to_string()));
        assert!(brokers.contains(&"broker-2:1000".to_string()));
    }

    #[test]
    fn extract_broker_host_port_extracts_valid_address() {
        let (host, port) = extract_broker_host_port("BROKER_broker-1_1000").unwrap();
        assert_eq!(host, "broker-1".to_string());
        assert_eq!(port, 1000);
    }

    #[test]
    fn extract_broker_host_port_returns_error_for_invalid_broker_key() {
        let broker_key = "broker-1:1000";
        match extract_broker_host_port(broker_key).unwrap_err() {
            Error::InvalidBrokerKey(captured_key) => assert_eq!(
                captured_key, broker_key.to_string()),
            _ => panic!("Incorrect error kind"),
        }
    }

    #[test]
    fn extract_broker_host_port_returns_error_for_invalid_port() {
        match extract_broker_host_port("BROKER_broker-1_aaa").unwrap_err() {
            Error::InvalidBrokerPort(captured_port, _) => assert_eq!(
                captured_port, "aaa".to_string()),
            _ => panic!("Incorrect error kind"),
        }
    }

    #[test]
    fn generate_new_broker_mapping_from_external_view_extracts_all_and_table_mapped_brokers() {
        let external_view = ExternalView {
            id: "brokerResponse".to_string(),
            simple_fields: Default::default(),
            map_fields: HashMap::from_iter(vec![
                ("baseballStats_OFFLINE".to_string(), HashMap::from_iter(vec![
                    ("Broker_127.0.0.1_8000".to_string(), "ONLINE".to_string()),
                    ("Broker_127.0.0.1_9000".to_string(), "ONLINE".to_string()),
                ])),
                ("".to_string(), HashMap::from_iter(vec![
                    ("Broker_127.0.0.1_1000".to_string(), "ONLINE".to_string()),
                    ("Broker_127.0.0.1_1100".to_string(), "ONLINE".to_string()),
                ])),
            ]),
            list_fields: Default::default(),
        };

        let (
            table_broker_map,
            all_broker_list
        ) = generate_new_broker_mapping_from_external_view(&external_view);

        assert_eq!(table_broker_map.len(), 1);
        assert_iters_equal_anyorder(
            table_broker_map.get("baseballStats").unwrap().iter(),
            to_string_vec(vec!["127.0.0.1:8000", "127.0.0.1:9000"]).iter(),
        );
        assert_iters_equal_anyorder(
            all_broker_list.iter(),
            AllBrokerList::from_iter(to_string_vec(vec![
                "127.0.0.1:1000", "127.0.0.1:1100", "127.0.0.1:8000", "127.0.0.1:9000",
            ])).iter(),
        );
    }

    #[test]
    fn dynamic_broker_selector_refresh_external_view_replaces_old_map_contents() {
        let external_view = ExternalView {
            id: "brokerResponse".to_string(),
            simple_fields: Default::default(),
            map_fields: HashMap::from_iter(vec![
                ("baseballStats_OFFLINE".to_string(), HashMap::from_iter(vec![
                    ("Broker_127.0.0.1_8000".to_string(), "ONLINE".to_string()),
                ])),
                ("".to_string(), HashMap::from_iter(vec![
                    ("Broker_127.0.0.1_1000".to_string(), "ONLINE".to_string()),
                ])),
            ]),
            list_fields: Default::default(),
        };
        let broker = DynamicBrokerSelector::new(&external_view);

        assert_eq!(*broker.table_broker_map.write().unwrap(), TableBrokerMap::from_iter(
            vec![("baseballStats".to_string(), to_string_vec(vec!["127.0.0.1:8000"]))]));
        assert_iters_equal_anyorder(
            broker.all_broker_list.write().unwrap().iter(),
            AllBrokerList::from_iter(to_string_vec(vec!["127.0.0.1:8000", "127.0.0.1:1000"])).iter(),
        );

        let new_external_view = ExternalView {
            id: "brokerResponse".to_string(),
            simple_fields: Default::default(),
            map_fields: HashMap::from_iter(vec![
                ("baseballStats_OFFLINE".to_string(), HashMap::from_iter(vec![
                    ("Broker_127.0.0.1_8000".to_string(), "ONLINE".to_string()),
                ])),
            ]),
            list_fields: Default::default(),
        };

        assert!(broker.refresh_external_view(&new_external_view).is_ok());
        assert_eq!(*broker.table_broker_map.write().unwrap(), TableBrokerMap::from_iter(
            vec![("baseballStats".to_string(), to_string_vec(vec!["127.0.0.1:8000"]))]));
        assert_eq!(*broker.all_broker_list.write().unwrap(), AllBrokerList::from_iter(
            to_string_vec(vec!["127.0.0.1:8000"])));
    }

    #[test]
    fn dynamic_broker_selector_select_random_broker_returns_randomly() {
        let external_view = ExternalView {
            id: "brokerResponse".to_string(),
            simple_fields: Default::default(),
            map_fields: HashMap::from_iter(vec![
                ("baseballStats_OFFLINE".to_string(), HashMap::from_iter(vec![
                    ("Broker_127.0.0.1_8000".to_string(), "ONLINE".to_string()),
                    ("Broker_127.0.0.1_9000".to_string(), "ONLINE".to_string()),
                ])),
            ]),
            list_fields: Default::default(),
        };
        let dynamic_broker_selector = DynamicBrokerSelector::new(&external_view);
        let first = dynamic_broker_selector.select_random_broker().unwrap();
        assert!(first.starts_with("127.0.0.1"));
        let mut at_least_one_differs = false;
        for _ in 0..10 {
            let broker = dynamic_broker_selector.select_broker("").unwrap();
            assert!(broker.starts_with("127.0.0.1"));
            at_least_one_differs |= broker != first;
        }
        assert!(at_least_one_differs)
    }

    #[test]
    fn dynamic_broker_selector_select_random_broker_returns_error_when_no_broker_available() {
        let external_view = ExternalView {
            id: "brokerResponse".to_string(),
            simple_fields: Default::default(),
            map_fields: Default::default(),
            list_fields: Default::default(),
        };
        let dynamic_broker_selector = DynamicBrokerSelector::new(&external_view);
        match dynamic_broker_selector.select_random_broker().unwrap_err() {
            Error::NoAvailableBroker => {}
            _ => panic!("Incorrect error kind"),
        }
    }

    #[test]
    fn dynamic_broker_selector_select_broker_by_table_returns_randomly_for_table() {
        let external_view = ExternalView {
            id: "brokerResponse".to_string(),
            simple_fields: Default::default(),
            map_fields: HashMap::from_iter(vec![
                ("baseballStats_OFFLINE".to_string(), HashMap::from_iter(vec![
                    ("Broker_127.0.0.1_1100".to_string(), "ONLINE".to_string()),
                    ("Broker_127.0.0.1_1200".to_string(), "ONLINE".to_string()),
                ])),
                ("".to_string(), HashMap::from_iter(vec![
                    ("Broker_127.0.0.1_2000".to_string(), "ONLINE".to_string()),
                ])),
            ]),
            list_fields: Default::default(),
        };

        let dynamic_broker_selector = DynamicBrokerSelector::new(&external_view);
        let first = dynamic_broker_selector
            .select_random_broker_by_table_name("baseballStats").unwrap();
        assert!(first.starts_with("127.0.0.1:1"));
        let mut at_least_one_differs = false;
        for _ in 0..10 {
            let broker = dynamic_broker_selector
                .select_random_broker_by_table_name("baseballStats").unwrap();
            assert!(broker.starts_with("127.0.0.1:1"));
            at_least_one_differs |= broker != first;
        }
        assert!(at_least_one_differs)
    }

    #[test]
    fn dynamic_broker_selector_select_broker_by_table_returns_error_when_no_broker_available() {
        let external_view = ExternalView {
            id: "brokerResponse".to_string(),
            simple_fields: Default::default(),
            map_fields: Default::default(),
            list_fields: Default::default(),
        };
        let dynamic_broker_selector = DynamicBrokerSelector::new(&external_view);
        match dynamic_broker_selector
            .select_random_broker_by_table_name("table")
            .unwrap_err()

        {
            Error::NoAvailableBrokerForTable(table) => assert_eq!(table, "table".to_string()),
            _ => panic!("Incorrect error kind"),
        }
    }

    #[test]
    fn dynamic_broker_selector_select_broker_by_table_returns_error_when_table_name_unknown() {
        let external_view = ExternalView {
            id: "brokerResponse".to_string(),
            simple_fields: Default::default(),
            map_fields: HashMap::from_iter(vec![
                ("baseballStats_OFFLINE".to_string(), HashMap::from_iter(vec![
                    ("Broker_127.0.0.1_8000".to_string(), "ONLINE".to_string()),
                ])),
            ]),
            list_fields: Default::default(),
        };
        let dynamic_broker_selector = DynamicBrokerSelector::new(&external_view);
        match dynamic_broker_selector
            .select_random_broker_by_table_name("anotherTable")
            .unwrap_err()

        {
            Error::NoAvailableBrokerForTable(table) => assert_eq!(table, "anotherTable".to_string()),
            _ => panic!("Incorrect error kind"),
        }
        match dynamic_broker_selector
            .select_random_broker_by_table_name("")
            .unwrap_err()

        {
            Error::NoAvailableBrokerForTable(table) => assert_eq!(table, "".to_string()),
            _ => panic!("Incorrect error kind"),
        }
    }

    #[test]
    fn dynamic_broker_selector_select_broker_selects_from_all_if_table_name_empty() {
        let external_view = ExternalView {
            id: "brokerResponse".to_string(),
            simple_fields: Default::default(),
            map_fields: Default::default(),
            list_fields: Default::default(),
        };
        let dynamic_broker_selector = DynamicBrokerSelector::new(&external_view);

        // Artificially replace internal maps with differing addresses
        {
            let mut table_broker_map = dynamic_broker_selector.table_broker_map.write().unwrap();
            let mut all_brokers_list = dynamic_broker_selector.all_broker_list.write().unwrap();
            *table_broker_map = TableBrokerMap::from_iter(vec![
                ("baseballStats".to_string(), to_string_vec(vec!["127.0.0.1_8000"])),
            ]);
            *all_brokers_list = AllBrokerList::from_iter(to_string_vec(vec!["127.0.0.1_9000"]));
        }

        let select_all_broker = dynamic_broker_selector.select_broker("").unwrap();
        let select_table_broker = dynamic_broker_selector.select_broker("baseballStats").unwrap();

        assert_eq!(select_all_broker, "127.0.0.1_9000".to_string());
        assert_eq!(select_table_broker, "127.0.0.1_8000".to_string());
    }

    #[test]
    fn dynamic_broker_selector_select_broker_returns_error_if_no_table_available() {
        let external_view = ExternalView {
            id: "brokerResponse".to_string(),
            simple_fields: Default::default(),
            map_fields: Default::default(),
            list_fields: Default::default(),
        };
        let dynamic_broker_selector = DynamicBrokerSelector::new(&external_view);
        let select_all_broker = dynamic_broker_selector.select_broker("");
        let select_table_broker = dynamic_broker_selector.select_broker("baseballStats");

        match select_all_broker.unwrap_err()
        {
            Error::NoAvailableBroker => {}
            _ => panic!("Incorrect error kind"),
        }
        match select_table_broker.unwrap_err() {
            Error::NoAvailableBrokerForTable(table) => assert_eq!(table, "baseballStats".to_string()),
            _ => panic!("Incorrect error kind"),
        }
    }
}
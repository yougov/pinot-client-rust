use std::num::ParseIntError;

use log::error;
use reqwest::blocking::Response as ReqwestResponse;
use reqwest::Error as ReqwestError;
use serde_json::error::Error as JsonError;
use zookeeper::ZkError;

use crate::dynamic_broker_selector::DynamicBrokerSelectorError;
use crate::request::Request;
use crate::response::DataType;
use crate::simple_broker_selector::SimpleBrokerSelectorError;

/// Crate result.
pub type Result<T> = std::result::Result<T, Error>;

/// Crate error.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Failure to establish an external view watcher.
    #[error("Failed to set an external view watcher for the path '{0}' due to: {1}")]
    FailedExternalViewWatcher(String, #[source] ZkError),

    /// Failed Json deserialization.
    #[error("Failed Json deserialization: {0}")]
    FailedJsonDeserialization(#[from] JsonError),

    /// Failure to execute a `Request`.
    #[error("Failed execute request: {0:?}")]
    FailedRequest(Request, #[source] ReqwestError),

    /// Failure to connect to zookeeper.
    #[error("Failed connect to zookeeper: {0}")]
    FailedZookeeperConnection(#[from] ZkError),

    /// Request for incompatible data type.
    #[error("Requested result data of type {requested:?} as {actual:?}")]
    IncorrectResultDataType { requested: DataType, actual: DataType },

    /// Invalid broker key.
    #[error("Invalid broker Key: {0}, should be in the format of Broker_[hostname]_[port]")]
    InvalidBrokerKey(String),

    /// Invalid broker port.
    #[error("Invalid broker port: {0}, should be an integer: {1}")]
    InvalidBrokerPort(String, #[source] ParseIntError),

    /// Invalid result row index.
    #[error("Invalid result row index: {0}")]
    InvalidResultRowIndex(usize),

    /// Invalid result column index.
    #[error("Invalid result column index: {0}")]
    InvalidResultColumnIndex(usize),

    /// Invalid result column index.
    #[error("Invalid result column name: {0}")]
    InvalidResultColumnName(String),

    /// Invalid `Request`.
    #[error("Failed to build HTTP request from {0:?} due to: {1}")]
    InvalidRequest(Request, #[source] ReqwestError),

    /// Invalid `ReqwestResponse`.
    #[error("Encountered invalid HTTP response {0:?}")]
    InvalidResponse(ReqwestResponse),

    /// No broker available.
    #[error("No available broker found")]
    NoAvailableBroker,

    /// No broker available for given table.
    #[error("No available broker found for table: {0}")]
    NoAvailableBrokerForTable(String),

    /// Dynamic broker selector could not be accessed.
    #[error("Dynamic broker selector unavailable: {0}")]
    UnavailableDynamicBrokerSelector(#[from] DynamicBrokerSelectorError),

    /// Simple broker selector could not be accessed.
    #[error("Simple broker selector unavailable: {0}")]
    UnavailableSimpleBrokerSelector(#[from] SimpleBrokerSelectorError),
}

pub fn log_error(
    msg: &str, func: impl Fn() -> Result<()>,
) {
    let msg = msg.to_string();
    if let Err(e) = func() {
        error!("'{}': {:?}", msg, e);
    }
}

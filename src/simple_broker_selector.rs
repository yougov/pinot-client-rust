use crate::broker_selector::BrokerSelector;
use crate::errors::{Error, Result};
use crate::rand::clone_random_element;

/// Simple broker error.
#[derive(Clone, Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SimpleBrokerSelectorError {
    /// Poisoned all broker list read.
    #[error("Broker list is empty")]
    EmptyBrokerList,
}

/// A naive broke a selector that takes a list
/// of addresses from which it selects randomly.
#[derive(Clone, Debug)]
pub struct SimpleBrokerSelector {
    broker_list: Vec<String>,
}

impl SimpleBrokerSelector {
    pub fn new(broker_list: Vec<String>) -> Result<Self> {
        if broker_list.is_empty() {
            return Err(SimpleBrokerSelectorError::EmptyBrokerList.into());
        }
        Ok(SimpleBrokerSelector { broker_list })
    }
}

impl BrokerSelector for SimpleBrokerSelector {
    fn select_broker(&self, _table: &str) -> Result<String> {
        match clone_random_element(&self.broker_list) {
            None => Err(Error::NoAvailableBroker),
            Some(broker) => Ok(broker),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::tests::to_string_vec;

    use super::*;

    #[test]
    fn simple_broker_selector_initialises() {
        let simple_broker_selector = SimpleBrokerSelector::new(vec![
            "broker0".to_string(),
            "broker1".to_string(),
        ]);
        assert!(simple_broker_selector.is_ok());
    }

    #[test]
    fn simple_broker_selector_returns_error_for_empty_list() {
        let simple_broker_selector = SimpleBrokerSelector::new(vec![]);
        match simple_broker_selector.unwrap_err() {
            Error::UnavailableSimpleBrokerSelector(SimpleBrokerSelectorError::EmptyBrokerList) => {}
            _ => panic!("Incorrect error kind"),
        }
    }

    #[test]
    fn simple_broker_selector_returns_broker_for_any_string() {
        let simple_broker_selector = SimpleBrokerSelector::new(vec![
            "broker0".to_string(),
            "broker1".to_string(),
        ]).unwrap();
        let broker = simple_broker_selector.select_broker("").unwrap();
        assert!(broker.starts_with("broker"));
        let broker = simple_broker_selector.select_broker("table_name").unwrap();
        assert!(broker.starts_with("broker"));
    }

    #[test]
    fn simple_broker_selector_select_broker_returns_randomly() {
        let simple_broker_selector = SimpleBrokerSelector::new(vec![
            "broker0".to_string(),
            "broker1".to_string(),
        ]).unwrap();
        let first = simple_broker_selector.select_broker("").unwrap();
        assert!(first.starts_with("broker"));
        let mut at_least_one_differs = false;
        for _ in 0..10 {
            let broker = simple_broker_selector.select_broker("").unwrap();
            assert!(broker.starts_with("broker"));
            at_least_one_differs |= broker != first;
        }
        assert!(at_least_one_differs)
    }

    #[test]
    fn simple_broker_selector_select_broker_returns_error_when_no_broker_available() {
        let mut simple_broker_selector = SimpleBrokerSelector::new(
            to_string_vec(vec!["a"])).unwrap();
        simple_broker_selector.broker_list.clear();
        match simple_broker_selector.select_broker("").unwrap_err() {
            Error::NoAvailableBroker => {}
            _ => panic!("Incorrect error kind"),
        }
    }
}
use std::fmt::Debug;

use super::errors::Result;

/// A broker selector that provides a broker
/// given a table name.
pub trait BrokerSelector: Clone + Debug {
    /// Returns the broker address in the form host:port
    fn select_broker(&self, table: &str) -> Result<String>;
}

#[cfg(test)]
pub(crate) mod tests {
    use std::fmt::Formatter;
    use std::sync::Arc;

    use super::*;

    #[derive(Clone)]
    pub struct TestBrokerSelector {
        return_function: Arc<Box<dyn Fn(&str) -> Result<String>>>,
    }

    impl TestBrokerSelector {
        pub fn new<F>(return_function: F) -> Self where F: 'static + Fn(&str) -> Result<String> {
            Self { return_function: Arc::new(Box::new(return_function)) }
        }
    }

    impl Debug for TestBrokerSelector {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "TestBrokerSelector")
        }
    }

    impl BrokerSelector for TestBrokerSelector {
        fn select_broker(&self, table: &str) -> Result<String> {
            (self.return_function)(table)
        }
    }
}
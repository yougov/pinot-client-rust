use super::errors::Result;

pub trait BrokerSelector {
    /// Returns the broker address in the form host:port
    fn select_broker(&self, table: &str) -> Result<String>;
}

#[cfg(test)]
pub mod tests {
    use super::*;

    pub struct TestBrokerSelector {
        return_function: Box<dyn Fn(&str) -> Result<String>>,
    }

    impl TestBrokerSelector {
        pub fn new<F>(return_function: F) -> Self where F: 'static + Fn(&str) -> Result<String> {
            Self { return_function: Box::new(return_function) }
        }
    }

    impl BrokerSelector for TestBrokerSelector {
        fn select_broker(&self, table: &str) -> Result<String> {
            (self.return_function)(table)
        }
    }
}
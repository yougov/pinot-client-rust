use crate::errors::Result;
use crate::request::Request;
use crate::response::BrokerResponse;

pub trait ClientTransport {
    fn execute(&self, broker_address: &str, query: Request) -> Result<BrokerResponse>;
}

#[cfg(test)]
pub mod tests {
    use super::*;

    pub struct TestClientTransport {
        return_function: Box<dyn Fn(&str, Request) -> Result<BrokerResponse>>,
    }

    impl TestClientTransport {
        pub fn new<F>(return_function: F) -> Self
            where
                F: 'static + Fn(&str, Request) -> Result<BrokerResponse>
        {
            Self { return_function: Box::new(return_function) }
        }
    }

    impl ClientTransport for TestClientTransport {
        fn execute(&self, broker_address: &str, query: Request) -> Result<BrokerResponse> {
            (self.return_function)(broker_address, query)
        }
    }
}
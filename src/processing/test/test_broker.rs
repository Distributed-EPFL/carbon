use crate::{
    crypto::Identify,
    processing::{SignupRequest, SignupResponse},
    signup::IdRequest,
    view::View,
};

use talk::{
    crypto::KeyChain,
    link::context::ConnectDispatcher,
    net::{test::TestConnector, Connector},
};

pub(crate) struct TestBroker {
    keychain: KeyChain,
    view: View,
    dispatcher: ConnectDispatcher,
}

impl TestBroker {
    pub fn new(keychain: KeyChain, view: View, connector: TestConnector) -> TestBroker {
        let dispatcher = ConnectDispatcher::new(connector);

        Self {
            keychain,
            view,
            dispatcher,
        }
    }

    pub async fn id_requests(&self, id_requests: Vec<IdRequest>) -> SignupResponse {
        let signup_context = format!("{:?}::processor::signup", self.view.identifier());
        let broker_connector = self.dispatcher.register(signup_context);

        assert!(id_requests.len() > 0);
        assert!(id_requests
            .iter()
            .all(|id_request| id_request.view() == self.view.identifier()));

        let allocator = id_requests[0].allocator();

        assert!(self.view.members().contains_key(&allocator));
        assert!(id_requests
            .iter()
            .all(|id_request| id_request.allocator() == allocator));

        let mut connection = broker_connector.connect(allocator).await.unwrap();

        connection
            .send(&SignupRequest::IdRequests(id_requests))
            .await
            .unwrap();

        connection.receive::<SignupResponse>().await.unwrap()
    }
}

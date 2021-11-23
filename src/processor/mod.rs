use crate::{crypto::Identify, database::Database, view::View};

use std::sync::Arc;

use talk::link::context::{ConnectDispatcher, ListenDispatcher};
use talk::net::{Connector, Listener};
use talk::sync::fuse::Fuse;
use talk::sync::lenders::AtomicLender;

pub(crate) struct Processor {
    database: Arc<AtomicLender<Database>>,
    _fuse: Fuse,
}

impl Processor {
    pub fn new<C, L>(view: View, database: Database, connector: C, listener: L) -> Self
    where
        C: Connector,
        L: Listener,
    {
        let database = Arc::new(AtomicLender::new(database));

        let _connect_dispatcher = ConnectDispatcher::new(connector);
        let listen_dispatcher = ListenDispatcher::new(listener, Default::default()); // TODO: Forward settings

        let fuse = Fuse::new();

        let signup_context = format!("{:?}::processor::signup", view.identifier(),);
        let signup_listener = listen_dispatcher.register(signup_context);

        {
            let view = view.clone();
            let database = database.clone();

            fuse.spawn(async move {
                Processor::signup(view, database, signup_listener).await;
            });
        }

        todo!()
    }

    pub fn shutdown(self) -> Database {
        self.database.take()
    }
}

mod signup;

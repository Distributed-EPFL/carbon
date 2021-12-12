use crate::{
    crypto::Identify,
    data::{PingBoard, Sponge},
    discovery::Client,
    view::View,
};

use doomstack::{here, Doom, ResultExt, Top};

use std::{net::SocketAddr, sync::Arc};

use talk::{
    link::context::ConnectDispatcher,
    net::{Connector, SessionConnector},
    sync::fuse::Fuse,
};

use tokio::{
    io,
    net::{TcpListener, ToSocketAddrs},
};

pub(crate) struct Broker {
    address: SocketAddr,
    _fuse: Fuse,
}

#[derive(Doom)]
pub(crate) enum BrokerError {
    #[doom(description("Failed to initialize broker: {}", source))]
    #[doom(wrap(initialize_failed))]
    InitializeFailed { source: io::Error },
}

impl Broker {
    pub async fn new<A, C>(
        discovery: Arc<Client>,
        view: View,
        address: A,
        connector: C,
    ) -> Result<Self, Top<BrokerError>>
    where
        A: ToSocketAddrs,
        C: Connector,
    {
        let listener = TcpListener::bind(address)
            .await
            .map_err(BrokerError::initialize_failed)
            .map_err(Doom::into_top)
            .spot(here!())?;

        let address = listener
            .local_addr()
            .map_err(BrokerError::initialize_failed)
            .map_err(Doom::into_top)
            .spot(here!())?;

        let dispatcher = ConnectDispatcher::new(connector);
        let context = format!("{:?}::processor::commit", view.identifier());
        let connector = Arc::new(SessionConnector::new(dispatcher.register(context)));

        let brokerage_sponge = Arc::new(Sponge::new(Default::default())); // TODO: Add settings
        let ping_board = PingBoard::new(&view);

        let fuse = Fuse::new();

        {
            let discovery = discovery.clone();
            let brokerage_sponge = brokerage_sponge.clone();

            fuse.spawn(async move {
                Broker::listen(discovery, brokerage_sponge, listener).await;
            });
        }

        {
            let discovery = discovery.clone();
            let view = view.clone();
            let ping_board = ping_board.clone();
            let connector = connector.clone();

            fuse.spawn(async move {
                Broker::flush(discovery, view, brokerage_sponge, ping_board, connector).await;
            });
        }

        for replica in view.members().keys().copied() {
            let ping_board = ping_board.clone();
            let connector = connector.clone();

            fuse.spawn(async move { Broker::ping(ping_board, connector, replica).await });
        }

        Ok(Broker {
            address,
            _fuse: fuse,
        })
    }
}

mod broker;
mod flush;
mod frontend;
mod ping;

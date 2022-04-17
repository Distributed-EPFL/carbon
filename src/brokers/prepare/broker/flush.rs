use crate::{
    brokers::prepare::{
        broker::Brokerage, broker_settings::BrokerTaskSettings, Broker, BrokerFailure,
    },
    data::{PingBoard, Sponge},
    discovery::Client,
    view::View,
};

use std::sync::Arc;

use log::info;
use talk::{net::SessionConnector, sync::fuse::Fuse};

impl Broker {
    pub(in crate::brokers::prepare::broker) async fn flush(
        discovery: Arc<Client>,
        view: View,
        brokerage_sponge: Arc<Sponge<Brokerage>>,
        ping_board: PingBoard,
        connector: Arc<SessionConnector>,
        settings: BrokerTaskSettings,
    ) {
        let fuse = Fuse::new();

        loop {
            // Remark: `brokerage_sponge.flush()` always returns a non-empty
            // `Vec<Brokerage>`. Because `Broker::prepare` only filters `Id`
            // duplicates, it never produces an empty output on a non-empty input.
            let brokerages = Broker::prepare(brokerage_sponge.flush().await);

            info!("Number of brokerages: {}", brokerages.len());

            let discovery = discovery.clone();
            let view = view.clone();
            let ping_board = ping_board.clone();
            let connector = connector.clone();
            let settings = settings.clone();

            fuse.spawn(async move {
                Broker::broker(discovery, view, ping_board, connector, brokerages, settings).await;
            });
        }
    }

    fn prepare(mut brokerages: Vec<Brokerage>) -> Vec<Brokerage> {
        // Sort `brokerages` by requestor

        brokerages.sort_by_key(|brokerage| brokerage.request.id());

        // Deduplicate and fail `brokerages` by requestor

        // The following implementation does not use `Vec::dedup_*` because
        // duplicate `Brokerage`s must be failed explicitly. If an element
        // of `brokerages` duplicates the previous, its `reduction_inlet` is
        // consumed to `send` a `BrokerFailure`, to the appropriate `serve`
        // task, and the element is filtered out of `prepare`'s return.
        let mut previous = None;

        brokerages
            .into_iter()
            .filter_map(|brokerage| {
                if Some(brokerage.request.id()) == previous {
                    let _ = brokerage.reduction_inlet.send(Err(BrokerFailure::Throttle));
                    None
                } else {
                    previous = Some(brokerage.request.id());
                    Some(brokerage)
                }
            })
            .collect()
    }
}

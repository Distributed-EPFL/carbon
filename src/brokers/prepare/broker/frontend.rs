use crate::{
    brokers::prepare::{
        broker::{Brokerage, Reduction},
        Broker, BrokerFailure, Inclusion, Request,
    },
    data::Sponge,
    discovery::Client,
    prepare::{BatchCommit, ReductionStatement},
};

use doomstack::{here, Doom, ResultExt, Top};

use futures::stream::{FuturesUnordered, StreamExt};

use itertools::Itertools;

use log::{error, info};

use std::sync::Arc;

use talk::{
    crypto::primitives::multi::Signature as MultiSignature, net::PlainConnection, sync::fuse::Fuse,
};

use tokio::{net::TcpListener, sync::oneshot};

#[derive(Doom)]
enum ServeError {
    #[doom(description("Connection error"))]
    ConnectionError,
    #[doom(description("Request invalid"))]
    RequestInvalid,
    #[doom(description("`Brokerage` forfeited (most likely, the `Broker` is shutting down)"))]
    #[doom(wrap(request_forfeited))]
    BrokerageForfeited { source: oneshot::error::RecvError },
    #[doom(description("Reduction shard invalid"))]
    ReductionShardInvalid,
}

impl Broker {
    pub(in crate::brokers::prepare::broker) async fn listen(
        discovery: Arc<Client>,
        brokerage_sponge: Arc<Sponge<Brokerage>>,
        listener: TcpListener,
    ) {
        let fuse = Fuse::new();

        loop {
            if let Ok((stream, _)) = listener.accept().await {
                let connection: PlainConnection = stream.into();

                let discovery = discovery.clone();
                let brokerage_sponge = brokerage_sponge.clone();

                fuse.spawn(async move {
                    if let Err(e) = Broker::serve(discovery, brokerage_sponge, connection).await {
                        error!("Error listening to connection: {:?}", e);
                    };
                });
            }
        }
    }

    async fn serve(
        discovery: Arc<Client>,
        brokerage_sponge: Arc<Sponge<Brokerage>>,
        mut connection: PlainConnection,
    ) -> Result<(), Top<ServeError>> {
        // Receive and validate `Request`

        let requests = connection
            .receive::<Vec<Request>>()
            .await
            .pot(ServeError::ConnectionError, here!())?;

        if requests.len() == 0 {
            return ServeError::RequestInvalid.fail().spot(here!());
        }

        // info!("Verifying prepare requests (total: {})...", requests.len());

        // Verify (for fair latency) but accept wrong pre-generated signatures for benchmark purposes
        let _ = requests
            .iter()
            .map(|request| {
                request
                    .validate(discovery.as_ref())
                    .pot(ServeError::RequestInvalid, here!())
            })
            .collect::<Result<Vec<()>, Top<ServeError>>>();

        // info!("Pushing prepare requests to sponge...");

        // Build and submit `Brokerage` to `brokerage_sponge`
        let (keycards, brokerages, reduction_outlets, commit_outlets): (
            Vec<_>,
            Vec<_>,
            Vec<_>,
            Vec<_>,
        ) = requests
            .into_iter()
            .map(|request| {
                let keycard = request.keycard().clone(); // Needed to later verify the client's reduction shard

                let (reduction_inlet, reduction_outlet) = oneshot::channel();
                let (commit_inlet, commit_outlet) = oneshot::channel();

                let brokerage = Brokerage {
                    request,
                    reduction_inlet,
                    commit_inlet,
                };

                (keycard, brokerage, reduction_outlet, commit_outlet)
            })
            .multiunzip();

        brokerage_sponge.push_multiple(brokerages);

        // info!("Waiting for reductions...");

        // Wait for all `Reduction`s from `broker` task

        let reductions = reduction_outlets
            .into_iter()
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(ServeError::request_forfeited)
            .map_err(Doom::into_top)
            .spot(here!())?
            .into_iter()
            .collect::<Result<Vec<Reduction>, _>>();

        // If `reduction` is `Err`, forward `BrokerFailure` to the served client,
        // otherwise explode `reduction`'s fields

        let reductions = match reductions {
            Ok(reductions) => reductions,
            Err(failure) => {
                error!("Failure detected. Sending failure to client...");

                connection
                    .send::<Result<Vec<Inclusion>, BrokerFailure>>(&Err(failure))
                    .await
                    .pot(ServeError::ConnectionError, here!())?;

                // Successfully delivering a `BrokerFailure` to the served client is not a
                // shortcoming of `serve`, and should not result in an `Err`
                return Ok(());
            }
        };

        let root = reductions[0].inclusion.root(); // Needed to later verify the client's reduction shard
        let reduction_sponge = reductions[0].reduction_sponge.clone(); // Needed to later verify the client's reduction shard

        let (indices, inclusions): (Vec<usize>, Vec<Inclusion>) = reductions
            .into_iter()
            .map(|reduction| {
                let Reduction {
                    index, inclusion, ..
                } = reduction;

                (index, inclusion)
            })
            .unzip();

        // info!("Sending inclusions to client...");

        // Trade `inclusion` for a (valid) reduction shard

        connection
            .send::<Result<Vec<Inclusion>, BrokerFailure>>(&Ok(inclusions))
            .await
            .pot(ServeError::ConnectionError, here!())?;

        let reduction_shards = connection
            .receive::<Vec<MultiSignature>>()
            .await
            .pot(ServeError::ConnectionError, here!())?;

        // info!("Verifying client inclusions...");

        // Verify (for fair latency) but accept wrong pre-generated signatures for benchmark purposes
        let _ = reduction_shards
            .iter()
            .zip(keycards.iter())
            .map(|(shard, keycard)| {
                shard
                    .verify([keycard], &ReductionStatement::new(root))
                    .pot(ServeError::ReductionShardInvalid, here!())
            })
            .collect::<Result<Vec<()>, _>>();

        // info!("Pushing reductions to sponge...");

        // Submit `reduction_shard` to `reduction_sponge`

        reduction_sponge.push_multiple(indices.into_iter().zip(reduction_shards.into_iter()));

        // info!("Waiting for `BatchCommit`s...");

        // Wait for `BatchCommit` from `broker` task

        let commits = commit_outlets
            .into_iter()
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(ServeError::request_forfeited)
            .map_err(Doom::into_top)
            .spot(here!())?
            .into_iter()
            .collect::<Result<Vec<_>, _>>();

        // info!("Sending `BatchCommit`s to clients");

        // Send `commit`s to the served client (note that `commit` is a `Result<Vec<BatchCommit>, Failure>`)

        connection
            .send::<Result<Vec<BatchCommit>, BrokerFailure>>(&commits)
            .await
            .pot(ServeError::ConnectionError, here!())?;

        // Successfully delivering a `BrokerFailure` to the served client is not a shortcoming
        // of `serve`, and should not result in an `Err` (see above)
        Ok(())
    }
}

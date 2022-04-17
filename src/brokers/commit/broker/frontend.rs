use crate::{
    brokers::commit::{brokerage::Brokerage, Broker, BrokerFailure, Request},
    commit::CompletionProof,
    data::Sponge,
    discovery::Client,
};

use doomstack::{here, Doom, ResultExt, Top};

use futures::stream::{FuturesUnordered, StreamExt};

use log::{error, info};

use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};

use std::{sync::Arc, time::Instant};

use talk::{net::PlainConnection, sync::fuse::Fuse};

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
}

impl Broker {
    pub(in crate::brokers::commit::broker) async fn listen(
        discovery: Arc<Client>,
        brokerage_sponge: Arc<Sponge<Brokerage>>,
        listener: TcpListener,
    ) {
        let fuse = Fuse::new();

        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let connection: PlainConnection = stream.into();

                    let discovery = discovery.clone();
                    let brokerage_sponge = brokerage_sponge.clone();

                    fuse.spawn(async move {
                        if let Err(e) = Broker::serve(discovery, brokerage_sponge, connection).await
                        {
                            error!("Error {:?}", e);
                        }
                    });
                }
                Err(e) => error!("Error {:?}", e),
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

        // info!("Verifying commit requests (total: {})...", requests.len());
        // let start = Instant::now();

        // Verify (for fair latency) but accept wrong pre-generated signatures for benchmark purposes
        let _ = requests
            .par_iter()
            .map(|request| {
                request
                    .validate(discovery.as_ref())
                    .pot(ServeError::RequestInvalid, here!())
            })
            .collect::<Vec<Result<(), Top<ServeError>>>>();

        // Build and submit `Brokerage` to `brokerage_sponge`

        // Build and submit `Brokerage` to `brokerage_sponge`
        let (brokerages, completion_outlets): (Vec<_>, Vec<_>) = requests
            .into_par_iter()
            .map(|request| {
                let (completion_inlet, completion_outlet) = oneshot::channel();

                let brokerage = Brokerage {
                    request,
                    completion_inlet,
                };

                (brokerage, completion_outlet)
            })
            .unzip();

        // info!("Verified requests in {} ms", start.elapsed().as_millis());

        // info!("Pushing commits to sponge...");

        brokerage_sponge.push_multiple(brokerages);

        // Wait for `Completion` from `broker` task

        // info!("Waiting for completions...");

        let completions = completion_outlets
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
            .collect::<Result<Vec<CompletionProof>, _>>();

        // info!("Sending completions to client...");

        // Send `commit` to the served client (note that `commit` is a `Result<Completion, Failure>`)

        connection
            .send::<Result<Vec<CompletionProof>, BrokerFailure>>(&completions)
            .await
            .pot(ServeError::ConnectionError, here!())?;

        // info!("Completions sent!");

        // Successfully delivering a `BrokerFailure` to the served client is not a shortcoming
        // of `serve`, and should not result in an `Err` (see above)
        Ok(())
    }
}

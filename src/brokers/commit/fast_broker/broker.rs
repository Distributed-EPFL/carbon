use crate::{
    account::Id,
    brokers::commit::{Broker, BrokerFailure, FastBroker, Request, Submission},
    commit::{Commit, CommitProof, Completion, CompletionProof, Payload},
    data::PingBoard,
    processing::messages::CommitRequest,
    view::View,
};

use doomstack::{here, Doom, ResultExt, Top};

use futures::stream::{FuturesUnordered, StreamExt};
use log::info;

use std::{sync::Arc, time::Instant};

use talk::{crypto::Identity, net::SessionConnector};

use zebra::vector::Vector;

#[derive(Doom)]
enum PublishError {
    #[doom(description("Connection failed"))]
    ConnectionFailed,
    #[doom(description("Connection error"))]
    ConnectionError,
}

impl FastBroker {
    pub(in crate::brokers::commit::fast_broker) async fn broker(
        view: View,
        ping_board: PingBoard,
        connector: Arc<SessionConnector>,
        requests: Vec<Request>,
    ) -> Vec<CompletionProof> {
        // Unzip `brokerages` into its components

        let (payloads, commit_proofs, dependencies) = unzip(requests);

        let payloads = Vector::new(payloads).unwrap();
        let submission = Submission::new(payloads.clone(), commit_proofs, dependencies);

        // Orchestrate submission to obtain `BatchCompletion`

        let start = Instant::now();

        let batch_completion =
            Broker::orchestrate(view.clone(), ping_board, connector.clone(), submission)
                .await
                .map_err(|_| BrokerFailure::Error)
                .unwrap();

        // info!("Orchestrate took: {} ms", start.elapsed().as_millis());

        // Dispatch appropriate `CompletionProof` to all `serve` tasks
        let start = Instant::now();

        let completion_proofs = (0..payloads.len())
            .map(|index| {
                let inclusion = payloads.prove(index);
                CompletionProof::new(batch_completion.clone(), inclusion)
            })
            .collect::<Vec<_>>();

        // info!(
        //     "Creating completion proofs took: {} ms",
        //     start.elapsed().as_millis()
        // );

        // If `completion` is `Ok`, publish `BatchCommit` to all replicas

        tokio::spawn(async move {
            let request = CommitRequest::Completion(batch_completion);

            view.members()
                .keys()
                .copied()
                .map(|replica| FastBroker::publish(connector.as_ref(), &request, replica))
                .collect::<FuturesUnordered<_>>()
                .collect::<Vec<_>>()
                .await;
        });

        completion_proofs
    }

    async fn publish(
        connector: &SessionConnector,
        request: &CommitRequest,
        replica: Identity,
    ) -> Result<(), Top<PublishError>> {
        let mut session = connector
            .connect(replica)
            .await
            .pot(PublishError::ConnectionFailed, here!())?;

        session
            .send(request)
            .await
            .pot(PublishError::ConnectionError, here!())?;

        session.end();

        Ok(())
    }
}

fn unzip(requests: Vec<Request>) -> (Vec<Payload>, Vec<(Id, CommitProof)>, Vec<(Id, Completion)>) {
    let mut payloads = Vec::new();
    let mut commit_proofs = Vec::new();
    let mut dependencies = Vec::new();

    for request in requests {
        let Request {
            commit: Commit { proof, payload },
            dependency,
        } = request;

        let id = payload.id();

        payloads.push(payload);
        commit_proofs.push((id, proof));

        if let Some(dependency) = dependency {
            dependencies.push((id, dependency));
        }
    }

    (payloads, commit_proofs, dependencies)
}

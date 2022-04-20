use crate::{
    account::{Entry, Operation},
    brokers::{
        commit::Request,
        prepare::{broker_settings::BrokerTaskSettings, submission::Submission, FastBroker},
    },
    commit::{Commit, CommitProof, Payload},
    data::PingBoard,
    discovery::Client,
    prepare::{Prepare, ReductionStatement},
    signup::IdAssignment,
    view::View,
};

use std::{sync::Arc, time::Duration};

use log::{error, info};
use rayon::{
    iter::{
        IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator,
    },
    slice::ParallelSliceMut,
};
use talk::{
    crypto::{
        primitives::{hash, sign::Signature as SingleSignature},
        KeyChain,
    },
    net::SessionConnector,
};
use tokio::time;
use zebra::vector::Vector;

use super::CommitInlet;

impl FastBroker {
    pub(in crate::brokers::prepare::fast_broker) async fn flush(
        local_rate: usize,
        batch_size: usize,
        batch_number: usize,
        single_sign_percentage: usize,
        clients: Vec<(KeyChain, IdAssignment)>,
        discovery: Arc<Client>,
        view: View,
        ping_board: PingBoard,
        connector: Arc<SessionConnector>,
        settings: BrokerTaskSettings,
        inlet: CommitInlet,
    ) {
        if clients.len() < batch_number {
            error!(
                "Insufficient number of signups for prepare batches of size {}",
                batch_number
            );
            return;
        }

        let mut clients = clients[0..batch_size].to_vec();
        clients.par_sort_by_key(|(_, id_assignment)| id_assignment.id());

        info!("Pre-computing submissions...");

        let submissions: Vec<Submission> = (0..batch_number)
            .map(|number| FastBroker::prepare_fast(single_sign_percentage, number as u64, &clients))
            .collect();

        let proof = Vector::new(submissions[0].prepares().to_vec())
            .unwrap()
            .prove(0);

        info!("All submissions pre-computed!");

        info!("Starting prepare...");

        let interval = (1000 * batch_size / local_rate) as u64;

        for (height, submission) in submissions.into_iter().enumerate() {
            let discovery = discovery.clone();
            let view = view.clone();
            let ping_board = ping_board.clone();
            let connector = connector.clone();
            let settings = settings.clone();

            info!("Submitting prepare batch {}", height);

            {
                let inlet = inlet.clone();
                let proof = proof.clone();

                tokio::spawn(async move {
                    let assignments = submission.assignments().to_vec();

                    let batch_commit = FastBroker::broker(
                        discovery, view, ping_board, connector, submission, settings,
                    )
                    .await
                    .unwrap();

                    let requests = assignments
                        .into_par_iter()
                        .map(|assignment| {
                            let commit_proof =
                                CommitProof::new(batch_commit.clone(), proof.clone());
                            let operation = Operation::withdraw(assignment.id(), 0, 0);

                            let payload = Payload::new(
                                Entry {
                                    id: assignment.id(),
                                    height: (height + 1) as u64,
                                },
                                operation,
                            );

                            let commit = Commit::new(commit_proof, payload.clone());

                            Request::new(commit, None)
                        })
                        .collect::<Vec<_>>();

                    info!("Committed prepare batch {}", height);

                    inlet.send((height as u64, requests)).unwrap();
                });
            }

            time::sleep(Duration::from_millis(interval)).await;
        }

        info!("Exiting flush");
    }

    fn prepare_fast(
        single_sign_percentage: usize,
        height: u64,
        clients: &Vec<(KeyChain, IdAssignment)>,
    ) -> Submission {
        let operation = hash::hash(&0).unwrap();
        let num_individual = single_sign_percentage * clients.len() / 100;

        info!("Number of individual signatures: {}", num_individual);

        let assignments = clients
            .iter()
            .map(|(_, id_assignment)| id_assignment)
            .cloned()
            .collect();

        let prepare = Prepare::new(
            Entry {
                id: clients[0].1.id(),
                height: height + 1,
            },
            operation,
        );
        let sig = clients[0].0.sign(&prepare).unwrap();

        let (prepares, individual_signatures): (Vec<Prepare>, Vec<Option<SingleSignature>>) =
            clients
                .par_iter()
                .enumerate()
                .map(|(num, (_, assignment))| {
                    let prepare = Prepare::new(
                        Entry {
                            id: assignment.id(),
                            height: height + 1,
                        },
                        operation,
                    );
                    if num < num_individual {
                        (prepare, Some(sig.clone()))
                    } else {
                        (prepare, None)
                    }
                })
                .unzip();

        let prepares = Vector::new(prepares).unwrap();
        let root = prepares.root();

        info!("Batch root: {:?}", root);

        let reduction_signature = clients[0]
            .0
            .multisign(&ReductionStatement::new(root))
            .unwrap();

        // Prepare `Submission`

        let submission = Submission::new(
            assignments,
            prepares,
            Some(reduction_signature),
            individual_signatures,
        );

        submission
    }
}

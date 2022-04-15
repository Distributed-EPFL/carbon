use crate::{
    account::{Entry, Operation},
    brokers::{
        commit::{BrokerFailure as CommitBrokerFailure, Request as CommitRequest},
        prepare::{BrokerFailure as PrepareBrokerFailure, Inclusion, Request as PrepareRequest},
        signup::BrokerFailure as SignupBrokerFailure,
    },
    commit::{Commit, CommitProof, Completion, CompletionProof, Payload},
    external::parameters::{BrokerParameters, Export, Parameters},
    prepare::{BatchCommit, Prepare, ReductionStatement},
    signup::{IdAssignment, IdRequest},
    view::View,
};

use doomstack::{here, Doom, ResultExt, Top};

use log::{error, info};

use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};

use tokio::{net::TcpStream, time};

use std::{
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};

use talk::{
    crypto::{primitives::hash, KeyCard, KeyChain},
    link::rendezvous::{Client as RendezvousClient, ClientError as RendezvousClientError, ShardId},
    net::{traits::TcpConnect, PlainConnection},
};

pub struct Client {}

#[derive(Doom)]
pub enum ClientError {
    #[doom(description("Fail"))]
    Fail,
}

impl Client {
    pub async fn new<A: 'static + TcpConnect + Clone>(
        rendezvous: A,
        parameters_file: Option<&str>,
        broker_address: Option<&str>,
        _num_clients: usize,
    ) -> Result<Self, Top<ClientError>> {
        // Load default parameters if none are specified.
        let BrokerParameters {
            signup_batch_number,
            signup_batch_size,
            prepare_batch_number,
            prepare_batch_size,
            prepare_single_sign_percentage,
            ..
        } = match parameters_file {
            Some(filename) => {
                Parameters::read(filename)
                    .pot(ClientError::Fail, here!())?
                    .broker
            }
            None => Parameters::default().broker,
        };

        info!("Signup batch number: {}", signup_batch_number);
        info!("Signup batch size: {}", signup_batch_size);
        info!("Prepare batch number: {}", prepare_batch_number);
        info!("Prepare batch size: {}", prepare_batch_size);
        info!(
            "Prepare single sign percentage: {}",
            prepare_single_sign_percentage
        );

        info!("Getting broker keycard");

        let client = RendezvousClient::new(rendezvous.clone(), Default::default());

        let (batch_keychains, id_assignments) =
            get_assignments(&client, prepare_batch_size).await?;

        let prepare_request_batches = (0..prepare_batch_number)
            .map(|height| prepare(height as u64, &batch_keychains, &id_assignments))
            .collect::<Vec<_>>();

        let prepare_address = get_prepare_address(&client, broker_address).await?;
        let commit_address = get_commit_address(&client, broker_address).await?;

        let reduction_shard = batch_keychains[0]
            .multisign(&ReductionStatement::new(hash::hash::<u32>(&0).unwrap()))
            .unwrap();

        client
            .publish_card(KeyChain::random().keycard(), Some(1))
            .await
            .unwrap();

        let _ = get_shard(&client, 1).await?;

        // info!("Synced with other brokers. Making sure IdAssignments are published...");

        // time::sleep(Duration::from_secs(20)).await;

        info!("Awaiting to be in the middle of the throughput...");

        info!("Starting latency test...");

        for (height, batch) in prepare_request_batches.into_iter().enumerate() {
            let stream = TcpStream::connect(prepare_address).await.unwrap();
            let mut prepare_connection: PlainConnection = stream.into();

            let stream = TcpStream::connect(commit_address).await.unwrap();
            let mut commit_connection: PlainConnection = stream.into();

            info!("Client sending batch for height {}", height);

            prepare_connection
                .send::<Vec<PrepareRequest>>(&batch)
                .await
                .unwrap();

            let inclusions = prepare_connection
                .receive::<Result<Vec<Inclusion>, PrepareBrokerFailure>>()
                .await
                .unwrap()
                .unwrap();

            // When benchmarking, we only simulate the processing time of a single client
            // In real life, each client is separate and only processes their own transaction
            // so other clients' processing time should not be included in latency
            // if num == 0 {
            //     let _ = inclusion
            //         .certify_reduction(&keychain, prepare_request.prepare())
            //         .unwrap();
            // }

            let signatures = vec![reduction_shard; inclusions.len()];
            prepare_connection.send(&signatures).await.unwrap();

            let batch_commits = prepare_connection
                .receive::<Result<Vec<BatchCommit>, PrepareBrokerFailure>>()
                .await
                .unwrap()
                .unwrap();

            info!("Got batch commits!");

            let (commit_requests, payloads): (Vec<CommitRequest>, Vec<Payload>) = batch
                .into_par_iter()
                .zip(inclusions.into_par_iter())
                .zip(batch_commits.into_par_iter())
                .map(|((prepare_request, inclusion), commit)| {
                    let commit_proof = CommitProof::new(commit, inclusion.proof);

                    let payload = Payload::new(
                        Entry {
                            id: prepare_request.prepare().id(),
                            height: (prepare_request.prepare().height()),
                        },
                        Operation::withdraw(
                            prepare_request.prepare().id(),
                            prepare_request.prepare().height() - 1,
                            0,
                        ),
                    );

                    let commit = Commit::new(commit_proof, payload.clone());

                    let commit_request = CommitRequest::new(commit, None);

                    (commit_request, payload)
                })
                .unzip();

            commit_connection
                .send::<Vec<CommitRequest>>(&commit_requests)
                .await
                .unwrap();

            let completion_proofs = match commit_connection
                .receive::<Result<Vec<CompletionProof>, CommitBrokerFailure>>()
                .await
                .unwrap()
            {
                Ok(completion_proofs) => completion_proofs,
                Err(e) => {
                    error!("Completion error! {:?}", e);
                    Err(e).unwrap()
                }
            };

            info!("Got completion proofs!");

            let _withdrawals = completion_proofs
                .into_iter()
                .zip(payloads.into_iter())
                .map(|(proof, payload)| Completion::new(proof, payload))
                .collect::<Vec<_>>();

            info!("Client completed batch for height {}", height);
        }

        info!("Client done!");

        Ok(Client {})
    }
}

async fn get_address(
    client: &RendezvousClient,
    preferred_address: Option<&str>,
    shard: u32,
) -> Result<SocketAddr, Top<ClientError>> {
    info!("Getting prepare address...");
    let shard = get_shard(&client, shard).await?;

    let mut possible_addresses = Vec::new();
    for broker in shard.iter() {
        possible_addresses.push(client.get_address(broker.identity()).await.unwrap());
    }

    let mut address = possible_addresses[0];
    if let Some(broker_address) = preferred_address {
        address = possible_addresses
            .into_iter()
            .find(|address| address.ip() == broker_address.parse::<Ipv4Addr>().unwrap())
            .unwrap_or(address);
    }

    Ok(address)
}

async fn get_prepare_address(
    client: &RendezvousClient,
    preferred_address: Option<&str>,
) -> Result<SocketAddr, Top<ClientError>> {
    get_address(client, preferred_address, 3).await
}

async fn get_commit_address(
    client: &RendezvousClient,
    preferred_address: Option<&str>,
) -> Result<SocketAddr, Top<ClientError>> {
    get_address(client, preferred_address, 4).await
}

async fn get_assignments(
    client: &RendezvousClient,
    amount: usize,
) -> Result<(Vec<KeyChain>, Vec<IdAssignment>), Top<ClientError>> {
    let shard = get_shard(&client, 2).await?;

    info!(
        "Obtained shard! Honest broker identities {:?}",
        shard
            .iter()
            .map(|keycard| keycard.identity())
            .collect::<Vec<_>>()
    );

    let mut addresses = Vec::new();
    for broker in shard.iter() {
        addresses.push(client.get_address(broker.identity()).await.unwrap());
    }

    let mut shard = get_shard(&client, 0).await?;
    shard.sort_by_key(|keycard| keycard.identity());

    info!(
        "Obtained shard! Replica identities {:?}",
        shard
            .iter()
            .map(|keycard| keycard.identity())
            .collect::<Vec<_>>()
    );

    let allocator = shard.iter().next().unwrap().identity();
    let genesis = View::genesis(shard);

    info!("Generating IdRequests...");

    let (batch_key_chains, batch_requests): (Vec<KeyChain>, Vec<IdRequest>) = (0..amount)
        .map(|_| {
            let keychain = KeyChain::random();
            let request = IdRequest::new(&keychain, &genesis, allocator.clone(), 0);

            (keychain, request)
        })
        .unzip();

    info!("Getting assignments...");

    let stream = TcpStream::connect(addresses[0].clone()).await.unwrap();
    let mut signup_connection: PlainConnection = stream.into();

    signup_connection.send(&batch_requests).await.unwrap();

    let assignments: Vec<IdAssignment> = signup_connection
        .receive::<Vec<Result<IdAssignment, SignupBrokerFailure>>>()
        .await
        .unwrap()
        .into_iter()
        .collect::<Result<Vec<IdAssignment>, SignupBrokerFailure>>()
        .unwrap();

    info!("All IdAssignments obtained.");

    Ok((batch_key_chains, assignments))
}

async fn get_shard(
    client: &RendezvousClient,
    number: ShardId,
) -> Result<Vec<KeyCard>, Top<ClientError>> {
    loop {
        match client.get_shard(number).await {
            Ok(shard) => break Ok(shard),
            Err(e) => match e.top() {
                RendezvousClientError::ShardIncomplete => {
                    info!("Shard still incomplete, sleeping...");
                    time::sleep(Duration::from_millis(500)).await
                }
                _ => {
                    error!("Error obtaining first shard view");
                    return ClientError::Fail.fail();
                }
            },
        }
    }
}

fn prepare(
    height: u64,
    clients: &Vec<KeyChain>,
    id_assignments: &Vec<IdAssignment>,
) -> Vec<PrepareRequest> {
    let commitment = hash::hash(&0).unwrap();
    let fake_prepare = Prepare::new(
        Entry {
            id: id_assignments[0].id(),
            height,
        },
        commitment.clone(),
    );
    let fake_signature = clients[0].sign(&fake_prepare).unwrap();

    id_assignments
        .iter()
        .cloned()
        .map(|assignment| {
            let payload = Payload::new(
                Entry {
                    id: assignment.id(),
                    height: (height + 1),
                },
                Operation::withdraw(assignment.id(), height, 0),
            );

            let prepare = payload.prepare();

            PrepareRequest {
                assignment,
                prepare,
                signature: fake_signature.clone(),
            }
        })
        .collect()
}

use crate::{
    account::{Entry, Operation},
    brokers::{
        commit::{BrokerFailure as CommitBrokerFailure, Request as CommitRequest},
        prepare::{BrokerFailure as PrepareBrokerFailure, Inclusion, Request as PrepareRequest},
        signup::BrokerFailure as SignupBrokerFailure,
    },
    commit::{Commit, CommitProof, Completion, CompletionProof, Payload},
    external::parameters::{ClientParameters, Export, Parameters},
    prepare::{BatchCommit, Prepare, ReductionStatement},
    signup::{IdAssignment, IdRequest},
    view::View,
};

use doomstack::{here, Doom, ResultExt, Top};

use futures::stream::{FuturesUnordered, StreamExt};

use log::{error, info, warn};

use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};

use tokio::{net::TcpStream, sync::Semaphore, time};

use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant},
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
        individual_rate: usize,
    ) -> Result<Self, Top<ClientError>> {
        // Load default parameters if none are specified.
        let parameters = match parameters_file {
            Some(filename) => Parameters::read(filename).pot(ClientError::Fail, here!())?,
            None => Parameters::default(),
        };

        let ClientParameters {
            prepare_batch_number,
            prepare_batch_size,
            prepare_single_sign_percentage,
            parallel_streams,
        } = parameters.client;

        info!("Prepare batch number: {}", prepare_batch_number);
        info!("Prepare batch size: {}", prepare_batch_size);
        info!("Mini batch size: {}", prepare_batch_size / parallel_streams);
        info!(
            "Prepare single sign percentage: {}",
            prepare_single_sign_percentage
        );
        info!("Parallel TCP streams: {}", parallel_streams);
        info!("Individual rate: {}", individual_rate);

        info!("Getting broker keycard");

        let client = RendezvousClient::new(rendezvous.clone(), Default::default());

        let send_period = (prepare_batch_size as f64) / individual_rate as f64;
        let cyclical_batches = (6f64 / send_period).ceil() as usize;
        info!(
            "Send period: {}. Generating enough ids for {} non-interfering batches.",
            send_period, cyclical_batches
        );

        let mut vec_batch_keychains = Vec::new();
        let mut vec_id_assignments = Vec::new();
        for _ in 0..cyclical_batches {
            let (batch_keychains, id_assignments) =
                get_assignments(&client, prepare_batch_size).await?;
            vec_batch_keychains.push(batch_keychains);
            vec_id_assignments.push(id_assignments);
        }

        let prepare_request_batches = (0..(prepare_batch_number as f64 / cyclical_batches as f64)
            .ceil() as usize)
            .map(|height| {
                vec_batch_keychains
                    .iter()
                    .zip(vec_id_assignments.iter())
                    .map(move |(batch_keychains, id_assignments)| {
                        prepare(height as u64, &batch_keychains, &id_assignments)
                    })
            })
            .flatten()
            .take(prepare_batch_number)
            .collect::<Vec<Vec<PrepareRequest>>>();

        let prepare_address = get_prepare_address(&client, broker_address).await?;
        let commit_address = get_commit_address(&client, broker_address).await?;

        let reduction_shard = vec_batch_keychains[0][0]
            .multisign(&ReductionStatement::new(hash::hash::<u32>(&0).unwrap()))
            .unwrap();

        client
            .publish_card(KeyChain::random().keycard(), Some(1))
            .await
            .unwrap();

        let _ = get_shard(&client, 1).await?;

        info!("Synced with other brokers. Making sure IdAssignments are published...");

        time::sleep(Duration::from_secs(10)).await;

        info!("Connecting with brokers...");

        let mut connections_vec: Vec<Vec<(PlainConnection, PlainConnection)>> = (0..10)
            .map(|_| async move {
                get_connections(prepare_address, commit_address, parallel_streams).await
            })
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await;

        time::sleep(Duration::from_secs(10)).await;

        let semaphore = Arc::new(Semaphore::new(parallel_streams));

        let mut handles = Vec::new();

        for (height, batch) in prepare_request_batches.into_iter().enumerate() {
            let connections = connections_vec.pop().unwrap();
            connections_vec.push(get_connections(prepare_address, commit_address, parallel_streams).await);

            let mut permits = Vec::new();
            let mut instant = None;
            for _ in 0..parallel_streams {
                let permit = {
                    if let Ok(permit) = semaphore.clone().try_acquire_owned() {
                        permit
                    } else {
                        instant = Some(Instant::now());
                        semaphore.clone().acquire_owned().await.unwrap()
                    }
                };
                permits.push(permit);
            }

            if let Some(instant) = instant {
                warn!(
                    "Client could not keep up with rate! Delayed for {} ms",
                    instant.elapsed().as_millis()
                );
            }

            let handle = tokio::spawn(async move {
                let mini_batches = batch
                    .chunks_exact(batch.len() / parallel_streams)
                    .map(|chunk| chunk.to_vec())
                    .collect::<Vec<Vec<_>>>();

                info!("Client sending batch for height {}", height);

                let num_mini_batches = mini_batches.len();

                mini_batches
                    .into_iter()
                    .zip(connections.into_iter())
                    .zip(permits)
                    .enumerate()
                    .map(
                        |(
                            num,
                            ((batch, (mut prepare_connection, mut commit_connection)), permit),
                        )| async move {
                            info!(
                                "Client sending mini-batch {}",
                                height * num_mini_batches + num
                            );

                            prepare_connection
                                .send::<Vec<PrepareRequest>>(&batch)
                                .await
                                .unwrap();

                            // let bytes_sent = bincode::serialize(&batch).unwrap().len();
                            // info!("Sent {} bytes", bytes_sent);

                            let inclusions = prepare_connection
                                .receive::<Result<Vec<Inclusion>, PrepareBrokerFailure>>()
                                .await
                                .unwrap()
                                .unwrap();

                            drop(permit);

                            // let bytes_received = bincode::serialize(&inclusions).unwrap().len();
                            // info!("Received {} bytes", bytes_received);

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

                            // let bytes_sent = bincode::serialize(&signatures).unwrap().len();
                            // info!("Sent {} bytes", bytes_sent);

                            let batch_commits = prepare_connection
                                .receive::<Result<Vec<BatchCommit>, PrepareBrokerFailure>>()
                                .await
                                .unwrap()
                                .unwrap();

                            // let bytes_received = bincode::serialize(&batch_commits).unwrap().len();
                            // info!("Received {} bytes", bytes_received);

                            // info!("Got batch commits!");

                            let (commit_requests, payloads): (Vec<CommitRequest>, Vec<Payload>) =
                                batch
                                    .into_par_iter()
                                    .zip(inclusions.into_par_iter())
                                    .zip(batch_commits.into_par_iter())
                                    .map(|((prepare_request, inclusion), commit)| {
                                        let commit_proof =
                                            CommitProof::new(commit, inclusion.proof);

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

                            // info!("Got completion proofs!");

                            let _withdrawals = completion_proofs
                                .into_iter()
                                .zip(payloads.into_iter())
                                .map(|(proof, payload)| Completion::new(proof, payload))
                                .collect::<Vec<_>>();

                            info!(
                                "Client completed mini-batch {}",
                                height * num_mini_batches + num
                            );
                        },
                    )
                    .collect::<FuturesUnordered<_>>()
                    .collect::<Vec<()>>()
                    .await;

                info!("Client completed batch for height {}", height);
            });

            handles.push(handle);

            time::sleep(Duration::from_millis(
                ((1000 * prepare_batch_size) / individual_rate) as u64,
            ))
            .await;
        }

        for handle in handles {
            let _ = handle.await;
        }

        info!("Client done!");

        Ok(Client {})
    }
}

async fn get_connections(
    prepare_address: SocketAddr,
    commit_address: SocketAddr,
    parallel_streams: usize,
) -> Vec<(PlainConnection, PlainConnection)> {
    let mut connections = Vec::new();

    for _ in 0..parallel_streams {
        let stream = TcpStream::connect(prepare_address).await.unwrap();
        let prepare_connection: PlainConnection = stream.into();

        let stream = TcpStream::connect(commit_address).await.unwrap();
        let commit_connection: PlainConnection = stream.into();

        connections.push((prepare_connection, commit_connection));
    }

    connections
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

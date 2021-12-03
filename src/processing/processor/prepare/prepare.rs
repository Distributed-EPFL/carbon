use crate::{
    crypto::Identify,
    database::{
        prepare::{PrepareHandle, State},
        Database,
    },
    discovery::Client,
    prepare::{
        BatchCommitShard, Equivocation, ReductionStatement, SignedBatch, WitnessStatement,
        WitnessedBatch,
    },
    processing::{
        messages::{PrepareRequest, PrepareResponse},
        processor::prepare::errors::ServePrepareError,
        Processor,
    },
    view::View,
};

use doomstack::{here, Doom, ResultExt, Top};

use std::sync::Arc;

use talk::{
    crypto::KeyChain,
    net::{Listener, Session, SessionListener},
    sync::{fuse::Fuse, voidable::Voidable},
};

impl Processor {
    pub(in crate::processing) async fn run_prepare<L>(
        keychain: KeyChain,
        discovery: Arc<Client>,
        view: View,
        database: Arc<Voidable<Database>>,
        listener: L,
    ) where
        L: Listener,
    {
        let mut listener = SessionListener::new(listener);
        let fuse = Fuse::new();

        loop {
            let (_, session) = listener.accept().await;

            let keychain = keychain.clone();
            let discovery = discovery.clone();
            let view = view.clone();
            let database = database.clone();

            fuse.spawn(async move {
                let _ =
                    Processor::serve_prepare(keychain, discovery, view, database, session).await;
            });
        }
    }

    async fn serve_prepare(
        keychain: KeyChain,
        discovery: Arc<Client>,
        view: View,
        database: Arc<Voidable<Database>>,
        mut session: Session,
    ) -> Result<(), Top<ServePrepareError>> {
        let request = session
            .receive::<PrepareRequest>()
            .await
            .pot(ServePrepareError::ConnectionError, here!())?;

        let prepares = match request {
            PrepareRequest::Prepares(batch) => batch,
            _ => return ServePrepareError::UnexpectedRequest.fail().spot(here!()),
        };

        let request = session
            .receive::<PrepareRequest>()
            .await
            .pot(ServePrepareError::ConnectionError, here!())?;

        let batch = match request {
            PrepareRequest::Witness(witness) => {
                WitnessedBatch::new(view.identifier(), prepares, witness)
            }
            PrepareRequest::Signatures(reduction_signature, individual_signatures) => {
                let batch = SignedBatch::new(prepares, reduction_signature, individual_signatures);

                if !batch
                    .prepares()
                    .windows(2)
                    .all(|window| window[0].id() < window[1].id())
                {
                    return ServePrepareError::MalformedBatch.fail().spot(here!());
                }

                let unknown_ids = {
                    let database = database
                        .lock()
                        .pot(ServePrepareError::DatabaseVoid, here!())?;

                    batch
                        .prepares()
                        .iter()
                        .map(|prepare| prepare.id())
                        .filter(|id| !database.assignments.contains_key(&id))
                        .collect::<Vec<_>>()
                };

                if !unknown_ids.is_empty() {
                    session
                        .send(&PrepareResponse::UnknownIds(unknown_ids.clone())) // TODO: Remove unnecessary `clone`
                        .await
                        .pot(ServePrepareError::ConnectionError, here!())?;

                    let request = session
                        .receive::<PrepareRequest>()
                        .await
                        .pot(ServePrepareError::ConnectionError, here!())?;

                    let id_assignments = match request {
                        PrepareRequest::Assignments(id_assignments) => id_assignments,
                        _ => {
                            return ServePrepareError::UnexpectedRequest.fail().spot(here!());
                        }
                    };

                    if id_assignments.len() != unknown_ids.len() {
                        return ServePrepareError::MalformedIdAssignments
                            .fail()
                            .spot(here!());
                    }

                    if !unknown_ids
                        .iter()
                        .zip(id_assignments.iter())
                        .all(|(id, id_assignment)| {
                            id_assignment.id() == *id
                                && id_assignment.validate(discovery.as_ref()).is_ok()
                        })
                    {
                        return ServePrepareError::InvalidIdAssignment.fail().spot(here!());
                    }

                    {
                        let mut database = database
                            .lock()
                            .pot(ServePrepareError::DatabaseVoid, here!())?;

                        for id_assignment in id_assignments {
                            database
                                .assignments
                                .insert(id_assignment.id(), id_assignment);
                        }
                    }
                }

                {
                    let database = database
                        .lock()
                        .pot(ServePrepareError::DatabaseVoid, here!())?;

                    let mut reduction_signers = Vec::new();

                    for (prepare, individual_signature) in
                        batch.prepares().iter().zip(batch.individual_signatures())
                    {
                        let keycard = database.assignments[&prepare.id()].keycard();

                        match individual_signature {
                            Some(signature) => {
                                signature
                                    .verify(&keycard, prepare)
                                    .pot(ServePrepareError::InvalidBatch, here!())?;
                            }
                            None => {
                                reduction_signers.push(keycard);
                            }
                        }
                    }

                    let reduction_statement = ReductionStatement::new(batch.root());

                    batch
                        .reduction_signature()
                        .verify(reduction_signers, &reduction_statement)
                        .pot(ServePrepareError::InvalidBatch, here!())?;
                }

                let witness_statement = WitnessStatement::new(batch.root());
                let witness_shard = keychain.multisign(&witness_statement).unwrap();

                session
                    .send(&PrepareResponse::WitnessShard(witness_shard))
                    .await
                    .pot(ServePrepareError::ConnectionError, here!())?;

                let request = session
                    .receive::<PrepareRequest>()
                    .await
                    .pot(ServePrepareError::ConnectionError, here!())?;

                let witness = match request {
                    PrepareRequest::Witness(witness) => witness,
                    _ => {
                        return ServePrepareError::UnexpectedRequest.fail().spot(here!());
                    }
                };

                let witness_statement = WitnessStatement::new(batch.root());

                witness
                    .verify_plurality(&view, &witness_statement)
                    .pot(ServePrepareError::InvalidWitness, here!())?;

                batch.into_witnessed(view.identifier(), witness)
            }
            _ => return ServePrepareError::UnexpectedRequest.fail().spot(here!()),
        };

        let mut exceptions = Vec::new();
        let root = batch.root();

        {
            let mut database = database
                .lock()
                .pot(ServePrepareError::DatabaseVoid, here!())?;

            for (index, prepare) in batch.prepares().iter().enumerate() {
                let id = prepare.id();
                let height = prepare.height();
                let commitment = prepare.commitment();
                let prepare = PrepareHandle::Batched { batch: root, index };

                let (state, staled) = match database.prepare.states.get(&id) {
                    Some(state) => match state {
                        State::Consistent {
                            height: state_height,
                            commitment: state_commitment,
                            prepare: state_prepare,
                            stale: state_stale,
                        } => {
                            if height == *state_height {
                                if commitment == *state_commitment {
                                    continue;
                                } else {
                                    let state_extract = match state_prepare {
                                        PrepareHandle::Batched {
                                            batch: state_batch,
                                            index: state_index,
                                        } => database
                                            .prepare
                                            .batches
                                            .get(state_batch)
                                            .unwrap()
                                            .extract(*state_index),
                                        PrepareHandle::Standalone(extract) => extract.clone(),
                                    };

                                    let equivocation =
                                        Equivocation::new(batch.extract(index), state_extract);

                                    let state = State::Equivocated(equivocation);
                                    let staled = !state_stale;

                                    (state, staled)
                                }
                            } else {
                                let state = State::Consistent {
                                    height,
                                    commitment,
                                    prepare,
                                    stale: true,
                                };

                                let staled = !state_stale;

                                (state, staled)
                            }
                        }
                        equivocated => (equivocated.clone(), false),
                    },
                    None => {
                        let state = State::Consistent {
                            height,
                            commitment,
                            prepare,
                            stale: true,
                        };

                        let staled = true;

                        (state, staled)
                    }
                };

                if let State::Equivocated(equivocation) = &state {
                    exceptions.push(equivocation.clone());
                }

                database.prepare.states.insert(id, state);

                if staled {
                    database.prepare.stale.insert(id);
                }
            }
        }

        let shard = BatchCommitShard::new(&keychain, view.identifier(), root, exceptions);

        session
            .send(&PrepareResponse::CommitShard(shard))
            .await
            .pot(ServePrepareError::ConnectionError, here!())?;

        todo!()
    }
}

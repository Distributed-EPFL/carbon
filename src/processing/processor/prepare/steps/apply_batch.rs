use buckets::{Buckets, Split};
use log::info;

use crate::{
    account::Id,
    crypto::Identify,
    database::{
        prepare::{BatchHolder, PrepareHandle, State},
        Database,
    },
    prepare::{BatchCommitShard, Equivocation, WitnessedBatch},
    processing::processor::prepare::errors::ServePrepareError,
    view::View,
};

use doomstack::{here, ResultExt, Top};

use std::{collections::{HashMap, HashSet}, time::Instant};

use talk::{
    crypto::{primitives::hash::Hash, KeyChain},
    sync::voidable::Voidable,
};

pub(in crate::processing::processor::prepare) async fn apply_batch(
    keychain: &KeyChain,
    view: &View,
    database: &Voidable<Database>,
    batch: WitnessedBatch,
) -> Result<BatchCommitShard, Top<ServePrepareError>> {
    // Prepare `Split` to feed `database`'s `Buckets`

    // Each element of the `Split` contains an enumerated `Prepare`
    let split = Split::with_key(
        batch.prepares().iter().cloned().enumerate(),
        |(_, prepare)| prepare.id(),
    );

    // Lock `database`, acquire references to its appropriate fields

    let mut database = database
        .lock()
        .pot(ServePrepareError::DatabaseVoid, here!())?;

    let start = Instant::now();

    // This function extracts the appropriate (mutable and immutable) references to
    // `database`'s fields from a mutable reference to `database`. It is unclear
    // whether or not a more compact syntax exists to achieve the same.
    fn fields(
        database: &mut Database,
    ) -> (
        &mut Buckets<HashMap<Id, State>>,
        &mut Buckets<HashSet<Id>>,
        &HashMap<Hash, BatchHolder>,
    ) {
        (
            &mut database.prepare.states,
            &mut database.prepare.stale,
            &database.prepare.batches,
        )
    }

    let (states, stale, batches) = fields(&mut database);

    // The following applies each enumerated `Prepare` in `split` to `states` and
    // `stales`, while attaching immutable references to `batches` and `batch`
    let exceptions = buckets::apply_sparse_attached(
        (states, stale),
        &(batches, &batch),
        split,
        |(states, stale), &(batches, batch), (index, prepare)| {
            // Build `PrepareHandle` relevant to `prepare`
            let handle = PrepareHandle::Batched {
                batch: batch.root(),
                index,
            };

            let state = match states.get(&prepare.id()) {
                Some(state) => match state {
                    State::Consistent {
                        height: state_height,
                        commitment: state_commitment,
                        handle: state_handle,
                    } => {
                        if prepare.height() == *state_height {
                            // A `Prepare` for this `prepare.height()` was previously received.

                            if prepare.commitment() == *state_commitment {
                                // `prepare` does not collide with the previously observed `Prepare`:
                                // `prepare` is valid, and no further update is required
                                return None;
                            } else {
                                // `prepare` collides with a previously observed `Prepare`:
                                // retrieve `Extract` to prove `Equivocation`
                                let state_extract = match state_handle {
                                    PrepareHandle::Batched { batch, index } => {
                                        // `batch` is still in `database`, obtain `Extract` from there
                                        batches.get(batch).unwrap().extract(*index)
                                    }

                                    // The batch was garbage collected, leaving a ready-made `Extract` behind
                                    PrepareHandle::Standalone(extract) => extract.clone(),
                                };

                                // Obtain conflicting `Extract` from `batch`, build `Equivocation`
                                let extract = batch.extract(index);
                                let equivocation = Equivocation::new(extract, state_extract);

                                // State must be updated to reflect the equivocation
                                State::Equivocated(equivocation)
                            }
                        } else {
                            // No `Prepare` was previously observed for this height: initialize
                            // the state to `Consistent`.

                            // (*) Remark: currently, no further check is performed on `prepare.height()`.
                            // In the future, a proof will be optionally provided by the broker to
                            // prove that the client successfully reached `prepare.height() - 1`.
                            //
                            // As a result, the following should apply:
                            //  - If `prepare.height()` is greater than both the highest observed
                            //    `Commit` for `prepare.id()` AND `state_height`, then the `state`
                            //    should be updated as done below.
                            //  - Otherwise, a higher `Commit` should be provided to the broker
                            //    as evidence of misbehaviour / delay, and `prepare.id()` should
                            //    be represented in `exceptions`.

                            State::Consistent {
                                height: prepare.height(),
                                commitment: prepare.commitment(),
                                handle,
                            }
                        }
                    }

                    // `State::Equivocated` is absorbing and must not be updated
                    equivocated => equivocated.clone(),
                },
                None => State::Consistent {
                    // No `Prepare` was previously observed for this height: initialize
                    // the state to `Consistent`

                    // Remark: see above (*)
                    height: prepare.height(),
                    commitment: prepare.commitment(),
                    handle,
                },
            };

            // Extract, if available, the appropriate `Equivocation` from `state`
            let exception = if let State::Equivocated(equivocation) = &state {
                Some(equivocation.clone())
            } else {
                None
            };

            // Update `states`, flag new state in `stale` to allow efficient
            // flushing to `advertisements` (performed immediately before
            // state transfer)
            states.insert(prepare.id(), state);
            stale.insert(prepare.id());

            // If `exception` is `Some`, it is collected in `exceptions`
            exception
        },
    );

    // Store `batch` in `batches`

    let root = batch.root();
    let holder = BatchHolder::new(batch);

    database.prepare.batches.insert(root, holder);

    info!(
        "Prepare: applied batch in {} ms",
        start.elapsed().as_millis()
    );

    // Use `exceptions` to return an appropriate `BatchCommitShard`

    let shard = BatchCommitShard::new(&keychain, view.identifier(), root, exceptions);

    Ok(shard)
}

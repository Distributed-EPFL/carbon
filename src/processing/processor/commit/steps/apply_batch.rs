use buckets::{Buckets, Split};
use log::info;

use crate::{
    account::{Account, Entry, Id, Operation},
    commit::{BatchCompletionShard, Payload, WitnessedBatch},
    crypto::Identify,
    database::{
        commit::{BatchHolder, PayloadHandle},
        Database,
    },
    processing::processor::commit::errors::ServeCommitError,
    view::View,
};

use doomstack::{here, Doom, ResultExt, Top};

use std::{collections::HashMap, time::Instant};

use talk::{crypto::KeyChain, sync::voidable::Voidable};

use zebra::database::TableTransaction;

pub(in crate::processing::processor::commit) async fn apply_batch(
    keychain: &KeyChain,
    view: &View,
    database: &Voidable<Database>,
    batch: WitnessedBatch,
    dependencies: Vec<Option<Operation>>,
) -> Result<BatchCompletionShard, Top<ServeCommitError>> {
    let root = batch.root();

    // Check if `batch` can be applied to `database` (i.e.,
    // every `Entry` in `batch.payloads()` is applicable
    // to the relevant element of `database.accounts`)

    let entries = batch
        .payloads()
        .iter()
        .map(Payload::entry)
        .collect::<Split<_>>();

    let mut start;

    let inapplicable_ids = {
        let mut database = database
            .lock()
            .pot(ServeCommitError::DatabaseVoid, here!())?;

        start = Instant::now();

        buckets::apply_sparse(&mut database.accounts, entries, |accounts, entry| {
            // Fetch `entry.id`'s `Account` (if no operation was previously
            // processed from `entry.id`, initialize an empty `Account`)

            let account = accounts
                .entry(entry.id)
                .or_insert_with(|| Account::new(entry.id, &Default::default())); // TODO: Add settings

            // If `entry.height` is not applicable to `account`, return `entry.id`

            if account.applicable(entry.height) {
                None
            } else {
                Some(entry.id)
            }
        })
    };

    // The whole batch must be applicable in order to be processed

    // Skip this for benchmark
    if false && !inapplicable_ids.is_empty() {
        return ServeCommitError::BatchInapplicable.fail().spot(here!());
    }

    // Zip together in a `Split` an enumeration of `payloads` and `dependencies`

    let applications = Split::with_key(
        batch
            .payloads()
            .iter()
            .cloned()
            .zip(dependencies.iter().cloned())
            .enumerate(),
        |(_, (payload, _))| payload.id(),
    );

    let mut sum_elapsed = start.elapsed().as_millis();

    let flush = {
        let mut database = database
            .lock()
            .pot(ServeCommitError::DatabaseVoid, here!())?;

        start = Instant::now();

        // Apply each `(_, (payload, dependency))` in `applications` to `database.accounts`,
        // then store `payload` in `database.commit.payloads` as a `PayloadHandle`

        fn fields(
            database: &mut Database,
        ) -> (
            &mut Buckets<HashMap<Id, Account>>,
            &mut Buckets<HashMap<Entry, PayloadHandle>>,
        ) {
            (&mut database.accounts, &mut database.commit.payloads)
        }

        let (accounts, payloads) = fields(&mut database);

        let flush = buckets::apply_attached(
            (accounts, payloads),
            &root,
            applications,
            |(accounts, payloads), root, (index, (payload, dependency))| {
                // Apply `(payload, dependency)` to `accounts`

                // All missing accounts where created when checking applicability,
                // so the following `unwrap` is guaranteed to succeed
                let account = accounts.get_mut(&payload.id()).unwrap();

                let exception = if account.apply(
                    &payload,
                    dependency.as_ref(),
                    &Default::default(), // TODO: Add settings
                ) {
                    None
                } else {
                    Some(payload.id())
                };

                let id = payload.id();
                let summary = account.summarize();

                // Store (a reference to) `payload` in `payloads`

                payloads.insert(
                    payload.entry(),
                    PayloadHandle {
                        batch: *root,
                        index,
                    },
                );

                ((id, summary), exception)
            },
        );

        // Store `batch` in `database.commit.batches`

        database
            .commit
            .batches
            .insert(root, BatchHolder::new(batch));

        flush
    }
    .join();

    let mut transaction = TableTransaction::new();

    let exceptions = flush
        .into_iter()
        .filter_map(|((id, summary), exception)| {
            transaction.set(id, summary).unwrap();
            exception
        })
        .collect::<Vec<_>>();

    sum_elapsed += start.elapsed().as_millis();

    {
        let mut database = database
            .lock()
            .pot(ServeCommitError::DatabaseVoid, here!())?;

        start = Instant::now();    

        database.imminent.execute(transaction);
    }

    sum_elapsed += start.elapsed().as_millis();

    info!(
        "Commit: applied batch in {} ms",
        sum_elapsed
    );

    // Sign and return a `BatchCompletionShard` with the appropriate `exceptions`

    let shard = BatchCompletionShard::new(keychain, view.identifier(), root, exceptions);

    Ok(shard)
}

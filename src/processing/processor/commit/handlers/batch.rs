use std::time::Instant;

use crate::{
    commit::Payload,
    database::Database,
    discovery::Client,
    processing::{
        messages::CommitResponse,
        processor::commit::{errors::ServeCommitError, steps},
    },
    view::View,
};

use doomstack::{here, ResultExt, Top};

use log::info;

use talk::{crypto::KeyChain, net::Session, sync::voidable::Voidable};

use zebra::vector::Vector;

pub(in crate::processing::processor::commit) async fn batch(
    keychain: &KeyChain,
    discovery: &Client,
    view: &View,
    database: &Voidable<Database>,
    mut session: Session,
    payloads: Vector<Payload>,
) -> Result<(), Top<ServeCommitError>> {
    // Obtain a `WitnessedBatch`

    let batch =
        steps::witnessed_batch(keychain, discovery, view, database, &mut session, payloads).await?;

    // Retrieve the `Operation` (if any) on which each element of `payloads` depends. If any
    // `Operation` cannot be retrieved directly from a completed `WitnessedBatch` in `database`,
    // query `session` for the necessary `Completion`s.

    let dependencies = steps::fetch_dependencies(discovery, database, &mut session, &batch).await?;

    // Apply `batch` to `database` to obtain a `BatchCompletionShard`

    let shard = steps::apply_batch(keychain, view, database, batch, dependencies).await?;

    // Send `shard` and end `session`

    session
        .send(&CommitResponse::CompletionShard(shard))
        .await
        .pot(ServeCommitError::ConnectionError, here!())?;

    session.end();

    Ok(())
}

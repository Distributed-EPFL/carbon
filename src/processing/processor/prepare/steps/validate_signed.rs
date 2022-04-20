use std::time::Instant;

use crate::{
    database::Database,
    discovery::Client,
    prepare::{ReductionStatement, SignedBatch, WitnessStatement},
    processing::processor::prepare::{errors::ServePrepareError, steps},
};

use doomstack::{here, Doom, ResultExt, Top};

use log::info;
use rayon::prelude::*;

use talk::{
    crypto::{primitives::multi::Signature as MultiSignature, KeyCard, KeyChain},
    net::Session,
    sync::voidable::Voidable,
};

pub(in crate::processing::processor::prepare) async fn validate_signed(
    keychain: &KeyChain,
    discovery: &Client,
    database: &Voidable<Database>,
    session: &mut Session,
    batch: &SignedBatch,
) -> Result<MultiSignature, Top<ServePrepareError>> {
    // Verify that `batch.prepares()` is strictly increasing by `Id`
    // (this ensures searchability and non-duplication of `Id`s)
    if !batch
        .prepares()
        .windows(2)
        .all(|window| window[0].id() < window[1].id())
    {
        return ServePrepareError::MalformedBatch.fail().spot(here!());
    }

    // Retrieve the `KeyCard` relevant to each of the elements of `batch.prepares()`.
    // If any `KeyCard` is missing from `database`, query `session` for the necessary
    // `IdAssignment`s (store in `database` all newly discovered `IdAssignments`).

    let keycards = steps::fetch_keycards(discovery, database, session, batch).await?;

    // Check all individual signatures in `batch` while collecting signers to
    // `batch`'s reduction statement

    // `steps` zips together corresponding `KeyCard`s, `Prepare`s and individual
    // `Signature`'s from `keycards` and `batch`
    let steps = keycards.par_iter().zip(
        batch
            .prepares()
            .par_iter()
            .zip(batch.individual_signatures()),
    );

    let start = Instant::now();

    // Map and collect each element of `steps` into an optional reduction signer
    let reduction_signers = steps
        .map(
            |(keycard, (prepare, individual_signature))| match individual_signature {
                Some(signature) => {
                    let _ = signature
                        .verify(&keycard, prepare)
                        .pot(ServePrepareError::InvalidBatch, here!()); // Accept all signatures

                    Ok(None)
                }
                None => Ok(Some(keycard)),
            },
        )
        .collect::<Result<Vec<Option<&KeyCard>>, Top<ServePrepareError>>>()?;

    info!(
        "Prepare: validated individual batch signatures in {} ms",
        start.elapsed().as_millis()
    );

    let start = Instant::now();

    // Select all `Some` `reduction_signers`
    let reduction_signers = reduction_signers
        .into_iter()
        .filter_map(|signer| signer)
        .collect::<Vec<_>>();

    info!("Number of signers: {}", reduction_signers.len());

    if let Some(reduction_signature) = batch.reduction_signature() {
        // Verify `batch`'s reduction statement against `reduction_signers`
        info!("Batch root: {:?}", batch.root());
    
        let reduction_statement = ReductionStatement::new(batch.root());
    
        let _ = reduction_signature
            .verify(reduction_signers, &reduction_statement)
            .pot(ServePrepareError::InvalidBatch, here!()); // Accept all signatures
    
        info!(
            "Prepare: validated batch multisignature in {} ms",
            start.elapsed().as_millis()
        );
    }


    // `batch` is valid, generate and return witness shard

    let witness_statement = WitnessStatement::new(batch.root());
    let witness_shard = keychain.multisign(&witness_statement).unwrap();

    Ok(witness_shard)
}

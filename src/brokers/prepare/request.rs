use crate::{discovery::Client, prepare::Prepare, signup::IdAssignment};

use doomstack::{here, Doom, ResultExt, Top};

use serde::{Deserialize, Serialize};

use talk::crypto::{
    primitives::{hash::Hash, sign::Signature},
    KeyChain,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Request {
    assignment: IdAssignment,
    prepare: Prepare,
    signature: Signature,
}

#[derive(Doom)]
pub(crate) enum RequestError {
    #[doom(description("Assignment invalid"))]
    AssignmentInvalid,
    #[doom(description("Signature invalid"))]
    SignatureInvalid,
}

impl Request {
    pub fn new(
        keychain: &KeyChain,
        assignment: IdAssignment,
        height: u64,
        commitment: Hash,
    ) -> Self {
        let prepare = Prepare::new(assignment.id(), height, commitment);
        let signature = keychain.sign(&prepare).unwrap();

        Request {
            assignment,
            prepare,
            signature,
        }
    }

    pub fn validate(&self, discovery: &Client) -> Result<(), Top<RequestError>> {
        self.assignment
            .validate(&discovery)
            .pot(RequestError::AssignmentInvalid, here!())?;

        self.signature
            .verify(&self.assignment.keycard(), &self.prepare)
            .pot(RequestError::SignatureInvalid, here!())?;

        Ok(())
    }
}
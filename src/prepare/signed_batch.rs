use crate::{
    crypto::Certificate,
    prepare::{Prepare, WitnessedBatch},
};

use serde::{Deserialize, Serialize};

use talk::crypto::primitives::{hash::Hash, multi::Signature as MultiSignature, sign::Signature};

use zebra::vector::Vector;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SignedBatch {
    prepares: Vector<Prepare>,
    reduction_signature: Option<MultiSignature>,
    individual_signatures: Vec<Option<Signature>>,
}

impl SignedBatch {
    pub fn new(
        prepares: Vector<Prepare>,
        reduction_signature: Option<MultiSignature>,
        individual_signatures: Vec<Option<Signature>>,
    ) -> Self {
        SignedBatch {
            prepares,
            reduction_signature,
            individual_signatures,
        }
    }

    pub fn root(&self) -> Hash {
        self.prepares.root()
    }

    pub fn prepares(&self) -> &[Prepare] {
        self.prepares.items()
    }

    pub fn reduction_signature(&self) -> Option<MultiSignature> {
        self.reduction_signature
    }

    pub fn individual_signatures(&self) -> &[Option<Signature>] {
        self.individual_signatures.as_slice()
    }

    pub fn into_witnessed(self, view: Hash, witness: Certificate) -> WitnessedBatch {
        WitnessedBatch::new(view, self.prepares, witness)
    }
}

use crate::{
    crypto::Certificate,
    prepare::{Prepare, WitnessedBatch},
};

use serde::{Deserialize, Serialize};

use talk::crypto::primitives::{hash::Hash, multi::Signature as MultiSignature, sign::Signature};

use zebra::vector::Vector;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Batch {
    prepares: Vector<Prepare>,
    reduction_signature: MultiSignature,
    individual_signatures: Vec<Option<Signature>>,
}

impl Batch {
    pub fn new(
        prepares: Vector<Prepare>,
        reduction_signature: MultiSignature,
        individual_signatures: Vec<Option<Signature>>,
    ) -> Self {
        Batch {
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

    pub fn reduction_signature(&self) -> MultiSignature {
        self.reduction_signature
    }

    pub fn individual_signatures(&self) -> &[Option<Signature>] {
        self.individual_signatures.as_slice()
    }

    pub fn witness(self, witness: Certificate) -> WitnessedBatch {
        WitnessedBatch::new(self.prepares, witness)
    }
}

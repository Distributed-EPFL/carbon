use crate::{prepare::Prepare, processing::messages::PrepareRequest, signup::IdAssignment};

use talk::crypto::primitives::{hash::Hash, multi::Signature as MultiSignature, sign::Signature};

use zebra::vector::Vector;

pub(in crate::brokers::prepare) struct Submission {
    assignments: Vec<IdAssignment>,
    pub requests: Requests,
}

pub(in crate::brokers::prepare) struct Requests {
    batch: PrepareRequest,
    signatures: PrepareRequest,
}

// `Submission` stores the minimal amount of information necessary to make
// immutably available:
//  (1) All `PrepareRequest`s that are relevant to a batch and computable
//      a priori of any replica exchange.
//  (2) All information relevant to the batch, such as its root and `Prepare`s.
// In order to do so, `Submission` wraps a `Vector<Prepare>` into a
// `PrepareRequest::Batch`, but still accesses its immutable reference (via
// stubbed `match`es) to extract and make available the items of (2).
impl Submission {
    pub fn new(
        assignments: Vec<IdAssignment>,
        prepares: Vector<Prepare>,
        reduction_signature: Option<MultiSignature>,
        individual_signatures: Vec<Option<Signature>>,
    ) -> Self {
        Submission {
            assignments,
            requests: Requests {
                batch: PrepareRequest::Batch(prepares),
                signatures: PrepareRequest::Signatures(reduction_signature, individual_signatures),
            },
        }
    }

    pub fn root(&self) -> Hash {
        self.requests.prepares().root()
    }

    pub fn assignments(&self) -> &[IdAssignment] {
        self.assignments.as_slice()
    }

    pub fn prepares(&self) -> &[Prepare] {
        self.requests.prepares().items()
    }
}

impl Requests {
    pub fn batch(&self) -> &PrepareRequest {
        &self.batch
    }

    pub fn signatures(&self) -> &PrepareRequest {
        &self.signatures
    }

    // Extracts a reference to the `Vector<Prepare>` underlying `self.batch`
    fn prepares(&self) -> &Vector<Prepare> {
        match &self.batch {
            PrepareRequest::Batch(prepares) => prepares,
            _ => unreachable!(),
        }
    }
}

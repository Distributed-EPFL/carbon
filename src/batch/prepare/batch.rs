use crate::{batch::prepare::Attachment, broadcast::Prepare};

use talk::crypto::primitives::multi::Signature as MultiSignature;

use zebra::vector::Vector;

pub(crate) struct Batch {
    prepares: Vector<Prepare>,
    multisignature: MultiSignature,
    attachments: Vec<Attachment>,
}
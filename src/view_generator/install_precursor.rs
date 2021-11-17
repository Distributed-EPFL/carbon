use crate::{
    crypto::{Certificate, Identify},
    view_generator::SequenceLatticeBrief,
};

use serde::{Deserialize, Serialize};

use talk::crypto::primitives::hash::Hash;

#[derive(Clone, Serialize, Deserialize)]
pub(in crate::view_generator) struct InstallPrecursor {
    pub decisions: Vec<SequenceLatticeBrief>,
    pub certificate: Certificate,
}

impl Identify for InstallPrecursor {
    fn identifier(&self) -> Hash {
        self.decisions.identifier()
    }
}

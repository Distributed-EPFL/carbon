use crate::{
    crypto::Aggregator,
    lattice::{
        lattice_runner::{CertificationDatabase, Decision, State},
        messages::CertificationRequest,
        Element as LatticeElement, Instance as LatticeInstance, LatticeRunner, Message,
    },
};

use std::collections::BTreeSet;

use talk::broadcast::BestEffort;
use talk::crypto::primitives::hash::Hash;
use talk::sync::fuse::Fuse;

impl<Instance, Element> LatticeRunner<Instance, Element>
where
    Instance: LatticeInstance,
    Element: LatticeElement,
{
    pub(in crate::lattice::lattice_runner) fn certify(&mut self, elements: BTreeSet<Hash>) {
        let decision = Decision {
            view: self.view.identifier(),
            instance: self.instance.clone(),
            elements,
        };

        let identifier = decision.identifier();

        let aggregator = Aggregator::new(self.view.clone(), decision.clone());

        let message = CertificationRequest { decision };

        let broadcast = BestEffort::new(
            self.sender.clone(),
            self.members.keys().cloned(),
            Message::CertificationRequest(message),
            self.settings.broadcast.clone(),
        );

        let fuse = Fuse::new();
        broadcast.spawn(&fuse);

        let certification_database = CertificationDatabase {
            identifier,
            aggregator,
            fuse,
        };

        self.database.certification = Some(certification_database);
    }

    pub(in crate::lattice::lattice_runner) fn decide(&mut self) {
        self.state = State::Decided;

        let (decision, certificate) = self
            .database
            .certification
            .take()
            .unwrap()
            .aggregator
            .finalize_quorum();

        let elements = decision
            .elements
            .iter()
            .map(|element| self.database.elements.get(element).unwrap())
            .cloned()
            .collect::<Vec<_>>();

        let _ = self
            .decision_inlet
            .take()
            .unwrap()
            .send((elements, certificate));
    }
}

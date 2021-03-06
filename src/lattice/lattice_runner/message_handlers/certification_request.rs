use crate::{
    crypto::Identify,
    lattice::{
        message::Message,
        messages::{CertificationConfirmation, CertificationRequest, CertificationUpdate},
        Decision, Element as LatticeElement, Instance as LatticeInstance, LatticeRunner,
        MessageError,
    },
};

use doomstack::{Doom, Top};

use std::collections::BTreeSet;

use talk::{crypto::KeyCard, unicast::Acknowledger};

impl<Instance, Element> LatticeRunner<Instance, Element>
where
    Instance: LatticeInstance,
    Element: LatticeElement,
{
    pub(in crate::lattice::lattice_runner) fn validate_certification_request(
        &self,
        _source: &KeyCard,
        message: &CertificationRequest,
    ) -> Result<(), Top<MessageError>> {
        if message.elements.is_empty() {
            return MessageError::EmptyCertificationRequest.fail();
        }

        if !message.elements.is_subset(&self.database.safe_set) {
            return MessageError::UnsafeElement.fail();
        }

        Ok(())
    }

    pub(in crate::lattice::lattice_runner) fn process_certification_request(
        &mut self,
        source: &KeyCard,
        message: CertificationRequest,
        acknowledger: Acknowledger,
    ) {
        acknowledger.strong();

        if message.elements.is_superset(&self.database.accepted_set) {
            let identifier = message.elements.identifier();

            let decision = Decision {
                view: self.view.identifier(),
                instance: self.instance.clone(),
                elements: message.elements.clone(),
            };

            let signature = self.keychain.multisign(&decision).unwrap();

            let message = CertificationConfirmation {
                identifier,
                signature,
            };

            self.sender.spawn_push(
                source.identity(),
                Message::CertificationConfirmation(message),
                self.configuration.response.clone(),
                &self.fuse,
            );
        } else {
            let identifier = message.elements.identifier();

            let differences = self
                .database
                .accepted_set
                .difference(&message.elements)
                .cloned()
                .collect::<BTreeSet<_>>();

            let message = CertificationUpdate {
                identifier,
                differences,
            };

            self.sender.spawn_push(
                source.identity(),
                Message::CertificationUpdate(message),
                self.configuration.response.clone(),
                &self.fuse,
            );
        }

        self.database.accepted_set = self
            .database
            .accepted_set
            .union(&message.elements)
            .cloned()
            .collect::<BTreeSet<_>>();
    }
}

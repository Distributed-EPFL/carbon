use crate::{
    crypto::{Aggregator, Certificate},
    discovery::Client,
    lattice::{
        Decision, Element as LatticeElement, Instance as LatticeInstance, Message, MessageError,
    },
    view::View,
};

use doomstack::{here, Doom, ResultExt, Top};

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::Arc,
};

use talk::{
    broadcast::BestEffortSettings,
    crypto::{primitives::hash::Hash, Identity, KeyCard, KeyChain},
    sync::fuse::Fuse,
    unicast::{Acknowledgement, Acknowledger, PartialPushSettings, PushSettings, Receiver, Sender},
};

use tokio::sync::oneshot::{Receiver as OneshotReceiver, Sender as OneshotSender};

type ProposalInlet<Element> = OneshotSender<(Element, ResultInlet)>;
type ProposalOutlet<Element> = OneshotReceiver<(Element, ResultInlet)>;

type ResultInlet = OneshotSender<bool>;
type ResultOutlet = OneshotReceiver<bool>;

type DecisionInlet<Element> = OneshotSender<(Vec<Element>, Certificate)>;
type DecisionOutlet<Element> = OneshotReceiver<(Vec<Element>, Certificate)>;

pub(in crate::lattice) struct LatticeRunner<Instance: LatticeInstance, Element: LatticeElement> {
    view: View,
    instance: Instance,

    keychain: KeyChain,

    state: State,
    database: Database<Instance, Element>,

    discovery: Arc<Client>,
    sender: Sender<Message<Element>>,
    receiver: Receiver<Message<Element>>,

    proposal_outlet: ProposalOutlet<Element>,
    decision_inlet: Option<DecisionInlet<Element>>,

    configuration: Configuration,
    fuse: Fuse,
}

#[derive(PartialEq, Eq)]
pub(in crate::lattice) enum State {
    Disclosing,
    Proposing,
    Decided,
}

struct Database<Instance: LatticeInstance, Element: LatticeElement> {
    disclosure: DisclosureDatabase,
    certification: Option<CertificationDatabase<Instance>>,

    elements: HashMap<Hash, Element>,

    disclosures: usize,
    safe_set: BTreeSet<Hash>,

    proposed_set: BTreeSet<Hash>,
    accepted_set: BTreeSet<Hash>,
}

struct DisclosureDatabase {
    // `true` iff the local replica disclosed a value
    disclosed: bool,

    // origin is in `echoes_sent` iff the local replica issued an echo message
    // for _any_ message from origin
    echoes_sent: HashSet<Identity>,

    // (source, origin) is in `echoes_collected` iff the local replica
    // received an echo from source for _any_ message from origin
    echoes_collected: HashSet<(Identity, Identity)>,

    // (origin, identifier) -> number of distinct echoes received
    // (must be at least `self.view.quorum()` to issue a ready message)
    echo_support: HashMap<(Identity, Hash), usize>,

    // origin is in `ready_sent` iff the local replica issued a ready message
    // for _any_ message from origin
    ready_sent: HashSet<Identity>,

    // (source, origin) is in `ready_collected` iff the local replica
    // received a ready message from source for _any_ message from origin
    ready_collected: HashSet<(Identity, Identity)>,

    // (origin, identifier) -> number of distinct ready messages received
    // (must be at least `self.view.plurality()` to issue a ready message)
    // (must be at least `self.view.quorum()` to deliver)
    ready_support: HashMap<(Identity, Hash), usize>,

    // origin is in `disclosures_delivered` iff the local replica has delivered
    // (the only possible) disclosure from origin
    delivered: HashSet<Identity>,
}

pub(in crate::lattice) struct CertificationDatabase<Instance: LatticeInstance> {
    identifier: Hash,
    aggregator: Aggregator<Decision<Instance>>,
    fuse: Fuse,
}

struct Configuration {
    broadcast: BestEffortSettings,
    response: PushSettings,
}

#[derive(Doom)]
enum HandleError {
    #[doom(description("Message from a source foreign to the `View`"))]
    ForeignSource,
    #[doom(description("Invalid message"))]
    InvalidMessage,
}

impl<Instance, Element> LatticeRunner<Instance, Element>
where
    Instance: LatticeInstance,
    Element: LatticeElement,
{
    pub fn new(
        view: View,
        instance: Instance,
        keychain: KeyChain,
        discovery: Arc<Client>,
        sender: Sender<Message<Element>>,
        receiver: Receiver<Message<Element>>,
        proposal_outlet: ProposalOutlet<Element>,
        decision_inlet: DecisionInlet<Element>,
        push_settings: PartialPushSettings,
    ) -> Self {
        let state = State::Disclosing;

        let database = Database {
            disclosure: DisclosureDatabase {
                disclosed: false,
                echoes_sent: HashSet::new(),
                echoes_collected: HashSet::new(),
                echo_support: HashMap::new(),
                ready_sent: HashSet::new(),
                ready_collected: HashSet::new(),
                ready_support: HashMap::new(),
                delivered: HashSet::new(),
            },

            certification: None,

            elements: HashMap::new(),

            disclosures: 0,
            safe_set: BTreeSet::new(),

            proposed_set: BTreeSet::new(),
            accepted_set: BTreeSet::new(),
        };

        let configuration = Configuration {
            broadcast: BestEffortSettings {
                push_settings: PushSettings::compose(
                    Acknowledgement::Strong,
                    push_settings.clone(),
                ),
            },
            response: PushSettings::compose(Acknowledgement::Weak, push_settings),
        };

        let fuse = Fuse::new();

        LatticeRunner {
            view,
            instance,
            keychain,
            state,
            database,
            discovery,
            sender,
            receiver,
            proposal_outlet,
            decision_inlet: Some(decision_inlet),
            configuration,
            fuse,
        }
    }

    pub async fn run(&mut self) {
        let mut proposed = false;

        loop {
            tokio::select! {
                Ok((proposal, result_inlet)) = &mut self.proposal_outlet, if !proposed => {
                    proposed = true;
                    self.handle_proposal(proposal, result_inlet);
                }

                (source, message, acknowledger) = self.receiver.receive() => {
                    let _ = self.handle_message(source, message, acknowledger);
                }
            }
        }
    }

    fn handle_proposal(&mut self, proposal: Element, result_inlet: ResultInlet) {
        if !self.disclosed() {
            self.disclose(proposal);
            let _ = result_inlet.send(true);
        } else {
            let _ = result_inlet.send(false);
        }
    }

    fn handle_message(
        &mut self,
        source: Identity,
        message: Message<Element>,
        acknowledger: Acknowledger,
    ) -> Result<(), Top<HandleError>> {
        if let Some(keycard) = self.view.members().get(&source).cloned() {
            self.validate_message(&keycard, &message)
                .pot(HandleError::InvalidMessage, here!())?;

            self.process_message(&keycard, message, acknowledger);

            Ok(())
        } else {
            HandleError::ForeignSource.fail().spot(here!())
        }
    }

    fn validate_message(
        &self,
        source: &KeyCard,
        message: &Message<Element>,
    ) -> Result<(), Top<MessageError>> {
        match message {
            Message::DisclosureSend(message) => self.validate_disclosure_send(source, message),
            Message::DisclosureEcho(message) => self.validate_disclosure_echo(source, message),
            Message::DisclosureReady(message) => self.validate_disclosure_ready(source, message),
            Message::CertificationRequest(message) => {
                self.validate_certification_request(source, message)
            }
            Message::CertificationConfirmation(message) => {
                self.validate_certification_confirmation(source, message)
            }
            Message::CertificationUpdate(message) => {
                self.validate_certification_update(source, message)
            }
        }
    }

    fn process_message(
        &mut self,
        source: &KeyCard,
        message: Message<Element>,
        acknowledger: Acknowledger,
    ) {
        match message {
            Message::DisclosureSend(message) => {
                self.process_disclosure_send(source, message, acknowledger);
            }
            Message::DisclosureEcho(message) => {
                self.process_disclosure_echo(source, message, acknowledger);
            }
            Message::DisclosureReady(message) => {
                self.process_disclosure_ready(source, message, acknowledger);
            }
            Message::CertificationRequest(message) => {
                self.process_certification_request(source, message, acknowledger);
            }
            Message::CertificationConfirmation(message) => {
                self.process_certification_confirmation(source, message, acknowledger);
            }
            Message::CertificationUpdate(message) => {
                self.process_certification_update(source, message, acknowledger);
            }
        }
    }
}

// Implementations

mod certification;
mod disclosure;
mod message_handlers;

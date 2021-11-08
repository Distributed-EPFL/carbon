use crate::{
    discovery::Client,
    lattice::{
        messages::DisclosureSend, Element as LatticeElement, Instance as LatticeInstance, Message,
        MessageError,
    },
    view::View,
};

use doomstack::{here, Doom, ResultExt, Top};

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use talk::crypto::Identity;
use talk::crypto::{KeyCard, KeyChain};
use talk::sync::fuse::Fuse;
use talk::unicast::{Acknowledgement, Acknowledger, PushSettings, Receiver, Sender};
use talk::{broadcast::BestEffortSettings, crypto::primitives::hash::Hash};

use tokio::sync::oneshot::{Receiver as OneshotReceiver, Sender as OneshotSender};

type ProposalInlet<Element> = OneshotSender<(Element, ResultInlet)>;
type ProposalOutlet<Element> = OneshotReceiver<(Element, ResultInlet)>;

type ResultInlet = OneshotSender<bool>;
type ResultOutlet = OneshotReceiver<bool>;

pub(in crate::lattice) struct LatticeRunner<Instance: LatticeInstance, Element: LatticeElement> {
    view: View,
    instance: Instance,

    members: HashMap<Identity, KeyCard>,

    keychain: KeyChain,
    database: Database<Instance, Element>,

    discovery: Arc<Client>,
    sender: Sender<Message<Instance, Element>>,
    receiver: Receiver<Message<Instance, Element>>,

    proposal_outlet: ProposalOutlet<Element>,

    settings: Settings,
    fuse: Fuse,
}

struct Database<Instance: LatticeInstance, Element: LatticeElement> {
    safe_elements: HashMap<Hash, Element>,
    disclosure: DisclosureDatabase<Instance, Element>,
}

struct DisclosureDatabase<Instance: LatticeInstance, Element: LatticeElement> {
    // `true` iff the local replica disclosed a value
    disclosed: bool,

    // (origin of the message, identifier of disclosure) -> signed send message
    disclosures_received: HashMap<(Identity, Hash), DisclosureSend<Instance, Element>>,

    // origin -> identifier of disclosure the local replica echoed
    echoes_sent: HashMap<Identity, Hash>,

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
    disclosures_delivered: HashSet<Identity>,
}

struct Settings {
    broadcast: BestEffortSettings,
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
        sender: Sender<Message<Instance, Element>>,
        receiver: Receiver<Message<Instance, Element>>,
        proposal_outlet: ProposalOutlet<Element>,
    ) -> Self {
        let members = view
            .members()
            .iter()
            .cloned()
            .map(|keycard| (keycard.identity(), keycard))
            .collect();

        let database = Database {
            safe_elements: HashMap::new(),
            disclosure: DisclosureDatabase {
                disclosed: false,
                disclosures_received: HashMap::new(),
                echoes_sent: HashMap::new(),
                echoes_collected: HashSet::new(),
                echo_support: HashMap::new(),
                ready_sent: HashSet::new(),
                ready_collected: HashSet::new(),
                ready_support: HashMap::new(),
                disclosures_delivered: HashSet::new(),
            },
        };

        // TODO: Forward variable settings
        let settings = Settings {
            broadcast: BestEffortSettings {
                push_settings: PushSettings {
                    stop_condition: Acknowledgement::Strong,
                    ..Default::default()
                },
            },
        };

        let fuse = Fuse::new();

        LatticeRunner {
            view,
            instance,
            members,
            keychain,
            database,
            discovery,
            sender,
            receiver,
            proposal_outlet,
            settings,
            fuse,
        }
    }

    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                Ok((proposal, result_inlet)) = &mut self.proposal_outlet => {
                    self.handle_proposal(proposal, result_inlet).await;
                }

                (source, message, acknowledger) = self.receiver.receive() => {
                    let _ = self.handle_message(source, message, acknowledger).await;
                }
            }
        }
    }

    async fn handle_proposal(&mut self, proposal: Element, result_inlet: ResultInlet) {
        if !self.disclosed() {
            self.disclose(proposal);
            let _ = result_inlet.send(true);
        } else {
            let _ = result_inlet.send(false);
        }
    }

    async fn handle_message(
        &mut self,
        source: Identity,
        message: Message<Instance, Element>,
        acknowledger: Acknowledger,
    ) -> Result<(), Top<HandleError>> {
        if let Some(keycard) = self.members.get(&source).cloned() {
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
        message: &Message<Instance, Element>,
    ) -> Result<(), Top<MessageError>> {
        match message {
            Message::DisclosureSend(message) => self.validate_disclosure_send(source, message),
            Message::DisclosureEcho(message) => self.validate_disclosure_echo(source, message),
            Message::DisclosureReady(message) => self.validate_disclosure_ready(source, message),
        }
    }

    fn process_message(
        &mut self,
        source: &KeyCard,
        message: Message<Instance, Element>,
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
        }
    }
}

// Implementations

mod disclosure;
mod message_handlers;
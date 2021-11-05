use crate::{
    lattice::{messages::DisclosureSend, statements::Disclosure, LatticeElement, Message},
    view::View,
};

use std::collections::{HashMap, HashSet};

use talk::crypto::Identity;
use talk::crypto::{KeyCard, KeyChain};
use talk::sync::fuse::Fuse;
use talk::unicast::{
    Acknowledgement, Acknowledger, Message as UnicastMessage, PushSettings, Receiver, Sender,
};
use talk::{broadcast::BestEffort, crypto::primitives::hash};
use talk::{broadcast::BestEffortSettings, crypto::primitives::hash::Hash};

use tokio::sync::oneshot::{Receiver as OneshotReceiver, Sender as OneshotSender};

type ProposalInlet<Element> = OneshotSender<(Element, ResultInlet)>;
type ProposalOutlet<Element> = OneshotReceiver<(Element, ResultInlet)>;

type ResultInlet = OneshotSender<bool>;
type ResultOutlet = OneshotReceiver<bool>;

pub(in crate::lattice) struct LatticeRunner<
    Instance: UnicastMessage + Clone,
    Element: LatticeElement,
> {
    view: View,
    instance: Instance,

    members: HashSet<Identity>,

    keychain: KeyChain,
    database: Database<Instance, Element>,

    sender: Sender<Message<Instance, Element>>,
    receiver: Receiver<Message<Instance, Element>>,

    proposal_outlet: ProposalOutlet<Element>,

    settings: Settings,
    fuse: Fuse,
}

struct Database<Instance: UnicastMessage + Clone, Element: LatticeElement> {
    safe_elements: HashMap<Hash, Element>,
    disclosure: DisclosureDatabase<Instance, Element>,
}

struct DisclosureDatabase<Instance: UnicastMessage + Clone, Element: LatticeElement> {
    disclosed: bool,
    disclosures: HashMap<(Identity, Hash), DisclosureSend<Instance, Element>>,
}

struct Settings {
    broadcast: BestEffortSettings,
}

impl<Instance, Element> LatticeRunner<Instance, Element>
where
    Instance: UnicastMessage + Clone,
    Element: LatticeElement,
{
    pub fn new(
        view: View,
        instance: Instance,
        keychain: KeyChain,
        sender: Sender<Message<Instance, Element>>,
        receiver: Receiver<Message<Instance, Element>>,
        proposal_outlet: ProposalOutlet<Element>,
    ) -> Self {
        let members = view.members().iter().map(KeyCard::identity).collect();

        let database = Database {
            safe_elements: HashMap::new(),
            disclosure: DisclosureDatabase {
                disclosed: false,
                disclosures: HashMap::new(),
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
                    self.handle_message(source, message, acknowledger).await
                }
            }
        }
    }

    async fn handle_proposal(&mut self, proposal: Element, result_inlet: ResultInlet) {
        if !self.disclosed() {
            self.disclose(proposal).await;
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
    ) {
        if !self.members.contains(&source) {
            // Foreign source
            return;
        }
    }

    fn disclosed(&self) -> bool {
        self.database.disclosure.disclosed
    }

    async fn disclose(&mut self, proposal: Element) {
        self.database.disclosure.disclosed = true;

        self.database
            .safe_elements
            .insert(hash::hash(&proposal).unwrap(), proposal.clone());

        let disclosure = Disclosure {
            view: self.view.identifier(),
            instance: self.instance.clone(),
            element: proposal,
        };

        let signature = self.keychain.sign(&disclosure).unwrap();

        let disclosure_send = DisclosureSend {
            disclosure,
            signature,
        };

        let message = Message::DisclosureSend(disclosure_send);

        let broadcast = BestEffort::new(
            self.sender.clone(),
            self.members.iter().cloned(),
            message,
            self.settings.broadcast.clone(),
        );

        broadcast.spawn(&self.fuse);
    }
}

use crate::{
    crypto::Certificate,
    discovery::Client,
    lattice::{
        Element as LatticeElement, Instance as LatticeInstance, LatticeAgreementSettings,
        LatticeRunner, Message,
    },
    view::View,
};

use doomstack::{Doom, Top};

use std::sync::Arc;

use talk::{
    crypto::KeyChain,
    net::{Connector, Listener},
    sync::fuse::Fuse,
    unicast::{Receiver, Sender},
};

use tokio::sync::{
    oneshot,
    oneshot::{Receiver as OneshotReceiver, Sender as OneshotSender},
};

type ProposalInlet<Element> = OneshotSender<(Element, ResultInlet)>;
type ProposalOutlet<Element> = OneshotReceiver<(Element, ResultInlet)>;

type ResultInlet = OneshotSender<bool>;
type ResultOutlet = OneshotReceiver<bool>;

type DecisionInlet<Element> = OneshotSender<(Vec<Element>, Certificate)>;
type DecisionOutlet<Element> = OneshotReceiver<(Vec<Element>, Certificate)>;

pub(crate) struct LatticeAgreement<Instance: LatticeInstance, Element: LatticeElement> {
    instance: Instance,
    proposal_inlet: Option<ProposalInlet<Element>>,
    decision_outlet: DecisionOutlet<Element>,
    _fuse: Fuse,
}

#[derive(Doom)]
pub(crate) enum LatticeAgreementError {
    #[doom(description("Proposal superseded"))]
    ProposalSuperseded,
}

impl<Instance, Element> LatticeAgreement<Instance, Element>
where
    Instance: LatticeInstance,
    Element: LatticeElement,
{
    pub fn new<C, L>(
        view: View,
        instance: Instance,
        keychain: KeyChain,
        discovery: Arc<Client>,
        connector: C,
        listener: L,
        settings: LatticeAgreementSettings,
    ) -> Self
    where
        C: Connector,
        L: Listener,
    {
        let sender: Sender<Message<Element>> = Sender::new(connector, settings.sender_settings);

        let receiver: Receiver<Message<Element>> =
            Receiver::new(listener, settings.receiver_settings);

        let (proposal_inlet, proposal_outlet) = oneshot::channel();
        let (decision_inlet, decision_outlet) = oneshot::channel();

        let fuse = Fuse::new();

        {
            let instance = instance.clone();
            let mut runner = LatticeRunner::new(
                view,
                instance,
                keychain,
                discovery,
                sender,
                receiver,
                proposal_outlet,
                decision_inlet,
                settings.push_settings,
            );

            fuse.spawn(async move {
                let _ = runner.run().await;
            });
        }

        LatticeAgreement {
            instance,
            proposal_inlet: Some(proposal_inlet),
            decision_outlet: decision_outlet,
            _fuse: fuse,
        }
    }

    pub async fn propose(&mut self, element: Element) -> Result<(), Top<LatticeAgreementError>> {
        let proposal_inlet = self
            .proposal_inlet
            .take()
            .expect("called `LatticeAgreement::propose` more than once");

        let (result_inlet, result_outlet) = oneshot::channel();

        let _ = proposal_inlet.send((element, result_inlet));

        // This cannot fail as the corresponding `result_inlet` is
        // sent to `run`, which keeps running for as long as
        // `self` exists
        if result_outlet.await.unwrap() {
            Ok(())
        } else {
            LatticeAgreementError::ProposalSuperseded.fail()
        }
    }

    pub async fn decide(&mut self) -> (Vec<Element>, Certificate) {
        (&mut self.decision_outlet).await.unwrap()
    }
}

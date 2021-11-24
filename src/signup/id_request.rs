use crate::{
    crypto::{Header, Identify, Rogue},
    view::View,
};

use doomstack::{here, Doom, ResultExt, Top};

use serde::{Deserialize, Serialize};

use talk::crypto::{
    primitives::{hash::Hash, work::Work},
    Identity, KeyCard, KeyChain, Statement,
};

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct IdRequest {
    request: Request,
    work: Work,
    rogue: Rogue,
}

#[derive(Clone, Serialize, Deserialize)]
struct Request {
    view: Hash,
    allocator: Identity,
    client: KeyCard,
}

#[derive(Doom)]
pub(crate) enum RequestIdError {
    #[doom(description("View is unknown"))]
    UnknownView,
    #[doom(description("Allocator is not a member of view"))]
    ForeignAllocator,
    #[doom(description("Work invalid"))]
    WorkInvalid,
    #[doom(description("Rogue-safety proof invalid"))]
    RogueInvalid,
}

impl IdRequest {
    pub fn new(keychain: &KeyChain, view: &View, allocator: Identity) -> Self {
        let view = view.identifier();
        let client = keychain.keycard();

        let request = Request {
            view,
            allocator,
            client,
        };

        let work = Work::new(10, &request).unwrap(); // TODO: Add settings
        let rogue = Rogue::new(keychain);

        IdRequest {
            request,
            work,
            rogue,
        }
    }

    pub fn view(&self) -> Hash {
        self.request.view
    }
    
    pub fn allocator(&self) -> Identity {
        self.request.allocator
    }

    pub fn client(&self) -> Identity {
        self.request.client.identity()
    }
    
    pub fn validate(&self) -> Result<(), Top<RequestIdError>> {
        let view = View::get(self.request.view)
            .ok_or(RequestIdError::UnknownView.into_top())
            .spot(here!())?;

        if !view.members().contains_key(&self.request.allocator) {
            return RequestIdError::ForeignAllocator.fail().spot(here!());
        }

        self.work
            .verify(10, &self.request)
            .pot(RequestIdError::WorkInvalid, here!())?;

        self.rogue
            .validate(&self.request.client)
            .pot(RequestIdError::RogueInvalid, here!())?;

        Ok(())
    }
}

impl Statement for Request {
    type Header = Header;
    const HEADER: Header = Header::IdRequest;
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::view::test::InstallGenerator;

    #[test]
    fn correct() {
        let install_generator = InstallGenerator::new(4);

        let view = install_generator.view(4);
        let allocator = install_generator.keycards[0].identity();

        let client = KeyChain::random();

        let request = IdRequest::new(&client, &view, allocator);
        request.validate().unwrap();
    }
}

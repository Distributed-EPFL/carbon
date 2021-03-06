use crate::{
    crypto::{Certificate, Header, Identify},
    discovery::Client,
    view::{Change, View},
};

use doomstack::{here, Doom, ResultExt, Top};

use serde::{Deserialize, Serialize};

use talk::crypto::{primitives::hash::Hash, Statement as CryptoStatement};

#[derive(Clone, Serialize)]
#[serde(into = "ResolutionClaim")]
pub(crate) struct Resolution(ResolutionClaim);

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct ResolutionClaim {
    view: Hash,
    statement: Statement,
    certificate: Certificate,
}

#[derive(Clone, Serialize, Deserialize)]
struct Statement {
    change: Change,
}

#[derive(Doom)]
pub(crate) enum ResolutionError {
    #[doom(description("The `Resolution` pertains to an unknown `View`"))]
    UnknownView,
    #[doom(description("The `Resolution` did not pass in a past or current `View`"))]
    FutureVote,
    #[doom(description("The `Resolution`'s `Certificate` is invalid"))]
    CertificateInvalid,
    #[doom(description("The `Resolution`'s `Change` cannot be applied to the current `View`"))]
    ViewError,
}

impl Resolution {
    pub fn change(&self) -> Change {
        self.0.change()
    }
}

impl ResolutionClaim {
    pub(in crate::churn) fn change(&self) -> Change {
        self.statement.change.clone()
    }

    pub fn validate(&self, client: &Client, view: &View) -> Result<(), Top<ResolutionError>> {
        // Verify that `self.view` is known to `client`
        let resolution_view = client
            .view(&self.view)
            .ok_or(ResolutionError::UnknownView.into_top())
            .spot(here!())?;

        // Verify that `self.view` is not in the future
        if resolution_view.height() > view.height() {
            return ResolutionError::FutureVote.fail().spot(here!());
        }

        // (TODO: determine whether a quorum or a plurality are necessary to sign a `Resolution`)
        // Verify `self.certificate`
        self.certificate
            .verify_quorum(&resolution_view, &self.statement)
            .pot(ResolutionError::CertificateInvalid, here!())?;

        // Verify that `self.statement.change` can be used to extend `view`
        view.validate_extension(&self.statement.change)
            .pot(ResolutionError::ViewError, here!())?;

        Ok(())
    }

    pub fn to_resolution(
        self,
        client: &Client,
        view: &View,
    ) -> Result<Resolution, Top<ResolutionError>> {
        self.validate(client, view)?;
        Ok(Resolution(self))
    }
}

impl Identify for Resolution {
    fn identifier(&self) -> Hash {
        self.0.identifier()
    }
}

impl From<Resolution> for ResolutionClaim {
    fn from(resolution: Resolution) -> Self {
        resolution.0
    }
}

impl Identify for ResolutionClaim {
    fn identifier(&self) -> Hash {
        self.statement.change.identifier()
    }
}

impl CryptoStatement for Statement {
    type Header = Header;
    const HEADER: Header = Header::Resolution;
}

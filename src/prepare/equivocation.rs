use crate::{account::Id, discovery::Client, prepare::Extract};

use doomstack::{here, Doom, ResultExt, Top};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Equivocation(Extract, Extract);

#[derive(Doom)]
pub(crate) enum EquivocationError {
    #[doom(description("Id mismatch"))]
    IdMismatch,
    #[doom(description("Consistent extracts"))]
    ConsistentExtracts,
    #[doom(description("Invalid extract"))]
    InvalidExtract,
}

impl Equivocation {
    pub fn new(lhe: Extract, rhe: Extract) -> Self {
        Equivocation(lhe, rhe)
    }

    pub fn id(&self) -> Id {
        // Assuming that `self` is valid, `self.0.id() == self.1.id()`
        self.0.id()
    }

    pub fn validate(&self, discovery: &Client) -> Result<(), Top<EquivocationError>> {
        if self.0.id() != self.1.id() {
            return EquivocationError::IdMismatch.fail().spot(here!());
        }

        if self.0.commitment() == self.1.commitment() {
            return EquivocationError::ConsistentExtracts.fail().spot(here!());
        }

        for extract in [&self.0, &self.1] {
            extract
                .validate(discovery)
                .pot(EquivocationError::InvalidExtract, here!())?;
        }

        Ok(())
    }
}

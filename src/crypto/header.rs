use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(i8)]
pub(crate) enum Header {
    Install = 0,

    LatticeDecisions = 1,

    Resolution = 2,
}

use crate::account::Id;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Entry {
    pub id: Id,
    pub height: u64,
}

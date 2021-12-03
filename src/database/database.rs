use crate::{
    account::Id,
    database::{Families, Signup},
    signup::IdAssignment,
};

use std::collections::HashMap;

pub(crate) struct Database {
    pub assignments: HashMap<Id, IdAssignment>,
    pub signup: Signup,
    pub families: Families,
}

impl Database {
    pub fn new() -> Self {
        let families = Families::new();

        Database {
            assignments: HashMap::new(),
            signup: Signup::new(&families),
            families,
        }
    }
}

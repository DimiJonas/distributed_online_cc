use crate::types::RequestId;

// right now only used for test cli messages
// TODO: consider removing this and just generating uuids as request ids or something similar
pub struct RequestIdContainer {
    next_id: RequestId,
}

impl RequestIdContainer {
    pub fn new(initial_value: RequestId) -> Self {
        Self {
            next_id: initial_value,
        }
    }

    pub fn get_next_id(&mut self) -> RequestId {
        let id = self.next_id;
        self.next_id += 1;
        id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_at_initial_value() {
        let mut c = RequestIdContainer::new(42);
        assert_eq!(c.get_next_id(), 42);
    }

    #[test]
    fn increments_on_each_call() {
        let mut c = RequestIdContainer::new(0);
        assert_eq!(c.get_next_id(), 0);
        assert_eq!(c.get_next_id(), 1);
        assert_eq!(c.get_next_id(), 2);
    }

    #[test]
    fn node_id_offset_matches_formula() {
        // AppState sets initial_request_id = node_id as u64 * 1000
        let node_id: u8 = 3;
        let mut c = RequestIdContainer::new(node_id as u64 * 1000);
        assert_eq!(c.get_next_id(), 3000);
        assert_eq!(c.get_next_id(), 3001);
    }
}

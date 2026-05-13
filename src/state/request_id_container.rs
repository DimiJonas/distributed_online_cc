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

use std::collections::{HashMap, hash_map::Entry};

use crate::types::{CaseId, EventTime, SensorAttributeKey, SensorAttributeThreshold};

#[derive(Debug, Default, Clone)]
pub struct SensorEventMapEntry {
    pub value: SensorAttributeThreshold,
    pub timestamp: EventTime,
}

#[derive(Debug, Default)]
pub struct SensorEventStore {
    // tokens for remembering some meta information for constraints
    sensor_events: HashMap<CaseId, HashMap<SensorAttributeKey, SensorEventMapEntry>>,
}

impl SensorEventStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert_or_update(
        &mut self,
        case_id: CaseId,
        key: SensorAttributeKey,
        value: SensorAttributeThreshold,
        timestamp: EventTime,
    ) -> Option<SensorEventMapEntry> {
        let inner = self.sensor_events.entry(case_id).or_default();

        match inner.entry(key) {
            Entry::Occupied(mut occupied) => {
                if timestamp > occupied.get().timestamp {
                    Some(occupied.insert(SensorEventMapEntry { value, timestamp }))
                } else {
                    None
                }
            }
            Entry::Vacant(vacant) => {
                vacant.insert(SensorEventMapEntry { value, timestamp });
                None
            }
        }
    }

    pub fn get(&self, case_id: &CaseId, key: &SensorAttributeKey) -> Option<&SensorEventMapEntry> {
        self.sensor_events.get(case_id)?.get(key)
    }

    pub fn get_cloned(
        &self,
        case_id: &CaseId,
        key: &SensorAttributeKey,
    ) -> Option<SensorEventMapEntry> {
        self.sensor_events.get(case_id)?.get(key).cloned()
    }

    pub fn remove_case(
        &mut self,
        case_id: &CaseId,
    ) -> Option<HashMap<SensorAttributeKey, SensorEventMapEntry>> {
        self.sensor_events.remove(case_id)
    }
}

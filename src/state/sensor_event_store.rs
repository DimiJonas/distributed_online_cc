use std::collections::HashMap;

use crate::types::{CaseId, EventTime, SensorAttributeKey, SensorAttributeThreshold};

#[derive(Debug, Default, Clone)]
pub struct SensorEventMapEntry {
    pub value: SensorAttributeThreshold,
    pub timestamp: EventTime,
}

/// what happened when a sensor value was recorded
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SensorValueRecorded {
    /// first value for this (case, key)
    Inserted,
    /// appended behind the previously newest value
    Appended,
    /// inserted before an already known newer value (arrived out of order)
    Backfilled,
    /// a value with this exact event time was already known so the original was kept
    Duplicate,
}

#[derive(Debug, Default)]
pub struct SensorEventStore {
    /// the history of values per case and per attribute key
    sensor_events: HashMap<CaseId, HashMap<SensorAttributeKey, Vec<SensorEventMapEntry>>>,
}

impl SensorEventStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// record a sensor value into the history for (case, key)
    pub fn record(
        &mut self,
        case_id: CaseId,
        key: SensorAttributeKey,
        value: SensorAttributeThreshold,
        timestamp: EventTime,
    ) -> SensorValueRecorded {
        let history = self
            .sensor_events
            .entry(case_id)
            .or_default()
            .entry(key)
            .or_default();

        match history.binary_search_by(|entry| entry.timestamp.cmp(&timestamp)) {
            // keep the first value seen for an event time
            Ok(_) => SensorValueRecorded::Duplicate,
            Err(position) => {
                let outcome = if history.is_empty() {
                    SensorValueRecorded::Inserted
                } else if position == history.len() {
                    SensorValueRecorded::Appended
                } else {
                    SensorValueRecorded::Backfilled
                };
                history.insert(position, SensorEventMapEntry { value, timestamp });
                outcome
            }
        }
    }

    /// the newest value whose event time is still before 'at'
    pub fn value_at(
        &self,
        case_id: &CaseId,
        key: &SensorAttributeKey,
        at: EventTime,
    ) -> Option<&SensorEventMapEntry> {
        let history = self.sensor_events.get(case_id)?.get(key)?;
        match history.binary_search_by(|entry| entry.timestamp.cmp(&at)) {
            // exact hit on the event time
            Ok(position) => history.get(position),
            // every known value is newer than 'at'
            Err(0) => None,
            Err(position) => history.get(position - 1),
        }
    }

    pub fn value_at_cloned(
        &self,
        case_id: &CaseId,
        key: &SensorAttributeKey,
        at: EventTime,
    ) -> Option<SensorEventMapEntry> {
        self.value_at(case_id, key, at).cloned()
    }

    pub fn remove_case(
        &mut self,
        case_id: &CaseId,
    ) -> Option<HashMap<SensorAttributeKey, Vec<SensorEventMapEntry>>> {
        self.sensor_events.remove(case_id)
    }
}

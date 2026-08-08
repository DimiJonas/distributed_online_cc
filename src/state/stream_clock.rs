use std::future;

use tokio::sync::watch;

use crate::types::EventTime;

/// the nodes view of how far the event stream has progressed, in event time
/// this is needed specifically for the time constraint when replaying old event logs
pub struct StreamClock {
    tx: watch::Sender<EventTime>,
}

impl StreamClock {
    pub fn new() -> Self {
        Self {
            tx: watch::Sender::new(EventTime::MIN_UTC),
        }
    }

    /// advance the clock to the newly observed timestamp if that is later than the current value
    ///
    /// returns true if the clock actually moved
    pub fn advance(&self, observed: EventTime) -> bool {
        self.tx.send_if_modified(|current| {
            if observed > *current {
                *current = observed;
                true
            } else {
                false
            }
        })
    }

    /// advance to the latest of a batch
    pub fn advance_to_max(&self, observed: impl IntoIterator<Item = EventTime>) -> bool {
        match observed.into_iter().max() {
            Some(latest) => self.advance(latest),
            None => false,
        }
    }

    pub fn now(&self) -> EventTime {
        *self.tx.borrow()
    }

    /// has the stream progressed at all yet?
    pub fn is_started(&self) -> bool {
        self.now() > EventTime::MIN_UTC
    }

    pub fn subscribe(&self) -> watch::Receiver<EventTime> {
        self.tx.subscribe()
    }

    /// resolve once the stream has reached the given deadline
    pub async fn wait_for(&self, deadline: EventTime) {
        let mut rx = self.subscribe();
        loop {
            if *rx.borrow_and_update() >= deadline {
                return;
            }
            if rx.changed().await.is_err() {
                // the clock was dropped, which only happens at shutdown so park rather than reporting a spurious deadline
                future::pending::<()>().await;
            }
        }
    }
}

impl Default for StreamClock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    fn t(secs: i64) -> EventTime {
        Utc.with_ymd_and_hms(2014, 1, 1, 0, 0, 0).unwrap() + chrono::Duration::seconds(secs)
    }

    #[test]
    fn starts_before_any_event() {
        let clock = StreamClock::new();
        assert!(!clock.is_started());
    }

    #[test]
    fn advances_forwards_only() {
        let clock = StreamClock::new();
        assert!(clock.advance(t(100)));
        assert_eq!(clock.now(), t(100));

        // an out of order event should not rewind the clock
        assert!(!clock.advance(t(50)));
        assert_eq!(clock.now(), t(100));

        assert!(clock.advance(t(150)));
        assert_eq!(clock.now(), t(150));
    }

    #[test]
    fn advance_to_max_takes_the_latest() {
        let clock = StreamClock::new();
        assert!(clock.advance_to_max([t(10), t(90), t(40)]));
        assert_eq!(clock.now(), t(90));
        assert!(!clock.advance_to_max(std::iter::empty()));
    }

    #[tokio::test]
    async fn wait_for_returns_immediately_when_already_past() {
        let clock = StreamClock::new();
        clock.advance(t(100));
        clock.wait_for(t(50)).await;
    }

    #[tokio::test]
    async fn wait_for_resolves_when_the_clock_reaches_the_deadline() {
        let clock = std::sync::Arc::new(StreamClock::new());
        let waiter = {
            let clock = clock.clone();
            tokio::spawn(async move { clock.wait_for(t(100)).await })
        };

        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        clock.advance(t(60));
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(!waiter.is_finished(), "must not fire before the deadline");

        clock.advance(t(100));
        waiter.await.unwrap();
    }
}

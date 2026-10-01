use std::{collections::HashMap, net::SocketAddr};

use async_trait::async_trait;
use tokio::task::JoinSet;

use crate::{
    config::node_config::{DistributedAppearanceCounterReset, MinMaxMode},
    conformance_checking::{
        constraint_logic::constraint_handler_trait::ConstraintHandler,
        result_emitter::{emit_result_stamped, stamp_decision},
    },
    impl_constraint_handler_fields,
    measurement::{
        self,
        record::{CorrectionDropReason, ResultTrigger},
    },
    network::{
        message_types::{CCMessage, CCMessageType, PeerMessage, PeerMessagePayload},
        tcp_router::{send_and_receive_conformance, send_to_conformance_peer},
    },
    state::{
        app_state::AppState,
        evaluation_store::{EvaluationDetail, LastEvaluation},
    },
    types::{
        CaseId, ConstraintId, ConstraintResultType, ConstraintRole, ConstraintType, EventId,
        EventInstanceId, EventTime, HandlerIndex, RequestId, Token, TokenPayload, TracedEvent,
    },
};

pub struct DistributedAppearanceCounterHandler {
    pub index: HandlerIndex,
    pub role: ConstraintRole,
    pub event_id: EventId,
    pub constraint_id: ConstraintId,
    pub reset: DistributedAppearanceCounterReset,
}

#[async_trait]
impl ConstraintHandler for DistributedAppearanceCounterHandler {
    impl_constraint_handler_fields!(DistributedAppearanceCounterHandler);

    fn counter_reset(&self) -> Option<DistributedAppearanceCounterReset> {
        Some(self.reset)
    }

    // increment counter
    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        log::info!(
            "distributed appearance counter handler: received configured event for constraint {}; event={}",
            self.constraint_id,
            event
        );

        // check if a trigger event already evaluated the counter (and update the counter atomically)
        let (late_evaluation, count) = {
            let evaluations = state.evaluations.lock().unwrap();
            let late_evaluation = match evaluations.get_cloned(&event.case_id, self.index) {
                Some(evaluation) if event.timestamp <= evaluation.trigger_time => Some(evaluation),
                _ => None,
            };

            // for Zero the event should not be counted because it still belongs to the last trigger window
            let skip_count =
                late_evaluation.is_some() && self.reset == DistributedAppearanceCounterReset::Zero;

            let count = if skip_count {
                None
            } else {
                let mut meta_tokens = state.meta_token_store.lock().unwrap();
                Some(
                    match meta_tokens.get_token_mut(&event.case_id, self.index) {
                        Some(counter_token) => {
                            counter_token.payload.increment_counter();
                            counter_token.payload.get_count()
                        }
                        None => {
                            log::info!(
                                "distributed appearance counter handler: starting counter for event_id={} constraint_id={} case_id={}",
                                self.event_id,
                                self.constraint_id,
                                event.case_id
                            );

                            let counter_token = Token::new_with_payload(
                                event.case_id.clone(),
                                self.constraint_id.clone(),
                                ConstraintType::DistributedAppearance,
                                None,
                                TokenPayload::Counter(1),
                            );
                            meta_tokens.insert_meta_token(counter_token.clone(), self.index);
                            counter_token.payload.get_count()
                        }
                    },
                )
            };

            (late_evaluation, count)
        };

        // only report a delta (notify trigger to correct the result)
        // emitted after the guards are dropped, an await may not hold a lock
        if let Some(evaluation) = late_evaluation {
            self.report_late_event(event, evaluation, state.clone())
                .await;
        }

        match count {
            Some(count) => log::info!(
                "distributed appearance counter handler: counter is now at {}",
                count
            ),
            None => log::info!(
                "distributed appearance counter handler: event was reported as a late delta, counter left unchanged"
            ),
        }
    }

    // send counter value to requestor
    // reset counter
    async fn handle_message(
        &self,
        state: AppState,
        message: CCMessage,
        request_id: Option<RequestId>,
    ) {
        log::info!(
            "distributed appearance counter handler: received message for constraint {}; message={}",
            self.constraint_id,
            message
        );
        log::debug!("message={:?}", message);

        // drop message if it was destined for a trigger handler on the same node
        if message.creating_handler == Some(ConstraintRole::DistributedAppearanceCounter) {
            return;
        }

        if message.message_type != CCMessageType::PullTokenRequest {
            log::warn!(
                "distributed appearance counter handler: unexpected message type {:?} for constraint {}, ignoring",
                message.message_type,
                self.constraint_id
            );
            return;
        }

        let request_id = match request_id {
            Some(id) => id,
            None => {
                log::error!(
                    "distributed appearance counter handler: message does not contain a request id. cannot respond to it"
                );
                return;
            }
        };

        let sender_address = match message.sender_address {
            Some(addr) => addr,
            None => {
                log::error!(
                    "distributed appearance counter handler: message does not contain sender address. cannot respond to it"
                );
                return;
            }
        };

        // accumulate all local distributed appearance counters (if there are multiple configured for the same constraint)
        // in this case only 1 handler will receive the pulltokenrequest and answer it
        let dist_appear_counter_handlers: Vec<&dyn ConstraintHandler> = state
            .handlers
            .get_by_constraint_id(&self.constraint_id)
            .into_iter()
            .filter(|h| h.role() == ConstraintRole::DistributedAppearanceCounter)
            .collect();
        // reading + resetting the counters and recording the evaluation are ONE atomic step, under
        // the same evaluations -> meta_token_store order used in handle_event(). otherwise a
        // configured event can be counted in between: it is not seen as late (the evaluation is not
        // recorded yet) and its increment lands after the reset, so it is reported to nobody
        let mut total = 0u64;
        {
            let mut evaluations = state.evaluations.lock().unwrap();
            {
                let mut meta_tokens = state.meta_token_store.lock().unwrap();
                for handler in dist_appear_counter_handlers {
                    if let Some(token) =
                        meta_tokens.get_token_mut(&message.case_id, handler.index())
                    {
                        total += token.payload.get_count();
                        // reset counter
                        if let Some(reset) = handler.counter_reset() {
                            match reset {
                                DistributedAppearanceCounterReset::Keep => {}
                                DistributedAppearanceCounterReset::Zero => {
                                    token.payload.reset_counter()
                                }
                            }
                        }
                    }
                }
            }

            // remember trigger node for late counter event corrections
            match message.timestamp {
                Some(trigger_time) => {
                    let local_counter_indices: Vec<HandlerIndex> = state
                        .handlers
                        .get_by_constraint_id(&self.constraint_id)
                        .into_iter()
                        .filter(|h| h.role() == ConstraintRole::DistributedAppearanceCounter)
                        .map(|h| h.index())
                        .collect();
                    for index in local_counter_indices {
                        evaluations.insert(
                            &message.case_id,
                            index,
                            LastEvaluation {
                                trigger_time,
                                triggering_event: message.triggering_event,
                                detail: EvaluationDetail::DistributedCounterReport {
                                    trigger_address: sender_address,
                                },
                            },
                        );
                    }
                }
                None => {
                    log::warn!(
                        "distributed appearance counter handler: PullTokenRequest for ({}/{}) has no timestamp. late events will not be able to correct this result",
                        message.case_id,
                        self.constraint_id
                    );
                }
            }
        }

        let token_with_total = Token::new_with_payload(
            message.case_id.clone(),
            self.constraint_id.clone(),
            ConstraintType::DistributedAppearance,
            None,
            TokenPayload::Counter(total),
        );

        // send counter token
        log::info!(
            "sending counter token to {}",
            message.sender_address.unwrap()
        );
        let response = PeerMessage::conformance_res(
            request_id,
            CCMessage::pull_token_response(token_with_total),
        );
        if send_to_conformance_peer(sender_address, response, state.clone())
            .await
            .is_err()
        {
            log::error!(
                "distributed appearance counter handler: error responding with counter token to {}",
                sender_address
            );
        }
    }

    async fn clean_up_state(&self, case_id: &CaseId, state: AppState) {
        log::debug!(
            "distributed appearance counter handler: clean up state for ({}/{})",
            case_id,
            self.constraint_id
        );

        // same order as everywhere else in this handler: evaluations -> meta_token_store
        // remove evaluations
        {
            let mut evaluations = state.evaluations.lock().unwrap();
            evaluations.remove(case_id, self.index);
        }
        // delete counter token
        {
            let mut meta_tokens = state.meta_token_store.lock().unwrap();
            meta_tokens.remove_meta_token(case_id, self.index);
        }
    }
}

impl DistributedAppearanceCounterHandler {
    /// report an event that was delayed past the trigger pull request as a +1 delta
    async fn report_late_event(
        &self,
        event: &TracedEvent,
        evaluation: LastEvaluation,
        state: AppState,
    ) {
        let trigger_address = match evaluation.detail {
            EvaluationDetail::DistributedCounterReport { trigger_address } => trigger_address,
            _ => {
                log::error!(
                    "distributed appearance counter handler: recorded evaluation for ({}/{}) is not a distributed counter report",
                    event.case_id,
                    self.constraint_id
                );
                return;
            }
        };

        let mut delta_token = Token::new_with_payload(
            event.case_id.clone(),
            self.constraint_id.clone(),
            ConstraintType::DistributedAppearance,
            Some(event.timestamp),
            TokenPayload::Counter(1),
        );
        delta_token.event_time = Some(event.timestamp);

        let mut cc_message = CCMessage::push_token(delta_token, event.instance_id);
        // needed so counter handlers on the same node as the trigger handler can ignore it
        cc_message.creating_handler = Some(ConstraintRole::DistributedAppearanceCounter);

        log::debug!(
            "distributed appearance counter handler: late event for ({}/{}) (event time {} <= trigger time {}); reporting +1 to trigger at {}",
            event.case_id,
            self.constraint_id,
            event.timestamp,
            evaluation.trigger_time,
            trigger_address
        );

        if let Err(e) = send_to_conformance_peer(
            trigger_address,
            PeerMessage::conformance(cc_message),
            state.clone(),
        )
        .await
        {
            log::error!(
                "distributed appearance counter handler: error sending late delta for ({}/{}) to {}: {}. the emitted result stays uncorrected",
                event.case_id,
                self.constraint_id,
                trigger_address,
                e
            );
            measurement::api::correction_dropped(
                &event.case_id,
                &self.constraint_id,
                ConstraintType::DistributedAppearance,
                CorrectionDropReason::CaseAlreadyClosed,
                Some(trigger_address),
            );
        }
    }
}

pub struct DistributedAppearanceTriggerHandler {
    pub index: HandlerIndex,
    pub role: ConstraintRole,
    pub event_id: EventId,
    pub constraint_id: ConstraintId,
    pub mode: MinMaxMode,
    pub frequency: u64,
    pub counter_peers: Vec<SocketAddr>,
}

#[async_trait]
impl ConstraintHandler for DistributedAppearanceTriggerHandler {
    impl_constraint_handler_fields!(DistributedAppearanceTriggerHandler);

    fn configured_peers(&self) -> Vec<SocketAddr> {
        self.counter_peers.clone()
    }

    /// record the evaluation and spawn the counter pull background task
    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        log::info!(
            "distributed appearance trigger handler: received configured event for constraint {}; event={}",
            self.constraint_id,
            event
        );

        // record the evaluation before pulling with every configured peer seeded at count=0
        {
            let mut evaluations = state.evaluations.lock().unwrap();
            // if two trigger events for the same pair overlap (counter pulls are detached from serialized event q processing), the older pull discards itself
            // when it comes back (it compares trigger times), but its counters were already reset by the pull so the window boundary is blurred
            if let Some(LastEvaluation {
                detail:
                    EvaluationDetail::DistributedCounterTotals {
                        pending_pull: true, ..
                    },
                trigger_time,
                ..
            }) = evaluations.get_cloned(&event.case_id, self.index)
            {
                log::warn!(
                    "distributed appearance trigger handler: a new trigger event for ({}/{}) at {} arrived while the pull of the trigger at {} was still running",
                    event.case_id,
                    self.constraint_id,
                    event.timestamp,
                    trigger_time
                );
            }
            evaluations.insert(
                &event.case_id,
                self.index,
                LastEvaluation {
                    trigger_time: event.timestamp,
                    triggering_event: Some(event.instance_id),
                    detail: EvaluationDetail::DistributedCounterTotals {
                        per_peer: self.counter_peers.iter().map(|peer| (*peer, 0)).collect(),
                        pending_pull: true,
                    },
                },
            );
        }

        let pull = CounterPull {
            index: self.index,
            role: self.role,
            constraint_id: self.constraint_id.clone(),
            mode: self.mode.clone(),
            frequency: self.frequency,
            counter_peers: self.counter_peers.clone(),
            case_id: event.case_id.clone(),
            trigger_time: event.timestamp,
            triggering_event: event.instance_id,
        };
        tokio::spawn(pull.run(state));
    }

    // a counter peer reports an event that was delayed past the triggers pulltokenrequest
    async fn handle_message(
        &self,
        state: AppState,
        message: CCMessage,
        _request_id: Option<RequestId>,
    ) {
        if message.message_type != CCMessageType::PushToken
            || message.creating_handler != Some(ConstraintRole::DistributedAppearanceCounter)
        {
            return;
        }

        log::info!(
            "distributed appearance trigger handler: received message for constraint {}; message={}",
            self.constraint_id,
            message
        );
        log::debug!("message={:?}", message);

        let peer = match message.sender_address {
            Some(addr) => addr,
            None => {
                log::error!(
                    "distributed appearance trigger handler: late delta for ({}/{}) has no sender address, cannot attribute it to a counter peer",
                    message.case_id,
                    self.constraint_id
                );
                return;
            }
        };

        let delta = message
            .token
            .as_ref()
            .map(|token| token.payload.get_count())
            .unwrap_or(0);
        if delta == 0 {
            log::warn!(
                "distributed appearance trigger handler: late delta for ({}/{}) from {} carries no count. ignoring",
                message.case_id,
                self.constraint_id,
                peer
            );
            return;
        }

        let (corrected, stamp) = {
            let mut evaluations = state.evaluations.lock().unwrap();
            match evaluations.add_to_peer_total(&message.case_id, self.index, peer, delta) {
                Some(update) if update.pending_pull => {
                    log::info!(
                        "distributed appearance trigger handler: late delta of {} from {} for ({}/{}) arrived while the counter pull is still running. recorded, the pull will include it",
                        delta,
                        peer,
                        message.case_id,
                        self.constraint_id
                    );
                    return;
                }
                Some(update) => (
                    update.total,
                    stamp_decision(&state, &message.case_id, &self.constraint_id),
                ),
                None => {
                    // the trigger event never fired here, or the case was already cleaned up
                    log::warn!(
                        "distributed appearance trigger handler: no recorded evaluation for ({}/{}). dropping late delta from {}",
                        message.case_id,
                        self.constraint_id,
                        peer
                    );
                    measurement::api::correction_dropped(
                        &message.case_id,
                        &self.constraint_id,
                        ConstraintType::DistributedAppearance,
                        CorrectionDropReason::NoRecordedEvaluation,
                        Some(peer),
                    );
                    return;
                }
            }
        };

        log::info!(
            "distributed appearance trigger handler: late delta of {} from {} for ({}/{}). corrected distributed counter is now {}",
            delta,
            peer,
            message.case_id,
            self.constraint_id,
            corrected
        );

        let (info, result) = evaluate_distributed_count(&self.mode, self.frequency, corrected);
        if !publish_now(&result, &state, &message.case_id, &self.constraint_id) {
            // skip satisfied verdicts is the case has not ended and its not a correction
            return;
        }
        emit_result_stamped(
            stamp,
            &message.case_id,
            &self.constraint_id,
            ConstraintType::DistributedAppearance,
            result,
            format!("{info} (late correction)"),
            state.clone(),
            ResultTrigger::Event,
            message.triggering_event,
        )
        .await;
    }

    async fn clean_up_state(&self, case_id: &CaseId, state: AppState) {
        log::debug!(
            "distributed appearance trigger handler: clean up state for ({}/{})",
            case_id,
            self.constraint_id
        );

        // remove evaluations
        {
            let mut evaluations = state.evaluations.lock().unwrap();
            evaluations.remove(case_id, self.index);
        }
    }

    async fn handle_case_end(&self, case_id: &CaseId, state: AppState) {
        log::info!(
            "distributed appearance trigger handler: received case end signal for constraint {}; case={}",
            self.constraint_id,
            case_id
        );

        // check if a result exists
        {
            let results = state.cc_results.lock().unwrap();
            if results
                .get_constraint(case_id, &self.constraint_id)
                .is_some()
            {
                return;
            }
        }

        let pending = {
            let recorded = {
                let evaluations = state.evaluations.lock().unwrap();
                match evaluations.get_cloned(case_id, self.index) {
                    Some(LastEvaluation {
                        detail:
                            EvaluationDetail::DistributedCounterTotals {
                                per_peer,
                                pending_pull,
                            },
                        ..
                    }) => Some((per_peer.values().sum::<u64>(), pending_pull)),
                    _ => None,
                }
            };

            let result_exists = {
                let results = state.cc_results.lock().unwrap();
                results
                    .get_constraint(case_id, &self.constraint_id)
                    .is_some()
            };

            if result_exists {
                None
            } else {
                match recorded {
                    // the pull came back but somehow did not produce a verdict -  decide on it now
                    Some((total, false)) => {
                        let (info, result) =
                            evaluate_distributed_count(&self.mode, self.frequency, total);
                        Some((
                            stamp_decision(&state, case_id, &self.constraint_id),
                            result,
                            format!("{info} (decided at case end)"),
                        ))
                    }
                    // the pull never came back. the recorded total is incomplete, so say so
                    // instead of passing it off as a finished check
                    Some((total, true)) => {
                        log::warn!(
                            "distributed appearance trigger handler: the counter pull for ({}/{}) never finished. deciding on the incomplete total {}",
                            case_id,
                            self.constraint_id,
                            total
                        );
                        measurement::api::correction_dropped(
                            case_id,
                            &self.constraint_id,
                            ConstraintType::DistributedAppearance,
                            CorrectionDropReason::IncompletePeerPull,
                            self.counter_peers.first().copied(),
                        );
                        let (info, result) =
                            evaluate_distributed_count(&self.mode, self.frequency, total);
                        Some((
                            stamp_decision(&state, case_id, &self.constraint_id),
                            result,
                            format!("{info} (decided at case end, counter pull never finished)"),
                        ))
                    }
                    // no evaluation was ever recorded and no trigger has produced a verdict
                    None => Some((
                        stamp_decision(&state, case_id, &self.constraint_id),
                        ConstraintResultType::Satisfied,
                        "counter evaluation was never triggered".to_string(),
                    )),
                }
            }
        };

        if let Some((stamp, result, info)) = pending {
            // produce result because none was emitted yet
            emit_result_stamped(
                stamp,
                case_id,
                &self.constraint_id,
                ConstraintType::DistributedAppearance,
                result,
                info,
                state.clone(),
                ResultTrigger::CaseEnd,
                None,
            )
            .await;
        }
    }
}

fn evaluate_distributed_count(
    mode: &MinMaxMode,
    frequency: u64,
    distributed_count: u64,
) -> (String, ConstraintResultType) {
    match mode {
        MinMaxMode::Min => {
            if distributed_count < frequency {
                (
                    format!(
                        "CounterMin: counter {:?} < limit {:?}",
                        distributed_count, frequency
                    ),
                    ConstraintResultType::Violated,
                )
            } else {
                (
                    format!(
                        "CounterMin: counter {:?} >= limit {:?}",
                        distributed_count, frequency
                    ),
                    ConstraintResultType::Satisfied,
                )
            }
        }
        MinMaxMode::Max => {
            if distributed_count > frequency {
                (
                    format!(
                        "CounterMax: counter {:?} > limit {:?}",
                        distributed_count, frequency
                    ),
                    ConstraintResultType::Violated,
                )
            } else {
                (
                    format!(
                        "CounterMax: counter {:?} <= limit {:?}",
                        distributed_count, frequency
                    ),
                    ConstraintResultType::Satisfied,
                )
            }
        }
    }
}

/// decide if verdict has to be published right away (violation or correction)
fn publish_now(
    result: &ConstraintResultType,
    state: &AppState,
    case_id: &CaseId,
    constraint_id: &ConstraintId,
) -> bool {
    if matches!(result, ConstraintResultType::Violated) {
        return true;
    }
    let results = state.cc_results.lock().unwrap();
    results.get_constraint(case_id, constraint_id).is_some()
}

/// everything the counter pull of one trigger event needs so it can run detached in its own task
struct CounterPull {
    index: HandlerIndex,
    role: ConstraintRole,
    constraint_id: ConstraintId,
    mode: MinMaxMode,
    frequency: u64,
    counter_peers: Vec<SocketAddr>,
    case_id: CaseId,
    trigger_time: EventTime,
    triggering_event: EventInstanceId,
}

impl CounterPull {
    /// pull all counter peers merge the counters into the recorded evaluation and publish
    async fn run(self, state: AppState) {
        let (per_peer, failed_peers) = self.pull_all(&state).await;

        if !failed_peers.is_empty() {
            log::error!(
                "distributed appearance trigger handler: {} of {} counter peer(s) did not answer for ({}/{}): {:?}. 
                the distributed count is incomplete and no verdict is emitted from this trigger event (case end will produce the verdict)",
                failed_peers.len(),
                self.counter_peers.len(),
                self.case_id,
                self.constraint_id,
                failed_peers
            );
            measurement::api::correction_dropped(
                &self.case_id,
                &self.constraint_id,
                ConstraintType::DistributedAppearance,
                CorrectionDropReason::IncompletePeerPull,
                failed_peers.first().copied(),
            );
        }

        let pending = {
            let mut evaluations = state.evaluations.lock().unwrap();

            // a later trigger event for the same pair replaced one while still pulling
            // (merging into it would mix two windows and the later trigger publishes its verdict anyway)
            let superseded = evaluations
                .get_cloned(&self.case_id, self.index)
                .is_some_and(|evaluation| evaluation.trigger_time != self.trigger_time);
            if superseded {
                log::warn!(
                    "distributed appearance trigger handler: the counter pull of the trigger at {} for ({}/{}) was superseded by a later trigger. discarding it",
                    self.trigger_time,
                    self.case_id,
                    self.constraint_id
                );
                return;
            }

            let distributed_count = match evaluations.merge_peer_totals(
                &self.case_id,
                self.index,
                per_peer.clone(),
            ) {
                Some(total) => total,
                None => {
                    // the case was cleaned up while the pull was ongoing and the evaluation for it is gone
                    log::warn!(
                        "distributed appearance trigger handler: no recorded evaluation for ({}/{}) after the pull (the case was cleaned up mid pull). evaluating the pulled counts alone",
                        self.case_id,
                        self.constraint_id
                    );
                    per_peer.values().sum()
                }
            };

            if failed_peers.is_empty() {
                let (info, result) =
                    evaluate_distributed_count(&self.mode, self.frequency, distributed_count);
                Some((
                    stamp_decision(&state, &self.case_id, &self.constraint_id),
                    info,
                    result,
                ))
            } else {
                None
            }
        };

        let Some((stamp, info, result)) = pending else {
            return;
        };

        if !publish_now(&result, &state, &self.case_id, &self.constraint_id) {
            log::debug!(
                "distributed appearance trigger handler: ({}/{}) is satisfied so far ({}). leaving the verdict to the case end",
                self.case_id,
                self.constraint_id,
                info
            );
            return;
        }

        // check if the case was closed during the pull
        {
            let case_state = state.case_state.lock().unwrap();
            if case_state.is_closed(&self.case_id) {
                log::warn!(
                    "distributed appearance trigger handler: the case of ({}/{}) closed while the counter pull was running. dropping the verdict",
                    self.case_id,
                    self.constraint_id
                );
                drop(case_state);
                measurement::api::correction_dropped(
                    &self.case_id,
                    &self.constraint_id,
                    ConstraintType::DistributedAppearance,
                    CorrectionDropReason::CaseAlreadyClosed,
                    None,
                );
                return;
            }
        }

        emit_result_stamped(
            stamp,
            &self.case_id,
            &self.constraint_id,
            ConstraintType::DistributedAppearance,
            result,
            info,
            state.clone(),
            ResultTrigger::Event,
            Some(self.triggering_event),
        )
        .await;
    }

    /// pull every counter peer concurrently
    async fn pull_all(&self, state: &AppState) -> (HashMap<SocketAddr, u64>, Vec<SocketAddr>) {
        let mut requests = JoinSet::new();

        for counter_peer in &self.counter_peers {
            let counter_peer = *counter_peer;
            let case_id = self.case_id.clone();
            let constraint_id = self.constraint_id.clone();
            let role = self.role;
            let trigger_time = self.trigger_time;
            let triggering_event = self.triggering_event;
            let state = state.clone();

            requests.spawn(async move {
                let mut cc_message = CCMessage::pull_token_request_with_creator(case_id, constraint_id, role);
                // the counter needs the triggers event time to tell a delayed event from one that belongs to the next window
                cc_message.timestamp = Some(trigger_time);
                cc_message.triggering_event = Some(triggering_event);
                let pull_token_request = PeerMessage::conformance_req(RequestId::new_v4(), cc_message);

                let count = match send_and_receive_conformance(
                    counter_peer,
                    pull_token_request,
                    state,
                )
                .await
                {
                    Ok(response) => match response.payload {
                        PeerMessagePayload::Conformance(cc_msg) => match cc_msg.token {
                            Some(token) => Some(token.payload.get_count()),
                            None => {
                                log::error!(
                                    "distributed appearance trigger handler: {} answered without a counter token",
                                    counter_peer
                                );
                                None
                            }
                        },
                        other => {
                            log::error!(
                                "distributed appearance trigger handler: received wrong message type: \"{}\"; expected type: \"conformance\"",
                                other
                            );
                            None
                        }
                    },
                    Err(e) => {
                        log::error!(
                            "distributed appearance trigger handler: error requesting token from {}: {}",
                            counter_peer,
                            e
                        );
                        None
                    }
                };
                (counter_peer, count)
            });
        }

        let mut per_peer: HashMap<SocketAddr, u64> = HashMap::new();
        let mut failed_peers: Vec<SocketAddr> = Vec::new();
        while let Some(joined) = requests.join_next().await {
            match joined {
                Ok((peer, Some(count))) => {
                    *per_peer.entry(peer).or_insert(0) += count;
                }
                Ok((peer, None)) => failed_peers.push(peer),
                Err(e) => {
                    log::error!(
                        "distributed appearance trigger handler: counter pull task for ({}/{}) failed: {}",
                        self.case_id,
                        self.constraint_id,
                        e
                    );
                    // the peer of a task that panicked is unknown, so mark the pull incomplete by blaming a peer that has not answered yet
                    if let Some(peer) = self
                        .counter_peers
                        .iter()
                        .find(|peer| !per_peer.contains_key(*peer) && !failed_peers.contains(peer))
                    {
                        failed_peers.push(*peer);
                    }
                }
            }
        }
        (per_peer, failed_peers)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::{
        config::node_config::{ConstraintConfig, MinMaxMode},
        conformance_checking::constraint_logic::{
            constraint_handler_trait::ConstraintHandler,
            distributed_appearance_constraint_handler::DistributedAppearanceTriggerHandler,
        },
        network::message_types::CCMessage,
        state::app_state::AppState,
        types::{ConstraintResultType, ConstraintRole, PeerAddress},
        util::test_tools::{
            addr, answer_one_conformance_request, make_config_with_constraints_and_port,
            make_counter_token, make_state_from_config, make_traced_event_with_fixed_timestamp,
            register_manager,
        },
    };

    const CID: &str = "dapp";
    const EID: &str = "trigger";

    /// Build a trigger-side state. The counter peer (if any) is a remote node we
    /// mock; the local node only hosts the trigger handler.
    fn trigger_state(
        node_port: u16,
        counter_port: Option<u16>,
        case: &str,
        mode: MinMaxMode,
        frequency: u64,
    ) -> (AppState, Vec<std::net::SocketAddr>) {
        let counter_peers: Vec<std::net::SocketAddr> = counter_port.into_iter().map(addr).collect();
        let config = make_config_with_constraints_and_port(
            vec![ConstraintConfig::DistributedAppearanceTrigger {
                constraint_id: CID.to_string(),
                event_id: EID.to_string(),
                mode,
                frequency,
                counter_peers: counter_peers.clone(),
            }],
            node_port,
        );
        let state = make_state_from_config(config);
        state
            .cc_results
            .lock()
            .unwrap()
            .register_case(case.to_string());
        (state, counter_peers)
    }

    fn handler(
        mode: MinMaxMode,
        frequency: u64,
        counter_peers: Vec<std::net::SocketAddr>,
    ) -> DistributedAppearanceTriggerHandler {
        DistributedAppearanceTriggerHandler {
            index: 1,
            role: ConstraintRole::DistributedAppearanceTrigger,
            event_id: EID.to_string(),
            constraint_id: CID.to_string(),
            mode,
            frequency,
            counter_peers,
        }
    }

    /// A pull-token response from the counter peer carrying `count`.
    fn counter_response(case: &str, count: u64) -> CCMessage {
        CCMessage::pull_token_response(make_counter_token(case, CID, count))
    }

    fn result_type(state: &AppState, case: &str) -> Option<ConstraintResultType> {
        let store = state.cc_results.lock().unwrap();
        store
            .get_constraint(&case.to_string(), &CID.to_string())
            .map(|r| r.constraint_result)
    }

    /// handle_event doesnt wait for the counter pull, it hands it to a background task, so the
    /// verdict shows up a moment after handle_event returned
    async fn await_result(state: &AppState, case: &str) -> Option<ConstraintResultType> {
        for _ in 0..200 {
            if let Some(result) = result_type(state, case) {
                return Some(result);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        None
    }

    /// none: no counter peers, no trigger event → case end pulls 0 → Max 0<=2 → Satisfied.
    #[tokio::test]
    async fn none_satisfied_at_case_end_max() {
        let case = "c1";
        let (state, peers) = trigger_state(26000, None, case, MinMaxMode::Max, 2);
        handler(MinMaxMode::Max, 2, peers)
            .handle_case_end(&case.to_string(), state.clone())
            .await;
        assert!(matches!(
            result_type(&state, case),
            Some(ConstraintResultType::Satisfied)
        ));
    }

    /// only trigger event, distributed count 0 → evaluated against 0.
    /// Min freq 1: 0 >= 1 false → Violated.
    #[tokio::test]
    async fn only_trigger_evaluated_against_zero() {
        let case = "c2";
        let (state, peers) = trigger_state(26010, Some(26011), case, MinMaxMode::Min, 1);
        let mut mgr_rx = register_manager(&state, PeerAddress::new_conformance_address(peers[0]));

        let h = handler(MinMaxMode::Min, 1, peers);
        let ev = make_traced_event_with_fixed_timestamp(case, EID);
        h.handle_event(&ev, state.clone()).await;
        answer_one_conformance_request(state.clone(), &mut mgr_rx, counter_response(case, 0)).await;

        assert!(matches!(
            await_result(&state, case).await,
            Some(ConstraintResultType::Violated)
        ));
    }

    /// both: distributed count meets Min frequency -> the trigger event publishes nothing
    /// the satisfied verdict is left to the case end, which decides on the recorded (finished) pull
    #[tokio::test]
    async fn both_min_no_violation() {
        let case = "c3";
        let (state, peers) = trigger_state(26020, Some(26021), case, MinMaxMode::Min, 2);
        let mut mgr_rx = register_manager(&state, PeerAddress::new_conformance_address(peers[0]));

        let h = handler(MinMaxMode::Min, 2, peers.clone());
        let ev = make_traced_event_with_fixed_timestamp(case, EID);
        h.handle_event(&ev, state.clone()).await;
        answer_one_conformance_request(state.clone(), &mut mgr_rx, counter_response(case, 3)).await;

        // give the detached pull time to finish: it must not publish a satisfied verdict
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            result_type(&state, case).is_none(),
            "a satisfied verdict must wait for the case end"
        );

        h.handle_case_end(&case.to_string(), state.clone()).await;
        assert!(matches!(
            result_type(&state, case),
            Some(ConstraintResultType::Satisfied)
        ));
    }

    /// both: distributed count below Min frequency → Violated.
    #[tokio::test]
    async fn both_min_violated() {
        let case = "c4";
        let (state, peers) = trigger_state(26030, Some(26031), case, MinMaxMode::Min, 3);
        let mut mgr_rx = register_manager(&state, PeerAddress::new_conformance_address(peers[0]));

        let h = handler(MinMaxMode::Min, 3, peers);
        let ev = make_traced_event_with_fixed_timestamp(case, EID);
        h.handle_event(&ev, state.clone()).await;
        answer_one_conformance_request(state.clone(), &mut mgr_rx, counter_response(case, 2)).await;

        assert!(matches!(
            await_result(&state, case).await,
            Some(ConstraintResultType::Violated)
        ));
    }

    /// only counter events (no trigger) → Satisfied
    #[tokio::test]
    async fn only_counter_events_satisfied_desired() {
        let case = "c5";
        let (state, peers) = trigger_state(26040, Some(26041), case, MinMaxMode::Max, 2);
        let h = handler(MinMaxMode::Max, 2, peers);
        h.handle_case_end(&case.to_string(), state.clone()).await;
        assert!(
            matches!(
                result_type(&state, case),
                Some(ConstraintResultType::Satisfied)
            ),
            "counter-only (no trigger) should be Satisfied"
        );
    }

    /// handle_event must return before the counter peer has answered: the event queue processor
    /// awaits it, so a round trip inside it would cost this node one rtt of throughput per event
    #[tokio::test]
    async fn handle_event_does_not_wait_for_the_counter_pull() {
        let case = "c6";
        let (state, peers) = trigger_state(26050, Some(26051), case, MinMaxMode::Min, 5);
        let _mgr_rx = register_manager(&state, PeerAddress::new_conformance_address(peers[0]));

        let h = handler(MinMaxMode::Min, 5, peers);
        let ev = make_traced_event_with_fixed_timestamp(case, EID);

        // nobody ever answers the pull, so this would block for the response timeout if the pull
        // were still inline
        tokio::time::timeout(
            Duration::from_millis(500),
            h.handle_event(&ev, state.clone()),
        )
        .await
        .expect("handle_event must not block on the counter pull");
    }
}

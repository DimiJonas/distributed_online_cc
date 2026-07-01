use crate::state::app_state::AppState;

/// creates and manages a cyclon overlay
///
/// P: initiating peer
/// Q: random peer of P
/// l: shuffling subset size ("shuffle length")
/// c: peer cache size (how many peers are kept at max)
///
/// basic shuffling:
///     each peer P periodically initiates shuffle:
///     1. select random subset of l neighbors (1 <= l <= c) of P cache and random peer Q in this subset
///     2. replace Q entry with P's own address in the subset
///     3. send updated subset to Q
///     4. receive subset from Q of length l
///     5. discard entries pointing to P and entries already known to P
///     6. update P's cache to include all remaining entries using empty slots first, then replacing entries that were originally sent to Q (subset sent by P)
///     when Q receives the subset it randomly selects a subset of its own of size t, sends it to P and updates its own cache using steps 5 and 6
///
/// enhanced shuffling (cyclon):
///     P chooses Q by age (oldest) and initiates the enhanced shuffle:
///     1. increase all neighbor ages by 1
///     2. select oldest neighbor as Q and additionally l-1 other random neighbors
///     3. replace Q's entry with P's own address and age 0
///     4. send updated subset to Q
///     5. receive subset from Q with i entries
///     6. discard entries of P and entries already known to P
///     7. update P's cache with the remaining entries by first using empty cache slots and then replacing entries that were sent to Q
///     when Q receives the subset from P it replies with a random subset of its own and updates entries using the received subset BUT it does not increase age
///     age is only increased when shuffling is initiated on its own
pub async fn cyclon_management_task(state: AppState) {}

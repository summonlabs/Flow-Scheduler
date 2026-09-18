// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#ifndef FLOW_SCHEDULER_SCHEDULE_HPP
#define FLOW_SCHEDULER_SCHEDULE_HPP

#include <cstdint>
#include <string>
#include <vector>

#include "flow_scheduler/identity.hpp"
#include "flow_scheduler/model.hpp"
#include "flow_scheduler/policy.hpp"
#include "flow_scheduler/time.hpp"

namespace flow_scheduler {

/// What the arbitration decided about one flow in one round.
enum class DecisionKind : std::uint8_t {
  /// Grant a service window to a ready flow.
  Run = 0,
  /// Revoke the current service window of a running flow.
  Preempt = 1,
};

[[nodiscard]] const char* to_string(DecisionKind kind) noexcept;

/// Why a flow was not selected, or why it was. Bounded, enumerable, and stable
/// so that explanations are machine checkable.
enum class DecisionReason : std::uint8_t {
  Selected = 0,
  /// Not released yet.
  AwaitingRelease = 1,
  /// A dependency is not authoritatively complete.
  AwaitingDependency = 2,
  /// The reservation window has not opened.
  AwaitingReservationWindow = 3,
  /// The reservation window closed before the flow could be dispatched.
  ReservationWindowClosed = 4,
  /// The resource had no remaining capacity in this interval.
  ResourceCapacityExhausted = 5,
  /// The resource concurrency ceiling was reached.
  ConcurrencyLimitReached = 6,
  /// The flow's deadline already passed.
  DeadlineMissed = 7,
  /// The schedule entry limit for this round was reached.
  ScheduleEntryLimitReached = 8,
  /// The flow was displaced by a strictly better candidate.
  DisplacedByHigherAuthority = 9,
  /// The candidate was prevented from running by the preemption throttle.
  PreemptionThrottled = 10,
  /// The incumbent was not preemptible.
  IncumbentNotPreemptible = 11,
  /// No legal quantum could be formed (zero remaining work or capacity).
  NoLegalQuantum = 12,
  /// The flow holds open work already.
  AlreadyInFlight = 13,
  /// The flow is terminal.
  Terminal = 14,
  /// The flow is in an unresolved ambiguous state after recovery.
  AmbiguousRequiresResolution = 15,
  /// The flow requires an explicit readiness signal that has not arrived.
  AwaitingReadinessSignal = 16,
  /// An undelivered Run entry was reclaimed because the delivery horizon
  /// elapsed before the entry was dispatched.
  DeliveryHorizonExpired = 17,
  /// The bound resource is missing or its generation moved on.
  ResourceUnavailable = 18,
  /// The active policy forbids displacing running work.
  PreemptionDisabled = 19,
  /// The active preemption mode does not authorize displacing this incumbent.
  PreemptionNotAuthorized = 20,
  /// The bound reservation has granted all of its capacity.
  ReservationExhausted = 21,
  /// A dependency reached a terminal state other than authoritative
  /// completion, so this flow can never become ready.
  DependencyFailed = 22,
};

[[nodiscard]] const char* to_string(DecisionReason reason) noexcept;

/// One arbitration decision. Every identity that justified the decision is
/// captured with its generation, so the decision can be revalidated later
/// against the exact authority it was made under.
struct ScheduleEntry {
  std::uint32_t ordinal{0};
  DecisionKind kind{DecisionKind::Run};

  FlowId flow{};
  Generation flow_generation{};
  DispatchAttemptId attempt{};

  PathId path{};
  Generation path_generation{};
  ResourceId resource{};
  Generation resource_generation{};
  ReservationId reservation{};
  Generation reservation_generation{};
  QoSClassId qos{};
  Generation qos_generation{};
  PriorityClassId priority{};
  Generation priority_generation{};

  /// Authorized service units. Always >= 1 for a Run entry.
  std::uint64_t quantum{0};
  Ticks start_tick{0};
  /// Absolute tick past which execution is illegal for this entry.
  Ticks deadline_tick{kNoDeadline};
  /// Exclusive end of the reservation window, when bound.
  Ticks reservation_end{kNoWindowBound};

  /// Arbitration tier that selected this entry (0 == most urgent).
  std::uint32_t tier{0};
  DecisionReason reason{DecisionReason::Selected};
  /// Bounded human readable explanation.
  std::string detail{};
  /// Digest over the ordering key that placed this entry.
  std::uint64_t ordering_digest{0};
};

/// One arbitration round. Fully self-describing: a Schedule can be validated,
/// replayed, and explained without reference to mutable scheduler state.
struct Schedule {
  ScheduleId schedule{};
  Generation generation{};
  FabricEpoch epoch{};
  PolicyId policy{};
  Generation policy_generation{};
  PolicyBinding policy_binding{};
  Ticks at_tick{0};
  std::vector<ScheduleEntry> entries{};
  /// Digest over the canonical encoding of the header and every entry.
  std::uint64_t digest{0};
  Provenance provenance{};

  [[nodiscard]] bool empty() const noexcept { return entries.empty(); }
  [[nodiscard]] std::size_t run_count() const noexcept;
  [[nodiscard]] std::size_t preempt_count() const noexcept;
  /// Deterministic canonical encoding, used for digests and durable records.
  [[nodiscard]] std::string canonical_form() const;

  friend bool operator==(const Schedule& a, const Schedule& b) noexcept;
  friend bool operator!=(const Schedule& a, const Schedule& b) noexcept {
    return !(a == b);
  }
};

[[nodiscard]] std::uint64_t compute_schedule_digest(const Schedule& schedule);

/// Per-round disposition of every flow the arbiter considered, so callers can
/// explain "why not this one" without guessing.
struct DeferredFlow {
  FlowId flow{};
  Generation generation{};
  DecisionReason reason{DecisionReason::AwaitingRelease};
  std::string detail{};
};

struct ArbitrationOutcome {
  Schedule schedule{};
  std::vector<DeferredFlow> deferred{};
  /// True when the deferred list was cut off at the per-round bound. The
  /// authoritative decision is still the schedule; this only bounds
  /// diagnostics.
  bool deferred_truncated{false};
  /// Flows whose deadline crossed during this round.
  std::vector<FlowId> deadline_misses{};
  /// Flows whose undelivered schedule entry was reclaimed this round.
  std::vector<FlowId> delivery_reclaims{};
};

}  // namespace flow_scheduler

#endif  // FLOW_SCHEDULER_SCHEDULE_HPP

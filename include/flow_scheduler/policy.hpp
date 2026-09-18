// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#ifndef FLOW_SCHEDULER_POLICY_HPP
#define FLOW_SCHEDULER_POLICY_HPP

#include <cstdint>
#include <string>

#include "flow_scheduler/identity.hpp"
#include "flow_scheduler/model.hpp"
#include "flow_scheduler/time.hpp"

namespace flow_scheduler {

/// How aggressively the runtime may displace running work.
enum class PreemptionMode : std::uint8_t {
  /// Never displace running work. A candidate simply waits.
  None = 0,
  /// Displace only when the candidate's priority rank is strictly better.
  StrictPriority = 1,
  /// Displace only when the candidate is deadline-urgent and the incumbent is
  /// not, or the incumbent has already crossed its deadline.
  Deadline = 2,
  /// Either of the above, plus displacement when the candidate's arbitration
  /// tier is strictly better than the incumbent's.
  Tiered = 3,
};

[[nodiscard]] const char* to_string(PreemptionMode mode) noexcept;

/// Ordering stage configuration. Every stage is a total order refinement, so
/// the composed ordering is always a strict total order over flow identities.
struct SchedulingPolicy {
  PolicyId policy{};
  Generation generation{};

  PreemptionMode preemption{PreemptionMode::Tiered};
  /// Order equal-tier, equal-priority candidates by earliest deadline.
  bool deadline_ordering{true};
  /// Order by weighted-fair virtual finish time inside a fairness group.
  bool weighted_fairness{true};
  /// Apply the minimum-service deficit boost.
  bool min_service_guarantee{true};
  /// A flow that reached its deadline is never dispatched, and a flow whose
  /// remaining slack is too small to fit a legal quantum is reported as a
  /// deadline miss rather than started.
  bool strict_deadlines{true};

  /// A continuously eligible flow must be scheduled within this many ticks.
  /// This is the starvation bound; it is enforced by the age-boost tier.
  Ticks starvation_bound_ticks{1'000'000};
  /// Deadline proximity at which a flow is promoted to the deadline tier.
  Ticks deadline_urgency_ticks{1'000'000};
  /// Minimum service an incumbent must have received before it can be
  /// displaced, which bounds preemption thrash.
  std::uint64_t min_preempt_service{1};
  /// Maximum preemptions allowed inside one preemption interval.
  std::uint32_t max_preemptions_per_interval{64};
  Ticks preemption_interval_ticks{1'000'000};

  /// Structural bounds applied when the policy is installed.
  std::uint32_t max_flows{kMaxFlows};
  std::uint32_t max_resources{kMaxResources};
  std::uint32_t max_reservations{kMaxReservations};
  std::uint32_t max_entries_per_schedule{kMaxEntriesPerSchedule};
  std::uint64_t max_quantum{kMaxQuantum};
  /// Default concurrency ceiling for resources that do not declare one.
  std::uint64_t default_max_overlap{1};
  /// Schedules retained for revalidation and explanation. Older schedules are
  /// retired together with their attempts; completions referencing them are
  /// rejected as stale.
  std::uint32_t retained_schedule_history{4096};

  /// A Run entry must be dispatched within this many ticks of the schedule it
  /// belongs to, or its reservation is reclaimed and the flow returns to
  /// Ready. Bounds how long capacity can be held by an undelivered decision.
  Ticks dispatch_delivery_horizon{1'000'000};

  Provenance provenance{};
};

[[nodiscard]] Status validate(const SchedulingPolicy& policy);

/// SchedulingPolicy plus its installed digest, so that every schedule can be
/// bound to the exact policy bytes that produced it.
struct PolicyBinding {
  PolicyId policy{};
  Generation generation{};
  std::uint64_t digest{0};

  friend bool operator==(const PolicyBinding& a, const PolicyBinding& b) noexcept {
    return a.policy == b.policy && a.generation == b.generation && a.digest == b.digest;
  }
};

[[nodiscard]] std::uint64_t digest_of(const SchedulingPolicy& policy);

}  // namespace flow_scheduler

#endif  // FLOW_SCHEDULER_POLICY_HPP

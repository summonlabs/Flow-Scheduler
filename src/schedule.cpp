// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#include "flow_scheduler/schedule.hpp"

#include "flow_scheduler/hash.hpp"

namespace flow_scheduler {
namespace {

/// canonical_form() is a debugging and test rendering. It is bounded so that a
/// maximal schedule cannot be asked to materialize an unbounded string; the
/// authoritative digest is computed incrementally over all fields instead.
constexpr std::size_t kMaxCanonicalFormEntries = 256;

void append_u64(std::string& out, std::uint64_t value) {
  out.push_back('=');
  out.append(std::to_string(value));
}

}  // namespace

const char* to_string(DecisionKind kind) noexcept {
  switch (kind) {
    case DecisionKind::Run: return "run";
    case DecisionKind::Preempt: return "preempt";
  }
  return "unknown-decision";
}

const char* to_string(DecisionReason reason) noexcept {
  switch (reason) {
    case DecisionReason::Selected: return "selected";
    case DecisionReason::AwaitingRelease: return "awaiting-release";
    case DecisionReason::AwaitingDependency: return "awaiting-dependency";
    case DecisionReason::AwaitingReservationWindow: return "awaiting-reservation-window";
    case DecisionReason::ReservationWindowClosed: return "reservation-window-closed";
    case DecisionReason::ResourceCapacityExhausted: return "resource-capacity-exhausted";
    case DecisionReason::ConcurrencyLimitReached: return "concurrency-limit-reached";
    case DecisionReason::DeadlineMissed: return "deadline-missed";
    case DecisionReason::ScheduleEntryLimitReached: return "schedule-entry-limit-reached";
    case DecisionReason::DisplacedByHigherAuthority: return "displaced-by-higher-authority";
    case DecisionReason::PreemptionThrottled: return "preemption-throttled";
    case DecisionReason::IncumbentNotPreemptible: return "incumbent-not-preemptible";
    case DecisionReason::NoLegalQuantum: return "no-legal-quantum";
    case DecisionReason::AlreadyInFlight: return "already-in-flight";
    case DecisionReason::Terminal: return "terminal";
    case DecisionReason::AmbiguousRequiresResolution: return "ambiguous-requires-resolution";
    case DecisionReason::AwaitingReadinessSignal: return "awaiting-readiness-signal";
    case DecisionReason::DeliveryHorizonExpired: return "delivery-horizon-expired";
    case DecisionReason::ResourceUnavailable: return "resource-unavailable";
    case DecisionReason::PreemptionDisabled: return "preemption-disabled";
    case DecisionReason::PreemptionNotAuthorized: return "preemption-not-authorized";
    case DecisionReason::ReservationExhausted: return "reservation-exhausted";
    case DecisionReason::DependencyFailed: return "dependency-failed";
  }
  return "unknown-reason";
}

std::size_t Schedule::run_count() const noexcept {
  std::size_t count = 0;
  for (const ScheduleEntry& entry : entries) {
    if (entry.kind == DecisionKind::Run) {
      ++count;
    }
  }
  return count;
}

std::size_t Schedule::preempt_count() const noexcept {
  std::size_t count = 0;
  for (const ScheduleEntry& entry : entries) {
    if (entry.kind == DecisionKind::Preempt) {
      ++count;
    }
  }
  return count;
}

std::string Schedule::canonical_form() const {
  std::string out;
  out.reserve(1024);
  out += "schedule";
  append_u64(out, schedule.value());
  out += " generation";
  append_u64(out, generation.value());
  out += " epoch";
  append_u64(out, epoch.value());
  out += " policy";
  append_u64(out, policy.value());
  out += " policy-generation";
  append_u64(out, policy_generation.value());
  out += " at";
  append_u64(out, at_tick);
  out += " entries";
  append_u64(out, entries.size());
  const std::size_t limit = entries.size() < kMaxCanonicalFormEntries
                                ? entries.size()
                                : kMaxCanonicalFormEntries;
  for (std::size_t index = 0; index < limit; ++index) {
    const ScheduleEntry& entry = entries[index];
    out += " | ";
    out += to_string(entry.kind);
    out += " ordinal";
    append_u64(out, entry.ordinal);
    out += " flow";
    append_u64(out, entry.flow.value());
    out += " flow-generation";
    append_u64(out, entry.flow_generation.value());
    out += " attempt";
    append_u64(out, entry.attempt.value());
    out += " path";
    append_u64(out, entry.path.value());
    out += " path-generation";
    append_u64(out, entry.path_generation.value());
    out += " resource";
    append_u64(out, entry.resource.value());
    out += " resource-generation";
    append_u64(out, entry.resource_generation.value());
    out += " reservation";
    append_u64(out, entry.reservation.value());
    out += " reservation-generation";
    append_u64(out, entry.reservation_generation.value());
    out += " qos";
    append_u64(out, entry.qos.value());
    out += " qos-generation";
    append_u64(out, entry.qos_generation.value());
    out += " priority";
    append_u64(out, entry.priority.value());
    out += " priority-generation";
    append_u64(out, entry.priority_generation.value());
    out += " quantum";
    append_u64(out, entry.quantum);
    out += " start";
    append_u64(out, entry.start_tick);
    out += " deadline";
    append_u64(out, entry.deadline_tick);
    out += " reservation-end";
    append_u64(out, entry.reservation_end);
    out += " tier";
    append_u64(out, entry.tier);
    out += " reason";
    out += to_string(entry.reason);
    out += " ordering";
    append_u64(out, entry.ordering_digest);
  }
  if (limit < entries.size()) {
    out += " | truncated";
    append_u64(out, entries.size() - limit);
  }
  return out;
}

std::uint64_t compute_schedule_digest(const Schedule& schedule) {
  Digest64 digest;
  digest.add_u64(schedule.schedule.value());
  digest.add_u64(schedule.generation.value());
  digest.add_u64(schedule.epoch.value());
  digest.add_u64(schedule.policy.value());
  digest.add_u64(schedule.policy_generation.value());
  digest.add_u64(schedule.policy_binding.digest);
  digest.add_u64(schedule.at_tick);
  digest.add_u64(schedule.entries.size());
  for (const ScheduleEntry& entry : schedule.entries) {
    digest.add_u32(entry.ordinal);
    digest.add_u8(static_cast<std::uint8_t>(entry.kind));
    digest.add_u64(entry.flow.value());
    digest.add_u64(entry.flow_generation.value());
    digest.add_u64(entry.attempt.value());
    digest.add_u64(entry.path.value());
    digest.add_u64(entry.path_generation.value());
    digest.add_u64(entry.resource.value());
    digest.add_u64(entry.resource_generation.value());
    digest.add_u64(entry.reservation.value());
    digest.add_u64(entry.reservation_generation.value());
    digest.add_u64(entry.qos.value());
    digest.add_u64(entry.qos_generation.value());
    digest.add_u64(entry.priority.value());
    digest.add_u64(entry.priority_generation.value());
    digest.add_u64(entry.quantum);
    digest.add_u64(entry.start_tick);
    digest.add_u64(entry.deadline_tick);
    digest.add_u64(entry.reservation_end);
    digest.add_u32(entry.tier);
    digest.add_u8(static_cast<std::uint8_t>(entry.reason));
    digest.add_u64(entry.ordering_digest);
    digest.add_string(entry.detail);
  }
  return digest.value();
}

bool operator==(const Schedule& a, const Schedule& b) noexcept {
  if (a.schedule != b.schedule || a.generation != b.generation || a.epoch != b.epoch ||
      a.policy != b.policy || a.policy_generation != b.policy_generation ||
      a.at_tick != b.at_tick || a.entries.size() != b.entries.size()) {
    return false;
  }
  for (std::size_t index = 0; index < a.entries.size(); ++index) {
    const ScheduleEntry& left = a.entries[index];
    const ScheduleEntry& right = b.entries[index];
    if (left.ordinal != right.ordinal || left.kind != right.kind || left.flow != right.flow ||
        left.flow_generation != right.flow_generation || left.attempt != right.attempt ||
        left.path != right.path || left.path_generation != right.path_generation ||
        left.resource != right.resource || left.resource_generation != right.resource_generation ||
        left.reservation != right.reservation ||
        left.reservation_generation != right.reservation_generation || left.qos != right.qos ||
        left.qos_generation != right.qos_generation || left.priority != right.priority ||
        left.priority_generation != right.priority_generation || left.quantum != right.quantum ||
        left.start_tick != right.start_tick || left.deadline_tick != right.deadline_tick ||
        left.reservation_end != right.reservation_end || left.tier != right.tier ||
        left.reason != right.reason || left.ordering_digest != right.ordering_digest) {
      return false;
    }
  }
  return true;
}

const char* to_string(PreemptionMode mode) noexcept {
  switch (mode) {
    case PreemptionMode::None: return "none";
    case PreemptionMode::StrictPriority: return "strict-priority";
    case PreemptionMode::Deadline: return "deadline";
    case PreemptionMode::Tiered: return "tiered";
  }
  return "unknown-preemption-mode";
}

Status validate(const SchedulingPolicy& policy) {
  if (!policy.policy.valid()) {
    return Status::failure(ErrorCode::InvalidArgument, "policy identity is unbound");
  }
  if (!policy.generation.valid()) {
    return Status::failure(ErrorCode::InvalidArgument, "policy generation is unbound");
  }
  if (policy.max_flows == 0 || policy.max_flows > kMaxFlows) {
    return Status::failure(ErrorCode::OutOfRange, "policy max_flows outside [1, kMaxFlows]");
  }
  if (policy.max_resources == 0 || policy.max_resources > kMaxResources) {
    return Status::failure(ErrorCode::OutOfRange, "policy max_resources outside [1, kMaxResources]");
  }
  if (policy.max_reservations == 0 || policy.max_reservations > kMaxReservations) {
    return Status::failure(ErrorCode::OutOfRange,
                           "policy max_reservations outside [1, kMaxReservations]");
  }
  if (policy.max_entries_per_schedule == 0 ||
      policy.max_entries_per_schedule > kMaxEntriesPerSchedule) {
    return Status::failure(ErrorCode::OutOfRange,
                           "policy max_entries_per_schedule outside [1, kMaxEntriesPerSchedule]");
  }
  if (policy.max_quantum == 0 || policy.max_quantum > kMaxQuantum) {
    return Status::failure(ErrorCode::OutOfRange, "policy max_quantum outside [1, kMaxQuantum]");
  }
  if (policy.default_max_overlap == 0 || policy.default_max_overlap > kMaxServiceUnits) {
    return Status::failure(ErrorCode::OutOfRange, "policy default_max_overlap is out of range");
  }
  if (policy.starvation_bound_ticks == 0 || policy.starvation_bound_ticks > kMaxTickHorizon) {
    return Status::failure(ErrorCode::OutOfRange,
                           "policy starvation_bound_ticks outside [1, kMaxTickHorizon]");
  }
  if (policy.deadline_urgency_ticks > kMaxTickHorizon) {
    return Status::failure(ErrorCode::OutOfRange,
                           "policy deadline_urgency_ticks beyond the tick horizon");
  }
  if (policy.min_preempt_service > kMaxServiceUnits) {
    return Status::failure(ErrorCode::OutOfRange, "policy min_preempt_service exceeds the bound");
  }
  if (policy.preemption_interval_ticks == 0 || policy.preemption_interval_ticks > kMaxTickHorizon) {
    return Status::failure(ErrorCode::OutOfRange,
                           "policy preemption_interval_ticks outside [1, kMaxTickHorizon]");
  }
  if (policy.max_preemptions_per_interval > (1u << 24)) {
    return Status::failure(ErrorCode::OutOfRange, "policy preemption throttle exceeds the bound");
  }
  if (policy.dispatch_delivery_horizon == 0 || policy.dispatch_delivery_horizon > kMaxTickHorizon) {
    return Status::failure(ErrorCode::OutOfRange,
                           "policy dispatch_delivery_horizon outside [1, kMaxTickHorizon]");
  }
  if (policy.retained_schedule_history < 16 || policy.retained_schedule_history > (1u << 20)) {
    return Status::failure(ErrorCode::OutOfRange,
                           "policy retained_schedule_history outside [16, 2^20]");
  }
  if (!policy.provenance.established()) {
    return Status::failure(ErrorCode::InvalidArgument, "policy provenance is not established");
  }
  return Status::success();
}

std::uint64_t digest_of(const SchedulingPolicy& policy) {
  Digest64 digest;
  digest.add_u64(policy.policy.value());
  digest.add_u64(policy.generation.value());
  digest.add_u8(static_cast<std::uint8_t>(policy.preemption));
  digest.add_bool(policy.deadline_ordering);
  digest.add_bool(policy.weighted_fairness);
  digest.add_bool(policy.min_service_guarantee);
  digest.add_bool(policy.strict_deadlines);
  digest.add_u64(policy.starvation_bound_ticks);
  digest.add_u64(policy.deadline_urgency_ticks);
  digest.add_u64(policy.min_preempt_service);
  digest.add_u32(policy.max_preemptions_per_interval);
  digest.add_u64(policy.preemption_interval_ticks);
  digest.add_u32(policy.max_flows);
  digest.add_u32(policy.max_resources);
  digest.add_u32(policy.max_reservations);
  digest.add_u32(policy.max_entries_per_schedule);
  digest.add_u64(policy.max_quantum);
  digest.add_u64(policy.default_max_overlap);
  digest.add_u32(policy.retained_schedule_history);
  digest.add_u64(policy.dispatch_delivery_horizon);
  return digest.value();
}

}  // namespace flow_scheduler

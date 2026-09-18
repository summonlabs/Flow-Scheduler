// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#include "flow_scheduler/model.hpp"

#include <algorithm>
#include <array>

#include "flow_scheduler/hash.hpp"

namespace flow_scheduler {

const char* to_string(FlowLifecycle lifecycle) noexcept {
  switch (lifecycle) {
    case FlowLifecycle::Waiting: return "waiting";
    case FlowLifecycle::Ready: return "ready";
    case FlowLifecycle::Scheduled: return "scheduled";
    case FlowLifecycle::Dispatched: return "dispatched";
    case FlowLifecycle::Running: return "running";
    case FlowLifecycle::CompletionReported: return "completion-reported";
    case FlowLifecycle::Completed: return "completed";
    case FlowLifecycle::Cancelling: return "cancelling";
    case FlowLifecycle::Cancelled: return "cancelled";
    case FlowLifecycle::Preempting: return "preempting";
    case FlowLifecycle::Failed: return "failed";
    case FlowLifecycle::Ambiguous: return "ambiguous";
  }
  return "unknown-lifecycle";
}

bool is_terminal(FlowLifecycle lifecycle) noexcept {
  return lifecycle == FlowLifecycle::Completed || lifecycle == FlowLifecycle::Cancelled ||
         lifecycle == FlowLifecycle::Failed;
}

bool holds_open_work(FlowLifecycle lifecycle) noexcept {
  switch (lifecycle) {
    case FlowLifecycle::Dispatched:
    case FlowLifecycle::Running:
    case FlowLifecycle::CompletionReported:
    case FlowLifecycle::Cancelling:
    case FlowLifecycle::Preempting:
      return true;
    default:
      return false;
  }
}

AccountingBucket bucket_of(FlowLifecycle lifecycle) noexcept {
  switch (lifecycle) {
    case FlowLifecycle::Waiting: return AccountingBucket::Waiting;
    case FlowLifecycle::Ready: return AccountingBucket::Ready;
    case FlowLifecycle::Scheduled: return AccountingBucket::Scheduled;
    case FlowLifecycle::Dispatched:
    case FlowLifecycle::Running:
    case FlowLifecycle::CompletionReported:
    case FlowLifecycle::Cancelling:
    case FlowLifecycle::Preempting:
      return AccountingBucket::InFlight;
    case FlowLifecycle::Completed: return AccountingBucket::Completed;
    case FlowLifecycle::Cancelled: return AccountingBucket::Cancelled;
    case FlowLifecycle::Failed: return AccountingBucket::Failed;
    case FlowLifecycle::Ambiguous: return AccountingBucket::Ambiguous;
  }
  return AccountingBucket::Waiting;
}

const char* to_string(AccountingBucket bucket) noexcept {
  switch (bucket) {
    case AccountingBucket::Waiting: return "waiting";
    case AccountingBucket::Ready: return "ready";
    case AccountingBucket::Scheduled: return "scheduled";
    case AccountingBucket::InFlight: return "in-flight";
    case AccountingBucket::Completed: return "completed";
    case AccountingBucket::Cancelled: return "cancelled";
    case AccountingBucket::Failed: return "failed";
    case AccountingBucket::Ambiguous: return "ambiguous";
  }
  return "unknown-bucket";
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

Status validate(const ResourceDescriptor& descriptor) {
  if (!descriptor.resource.valid()) {
    return Status::failure(ErrorCode::InvalidArgument, "resource identity is unbound");
  }
  if (!descriptor.generation.valid()) {
    return Status::failure(ErrorCode::InvalidArgument, "resource generation is unbound");
  }
  if (descriptor.capacity == 0 || descriptor.capacity > kMaxServiceUnits) {
    return Status::failure(ErrorCode::OutOfRange, "resource capacity outside [1, kMaxServiceUnits]");
  }
  if (descriptor.interval == 0 || descriptor.interval > kMaxTickHorizon) {
    return Status::failure(ErrorCode::OutOfRange, "resource interval outside [1, kMaxTickHorizon]");
  }
  if (descriptor.max_overlap == 0 || descriptor.max_overlap > kMaxServiceUnits) {
    return Status::failure(ErrorCode::OutOfRange, "resource max_overlap outside [1, kMaxServiceUnits]");
  }
  if (!descriptor.provenance.established()) {
    return Status::failure(ErrorCode::InvalidArgument, "resource provenance is not established");
  }
  return Status::success();
}

Status validate(const ReservationDescriptor& descriptor) {
  if (!descriptor.reservation.valid()) {
    return Status::failure(ErrorCode::InvalidArgument, "reservation identity is unbound");
  }
  if (!descriptor.generation.valid()) {
    return Status::failure(ErrorCode::InvalidArgument, "reservation generation is unbound");
  }
  if (!descriptor.resource.valid() || !descriptor.resource_generation.valid()) {
    return Status::failure(ErrorCode::InvalidArgument, "reservation resource binding is incomplete");
  }
  if (descriptor.window_end <= descriptor.window_begin) {
    return Status::failure(ErrorCode::InvalidArgument, "reservation window is empty or inverted");
  }
  if (descriptor.window_end > kMaxTickHorizon || descriptor.window_begin > kMaxTickHorizon) {
    return Status::failure(ErrorCode::OutOfRange, "reservation window beyond the tick horizon");
  }
  if (descriptor.capacity == 0 || descriptor.capacity > kMaxServiceUnits) {
    return Status::failure(ErrorCode::OutOfRange, "reservation capacity outside [1, kMaxServiceUnits]");
  }
  if (!descriptor.provenance.established()) {
    return Status::failure(ErrorCode::InvalidArgument, "reservation provenance is not established");
  }
  return Status::success();
}

Status validate(const PriorityClassDescriptor& descriptor) {
  if (!descriptor.priority.valid()) {
    return Status::failure(ErrorCode::InvalidArgument, "priority class identity is unbound");
  }
  if (!descriptor.generation.valid()) {
    return Status::failure(ErrorCode::InvalidArgument, "priority class generation is unbound");
  }
  if (descriptor.weight == 0 || descriptor.weight > kMaxServiceUnits) {
    return Status::failure(ErrorCode::OutOfRange, "priority class weight outside [1, kMaxServiceUnits]");
  }
  if (!descriptor.provenance.established()) {
    return Status::failure(ErrorCode::InvalidArgument, "priority class provenance is not established");
  }
  return Status::success();
}

Status validate(const QoSClassDescriptor& descriptor) {
  if (!descriptor.qos.valid()) {
    return Status::failure(ErrorCode::InvalidArgument, "qos class identity is unbound");
  }
  if (!descriptor.generation.valid()) {
    return Status::failure(ErrorCode::InvalidArgument, "qos class generation is unbound");
  }
  if (descriptor.max_service_per_dispatch > kMaxQuantum) {
    return Status::failure(ErrorCode::OutOfRange, "qos class dispatch bound exceeds kMaxQuantum");
  }
  if (!descriptor.provenance.established()) {
    return Status::failure(ErrorCode::InvalidArgument, "qos class provenance is not established");
  }
  return Status::success();
}

Status validate(const FlowDescriptor& descriptor) {
  if (!descriptor.flow.valid()) {
    return Status::failure(ErrorCode::InvalidArgument, "flow identity is unbound");
  }
  if (!descriptor.generation.valid()) {
    return Status::failure(ErrorCode::InvalidArgument, "flow generation is unbound");
  }
  if (!descriptor.path.bound()) {
    return Status::failure(ErrorCode::InvalidArgument, "flow path binding is incomplete");
  }
  if (!descriptor.resource.bound()) {
    return Status::failure(ErrorCode::InvalidArgument, "flow resource binding is incomplete");
  }
  if (!descriptor.priority.bound()) {
    return Status::failure(ErrorCode::InvalidArgument, "flow priority binding is incomplete");
  }
  if (!descriptor.qos.bound()) {
    return Status::failure(ErrorCode::InvalidArgument, "flow qos binding is incomplete");
  }
  if (!descriptor.fairness_group.valid()) {
    return Status::failure(ErrorCode::InvalidArgument, "flow fairness group is unbound");
  }
  // A partially bound reservation binding is never acceptable: either the flow
  // is unreserved, or it names both a reservation and its generation.
  if (descriptor.reservation.reservation.valid() != descriptor.reservation.generation.valid()) {
    return Status::failure(ErrorCode::InvalidArgument,
                           "flow reservation binding is partially specified");
  }
  if (descriptor.estimated_work == 0 || descriptor.estimated_work > kMaxServiceUnits) {
    return Status::failure(ErrorCode::OutOfRange,
                           "flow estimated_work outside [1, kMaxServiceUnits]");
  }
  if (descriptor.service_quantum == 0 || descriptor.service_quantum > kMaxQuantum) {
    return Status::failure(ErrorCode::OutOfRange,
                           "flow service_quantum outside [1, kMaxQuantum]");
  }
  if (descriptor.min_service > descriptor.estimated_work) {
    return Status::failure(ErrorCode::InvalidArgument,
                           "flow min_service exceeds estimated_work");
  }
  if (descriptor.min_service > 0 && descriptor.min_service_window == 0) {
    return Status::failure(ErrorCode::InvalidArgument,
                           "flow min_service requires a non-zero window");
  }
  if (descriptor.min_service_window > kMaxTickHorizon) {
    return Status::failure(ErrorCode::OutOfRange, "flow min_service_window beyond the tick horizon");
  }
  if (descriptor.release_tick > kMaxTickHorizon) {
    return Status::failure(ErrorCode::OutOfRange, "flow release_tick beyond the tick horizon");
  }
  if (has_deadline(descriptor.deadline_tick)) {
    if (descriptor.deadline_tick > kMaxTickHorizon) {
      return Status::failure(ErrorCode::OutOfRange, "flow deadline_tick beyond the tick horizon");
    }
    if (descriptor.deadline_tick <= descriptor.release_tick) {
      return Status::failure(ErrorCode::InvalidArgument,
                             "flow deadline_tick is not after release_tick");
    }
  }
  if (descriptor.weight == 0 || descriptor.weight > kMaxServiceUnits) {
    return Status::failure(ErrorCode::OutOfRange, "flow weight outside [1, kMaxServiceUnits]");
  }
  if (descriptor.dependencies.size() > kMaxDependenciesPerFlow) {
    return Status::failure(ErrorCode::Bounded, "flow dependency list exceeds the configured bound");
  }
  if (descriptor.readiness_signal > 2u) {
    return Status::failure(ErrorCode::InvalidArgument, "flow readiness signal is not a known value");
  }
  // Dependency list must be canonical: no self reference, no duplicates. A
  // non-canonical list would make the readiness computation order sensitive.
  std::vector<FlowId> sorted(descriptor.dependencies);
  std::sort(sorted.begin(), sorted.end());
  if (std::adjacent_find(sorted.begin(), sorted.end()) != sorted.end()) {
    return Status::failure(ErrorCode::InvalidArgument, "flow dependency list contains duplicates");
  }
  if (std::binary_search(sorted.begin(), sorted.end(), descriptor.flow)) {
    return Status::failure(ErrorCode::InvalidArgument, "flow depends on itself");
  }
  for (const FlowId dependency : descriptor.dependencies) {
    if (!dependency.valid()) {
      return Status::failure(ErrorCode::InvalidArgument, "flow dependency identity is unbound");
    }
  }
  if (!descriptor.provenance.established()) {
    return Status::failure(ErrorCode::InvalidArgument, "flow provenance is not established");
  }
  return Status::success();
}

// ---------------------------------------------------------------------------
// Digests
// ---------------------------------------------------------------------------

std::uint64_t digest_of(const ResourceDescriptor& descriptor) {
  Digest64 digest;
  digest.add_u64(descriptor.resource.value());
  digest.add_u64(descriptor.generation.value());
  digest.add_u64(descriptor.capacity);
  digest.add_u64(descriptor.interval);
  digest.add_u64(descriptor.max_overlap);
  return digest.value();
}

std::uint64_t digest_of(const ReservationDescriptor& descriptor) {
  Digest64 digest;
  digest.add_u64(descriptor.reservation.value());
  digest.add_u64(descriptor.generation.value());
  digest.add_u64(descriptor.resource.value());
  digest.add_u64(descriptor.resource_generation.value());
  digest.add_u64(descriptor.window_begin);
  digest.add_u64(descriptor.window_end);
  digest.add_u64(descriptor.capacity);
  digest.add_bool(descriptor.exclusive);
  return digest.value();
}

std::uint64_t digest_of(const PriorityClassDescriptor& descriptor) {
  Digest64 digest;
  digest.add_u64(descriptor.priority.value());
  digest.add_u64(descriptor.generation.value());
  digest.add_u32(descriptor.rank);
  digest.add_u64(descriptor.weight);
  return digest.value();
}

std::uint64_t digest_of(const QoSClassDescriptor& descriptor) {
  Digest64 digest;
  digest.add_u64(descriptor.qos.value());
  digest.add_u64(descriptor.generation.value());
  digest.add_bool(descriptor.preemption_protected);
  digest.add_u64(descriptor.max_service_per_dispatch);
  return digest.value();
}

std::uint64_t digest_of(const FlowDescriptor& descriptor) {
  Digest64 digest;
  digest.add_u64(descriptor.flow.value());
  digest.add_u64(descriptor.generation.value());
  digest.add_u64(descriptor.path.path.value());
  digest.add_u64(descriptor.path.generation.value());
  digest.add_u64(descriptor.resource.resource.value());
  digest.add_u64(descriptor.resource.generation.value());
  digest.add_u64(descriptor.reservation.reservation.value());
  digest.add_u64(descriptor.reservation.generation.value());
  digest.add_u64(descriptor.priority.priority.value());
  digest.add_u64(descriptor.priority.generation.value());
  digest.add_u64(descriptor.qos.qos.value());
  digest.add_u64(descriptor.qos.generation.value());
  digest.add_u64(descriptor.fairness_group.value());
  digest.add_u64(descriptor.estimated_work);
  digest.add_u64(descriptor.service_quantum);
  digest.add_u64(descriptor.min_service);
  digest.add_u64(descriptor.min_service_window);
  digest.add_u64(descriptor.release_tick);
  digest.add_u64(descriptor.deadline_tick);
  digest.add_u64(descriptor.weight);
  digest.add_bool(descriptor.preemptible);
  digest.add_bool(descriptor.auto_ready);
  digest.add_u8(descriptor.readiness_signal);
  digest.add_u64(descriptor.dependencies.size());
  // Dependencies are canonically sorted by validate(), so the digest is
  // independent of the order the caller supplied.
  std::vector<FlowId> sorted(descriptor.dependencies);
  std::sort(sorted.begin(), sorted.end());
  for (const FlowId dependency : sorted) {
    digest.add_u64(dependency.value());
  }
  digest.add_u64(descriptor.trace.value());
  return digest.value();
}

bool window_contains(const ReservationDescriptor& reservation, Ticks tick) noexcept {
  return tick >= reservation.window_begin && tick < reservation.window_end;
}

std::uint64_t reservation_grant_at(const ReservationDescriptor& reservation, Ticks tick) noexcept {
  return window_contains(reservation, tick) ? reservation.capacity : 0;
}

}  // namespace flow_scheduler

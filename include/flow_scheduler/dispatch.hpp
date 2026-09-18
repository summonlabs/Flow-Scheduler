// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#ifndef FLOW_SCHEDULER_DISPATCH_HPP
#define FLOW_SCHEDULER_DISPATCH_HPP

#include <cstdint>
#include <string>

#include "flow_scheduler/identity.hpp"
#include "flow_scheduler/model.hpp"
#include "flow_scheduler/schedule.hpp"
#include "flow_scheduler/time.hpp"

namespace flow_scheduler {

/// Lifecycle of a single dispatch attempt. Exactly one terminal state is
/// reached, and only Committed represents authoritative service.
enum class AttemptState : std::uint8_t {
  /// The dispatch frame was issued and durably journaled. The worker may or
  /// may not have observed it.
  Open = 0,
  /// The worker revalidated and acknowledged the start of execution.
  Started = 1,
  /// Service was reported and committed under current authority.
  Committed = 2,
  /// The attempt was legally revoked before the completion boundary.
  Preempted = 3,
  /// The owning flow was cancelled while the attempt was open.
  Cancelled = 4,
  /// The owning worker died, or the coordinator restarted, leaving the
  /// outcome unknown. Never silently converted into success or failure.
  Abandoned = 5,
  /// The worker rejected the frame at revalidation (stale authority).
  Rejected = 6,
};

[[nodiscard]] const char* to_string(AttemptState state) noexcept;
[[nodiscard]] bool is_attempt_terminal(AttemptState state) noexcept;

/// Total authority that justifies one dispatch. Every element is an exact
/// generation. A dispatch is legal only while every element still matches
/// current state, which is what makes stale work unable to complete.
struct DispatchTicket {
  DispatchAttemptId attempt{};
  FabricEpoch epoch{};

  ScheduleId schedule{};
  Generation schedule_generation{};

  FlowId flow{};
  Generation flow_generation{};

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

  PolicyId policy{};
  Generation policy_generation{};

  WorkerId worker{};
  BootId boot{};

  Ticks start_tick{0};
  /// Absolute tick past which execution under this ticket is illegal.
  Ticks deadline_tick{kNoDeadline};
  /// Exclusive end of the reservation window, when bound.
  Ticks reservation_end{kNoWindowBound};
  /// Authorized service units. Always >= 1 for an issued ticket.
  std::uint64_t quantum{0};

  AttemptState state{AttemptState::Open};
  /// Digest over every field above. Any change produces a different ticket.
  std::uint64_t digest{0};
};

/// Result of the observed effect reported by a worker. Only Served may credit
/// authoritative progress.
enum class CompletionOutcome : std::uint8_t {
  /// The worker performed the authorized service.
  Served = 0,
  /// The worker performed no service but stayed within authority.
  NoService = 1,
  /// The worker observed a failure. No progress is credited and the flow is
  /// failed; the attempt is still closed exactly once.
  Failed = 2,
};

[[nodiscard]] const char* to_string(CompletionOutcome outcome) noexcept;

/// Completion evidence must bind the schedule generation, dispatch attempt,
/// worker boot, epoch, flow generation, and observed result/effect. Evidence
/// that does not bind all of these is rejected as structurally invalid.
struct CompletionEvidence {
  ScheduleId schedule{};
  Generation schedule_generation{};
  DispatchAttemptId attempt{};
  FabricEpoch epoch{};

  FlowId flow{};
  Generation flow_generation{};

  WorkerId worker{};
  BootId boot{};

  CompletionOutcome outcome{CompletionOutcome::Served};
  /// Service units the worker claims to have performed.
  std::uint64_t served{0};
  /// Externally supplied effect identifier and digest of the observed effect.
  std::uint32_t effect_code{0};
  std::uint64_t effect_digest{0};
  /// Ticks observed at the worker; used for bounds checks, never for ordering.
  Ticks observed_tick{0};

  Provenance provenance{};

  /// Digest over the entire evidence tuple. Two byte-identical evidence
  /// payloads produce the same digest, which is what makes exact duplicate
  /// completion idempotent.
  [[nodiscard]] std::uint64_t digest() const;
};

/// What actually happened to authoritative state.
enum class CommitDisposition : std::uint8_t {
  /// The completion was applied and mutated authoritative state.
  Applied = 0,
  /// Byte-identical evidence for an already committed attempt. No mutation.
  IdempotentDuplicate = 1,
  /// Rejected. No mutation.
  Rejected = 2,
};

[[nodiscard]] const char* to_string(CommitDisposition disposition) noexcept;

struct CommitOutcome {
  CommitDisposition disposition{CommitDisposition::Rejected};
  ErrorCode code{ErrorCode::Ok};
  FlowLifecycle lifecycle_after{FlowLifecycle::Waiting};
  std::uint64_t credited{0};
  bool flow_completed{false};
  std::uint64_t outstanding_reserved_released{0};
  std::string detail{};
};

}  // namespace flow_scheduler

#endif  // FLOW_SCHEDULER_DISPATCH_HPP

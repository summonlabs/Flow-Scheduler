// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#ifndef FLOW_SCHEDULER_SCHEDULER_HPP
#define FLOW_SCHEDULER_SCHEDULER_HPP

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "flow_scheduler/accounting.hpp"
#include "flow_scheduler/dispatch.hpp"
#include "flow_scheduler/error.hpp"
#include "flow_scheduler/explain.hpp"
#include "flow_scheduler/identity.hpp"
#include "flow_scheduler/model.hpp"
#include "flow_scheduler/policy.hpp"
#include "flow_scheduler/schedule.hpp"
#include "flow_scheduler/time.hpp"

namespace flow_scheduler {

/// Why an epoch advanced. Every reason invalidates all outstanding dispatch
/// authority, because an epoch change means the coordinator's world view is no
/// longer the one that issued that authority.
enum class EpochReason : std::uint8_t {
  Explicit = 0,
  CoordinatorRestart = 1,
  PolicyChange = 2,
  TopologyChange = 3,
  Recovery = 4,
};

[[nodiscard]] const char* to_string(EpochReason reason) noexcept;

/// How an operator resolves a flow whose outcome was left unknown by a worker
/// death or coordinator restart.
enum class AmbiguityResolution : std::uint8_t {
  /// The flow still has remaining work and may be re-arbitrated.
  Retry = 0,
  /// The flow is declared failed and becomes terminal.
  Abandon = 1,
};

[[nodiscard]] const char* to_string(AmbiguityResolution resolution) noexcept;

/// Caller supplied readiness signal. Unset means readiness is derived purely
/// from release tick and dependencies.
enum class ReadinessSignal : std::uint8_t {
  Unset = 0,
  Ready = 1,
  NotReady = 2,
};

struct SchedulerOptions {
  SchedulingPolicy policy{};
  /// Empty string selects a volatile in-memory scheduler with no durability.
  /// Non-empty selects a durable scheduler whose directory holds the journal
  /// and snapshot files.
  std::string state_directory{};
  /// Flush to stable storage before acknowledging a durable mutation. Disabling
  /// this is only meaningful for synthetic throughput measurement.
  bool flush_to_storage{true};
  /// Refuse to open a state directory written by an incompatible format.
  bool require_compatible_format{true};
};

/// What happened while opening an existing state directory.
struct RecoveryReport {
  bool recovered{false};
  FabricEpoch epoch_before{};
  FabricEpoch epoch_after{};
  std::uint64_t flows_restored{0};
  std::uint64_t resources_restored{0};
  std::uint64_t reservations_restored{0};
  std::uint64_t priority_classes_restored{0};
  std::uint64_t qos_classes_restored{0};
  std::uint64_t attempts_abandoned{0};
  std::uint64_t flows_made_ambiguous{0};
  std::uint64_t schedules_retired{0};
  std::uint64_t journal_records_replayed{0};
  std::uint64_t journal_bytes_replayed{0};
  std::uint64_t journal_records_skipped{0};
  std::uint64_t snapshot_records{0};
  /// True when a partial record at the journal tail was discarded. This is the
  /// expected artifact of a crash during append, not corruption.
  bool tail_truncated{false};
  std::string detail{};
};

/// Deterministic temporal arbiter over already-admitted flows.
///
/// Thread safety: every public method takes one internal, non-recursive mutex
/// for the whole call. The runtime never invokes caller callbacks, never emits
/// events, and never acquires a second lock while holding that mutex, so there
/// is no re-entry path and no lock ordering to get wrong. Durable writes happen
/// inside the same critical section because "durable before acknowledged" is
/// only meaningful if the acknowledgement and the write are ordered together.
class Scheduler {
 public:
  Scheduler(const Scheduler&) = delete;
  Scheduler& operator=(const Scheduler&) = delete;
  Scheduler(Scheduler&&) = delete;
  Scheduler& operator=(Scheduler&&) = delete;
  ~Scheduler();

  /// Create a scheduler. With a state directory that already holds durable
  /// state, this recovers it: committed state is restored, unresolved attempts
  /// become Abandoned, and the fabric epoch advances. Liveness is never
  /// restored.
  [[nodiscard]] static Result<std::unique_ptr<Scheduler>> create(
      const SchedulerOptions& options);

  // --- external topology ---------------------------------------------------

  [[nodiscard]] Status register_resource(const ResourceDescriptor& descriptor);
  [[nodiscard]] Status register_reservation(const ReservationDescriptor& descriptor);
  [[nodiscard]] Status retire_reservation(ReservationId reservation, Generation generation);
  [[nodiscard]] Status register_priority_class(const PriorityClassDescriptor& descriptor);
  [[nodiscard]] Status register_qos_class(const QoSClassDescriptor& descriptor);

  // --- flow lifecycle ------------------------------------------------------

  [[nodiscard]] Status admit_flow(const FlowDescriptor& descriptor);
  /// Replace a flow's descriptor. The new generation must be exactly the
  /// current generation plus one; any other value is rejected, and the old
  /// generation's authority is retired.
  [[nodiscard]] Status update_flow(const FlowDescriptor& descriptor);
  [[nodiscard]] Status cancel_flow(FlowId flow, Generation generation,
                                   std::string_view reason);
  [[nodiscard]] Status notify_readiness(FlowId flow, Generation generation,
                                        ReadinessSignal signal, Ticks now);
  [[nodiscard]] Status resolve_ambiguous(FlowId flow, Generation generation,
                                         AmbiguityResolution resolution, Ticks now);
  /// Remove a terminal flow from the table. Non-terminal flows are refused.
  [[nodiscard]] Status retire_flow(FlowId flow, Generation generation);

  // --- authority -----------------------------------------------------------

  [[nodiscard]] Status install_policy(const SchedulingPolicy& policy);
  [[nodiscard]] Result<FabricEpoch> advance_epoch(EpochReason reason);
  [[nodiscard]] Result<FabricEpoch> epoch() const;
  [[nodiscard]] Result<PolicyBinding> policy_binding() const;

  // --- arbitration ---------------------------------------------------------

  /// Run one arbitration round at the supplied tick. Repeated calls with the
  /// same tick and unchanged state produce schedules with identical structure
  /// and ordering; only the schedule identity advances.
  [[nodiscard]] Result<ArbitrationOutcome> arbitrate(Ticks now);

  /// Release the still-undelivered Run entries of a schedule, returning those
  /// flows to Ready and releasing their reserved capacity.
  [[nodiscard]] Status release_schedule(ScheduleId schedule, Generation generation);

  // --- dispatch ------------------------------------------------------------

  /// Revalidate the whole authority tuple and open a dispatch attempt. Fails
  /// with a staleness code when any generation, the epoch, the deadline, the
  /// reservation window, the capacity, or the flow lifecycle no longer
  /// permits execution.
  [[nodiscard]] Result<DispatchTicket> begin_dispatch(ScheduleId schedule,
                                                      Generation generation,
                                                      std::uint32_t ordinal,
                                                      WorkerId worker, BootId boot,
                                                      Ticks now);

  /// Re-check a ticket against current state without mutating anything.
  [[nodiscard]] Status revalidate(const DispatchTicket& ticket, Ticks now) const;

  [[nodiscard]] Status mark_started(const DispatchTicket& ticket, Ticks now);
  [[nodiscard]] Status abandon_attempt(DispatchAttemptId attempt, std::string_view reason,
                                       Ticks now);
  [[nodiscard]] Status preempt_attempt(DispatchAttemptId attempt, std::string_view reason,
                                       Ticks now);

  // --- completion ----------------------------------------------------------

  /// Commit completion evidence. Exact duplicates are idempotent and mutate
  /// nothing; stale, contradictory, cancelled, or preempted evidence is
  /// rejected without mutating authoritative state.
  [[nodiscard]] Result<CommitOutcome> complete(const CompletionEvidence& evidence,
                                               Ticks now);

  // --- observation ---------------------------------------------------------

  [[nodiscard]] Result<FlowSnapshot> flow(FlowId id) const;
  [[nodiscard]] Result<FlowExplanation> explain_flow(FlowId id) const;
  [[nodiscard]] Result<Schedule> schedule(ScheduleId id) const;
  [[nodiscard]] Result<ScheduleExplanation> explain_schedule(ScheduleId id) const;
  [[nodiscard]] Result<DispatchTicket> attempt(DispatchAttemptId id) const;
  [[nodiscard]] AccountingReport accounting() const;
  [[nodiscard]] const RecoveryReport& recovery_report() const;
  [[nodiscard]] Result<SchedulingPolicy> policy() const;
  [[nodiscard]] std::size_t flow_count() const;
  [[nodiscard]] std::size_t attempt_count() const;
  /// Durable journal sequence position, or 0 in volatile mode.
  [[nodiscard]] std::uint64_t journal_position() const;
  /// Force the durable store to fold the journal into a snapshot.
  [[nodiscard]] Status checkpoint();

 private:
  class Impl;
  explicit Scheduler(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

}  // namespace flow_scheduler

#endif  // FLOW_SCHEDULER_SCHEDULER_HPP

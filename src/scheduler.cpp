// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#include "flow_scheduler/scheduler.hpp"

#include <algorithm>
#include <deque>
#include <limits>
#include <map>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include <set>

#include "flow_scheduler/hash.hpp"
#include "flow_scheduler/journal.hpp"
#include "flow_scheduler/protocol.hpp"
#include "flow_scheduler/version.hpp"

namespace flow_scheduler {
namespace {

/// Bounded per-round diagnostic list. The schedule itself is the authoritative
/// decision; the deferral list exists only to explain it.
constexpr std::size_t kMaxDeferredPerRound = 4096;

/// Retention bound on a flow's explanation history.
constexpr std::size_t kMaxExplanationSteps = 32;

/// Conservative serialized size budget for one schedule entry. Used to cap the
/// number of entries in a round so that a schedule record always fits inside a
/// single journal record. Deterministic and independent of machine state.
constexpr std::size_t kScheduleEntryBudgetBytes = 320;

/// Virtual-time scale for weighted fair ordering. Fixed point, so ordering is
/// integer exact and never depends on floating point rounding.
constexpr std::uint64_t kVirtualTimeScale = 1ull << 20;

/// Arbitration tiers, most urgent first. The tier is the primary ordering key
/// and is what bounds starvation: a continuously eligible flow is promoted to
/// the starvation tier and then outranks all lower tiers deterministically.
constexpr std::uint32_t kTierDeadlineUrgent = 0;
constexpr std::uint32_t kTierStarvation = 1;
constexpr std::uint32_t kTierMinServiceDeficient = 2;
constexpr std::uint32_t kTierNormal = 3;

std::uint64_t saturating_add_u64(std::uint64_t a, std::uint64_t b) {
  return (std::numeric_limits<std::uint64_t>::max() - a < b) ? std::numeric_limits<std::uint64_t>::max()
                                                             : a + b;
}

}  // namespace

const char* to_string(EpochReason reason) noexcept {
  switch (reason) {
    case EpochReason::Explicit: return "explicit";
    case EpochReason::CoordinatorRestart: return "coordinator-restart";
    case EpochReason::PolicyChange: return "policy-change";
    case EpochReason::TopologyChange: return "topology-change";
    case EpochReason::Recovery: return "recovery";
  }
  return "unknown-epoch-reason";
}

const char* to_string(AmbiguityResolution resolution) noexcept {
  switch (resolution) {
    case AmbiguityResolution::Retry: return "retry";
    case AmbiguityResolution::Abandon: return "abandon";
  }
  return "unknown-ambiguity-resolution";
}

// ---------------------------------------------------------------------------
// Runtime state
// ---------------------------------------------------------------------------

namespace {

struct FlowRuntime {
  FlowDescriptor descriptor{};
  FlowLifecycle lifecycle{FlowLifecycle::Waiting};
  std::uint64_t served_work{0};
  std::uint64_t outstanding_reserved{0};

  DispatchAttemptId open_attempt{};
  ScheduleId scheduled_schedule{};
  Generation scheduled_generation{};
  std::uint32_t scheduled_ordinal{0};
  Ticks scheduled_tick{0};

  Ticks last_change_tick{0};
  Ticks eligible_since_tick{0};
  bool ever_ready{false};
  Ticks min_service_window_start{0};
  std::uint64_t min_service_delivered{0};
  /// Weighted-fair-queueing virtual finish number, integer fixed point.
  std::uint64_t virtual_finish{0};
  bool deadline_missed_counted{false};
  /// True once the age tier has been recorded for the current eligibility
  /// interval, so the counter measures episodes rather than rounds.
  bool starvation_counted{false};

  FlowAccounting accounting{};
  std::vector<ExplanationStep> history{};
  bool history_truncated{false};
};

struct ResourceRuntime {
  ResourceDescriptor descriptor{};
  std::uint64_t interval_index{0};
  bool interval_initialized{false};
  std::uint64_t reserved_this_interval{0};
  std::uint64_t in_flight{0};
};

struct ReservationRuntime {
  ReservationDescriptor descriptor{};
  std::uint64_t reserved_units{0};
  bool retired{false};
};

struct ScheduleRuntime {
  Schedule schedule{};
  /// 0 == undelivered Run entry, 1 == consumed (dispatched), 2 == released.
  std::vector<std::uint8_t> consumed{};
  /// Open (non-terminal) attempts belonging to this schedule.
  std::uint64_t open_attempts{0};
  /// Run entries that have neither been dispatched nor released.
  std::uint64_t pending_entries{0};
};

struct AttemptRuntime {
  DispatchTicket ticket{};
  AttemptState state{AttemptState::Open};
  Ticks opened_tick{0};
  Ticks closed_tick{0};
  bool has_evidence{false};
  std::uint64_t evidence_digest{0};
  std::uint64_t credited{0};
  std::string close_detail{};
};

/// Total ordering key. Comparison is lexicographic over these fields and ends
/// in the flow identity, so the order is always strict and total.
struct OrderKey {
  std::uint32_t tier{kTierNormal};
  std::uint32_t priority_rank{0};
  Ticks deadline{0};
  std::uint64_t virtual_finish{0};
  std::uint64_t inverted_weight{0};
  FlowId flow{};
  Generation generation{};
};

bool operator<(const OrderKey& a, const OrderKey& b) {
  if (a.tier != b.tier) return a.tier < b.tier;
  if (a.priority_rank != b.priority_rank) return a.priority_rank < b.priority_rank;
  if (a.deadline != b.deadline) return a.deadline < b.deadline;
  if (a.virtual_finish != b.virtual_finish) return a.virtual_finish < b.virtual_finish;
  if (a.inverted_weight != b.inverted_weight) return a.inverted_weight < b.inverted_weight;
  if (a.flow != b.flow) return a.flow < b.flow;
  return a.generation < b.generation;
}

std::uint64_t digest_of(const OrderKey& key) {
  Digest64 digest;
  digest.add_u32(key.tier);
  digest.add_u32(key.priority_rank);
  digest.add_u64(key.deadline);
  digest.add_u64(key.virtual_finish);
  digest.add_u64(key.inverted_weight);
  digest.add_u64(key.flow.value());
  digest.add_u64(key.generation.value());
  return digest.value();
}

// --- descriptor serialization ----------------------------------------------

void write_flow_descriptor(RecordWriter& writer, const FlowDescriptor& descriptor) {
  writer.u64(descriptor.flow.value());
  writer.u64(descriptor.generation.value());
  writer.u64(descriptor.path.path.value());
  writer.u64(descriptor.path.generation.value());
  writer.u64(descriptor.resource.resource.value());
  writer.u64(descriptor.resource.generation.value());
  writer.u64(descriptor.reservation.reservation.value());
  writer.u64(descriptor.reservation.generation.value());
  writer.u64(descriptor.priority.priority.value());
  writer.u64(descriptor.priority.generation.value());
  writer.u64(descriptor.qos.qos.value());
  writer.u64(descriptor.qos.generation.value());
  writer.u64(descriptor.fairness_group.value());
  writer.u64(descriptor.estimated_work);
  writer.u64(descriptor.service_quantum);
  writer.u64(descriptor.min_service);
  writer.u64(descriptor.min_service_window);
  writer.u64(descriptor.release_tick);
  writer.u64(descriptor.deadline_tick);
  writer.u64(descriptor.weight);
  writer.boolean(descriptor.preemptible);
  writer.boolean(descriptor.auto_ready);
  writer.u8(descriptor.readiness_signal);
  writer.u64(descriptor.dependencies.size());
  for (const FlowId dependency : descriptor.dependencies) {
    writer.u64(dependency.value());
  }
  writer.u64(descriptor.trace.value());
  writer.u8(static_cast<std::uint8_t>(descriptor.provenance.origin));
  writer.u64(descriptor.provenance.sequence);
  writer.u64(descriptor.provenance.digest);
}

Result<FlowDescriptor> read_flow_descriptor(RecordReader& reader) {
  FlowDescriptor descriptor;
  std::uint64_t value = 0;
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.flow = FlowId(value);
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.generation = Generation(value);
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.path.path = PathId(value);
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.path.generation = Generation(value);
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.resource.resource = ResourceId(value);
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.resource.generation = Generation(value);
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.reservation.reservation = ReservationId(value);
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.reservation.generation = Generation(value);
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.priority.priority = PriorityClassId(value);
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.priority.generation = Generation(value);
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.qos.qos = QoSClassId(value);
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.qos.generation = Generation(value);
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.fairness_group = FairnessGroupId(value);
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.estimated_work = value;
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.service_quantum = value;
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.min_service = value;
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.min_service_window = value;
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.release_tick = value;
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.deadline_tick = value;
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.weight = value;
  FS_TRY_ASSIGN(descriptor.preemptible, reader.boolean());
  FS_TRY_ASSIGN(descriptor.auto_ready, reader.boolean());
  std::uint8_t signal = 0;
  FS_TRY_ASSIGN(signal, reader.u8());
  if (signal > 2u) {
    return Error(ErrorCode::CorruptData, "durable flow readiness signal is out of range");
  }
  descriptor.readiness_signal = signal;
  std::uint64_t dependency_count = 0;
  FS_TRY_ASSIGN(dependency_count, reader.u64());
  if (dependency_count > kMaxDependenciesPerFlow) {
    return Error(ErrorCode::CorruptData, "durable flow dependency count exceeds the bound");
  }
  descriptor.dependencies.reserve(static_cast<std::size_t>(dependency_count));
  for (std::uint64_t index = 0; index < dependency_count; ++index) {
    std::uint64_t dependency = 0;
    FS_TRY_ASSIGN(dependency, reader.u64());
    descriptor.dependencies.push_back(FlowId(dependency));
  }
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.trace = TraceId(value);
  std::uint8_t origin = 0;
  FS_TRY_ASSIGN(origin, reader.u8());
  if (origin > 4u) {
    return Error(ErrorCode::CorruptData, "durable flow provenance origin is out of range");
  }
  descriptor.provenance.origin = static_cast<ProvenanceOrigin>(origin);
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.provenance.sequence = value;
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.provenance.digest = value;
  return descriptor;
}

void write_schedule(RecordWriter& writer, const Schedule& schedule) {
  writer.u64(schedule.schedule.value());
  writer.u64(schedule.generation.value());
  writer.u64(schedule.epoch.value());
  writer.u64(schedule.policy.value());
  writer.u64(schedule.policy_generation.value());
  writer.u64(schedule.policy_binding.policy.value());
  writer.u64(schedule.policy_binding.generation.value());
  writer.u64(schedule.policy_binding.digest);
  writer.u64(schedule.at_tick);
  writer.u64(schedule.entries.size());
  for (const ScheduleEntry& entry : schedule.entries) {
    writer.u32(entry.ordinal);
    writer.u8(static_cast<std::uint8_t>(entry.kind));
    writer.u64(entry.flow.value());
    writer.u64(entry.flow_generation.value());
    writer.u64(entry.attempt.value());
    writer.u64(entry.path.value());
    writer.u64(entry.path_generation.value());
    writer.u64(entry.resource.value());
    writer.u64(entry.resource_generation.value());
    writer.u64(entry.reservation.value());
    writer.u64(entry.reservation_generation.value());
    writer.u64(entry.qos.value());
    writer.u64(entry.qos_generation.value());
    writer.u64(entry.priority.value());
    writer.u64(entry.priority_generation.value());
    writer.u64(entry.quantum);
    writer.u64(entry.start_tick);
    writer.u64(entry.deadline_tick);
    writer.u64(entry.reservation_end);
    writer.u32(entry.tier);
    writer.u8(static_cast<std::uint8_t>(entry.reason));
    writer.u64(entry.ordering_digest);
    writer.text(entry.detail);
  }
  writer.u64(schedule.digest);
  writer.u8(static_cast<std::uint8_t>(schedule.provenance.origin));
  writer.u64(schedule.provenance.sequence);
  writer.u64(schedule.provenance.digest);
}

Result<Schedule> read_schedule(RecordReader& reader) {
  Schedule schedule;
  std::uint64_t value = 0;
  FS_TRY_ASSIGN(value, reader.u64()); schedule.schedule = ScheduleId(value);
  FS_TRY_ASSIGN(value, reader.u64()); schedule.generation = Generation(value);
  FS_TRY_ASSIGN(value, reader.u64()); schedule.epoch = FabricEpoch(value);
  FS_TRY_ASSIGN(value, reader.u64()); schedule.policy = PolicyId(value);
  FS_TRY_ASSIGN(value, reader.u64()); schedule.policy_generation = Generation(value);
  FS_TRY_ASSIGN(value, reader.u64()); schedule.policy_binding.policy = PolicyId(value);
  FS_TRY_ASSIGN(value, reader.u64()); schedule.policy_binding.generation = Generation(value);
  FS_TRY_ASSIGN(value, reader.u64()); schedule.policy_binding.digest = value;
  FS_TRY_ASSIGN(value, reader.u64()); schedule.at_tick = value;
  std::uint64_t entry_count = 0;
  FS_TRY_ASSIGN(entry_count, reader.u64());
  if (entry_count > kMaxEntriesPerSchedule) {
    return Error(ErrorCode::CorruptData, "durable schedule entry count exceeds the bound");
  }
  schedule.entries.reserve(static_cast<std::size_t>(entry_count));
  for (std::uint64_t index = 0; index < entry_count; ++index) {
    ScheduleEntry entry;
    std::uint32_t ordinal = 0;
    FS_TRY_ASSIGN(ordinal, reader.u32()); entry.ordinal = ordinal;
    std::uint8_t kind = 0;
    FS_TRY_ASSIGN(kind, reader.u8());
    if (kind > 1u) {
      return Error(ErrorCode::CorruptData, "durable schedule decision kind is out of range");
    }
    entry.kind = static_cast<DecisionKind>(kind);
    FS_TRY_ASSIGN(value, reader.u64()); entry.flow = FlowId(value);
    FS_TRY_ASSIGN(value, reader.u64()); entry.flow_generation = Generation(value);
    FS_TRY_ASSIGN(value, reader.u64()); entry.attempt = DispatchAttemptId(value);
    FS_TRY_ASSIGN(value, reader.u64()); entry.path = PathId(value);
    FS_TRY_ASSIGN(value, reader.u64()); entry.path_generation = Generation(value);
    FS_TRY_ASSIGN(value, reader.u64()); entry.resource = ResourceId(value);
    FS_TRY_ASSIGN(value, reader.u64()); entry.resource_generation = Generation(value);
    FS_TRY_ASSIGN(value, reader.u64()); entry.reservation = ReservationId(value);
    FS_TRY_ASSIGN(value, reader.u64()); entry.reservation_generation = Generation(value);
    FS_TRY_ASSIGN(value, reader.u64()); entry.qos = QoSClassId(value);
    FS_TRY_ASSIGN(value, reader.u64()); entry.qos_generation = Generation(value);
    FS_TRY_ASSIGN(value, reader.u64()); entry.priority = PriorityClassId(value);
    FS_TRY_ASSIGN(value, reader.u64()); entry.priority_generation = Generation(value);
    FS_TRY_ASSIGN(value, reader.u64()); entry.quantum = value;
    FS_TRY_ASSIGN(value, reader.u64()); entry.start_tick = value;
    FS_TRY_ASSIGN(value, reader.u64()); entry.deadline_tick = value;
    FS_TRY_ASSIGN(value, reader.u64()); entry.reservation_end = value;
    std::uint32_t tier = 0;
    FS_TRY_ASSIGN(tier, reader.u32()); entry.tier = tier;
    std::uint8_t reason = 0;
    FS_TRY_ASSIGN(reason, reader.u8());
    if (reason > static_cast<std::uint8_t>(DecisionReason::AmbiguousRequiresResolution)) {
      return Error(ErrorCode::CorruptData, "durable schedule decision reason is out of range");
    }
    entry.reason = static_cast<DecisionReason>(reason);
    FS_TRY_ASSIGN(value, reader.u64()); entry.ordering_digest = value;
    std::string_view detail;
    FS_TRY_ASSIGN(detail, reader.text());
    if (detail.size() > kMaxExplanationBytes) {
      return Error(ErrorCode::CorruptData, "durable schedule entry detail exceeds the bound");
    }
    entry.detail.assign(detail.data(), detail.size());
    schedule.entries.push_back(std::move(entry));
  }
  FS_TRY_ASSIGN(value, reader.u64()); schedule.digest = value;
  std::uint8_t origin = 0;
  FS_TRY_ASSIGN(origin, reader.u8());
  if (origin > 4u) {
    return Error(ErrorCode::CorruptData, "durable schedule provenance origin is out of range");
  }
  schedule.provenance.origin = static_cast<ProvenanceOrigin>(origin);
  FS_TRY_ASSIGN(value, reader.u64()); schedule.provenance.sequence = value;
  FS_TRY_ASSIGN(value, reader.u64()); schedule.provenance.digest = value;
  return schedule;
}

}  // namespace

// ---------------------------------------------------------------------------
// Scheduler::Impl
// ---------------------------------------------------------------------------

class Scheduler::Impl {
 public:
  explicit Impl(const SchedulerOptions& options);

  [[nodiscard]] Status initialize();

  Status register_resource(const ResourceDescriptor& descriptor);
  Status register_reservation(const ReservationDescriptor& descriptor);
  Status retire_reservation(ReservationId reservation, Generation generation);
  Status register_priority_class(const PriorityClassDescriptor& descriptor);
  Status register_qos_class(const QoSClassDescriptor& descriptor);

  Status admit_flow(const FlowDescriptor& descriptor);
  Status update_flow(const FlowDescriptor& descriptor);
  Status cancel_flow(FlowId flow, Generation generation, std::string_view reason, Ticks now);
  Status notify_readiness(FlowId flow, Generation generation, ReadinessSignal signal, Ticks now);
  Status resolve_ambiguous(FlowId flow, Generation generation, AmbiguityResolution resolution,
                           Ticks now);
  Status retire_flow(FlowId flow, Generation generation);

  Status install_policy(const SchedulingPolicy& policy);
  Result<FabricEpoch> advance_epoch(EpochReason reason, Ticks now);

  Result<ArbitrationOutcome> arbitrate(Ticks now);
  Status release_schedule(ScheduleId schedule, Generation generation, Ticks now);

  Result<DispatchTicket> begin_dispatch(ScheduleId schedule, Generation generation,
                                        std::uint32_t ordinal, WorkerId worker, BootId boot,
                                        Ticks now);
  Status revalidate(const DispatchTicket& ticket, Ticks now) const;
  [[nodiscard]] Status revalidate_unlocked(const DispatchTicket& ticket, Ticks now) const;
  Status mark_started(const DispatchTicket& ticket, Ticks now);
  Status abandon_attempt(DispatchAttemptId attempt, std::string_view reason, Ticks now);
  Status preempt_attempt(DispatchAttemptId attempt, std::string_view reason, Ticks now);

  Result<CommitOutcome> complete(const CompletionEvidence& evidence, Ticks now);

  Result<FlowSnapshot> flow(FlowId id) const;
  Result<FlowExplanation> explain_flow(FlowId id) const;
  Result<Schedule> schedule(ScheduleId id) const;
  Result<ScheduleExplanation> explain_schedule(ScheduleId id) const;
  Result<DispatchTicket> attempt(DispatchAttemptId id) const;
  AccountingReport accounting() const;
  std::size_t flow_count() const;
  std::size_t attempt_count() const;
  std::uint64_t journal_position() const;
  Status checkpoint();

  [[nodiscard]] const RecoveryReport& recovery_report() const { return recovery_; }
  [[nodiscard]] Result<SchedulingPolicy> policy_copy() const;
  [[nodiscard]] Result<FabricEpoch> epoch() const;
  [[nodiscard]] Result<PolicyBinding> policy_binding() const;
  [[nodiscard]] Ticks current_tick() const;

 private:
  // --- durable plumbing ---------------------------------------------------
  [[nodiscard]] Status journal(JournalRecordType type, const std::string& payload);
  [[nodiscard]] Status recover();
  [[nodiscard]] Status apply_record(JournalRecordType type, std::string_view payload);
  [[nodiscard]] std::string encode_state_snapshot() const;
  [[nodiscard]] Status apply_state_snapshot(std::string_view payload);

  // --- state transitions (journal-free; replay reuses them) ---------------
  Status apply_admit(const FlowDescriptor& descriptor);
  Status apply_update(const FlowDescriptor& descriptor);
  void apply_resource(const ResourceDescriptor& descriptor);
  void apply_reservation(const ReservationDescriptor& descriptor);
  void apply_retire_reservation(ReservationId reservation, Generation generation);
  void apply_priority_class(const PriorityClassDescriptor& descriptor);
  void apply_qos_class(const QoSClassDescriptor& descriptor);
  void apply_policy(const SchedulingPolicy& policy);
  void apply_epoch_advance(FabricEpoch previous, FabricEpoch next, EpochReason reason, Ticks now);
  void apply_schedule(const Schedule& schedule);
  void apply_release_schedule(ScheduleId schedule, Ticks now);
  void normalize_transient_states(Ticks now);
  void apply_attempt_opened(const DispatchTicket& ticket, std::uint64_t nominal_work, Ticks now);
  void apply_attempt_started(DispatchAttemptId attempt, Ticks now);
  void apply_attempt_closed(DispatchAttemptId attempt, AttemptState state,
                            std::string_view detail, Ticks now);
  void apply_completion(const CompletionEvidence& evidence, std::uint64_t credited,
                        FlowLifecycle lifecycle_after, Ticks now);
  void apply_readiness(FlowId flow, Generation generation, ReadinessSignal signal, Ticks now);
  void apply_cancel(FlowId flow, Generation generation, Ticks now);
  void apply_retire_flow(FlowId flow, Generation generation);
  void apply_ambiguity_resolved(FlowId flow, Generation generation,
                                AmbiguityResolution resolution, Ticks now);

  // --- arbitration helpers -------------------------------------------------
  [[nodiscard]] bool dependencies_satisfied(const FlowRuntime& runtime) const;
  [[nodiscard]] DecisionReason derived_readiness(const FlowRuntime& runtime, Ticks now,
                                                 bool& ready) const;
  [[nodiscard]] bool reservation_usable(const FlowRuntime& runtime, Ticks now,
                                        DecisionReason& reason) const;
  [[nodiscard]] std::uint32_t tier_of(const FlowRuntime& runtime, Ticks now) const;
  [[nodiscard]] OrderKey compute_key(const FlowRuntime& runtime, Ticks now) const;
  [[nodiscard]] std::uint64_t capacity_remaining(const ResourceRuntime& resource, Ticks now) const;
  void roll_interval(ResourceRuntime& resource, Ticks now);
  [[nodiscard]] std::uint64_t reservation_remaining(const FlowRuntime& runtime, Ticks now) const;
  [[nodiscard]] bool preemption_allowed(const OrderKey& candidate_key,
                                        const FlowRuntime& incumbent, Ticks now,
                                        DecisionReason& reason) const;
  [[nodiscard]] DispatchAttemptId worst_incumbent(ResourceId resource, Ticks now,
                                                  bool& found) const;
  void record_step(FlowRuntime& runtime, Ticks now, FlowLifecycle lifecycle,
                   DecisionReason reason, ScheduleId schedule, DispatchAttemptId attempt,
                   std::string_view detail);
  void set_lifecycle(FlowRuntime& runtime, FlowLifecycle next, Ticks now);
  void maybe_retire_schedules();
  void abandon_open_attempts(Ticks now, const char* detail);
  void refresh_lifecycles(Ticks now, ArbitrationOutcome& outcome);
  void reclaim_expired_deliveries(Ticks now, ArbitrationOutcome& outcome);
  void reindex_all();
  void update_index(FlowId flow, FlowLifecycle from, FlowLifecycle to);
  void defer(ArbitrationOutcome& outcome, const FlowRuntime& flow, DecisionReason reason,
             std::string_view detail) const;
  void handle_deadline_miss(FlowRuntime& flow, Ticks now, ArbitrationOutcome& outcome);
  [[nodiscard]] Status close_attempt(DispatchAttemptId attempt, AttemptState state,
                                     std::string_view detail, Ticks now);
  [[nodiscard]] Status validate_ticket_fields(FlowId flow, Generation flow_generation,
                                              ResourceId resource,
                                              Generation resource_generation,
                                              PathId path, Generation path_generation,
                                              ReservationId reservation,
                                              Generation reservation_generation,
                                              QoSClassId qos, Generation qos_generation,
                                              PriorityClassId priority,
                                              Generation priority_generation,
                                              PolicyId policy, Generation policy_generation,
                                              FabricEpoch epoch, Ticks deadline,
                                              Ticks reservation_end, Ticks now) const;
  void refresh_min_service_window(FlowRuntime& runtime, Ticks now);
  void release_run_entry(ScheduleRuntime& runtime, std::size_t index, Ticks now);
  [[nodiscard]] ::flow_scheduler::ScheduleRuntime* find_schedule(ScheduleId id);
  [[nodiscard]] const ::flow_scheduler::ScheduleRuntime* find_schedule(ScheduleId id) const;
  [[nodiscard]] Ticks reservation_window_end(ReservationId reservation) const;

  struct AccountSnapshot {
    std::uint64_t attempts_total{0};
    std::uint64_t attempts_open{0};
    std::uint64_t attempts_started{0};
    std::uint64_t attempts_committed{0};
    std::uint64_t attempts_preempted{0};
    std::uint64_t attempts_cancelled{0};
    std::uint64_t attempts_abandoned{0};
    std::uint64_t attempts_rejected{0};
    std::uint64_t outstanding_units{0};
  };

  [[nodiscard]] AccountSnapshot account_unlocked() const;

  SchedulerOptions options_{};
  SchedulingPolicy policy_{};
  PolicyBinding policy_binding_{};
  FabricEpoch epoch_{};
  mutable std::mutex mutex_{};

  std::map<FlowId, FlowRuntime> flows_{};
  std::map<ResourceId, ResourceRuntime> resources_{};
  std::map<ReservationId, ReservationRuntime> reservations_{};
  std::map<PriorityClassId, PriorityClassDescriptor> priority_classes_{};
  std::map<QoSClassId, QoSClassDescriptor> qos_classes_{};
  std::map<FairnessGroupId, std::uint64_t> virtual_time_{};
  /// Indexes over the flow table. Arbitration touches only flows that can
  /// actually change state this round instead of scanning the whole table.
  std::set<FlowId> waiting_index_{};
  std::set<FlowId> ready_index_{};
  std::set<FlowId> inflight_index_{};

  std::map<DispatchAttemptId, AttemptRuntime> attempts_{};
  std::map<ScheduleId, ScheduleRuntime> schedules_{};
  std::map<ScheduleId, std::vector<DispatchAttemptId>> schedule_attempts_{};
  std::deque<ScheduleId> schedule_order_{};

  DispatchAttemptId next_attempt_{DispatchAttemptId(1)};
  ScheduleId next_schedule_{ScheduleId(1)};
  Generation next_schedule_generation_{Generation(1)};

  Ticks last_tick_{0};
  std::uint64_t provenance_sequence_{0};
  std::uint64_t arbitration_rounds_{0};
  std::uint64_t schedules_issued_{0};
  std::uint64_t schedule_entries_issued_{0};
  std::uint64_t completion_reports_{0};
  std::uint64_t completions_applied_{0};
  std::uint64_t completions_duplicate_{0};
  std::uint64_t completions_rejected_{0};
  std::uint64_t deadline_misses_{0};
  std::uint64_t preemptions_issued_{0};
  std::uint64_t preemption_window_index_{0};
  std::uint32_t preemptions_in_window_{0};
  std::uint64_t reservations_retired_{0};

  std::unique_ptr<DurableStore> store_{};
  RecoveryReport recovery_{};
};

// ---------------------------------------------------------------------------
// Lifecycle plumbing
// ---------------------------------------------------------------------------

namespace {

void write_policy(RecordWriter& writer, const SchedulingPolicy& policy) {
  writer.u64(policy.policy.value());
  writer.u64(policy.generation.value());
  writer.u8(static_cast<std::uint8_t>(policy.preemption));
  writer.boolean(policy.deadline_ordering);
  writer.boolean(policy.weighted_fairness);
  writer.boolean(policy.min_service_guarantee);
  writer.boolean(policy.strict_deadlines);
  writer.u64(policy.starvation_bound_ticks);
  writer.u64(policy.deadline_urgency_ticks);
  writer.u64(policy.min_preempt_service);
  writer.u32(policy.max_preemptions_per_interval);
  writer.u64(policy.preemption_interval_ticks);
  writer.u32(policy.max_flows);
  writer.u32(policy.max_resources);
  writer.u32(policy.max_reservations);
  writer.u32(policy.max_entries_per_schedule);
  writer.u64(policy.max_quantum);
  writer.u64(policy.default_max_overlap);
  writer.u32(policy.retained_schedule_history);
  writer.u64(policy.dispatch_delivery_horizon);
  writer.u8(static_cast<std::uint8_t>(policy.provenance.origin));
  writer.u64(policy.provenance.sequence);
  writer.u64(policy.provenance.digest);
}

Result<SchedulingPolicy> read_policy(RecordReader& reader) {
  SchedulingPolicy policy;
  std::uint64_t value = 0;
  FS_TRY_ASSIGN(value, reader.u64()); policy.policy = PolicyId(value);
  FS_TRY_ASSIGN(value, reader.u64()); policy.generation = Generation(value);
  std::uint8_t mode = 0;
  FS_TRY_ASSIGN(mode, reader.u8());
  if (mode > 3u) {
    return Error(ErrorCode::CorruptData, "durable policy preemption mode is out of range");
  }
  policy.preemption = static_cast<PreemptionMode>(mode);
  FS_TRY_ASSIGN(policy.deadline_ordering, reader.boolean());
  FS_TRY_ASSIGN(policy.weighted_fairness, reader.boolean());
  FS_TRY_ASSIGN(policy.min_service_guarantee, reader.boolean());
  FS_TRY_ASSIGN(policy.strict_deadlines, reader.boolean());
  FS_TRY_ASSIGN(value, reader.u64()); policy.starvation_bound_ticks = value;
  FS_TRY_ASSIGN(value, reader.u64()); policy.deadline_urgency_ticks = value;
  FS_TRY_ASSIGN(value, reader.u64()); policy.min_preempt_service = value;
  std::uint32_t throttle = 0;
  FS_TRY_ASSIGN(throttle, reader.u32()); policy.max_preemptions_per_interval = throttle;
  FS_TRY_ASSIGN(value, reader.u64()); policy.preemption_interval_ticks = value;
  std::uint32_t bound = 0;
  FS_TRY_ASSIGN(bound, reader.u32()); policy.max_flows = bound;
  FS_TRY_ASSIGN(bound, reader.u32()); policy.max_resources = bound;
  FS_TRY_ASSIGN(bound, reader.u32()); policy.max_reservations = bound;
  FS_TRY_ASSIGN(bound, reader.u32()); policy.max_entries_per_schedule = bound;
  FS_TRY_ASSIGN(value, reader.u64()); policy.max_quantum = value;
  FS_TRY_ASSIGN(value, reader.u64()); policy.default_max_overlap = value;
  FS_TRY_ASSIGN(bound, reader.u32()); policy.retained_schedule_history = bound;
  FS_TRY_ASSIGN(value, reader.u64()); policy.dispatch_delivery_horizon = value;
  std::uint8_t origin = 0;
  FS_TRY_ASSIGN(origin, reader.u8());
  if (origin > 4u) {
    return Error(ErrorCode::CorruptData, "durable policy provenance origin is out of range");
  }
  policy.provenance.origin = static_cast<ProvenanceOrigin>(origin);
  FS_TRY_ASSIGN(value, reader.u64()); policy.provenance.sequence = value;
  FS_TRY_ASSIGN(value, reader.u64()); policy.provenance.digest = value;
  return policy;
}

void write_resource(RecordWriter& writer, const ResourceDescriptor& descriptor) {
  writer.u64(descriptor.resource.value());
  writer.u64(descriptor.generation.value());
  writer.u64(descriptor.capacity);
  writer.u64(descriptor.interval);
  writer.u64(descriptor.max_overlap);
  writer.u8(static_cast<std::uint8_t>(descriptor.provenance.origin));
  writer.u64(descriptor.provenance.sequence);
  writer.u64(descriptor.provenance.digest);
}

Result<ResourceDescriptor> read_resource(RecordReader& reader) {
  ResourceDescriptor descriptor;
  std::uint64_t value = 0;
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.resource = ResourceId(value);
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.generation = Generation(value);
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.capacity = value;
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.interval = value;
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.max_overlap = value;
  std::uint8_t origin = 0;
  FS_TRY_ASSIGN(origin, reader.u8());
  if (origin > 4u) {
    return Error(ErrorCode::CorruptData, "durable resource provenance origin is out of range");
  }
  descriptor.provenance.origin = static_cast<ProvenanceOrigin>(origin);
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.provenance.sequence = value;
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.provenance.digest = value;
  return descriptor;
}

void write_reservation(RecordWriter& writer, const ReservationDescriptor& descriptor) {
  writer.u64(descriptor.reservation.value());
  writer.u64(descriptor.generation.value());
  writer.u64(descriptor.resource.value());
  writer.u64(descriptor.resource_generation.value());
  writer.u64(descriptor.window_begin);
  writer.u64(descriptor.window_end);
  writer.u64(descriptor.capacity);
  writer.boolean(descriptor.exclusive);
  writer.u8(static_cast<std::uint8_t>(descriptor.provenance.origin));
  writer.u64(descriptor.provenance.sequence);
  writer.u64(descriptor.provenance.digest);
}

Result<ReservationDescriptor> read_reservation(RecordReader& reader) {
  ReservationDescriptor descriptor;
  std::uint64_t value = 0;
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.reservation = ReservationId(value);
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.generation = Generation(value);
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.resource = ResourceId(value);
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.resource_generation = Generation(value);
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.window_begin = value;
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.window_end = value;
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.capacity = value;
  FS_TRY_ASSIGN(descriptor.exclusive, reader.boolean());
  std::uint8_t origin = 0;
  FS_TRY_ASSIGN(origin, reader.u8());
  if (origin > 4u) {
    return Error(ErrorCode::CorruptData, "durable reservation provenance origin is out of range");
  }
  descriptor.provenance.origin = static_cast<ProvenanceOrigin>(origin);
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.provenance.sequence = value;
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.provenance.digest = value;
  return descriptor;
}

void write_priority_class(RecordWriter& writer, const PriorityClassDescriptor& descriptor) {
  writer.u64(descriptor.priority.value());
  writer.u64(descriptor.generation.value());
  writer.u32(descriptor.rank);
  writer.u64(descriptor.weight);
  writer.u8(static_cast<std::uint8_t>(descriptor.provenance.origin));
  writer.u64(descriptor.provenance.sequence);
  writer.u64(descriptor.provenance.digest);
}

Result<PriorityClassDescriptor> read_priority_class(RecordReader& reader) {
  PriorityClassDescriptor descriptor;
  std::uint64_t value = 0;
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.priority = PriorityClassId(value);
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.generation = Generation(value);
  std::uint32_t rank = 0;
  FS_TRY_ASSIGN(rank, reader.u32()); descriptor.rank = rank;
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.weight = value;
  std::uint8_t origin = 0;
  FS_TRY_ASSIGN(origin, reader.u8());
  if (origin > 4u) {
    return Error(ErrorCode::CorruptData, "durable priority provenance origin is out of range");
  }
  descriptor.provenance.origin = static_cast<ProvenanceOrigin>(origin);
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.provenance.sequence = value;
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.provenance.digest = value;
  return descriptor;
}

void write_qos_class(RecordWriter& writer, const QoSClassDescriptor& descriptor) {
  writer.u64(descriptor.qos.value());
  writer.u64(descriptor.generation.value());
  writer.boolean(descriptor.preemption_protected);
  writer.u64(descriptor.max_service_per_dispatch);
  writer.u8(static_cast<std::uint8_t>(descriptor.provenance.origin));
  writer.u64(descriptor.provenance.sequence);
  writer.u64(descriptor.provenance.digest);
}

Result<QoSClassDescriptor> read_qos_class(RecordReader& reader) {
  QoSClassDescriptor descriptor;
  std::uint64_t value = 0;
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.qos = QoSClassId(value);
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.generation = Generation(value);
  FS_TRY_ASSIGN(descriptor.preemption_protected, reader.boolean());
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.max_service_per_dispatch = value;
  std::uint8_t origin = 0;
  FS_TRY_ASSIGN(origin, reader.u8());
  if (origin > 4u) {
    return Error(ErrorCode::CorruptData, "durable qos provenance origin is out of range");
  }
  descriptor.provenance.origin = static_cast<ProvenanceOrigin>(origin);
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.provenance.sequence = value;
  FS_TRY_ASSIGN(value, reader.u64()); descriptor.provenance.digest = value;
  return descriptor;
}

Status require_exhausted(const RecordReader& reader) {
  if (!reader.exhausted()) {
    return Status::failure(ErrorCode::CorruptData,
                           "durable record carries trailing bytes after its declared fields");
  }
  return Status::success();
}

}  // namespace

Scheduler::Impl::Impl(const SchedulerOptions& options) : options_(options), policy_(options.policy) {
  policy_binding_.policy = policy_.policy;
  policy_binding_.generation = policy_.generation;
  policy_binding_.digest = digest_of(policy_);
}

Status Scheduler::Impl::initialize() {
  if (options_.state_directory.empty()) {
    // Volatile scheduler: still starts at a valid epoch so that a ticket's
    // epoch binding is always meaningful.
    epoch_ = FabricEpoch(1);
    return Status::success();
  }
  FS_TRY_ASSIGN(store_, DurableStore::open(options_.state_directory, options_.flush_to_storage));
  return recover();
}

// ---------------------------------------------------------------------------
// Durable plumbing
// ---------------------------------------------------------------------------

Status Scheduler::Impl::journal(JournalRecordType type, const std::string& payload) {
  if (!store_) {
    return Status::success();
  }
  return store_->append(type, payload);
}

Scheduler::Impl::AccountSnapshot Scheduler::Impl::account_unlocked() const {
  AccountSnapshot snapshot;
  for (const auto& entry : attempts_) {
    ++snapshot.attempts_total;
    switch (entry.second.state) {
      case AttemptState::Open: ++snapshot.attempts_open; break;
      case AttemptState::Started: ++snapshot.attempts_started; break;
      case AttemptState::Committed: ++snapshot.attempts_committed; break;
      case AttemptState::Preempted: ++snapshot.attempts_preempted; break;
      case AttemptState::Cancelled: ++snapshot.attempts_cancelled; break;
      case AttemptState::Abandoned: ++snapshot.attempts_abandoned; break;
      case AttemptState::Rejected: ++snapshot.attempts_rejected; break;
    }
    if (!is_attempt_terminal(entry.second.state)) {
      snapshot.outstanding_units =
          saturating_add_u64(snapshot.outstanding_units, entry.second.ticket.quantum);
    }
  }
  return snapshot;
}

// ---------------------------------------------------------------------------
// State transitions. These are the only writers of authoritative state and are
// shared by live operation and by replay, so recovery cannot diverge from the
// live path.
// ---------------------------------------------------------------------------

void Scheduler::Impl::set_lifecycle(FlowRuntime& runtime, FlowLifecycle next, Ticks now) {
  if (runtime.lifecycle == next) {
    return;
  }
  const FlowLifecycle previous = runtime.lifecycle;
  if (previous == FlowLifecycle::Ready && runtime.ever_ready && now >= runtime.eligible_since_tick) {
    runtime.accounting.wait_ticks_total =
        saturating_add_u64(runtime.accounting.wait_ticks_total, now - runtime.eligible_since_tick);
  }
  runtime.lifecycle = next;
  runtime.last_change_tick = now;
  update_index(runtime.descriptor.flow, previous, next);
  if (next == FlowLifecycle::Ready) {
    // Entering the ready set is the start of a new continuous eligibility
    // interval; leaving it accumulates the wait. A flow that is already Ready
    // never reaches this point because set_lifecycle returns early.
    runtime.eligible_since_tick = now;
    runtime.ever_ready = true;
    runtime.starvation_counted = false;
    if (policy_.weighted_fairness) {
      // Self-clocked fair queueing: every arrival (a flow becoming
      // backlogged) stamps the flow with
      //     F_i = max(F_i, V) + quantum / weight
      // where V is the monotone virtual clock of the fairness group. Ordering
      // by F_i then shares service in proportion to weight, and a flow that
      // was idle cannot burst ahead because its stamp is lifted to V first.
      std::uint64_t quantum = runtime.descriptor.service_quantum;
      if (quantum > policy_.max_quantum) {
        quantum = policy_.max_quantum;
      }
      const std::uint64_t weight = runtime.descriptor.weight == 0 ? 1u : runtime.descriptor.weight;
      const std::uint64_t cost = (quantum * kVirtualTimeScale) / weight;
      const auto clock = virtual_time_.find(runtime.descriptor.fairness_group);
      const std::uint64_t base = clock == virtual_time_.end() ? 0u : clock->second;
      runtime.virtual_finish =
          saturating_add_u64(runtime.virtual_finish < base ? base : runtime.virtual_finish, cost);
    }
    refresh_min_service_window(runtime, now);
  }
}

void Scheduler::Impl::update_index(FlowId flow, FlowLifecycle from, FlowLifecycle to) {
  const auto slot = [](FlowLifecycle lifecycle) -> int {
    switch (lifecycle) {
      case FlowLifecycle::Waiting:
      // An ambiguous flow is reported through the waiting set so that it is
      // re-evaluated every round and surfaces the resolution it needs, but it
      // is never eligible: derived_readiness() refuses it explicitly.
      case FlowLifecycle::Ambiguous: return 1;
      case FlowLifecycle::Ready: return 2;
      case FlowLifecycle::Scheduled: return 0;
      case FlowLifecycle::Dispatched:
      case FlowLifecycle::Running:
      case FlowLifecycle::CompletionReported:
      case FlowLifecycle::Cancelling:
      case FlowLifecycle::Preempting:
        return 3;
      default: return 0;
    }
  };
  switch (slot(from)) {
    case 1: waiting_index_.erase(flow); break;
    case 2: ready_index_.erase(flow); break;
    case 3: inflight_index_.erase(flow); break;
    default: break;
  }
  switch (slot(to)) {
    case 1: waiting_index_.insert(flow); break;
    case 2: ready_index_.insert(flow); break;
    case 3: inflight_index_.insert(flow); break;
    default: break;
  }
}

void Scheduler::Impl::reindex_all() {
  waiting_index_.clear();
  ready_index_.clear();
  inflight_index_.clear();
  for (const auto& entry : flows_) {
    switch (entry.second.lifecycle) {
      case FlowLifecycle::Waiting:
      case FlowLifecycle::Ambiguous: waiting_index_.insert(entry.first); break;
      case FlowLifecycle::Ready: ready_index_.insert(entry.first); break;
      case FlowLifecycle::Scheduled: break;
      case FlowLifecycle::Dispatched:
      case FlowLifecycle::Running:
      case FlowLifecycle::CompletionReported:
      case FlowLifecycle::Cancelling:
      case FlowLifecycle::Preempting:
        inflight_index_.insert(entry.first);
        break;
      default: break;
    }
  }
}

void Scheduler::Impl::defer(ArbitrationOutcome& outcome, const FlowRuntime& flow,
                            DecisionReason reason, std::string_view detail) const {
  if (outcome.deferred.size() >= kMaxDeferredPerRound) {
    outcome.deferred_truncated = true;
    return;
  }
  DeferredFlow deferred;
  deferred.flow = flow.descriptor.flow;
  deferred.generation = flow.descriptor.generation;
  deferred.reason = reason;
  deferred.detail = bounded_detail(detail);
  outcome.deferred.push_back(std::move(deferred));
}

void Scheduler::Impl::handle_deadline_miss(FlowRuntime& flow, Ticks now,
                                           ArbitrationOutcome& outcome) {
  if (flow.deadline_missed_counted) {
    return;
  }
  flow.deadline_missed_counted = true;
  flow.accounting.deadline_misses += 1;
  ++deadline_misses_;
  if (outcome.deadline_misses.size() < kMaxDeferredPerRound) {
    outcome.deadline_misses.push_back(flow.descriptor.flow);
  }
  if (policy_.strict_deadlines && !is_terminal(flow.lifecycle)) {
    set_lifecycle(flow, FlowLifecycle::Failed, now);
    record_step(flow, now, FlowLifecycle::Failed, DecisionReason::DeadlineMissed, ScheduleId{},
                DispatchAttemptId{}, "deadline crossed before the flow could be dispatched");
  }
}

Status Scheduler::Impl::close_attempt(DispatchAttemptId attempt, AttemptState state,
                                      std::string_view detail, Ticks now) {
  RecordWriter writer;
  writer.u64(attempt.value());
  writer.u8(static_cast<std::uint8_t>(state));
  writer.u64(now);
  writer.text(bounded_detail(detail));
  FS_RETURN_IF_ERROR(journal(JournalRecordType::AttemptClosed, writer.take()));
  apply_attempt_closed(attempt, state, detail, now);
  return Status::success();
}

void Scheduler::Impl::record_step(FlowRuntime& runtime, Ticks now, FlowLifecycle lifecycle,
                                  DecisionReason reason, ScheduleId schedule,
                                  DispatchAttemptId attempt, std::string_view detail) {
  ExplanationStep step;
  step.at_tick = now;
  step.lifecycle = lifecycle;
  step.reason = reason;
  step.schedule = schedule;
  step.attempt = attempt;
  step.detail = bounded_detail(detail);
  runtime.history.push_back(std::move(step));
  if (runtime.history.size() > kMaxExplanationSteps) {
    runtime.history.erase(runtime.history.begin());
    runtime.history_truncated = true;
  }
}

void Scheduler::Impl::refresh_min_service_window(FlowRuntime& runtime, Ticks now) {
  if (runtime.descriptor.min_service == 0) {
    return;
  }
  if (!runtime.ever_ready) {
    runtime.min_service_window_start = now;
    runtime.min_service_delivered = 0;
    return;
  }
  if (now < runtime.min_service_window_start) {
    runtime.min_service_window_start = now;
    runtime.min_service_delivered = 0;
    return;
  }
  if (now - runtime.min_service_window_start >= runtime.descriptor.min_service_window) {
    runtime.min_service_window_start = now;
    runtime.min_service_delivered = 0;
  }
}

bool Scheduler::Impl::dependencies_satisfied(const FlowRuntime& runtime) const {
  for (const FlowId dependency : runtime.descriptor.dependencies) {
    const auto found = flows_.find(dependency);
    if (found == flows_.end()) {
      return false;
    }
    // Only authoritative completion of the current generation satisfies a
    // dependency. A recovered Ambiguous dependency never counts.
    if (found->second.lifecycle != FlowLifecycle::Completed) {
      return false;
    }
  }
  return true;
}

DecisionReason Scheduler::Impl::derived_readiness(const FlowRuntime& runtime, Ticks now,
                                                  bool& ready) const {
  ready = false;
  if (is_terminal(runtime.lifecycle)) {
    return DecisionReason::Terminal;
  }
  if (runtime.lifecycle == FlowLifecycle::Ambiguous) {
    // Recovery could not establish whether the last attempt took effect.
    // Nothing is dispatched from an unresolved state.
    return DecisionReason::AmbiguousRequiresResolution;
  }
  if (runtime.descriptor.readiness_signal == 2u) {
    return DecisionReason::AwaitingReadinessSignal;
  }
  if (!runtime.descriptor.auto_ready && runtime.descriptor.readiness_signal != 1u) {
    return DecisionReason::AwaitingReadinessSignal;
  }
  if (now < runtime.descriptor.release_tick) {
    return DecisionReason::AwaitingRelease;
  }
  if (!dependencies_satisfied(runtime)) {
    // A dependency that reached a terminal state other than authoritative
    // completion can never be satisfied; the dependent is failed rather than
    // left waiting forever, so the books still close.
    for (const FlowId dependency : runtime.descriptor.dependencies) {
      const auto found = flows_.find(dependency);
      if (found == flows_.end()) {
        continue;
      }
      const FlowLifecycle state = found->second.lifecycle;
      if (is_terminal(state) && state != FlowLifecycle::Completed) {
        return DecisionReason::DependencyFailed;
      }
    }
    return DecisionReason::AwaitingDependency;
  }
  DecisionReason reservation_reason = DecisionReason::Selected;
  if (!reservation_usable(runtime, now, reservation_reason)) {
    return reservation_reason;
  }
  if (has_deadline(runtime.descriptor.deadline_tick) && now >= runtime.descriptor.deadline_tick) {
    return DecisionReason::DeadlineMissed;
  }
  ready = true;
  return DecisionReason::Selected;
}

bool Scheduler::Impl::reservation_usable(const FlowRuntime& runtime, Ticks now,
                                         DecisionReason& reason) const {
  reason = DecisionReason::Selected;
  const ReservationBinding& binding = runtime.descriptor.reservation;
  if (!binding.bound()) {
    // A reserved resource restricts its in-window service to bound flows.
    for (const auto& entry : reservations_) {
      const ReservationRuntime& candidate = entry.second;
      if (candidate.retired || candidate.descriptor.resource != runtime.descriptor.resource.resource) {
        continue;
      }
      if (candidate.descriptor.exclusive && window_contains(candidate.descriptor, now)) {
        reason = DecisionReason::AwaitingReservationWindow;
        return false;
      }
    }
    return true;
  }
  const auto found = reservations_.find(binding.reservation);
  if (found == reservations_.end() || found->second.retired) {
    reason = DecisionReason::ReservationWindowClosed;
    return false;
  }
  const ReservationRuntime& reservation = found->second;
  if (reservation.descriptor.generation != binding.generation) {
    reason = DecisionReason::ReservationWindowClosed;
    return false;
  }
  if (reservation.descriptor.resource != runtime.descriptor.resource.resource) {
    reason = DecisionReason::ReservationWindowClosed;
    return false;
  }
  if (now < reservation.descriptor.window_begin) {
    reason = DecisionReason::AwaitingReservationWindow;
    return false;
  }
  if (now >= reservation.descriptor.window_end) {
    reason = DecisionReason::ReservationWindowClosed;
    return false;
  }
  if (reservation.reserved_units >= reservation.descriptor.capacity) {
    reason = DecisionReason::ReservationExhausted;
    return false;
  }
  return true;
}

std::uint32_t Scheduler::Impl::tier_of(const FlowRuntime& runtime, Ticks now) const {
  const Ticks deadline = runtime.descriptor.deadline_tick;
  if (has_deadline(deadline) && policy_.deadline_ordering) {
    const Ticks slack = deadline > now ? deadline - now : 0;
    if (slack <= policy_.deadline_urgency_ticks) {
      return kTierDeadlineUrgent;
    }
  }
  if (runtime.ever_ready) {
    const Ticks waiting = now >= runtime.eligible_since_tick ? now - runtime.eligible_since_tick : 0;
    if (waiting >= policy_.starvation_bound_ticks) {
      return kTierStarvation;
    }
  }
  if (policy_.min_service_guarantee && runtime.descriptor.min_service > 0 &&
      runtime.min_service_delivered < runtime.descriptor.min_service) {
    return kTierMinServiceDeficient;
  }
  return kTierNormal;
}

OrderKey Scheduler::Impl::compute_key(const FlowRuntime& runtime, Ticks now) const {
  OrderKey key;
  key.flow = runtime.descriptor.flow;
  key.generation = runtime.descriptor.generation;
  key.tier = tier_of(runtime, now);

  const auto priority = priority_classes_.find(runtime.descriptor.priority.priority);
  key.priority_rank = priority == priority_classes_.end() ? 0u : priority->second.rank;

  key.deadline = policy_.deadline_ordering ? runtime.descriptor.deadline_tick : kNoDeadline;

  if (policy_.weighted_fairness) {
    // Standard weighted-fair-queueing ordering by the flow's own virtual
    // finish number. Every quantity is an integer, so the order is exact and
    // never depends on floating point rounding.
    key.virtual_finish = runtime.virtual_finish;
  }

  // Larger weight must sort earlier, so compare the inverted weight.
  const std::uint64_t weight = runtime.descriptor.weight == 0 ? 1u : runtime.descriptor.weight;
  key.inverted_weight = std::numeric_limits<std::uint64_t>::max() - weight;
  return key;
}

std::uint64_t Scheduler::Impl::capacity_remaining(const ResourceRuntime& resource,
                                                  Ticks now) const {
  const std::uint64_t interval = resource.descriptor.interval;
  const std::uint64_t index = now / interval;
  if (!resource.interval_initialized || index != resource.interval_index) {
    return resource.descriptor.capacity;
  }
  return resource.reserved_this_interval >= resource.descriptor.capacity
             ? 0u
             : resource.descriptor.capacity - resource.reserved_this_interval;
}

void Scheduler::Impl::roll_interval(ResourceRuntime& resource, Ticks now) {
  const std::uint64_t index = now / resource.descriptor.interval;
  if (!resource.interval_initialized || index != resource.interval_index) {
    resource.interval_index = index;
    resource.interval_initialized = true;
    resource.reserved_this_interval = 0;
  }
}

std::uint64_t Scheduler::Impl::reservation_remaining(const FlowRuntime& runtime, Ticks now) const {
  const ReservationBinding& binding = runtime.descriptor.reservation;
  if (!binding.bound()) {
    return std::numeric_limits<std::uint64_t>::max();
  }
  const auto found = reservations_.find(binding.reservation);
  if (found == reservations_.end() || found->second.retired ||
      !window_contains(found->second.descriptor, now)) {
    return 0;
  }
  const ReservationRuntime& reservation = found->second;
  return reservation.reserved_units >= reservation.descriptor.capacity
             ? 0u
             : reservation.descriptor.capacity - reservation.reserved_units;
}

::flow_scheduler::ScheduleRuntime* Scheduler::Impl::find_schedule(ScheduleId id) {
  const auto found = schedules_.find(id);
  return found == schedules_.end() ? nullptr : &found->second;
}

const ::flow_scheduler::ScheduleRuntime* Scheduler::Impl::find_schedule(ScheduleId id) const {
  const auto found = schedules_.find(id);
  return found == schedules_.end() ? nullptr : &found->second;
}

void Scheduler::Impl::apply_policy(const SchedulingPolicy& policy) {
  policy_ = policy;
  policy_binding_.policy = policy.policy;
  policy_binding_.generation = policy.generation;
  policy_binding_.digest = digest_of(policy);
}

void Scheduler::Impl::apply_resource(const ResourceDescriptor& descriptor) {
  ResourceRuntime& runtime = resources_[descriptor.resource];
  const bool known = runtime.descriptor.resource.valid();
  if (known && runtime.descriptor.generation != descriptor.generation) {
    // A generation change invalidates the interval phase of the previous
    // revision: capacity accounting restarts with the new descriptor.
    runtime.interval_initialized = false;
    runtime.reserved_this_interval = 0;
  }
  runtime.descriptor = descriptor;
}

void Scheduler::Impl::apply_reservation(const ReservationDescriptor& descriptor) {
  ReservationRuntime& runtime = reservations_[descriptor.reservation];
  runtime.descriptor = descriptor;
  runtime.retired = false;
}

void Scheduler::Impl::apply_retire_reservation(ReservationId reservation, Generation generation) {
  const auto found = reservations_.find(reservation);
  if (found == reservations_.end()) {
    return;
  }
  if (found->second.descriptor.generation != generation) {
    return;
  }
  found->second.retired = true;
  ++reservations_retired_;
}

void Scheduler::Impl::apply_priority_class(const PriorityClassDescriptor& descriptor) {
  priority_classes_[descriptor.priority] = descriptor;
}

void Scheduler::Impl::apply_qos_class(const QoSClassDescriptor& descriptor) {
  qos_classes_[descriptor.qos] = descriptor;
}

Status Scheduler::Impl::apply_admit(const FlowDescriptor& descriptor) {
  if (flows_.size() >= policy_.max_flows && flows_.find(descriptor.flow) == flows_.end()) {
    return Status::failure(ErrorCode::Bounded, "flow table bound reached");
  }
  FlowRuntime& runtime = flows_[descriptor.flow];
  runtime.descriptor = descriptor;
  runtime.lifecycle = FlowLifecycle::Waiting;
  runtime.served_work = 0;
  runtime.outstanding_reserved = 0;
  runtime.open_attempt = DispatchAttemptId{};
  runtime.scheduled_schedule = ScheduleId{};
  runtime.scheduled_generation = Generation{};
  runtime.scheduled_ordinal = 0;
  runtime.scheduled_tick = 0;
  runtime.ever_ready = false;
  runtime.eligible_since_tick = 0;
  runtime.min_service_window_start = 0;
  runtime.min_service_delivered = 0;
  runtime.virtual_finish = 0;
  runtime.deadline_missed_counted = false;
  runtime.starvation_counted = false;
  runtime.accounting = FlowAccounting{};
  runtime.history.clear();
  runtime.history_truncated = false;
  ready_index_.erase(descriptor.flow);
  inflight_index_.erase(descriptor.flow);
  waiting_index_.insert(descriptor.flow);
  return Status::success();
}

Status Scheduler::Impl::apply_update(const FlowDescriptor& descriptor) {
  const auto found = flows_.find(descriptor.flow);
  if (found == flows_.end()) {
    return Status::failure(ErrorCode::UnknownFlow, "cannot update an unregistered flow");
  }
  FlowRuntime& runtime = found->second;
  if (holds_open_work(runtime.lifecycle)) {
    return Status::failure(ErrorCode::InvalidState,
                           "cannot update a flow that holds open dispatch authority");
  }
  runtime.descriptor = descriptor;
  return Status::success();
}

void Scheduler::Impl::apply_readiness(FlowId flow, Generation generation, ReadinessSignal signal,
                                      Ticks now) {
  const auto found = flows_.find(flow);
  if (found == flows_.end() || found->second.descriptor.generation != generation) {
    return;
  }
  found->second.descriptor.readiness_signal = static_cast<std::uint8_t>(signal);
  found->second.last_change_tick = now;
}

void Scheduler::Impl::apply_cancel(FlowId flow, Generation generation, Ticks now) {
  const auto found = flows_.find(flow);
  if (found == flows_.end() || found->second.descriptor.generation != generation) {
    return;
  }
  FlowRuntime& runtime = found->second;
  if (runtime.open_attempt.valid()) {
    apply_attempt_closed(runtime.open_attempt, AttemptState::Cancelled, "flow cancelled", now);
  }
  if (runtime.lifecycle == FlowLifecycle::Scheduled) {
    ScheduleRuntime* scheduled = find_schedule(runtime.scheduled_schedule);
    const auto index = static_cast<std::size_t>(runtime.scheduled_ordinal);
    if (scheduled != nullptr && index < scheduled->consumed.size()) {
      release_run_entry(*scheduled, index, now);
    }
  }
  set_lifecycle(runtime, FlowLifecycle::Cancelled, now);
  runtime.outstanding_reserved = 0;
  runtime.scheduled_schedule = ScheduleId{};
  runtime.scheduled_generation = Generation{};
  runtime.scheduled_ordinal = 0;
  record_step(runtime, now, FlowLifecycle::Cancelled, DecisionReason::Terminal, ScheduleId{},
              DispatchAttemptId{}, "cancelled by caller");
}

void Scheduler::Impl::apply_retire_flow(FlowId flow, Generation generation) {
  const auto found = flows_.find(flow);
  if (found == flows_.end() || found->second.descriptor.generation != generation) {
    return;
  }
  waiting_index_.erase(flow);
  ready_index_.erase(flow);
  inflight_index_.erase(flow);
  flows_.erase(found);
}

void Scheduler::Impl::apply_ambiguity_resolved(FlowId flow, Generation generation,
                                               AmbiguityResolution resolution, Ticks now) {
  const auto found = flows_.find(flow);
  if (found == flows_.end() || found->second.descriptor.generation != generation) {
    return;
  }
  FlowRuntime& runtime = found->second;
  if (runtime.lifecycle != FlowLifecycle::Ambiguous) {
    return;
  }
  if (resolution == AmbiguityResolution::Abandon) {
    set_lifecycle(runtime, FlowLifecycle::Failed, now);
    record_step(runtime, now, FlowLifecycle::Failed, DecisionReason::Terminal, ScheduleId{},
                DispatchAttemptId{}, "recovered outcome abandoned by operator");
  } else {
    set_lifecycle(runtime, FlowLifecycle::Waiting, now);
    runtime.ever_ready = false;
    record_step(runtime, now, FlowLifecycle::Waiting, DecisionReason::AwaitingRelease, ScheduleId{},
                DispatchAttemptId{}, "recovered outcome retried by operator");
  }
}

void Scheduler::Impl::release_run_entry(ScheduleRuntime& runtime, std::size_t index, Ticks now) {
  if (index >= runtime.schedule.entries.size() || runtime.consumed[index] != 0) {
    return;
  }
  runtime.consumed[index] = 2;
  const ScheduleEntry& entry = runtime.schedule.entries[index];
  if (entry.kind != DecisionKind::Run) {
    return;
  }
  if (runtime.pending_entries > 0) {
    --runtime.pending_entries;
  }
  const auto found = flows_.find(entry.flow);
  if (found == flows_.end()) {
    return;
  }
  FlowRuntime& flow = found->second;
  if (flow.lifecycle != FlowLifecycle::Scheduled ||
      flow.scheduled_schedule != runtime.schedule.schedule) {
    return;
  }
  flow.outstanding_reserved = flow.outstanding_reserved >= entry.quantum
                                  ? flow.outstanding_reserved - entry.quantum
                                  : 0;
  flow.scheduled_schedule = ScheduleId{};
  flow.scheduled_generation = Generation{};
  flow.scheduled_ordinal = 0;
  const auto resource = resources_.find(entry.resource);
  if (resource != resources_.end() && resource->second.in_flight > 0) {
    --resource->second.in_flight;
  }
  set_lifecycle(flow, FlowLifecycle::Ready, now);
  record_step(flow, now, FlowLifecycle::Ready, DecisionReason::DeliveryHorizonExpired, ScheduleId{},
              DispatchAttemptId{}, "undelivered schedule entry reclaimed");
}

void Scheduler::Impl::apply_schedule(const Schedule& schedule) {
  ScheduleRuntime runtime;
  runtime.schedule = schedule;
  runtime.consumed.assign(schedule.entries.size(), 0);
  ScheduleRuntime& stored = schedules_[schedule.schedule];
  stored = std::move(runtime);
  schedule_order_.push_back(schedule.schedule);
  ++schedules_issued_;
  schedule_entries_issued_ += schedule.entries.size();

  for (const ScheduleEntry& entry : schedule.entries) {
    if (entry.kind != DecisionKind::Run) {
      continue;
    }
    const auto found = flows_.find(entry.flow);
    if (found == flows_.end()) {
      continue;
    }
    FlowRuntime& flow = found->second;
    if (flow.descriptor.generation != entry.flow_generation) {
      continue;
    }
    if (is_terminal(flow.lifecycle) || holds_open_work(flow.lifecycle)) {
      continue;
    }
    flow.outstanding_reserved = saturating_add_u64(flow.outstanding_reserved, entry.quantum);
    flow.scheduled_schedule = schedule.schedule;
    flow.scheduled_generation = schedule.generation;
    flow.scheduled_ordinal = entry.ordinal;
    flow.scheduled_tick = entry.start_tick;
    set_lifecycle(flow, FlowLifecycle::Scheduled, entry.start_tick);
    record_step(flow, entry.start_tick, FlowLifecycle::Scheduled, DecisionReason::Selected,
                schedule.schedule, DispatchAttemptId{}, entry.detail);
    stored.pending_entries += 1;

    const auto resource = resources_.find(entry.resource);
    if (resource != resources_.end() &&
        resource->second.descriptor.generation == entry.resource_generation) {
      roll_interval(resource->second, entry.start_tick);
      resource->second.reserved_this_interval =
          saturating_add_u64(resource->second.reserved_this_interval, entry.quantum);
      resource->second.in_flight += 1;
    }
    if (entry.reservation.valid()) {
      const auto reservation = reservations_.find(entry.reservation);
      if (reservation != reservations_.end() &&
          reservation->second.descriptor.generation == entry.reservation_generation) {
        reservation->second.reserved_units =
            saturating_add_u64(reservation->second.reserved_units, entry.quantum);
      }
    }
    if (policy_.weighted_fairness) {
      // The virtual clock is the finish stamp of the service window currently
      // being granted, and is monotone so that a long-waiting flow can never
      // drag it backwards.
      std::uint64_t& clock = virtual_time_[flow.descriptor.fairness_group];
      if (clock < flow.virtual_finish) {
        clock = flow.virtual_finish;
      }
    }
  }
}

void Scheduler::Impl::apply_release_schedule(ScheduleId schedule, Ticks now) {
  ScheduleRuntime* runtime = find_schedule(schedule);
  if (runtime == nullptr) {
    return;
  }
  for (std::size_t index = 0; index < runtime->schedule.entries.size(); ++index) {
    release_run_entry(*runtime, index, now);
  }
}

void Scheduler::Impl::apply_attempt_opened(const DispatchTicket& ticket, std::uint64_t nominal_work,
                                           Ticks now) {
  AttemptRuntime runtime;
  runtime.ticket = ticket;
  runtime.state = AttemptState::Open;
  runtime.opened_tick = now;
  runtime.has_evidence = false;
  runtime.evidence_digest = 0;
  runtime.credited = 0;
  runtime.close_detail.clear();
  attempts_[ticket.attempt] = std::move(runtime);
  schedule_attempts_[ticket.schedule].push_back(ticket.attempt);
  ScheduleRuntime* schedule = find_schedule(ticket.schedule);
  if (schedule != nullptr) {
    schedule->open_attempts += 1;
  }
  const auto found = flows_.find(ticket.flow);
  if (found == flows_.end() || found->second.descriptor.generation != ticket.flow_generation) {
    return;
  }
  FlowRuntime& flow = found->second;
  flow.open_attempt = ticket.attempt;
  flow.accounting.dispatches_issued += 1;
  set_lifecycle(flow, FlowLifecycle::Dispatched, now);
  record_step(flow, now, FlowLifecycle::Dispatched, DecisionReason::Selected, ticket.schedule,
              ticket.attempt,
              "dispatch issued with " + std::to_string(ticket.quantum) + " units of " +
                  std::to_string(nominal_work) + " nominal work");
}

void Scheduler::Impl::apply_attempt_started(DispatchAttemptId attempt, Ticks now) {
  const auto found = attempts_.find(attempt);
  if (found == attempts_.end() || is_attempt_terminal(found->second.state) ||
      found->second.state == AttemptState::Started) {
    return;
  }
  found->second.state = AttemptState::Started;
  const auto flow = flows_.find(found->second.ticket.flow);
  if (flow == flows_.end() || flow->second.open_attempt != attempt) {
    return;
  }
  flow->second.accounting.dispatches_started += 1;
  set_lifecycle(flow->second, FlowLifecycle::Running, now);
}

void Scheduler::Impl::apply_attempt_closed(DispatchAttemptId attempt, AttemptState state,
                                           std::string_view detail, Ticks now) {
  const auto found = attempts_.find(attempt);
  if (found == attempts_.end() || is_attempt_terminal(found->second.state)) {
    return;
  }
  AttemptRuntime& record = found->second;
  record.state = state;
  record.closed_tick = now;
  record.close_detail = bounded_detail(detail);
  ScheduleRuntime* schedule = find_schedule(record.ticket.schedule);
  if (schedule != nullptr && schedule->open_attempts > 0) {
    schedule->open_attempts -= 1;
  }
  const auto flow_entry = flows_.find(record.ticket.flow);
  if (flow_entry == flows_.end() || flow_entry->second.open_attempt != attempt) {
    return;
  }
  FlowRuntime& flow = flow_entry->second;
  const std::uint64_t quantum = record.ticket.quantum;
  flow.outstanding_reserved =
      flow.outstanding_reserved >= quantum ? flow.outstanding_reserved - quantum : 0;
  flow.open_attempt = DispatchAttemptId{};
  const auto resource = resources_.find(record.ticket.resource);
  if (resource != resources_.end() && resource->second.in_flight > 0) {
    --resource->second.in_flight;
  }
  switch (state) {
    case AttemptState::Preempted:
      flow.accounting.preemptions += 1;
      set_lifecycle(flow, FlowLifecycle::Preempting, now);
      record_step(flow, now, FlowLifecycle::Preempting,
                  DecisionReason::DisplacedByHigherAuthority, record.ticket.schedule, attempt,
                  record.close_detail);
      break;
    case AttemptState::Cancelled:
      set_lifecycle(flow, FlowLifecycle::Cancelling, now);
      record_step(flow, now, FlowLifecycle::Cancelling, DecisionReason::Terminal,
                  record.ticket.schedule, attempt, record.close_detail);
      break;
    case AttemptState::Abandoned:
      set_lifecycle(flow, FlowLifecycle::Ambiguous, now);
      record_step(flow, now, FlowLifecycle::Ambiguous, DecisionReason::AmbiguousRequiresResolution,
                  record.ticket.schedule, attempt, record.close_detail);
      break;
    case AttemptState::Rejected:
      set_lifecycle(flow, FlowLifecycle::Ready, now);
      record_step(flow, now, FlowLifecycle::Ready, DecisionReason::Selected, record.ticket.schedule,
                  attempt, record.close_detail);
      break;
    case AttemptState::Open:
    case AttemptState::Started:
    case AttemptState::Committed:
      break;
  }
}

void Scheduler::Impl::apply_completion(const CompletionEvidence& evidence, std::uint64_t credited,
                                       FlowLifecycle lifecycle_after, Ticks now) {
  const auto found = attempts_.find(evidence.attempt);
  if (found == attempts_.end()) {
    return;
  }
  AttemptRuntime& record = found->second;
  record.state = AttemptState::Committed;
  record.closed_tick = now;
  record.has_evidence = true;
  record.evidence_digest = evidence.digest();
  record.credited = credited;
  ScheduleRuntime* schedule = find_schedule(record.ticket.schedule);
  if (schedule != nullptr && schedule->open_attempts > 0) {
    schedule->open_attempts -= 1;
  }
  const auto flow_entry = flows_.find(evidence.flow);
  if (flow_entry == flows_.end()) {
    return;
  }
  FlowRuntime& flow = flow_entry->second;
  const std::uint64_t quantum = record.ticket.quantum;
  flow.outstanding_reserved =
      flow.outstanding_reserved >= quantum ? flow.outstanding_reserved - quantum : 0;
  flow.open_attempt = DispatchAttemptId{};
  flow.scheduled_schedule = ScheduleId{};
  flow.scheduled_generation = Generation{};
  flow.scheduled_ordinal = 0;
  const auto resource = resources_.find(record.ticket.resource);
  if (resource != resources_.end() && resource->second.in_flight > 0) {
    --resource->second.in_flight;
  }
  if (credited > 0) {
    flow.served_work = saturating_add_u64(flow.served_work, credited);
    flow.accounting.service_credit = saturating_add_u64(flow.accounting.service_credit, credited);
    flow.min_service_delivered = saturating_add_u64(flow.min_service_delivered, credited);
  }
  ++flow.accounting.completions_committed;
  // Counted here rather than in complete() so that replaying a durable
  // CompletionCommitted record reproduces the same cumulative figure.
  ++completions_applied_;
  set_lifecycle(flow, lifecycle_after, now);
  record_step(flow, now, lifecycle_after, DecisionReason::Selected, record.ticket.schedule,
              evidence.attempt,
              "completion committed with " + std::to_string(credited) + " units credited");
}

void Scheduler::Impl::normalize_transient_states(Ticks now) {
  // Preempting and Cancelling are decisions taken inside a single critical
  // section. They are normalized before the call returns so that no caller can
  // ever observe a flow resting in a transient state.
  for (auto& entry : flows_) {
    FlowRuntime& flow = entry.second;
    if (flow.lifecycle == FlowLifecycle::Preempting) {
      set_lifecycle(flow, FlowLifecycle::Ready, now);
    } else if (flow.lifecycle == FlowLifecycle::Cancelling) {
      set_lifecycle(flow, FlowLifecycle::Cancelled, now);
    }
  }
}

void Scheduler::Impl::abandon_open_attempts(Ticks now, const char* detail) {
  std::vector<DispatchAttemptId> open;
  open.reserve(attempts_.size());
  for (const auto& entry : attempts_) {
    if (!is_attempt_terminal(entry.second.state)) {
      open.push_back(entry.first);
    }
  }
  for (const DispatchAttemptId attempt : open) {
    apply_attempt_closed(attempt, AttemptState::Abandoned, detail, now);
    recovery_.attempts_abandoned += 1;
  }
  // Anything still holding a schedule reservation is no longer dispatchable.
  const std::deque<ScheduleId> order(schedule_order_);
  for (const ScheduleId id : order) {
    apply_release_schedule(id, now);
  }
  normalize_transient_states(now);
}

void Scheduler::Impl::apply_epoch_advance(FabricEpoch previous, FabricEpoch next,
                                          EpochReason reason, Ticks now) {
  (void)previous;
  (void)reason;
  abandon_open_attempts(now, "epoch advanced: prior dispatch authority retired");
  for (auto& entry : flows_) {
    FlowRuntime& flow = entry.second;
    if (flow.lifecycle == FlowLifecycle::Ambiguous) {
      recovery_.flows_made_ambiguous += 1;
    }
    flow.scheduled_schedule = ScheduleId{};
    flow.scheduled_generation = Generation{};
    flow.scheduled_ordinal = 0;
    flow.outstanding_reserved = 0;
  }
  for (auto& entry : resources_) {
    entry.second.in_flight = 0;
  }
  recovery_.schedules_retired += schedules_.size();
  reindex_all();
  schedules_.clear();
  schedule_attempts_.clear();
  schedule_order_.clear();
  attempts_.clear();
  epoch_ = next;
  preemptions_in_window_ = 0;
  preemption_window_index_ = 0;
}

void Scheduler::Impl::maybe_retire_schedules() {
  while (schedule_order_.size() > policy_.retained_schedule_history) {
    const ScheduleId front = schedule_order_.front();
    const ScheduleRuntime* runtime = find_schedule(front);
    if (runtime == nullptr) {
      schedule_order_.pop_front();
      continue;
    }
    if (runtime->open_attempts > 0 || runtime->pending_entries > 0) {
      // A schedule with outstanding authority is retained regardless of the
      // history bound: dropping it would silently retire live decisions.
      break;
    }
    const auto attempts = schedule_attempts_.find(front);
    if (attempts != schedule_attempts_.end()) {
      for (const DispatchAttemptId attempt : attempts->second) {
        attempts_.erase(attempt);
      }
      schedule_attempts_.erase(attempts);
    }
    schedules_.erase(front);
    schedule_order_.pop_front();
  }
}

// ---------------------------------------------------------------------------
// Snapshot and recovery
// ---------------------------------------------------------------------------

std::string Scheduler::Impl::encode_state_snapshot() const {
  RecordWriter writer(static_cast<std::size_t>(kMaxSnapshotPayload));
  writer.u16(kDurableFormatGeneration);
  writer.u64(epoch_.value());
  write_policy(writer, policy_);
  writer.u64(last_tick_);

  writer.u64(resources_.size());
  for (const auto& entry : resources_) {
    write_resource(writer, entry.second.descriptor);
    writer.u64(entry.second.interval_index);
    writer.boolean(entry.second.interval_initialized);
    writer.u64(entry.second.reserved_this_interval);
    writer.u64(entry.second.in_flight);
  }

  writer.u64(reservations_.size());
  for (const auto& entry : reservations_) {
    write_reservation(writer, entry.second.descriptor);
    writer.u64(entry.second.reserved_units);
    writer.boolean(entry.second.retired);
  }

  writer.u64(priority_classes_.size());
  for (const auto& entry : priority_classes_) {
    write_priority_class(writer, entry.second);
  }

  writer.u64(qos_classes_.size());
  for (const auto& entry : qos_classes_) {
    write_qos_class(writer, entry.second);
  }

  writer.u64(flows_.size());
  for (const auto& entry : flows_) {
    const FlowRuntime& flow = entry.second;
    write_flow_descriptor(writer, flow.descriptor);
    writer.u8(static_cast<std::uint8_t>(flow.lifecycle));
    writer.u64(flow.served_work);
    writer.u64(flow.outstanding_reserved);
    writer.u64(flow.open_attempt.value());
    writer.u64(flow.scheduled_schedule.value());
    writer.u64(flow.scheduled_generation.value());
    writer.u32(flow.scheduled_ordinal);
    writer.u64(flow.scheduled_tick);
    writer.u64(flow.last_change_tick);
    writer.u64(flow.eligible_since_tick);
    writer.boolean(flow.ever_ready);
    writer.u64(flow.min_service_window_start);
    writer.u64(flow.min_service_delivered);
    writer.u64(flow.virtual_finish);
    writer.boolean(flow.deadline_missed_counted);
    writer.u64(flow.accounting.service_credit);
    writer.u64(flow.accounting.dispatches_issued);
    writer.u64(flow.accounting.dispatches_started);
    writer.u64(flow.accounting.completions_committed);
    writer.u64(flow.accounting.duplicate_completions);
    writer.u64(flow.accounting.rejected_completions);
    writer.u64(flow.accounting.preemptions);
    writer.u64(flow.accounting.deadline_misses);
    writer.u64(flow.accounting.wait_ticks_total);
    writer.u64(flow.accounting.starvation_boosts);
  }

  writer.u64(attempts_.size());
  for (const auto& entry : attempts_) {
    const AttemptRuntime& attempt = entry.second;
    write_ticket(writer, attempt.ticket);
    writer.u8(static_cast<std::uint8_t>(attempt.state));
    writer.u64(attempt.opened_tick);
    writer.u64(attempt.closed_tick);
    writer.boolean(attempt.has_evidence);
    writer.u64(attempt.evidence_digest);
    writer.u64(attempt.credited);
  }

  writer.u64(schedules_.size());
  for (const auto& entry : schedules_) {
    write_schedule(writer, entry.second.schedule);
    writer.u64(entry.second.consumed.size());
    for (const std::uint8_t consumed : entry.second.consumed) {
      writer.u8(consumed);
    }
    writer.u64(entry.second.open_attempts);
    writer.u64(entry.second.pending_entries);
  }

  writer.u64(virtual_time_.size());
  for (const auto& entry : virtual_time_) {
    writer.u64(entry.first.value());
    writer.u64(entry.second);
  }

  writer.u64(next_attempt_.value());
  writer.u64(next_schedule_.value());
  writer.u64(next_schedule_generation_.value());
  writer.u64(arbitration_rounds_);
  writer.u64(schedules_issued_);
  writer.u64(schedule_entries_issued_);
  writer.u64(completion_reports_);
  writer.u64(completions_applied_);
  writer.u64(completions_duplicate_);
  writer.u64(completions_rejected_);
  writer.u64(deadline_misses_);
  writer.u64(preemptions_issued_);
  writer.u64(reservations_retired_);
  return writer.overflowed() ? std::string() : writer.take();
}

Status Scheduler::Impl::apply_state_snapshot(std::string_view payload) {
  RecordReader reader(payload);
  std::uint16_t format = 0;
  FS_TRY_ASSIGN(format, reader.u16());
  if (format != kDurableFormatGeneration) {
    return Status::failure(ErrorCode::Unsupported,
                           "state snapshot format generation is not supported");
  }
  flows_.clear();
  resources_.clear();
  reservations_.clear();
  priority_classes_.clear();
  qos_classes_.clear();
  virtual_time_.clear();
  attempts_.clear();
  schedules_.clear();
  schedule_attempts_.clear();
  schedule_order_.clear();

  std::uint64_t value = 0;
  FS_TRY_ASSIGN(value, reader.u64()); epoch_ = FabricEpoch(value);
  SchedulingPolicy policy;
  FS_TRY_ASSIGN(policy, read_policy(reader));
  FS_RETURN_IF_ERROR(validate(policy));
  apply_policy(policy);
  FS_TRY_ASSIGN(value, reader.u64()); last_tick_ = value;

  std::uint64_t count = 0;
  FS_TRY_ASSIGN(count, reader.u64());
  if (count > policy_.max_resources) {
    return Status::failure(ErrorCode::CorruptData, "snapshot resource count exceeds the bound");
  }
  for (std::uint64_t index = 0; index < count; ++index) {
    ResourceDescriptor descriptor;
    FS_TRY_ASSIGN(descriptor, read_resource(reader));
    ResourceRuntime runtime;
    runtime.descriptor = descriptor;
    FS_TRY_ASSIGN(value, reader.u64()); runtime.interval_index = value;
    FS_TRY_ASSIGN(runtime.interval_initialized, reader.boolean());
    FS_TRY_ASSIGN(value, reader.u64()); runtime.reserved_this_interval = value;
    FS_TRY_ASSIGN(value, reader.u64()); runtime.in_flight = value;
    resources_[descriptor.resource] = runtime;
  }

  FS_TRY_ASSIGN(count, reader.u64());
  if (count > policy_.max_reservations) {
    return Status::failure(ErrorCode::CorruptData, "snapshot reservation count exceeds the bound");
  }
  for (std::uint64_t index = 0; index < count; ++index) {
    ReservationDescriptor descriptor;
    FS_TRY_ASSIGN(descriptor, read_reservation(reader));
    ReservationRuntime runtime;
    runtime.descriptor = descriptor;
    FS_TRY_ASSIGN(value, reader.u64()); runtime.reserved_units = value;
    FS_TRY_ASSIGN(runtime.retired, reader.boolean());
    reservations_[descriptor.reservation] = runtime;
  }

  FS_TRY_ASSIGN(count, reader.u64());
  if (count > kMaxPriorityClasses) {
    return Status::failure(ErrorCode::CorruptData, "snapshot priority class count exceeds the bound");
  }
  for (std::uint64_t index = 0; index < count; ++index) {
    PriorityClassDescriptor descriptor;
    FS_TRY_ASSIGN(descriptor, read_priority_class(reader));
    priority_classes_[descriptor.priority] = descriptor;
  }

  FS_TRY_ASSIGN(count, reader.u64());
  if (count > kMaxQoSClasses) {
    return Status::failure(ErrorCode::CorruptData, "snapshot qos class count exceeds the bound");
  }
  for (std::uint64_t index = 0; index < count; ++index) {
    QoSClassDescriptor descriptor;
    FS_TRY_ASSIGN(descriptor, read_qos_class(reader));
    qos_classes_[descriptor.qos] = descriptor;
  }

  FS_TRY_ASSIGN(count, reader.u64());
  if (count > policy_.max_flows) {
    return Status::failure(ErrorCode::CorruptData, "snapshot flow count exceeds the bound");
  }
  for (std::uint64_t index = 0; index < count; ++index) {
    FlowRuntime runtime;
    FS_TRY_ASSIGN(runtime.descriptor, read_flow_descriptor(reader));
    std::uint8_t lifecycle = 0;
    FS_TRY_ASSIGN(lifecycle, reader.u8());
    if (lifecycle > static_cast<std::uint8_t>(FlowLifecycle::Ambiguous)) {
      return Status::failure(ErrorCode::CorruptData, "snapshot flow lifecycle is out of range");
    }
    runtime.lifecycle = static_cast<FlowLifecycle>(lifecycle);
    FS_TRY_ASSIGN(value, reader.u64()); runtime.served_work = value;
    FS_TRY_ASSIGN(value, reader.u64()); runtime.outstanding_reserved = value;
    FS_TRY_ASSIGN(value, reader.u64()); runtime.open_attempt = DispatchAttemptId(value);
    FS_TRY_ASSIGN(value, reader.u64()); runtime.scheduled_schedule = ScheduleId(value);
    FS_TRY_ASSIGN(value, reader.u64()); runtime.scheduled_generation = Generation(value);
    std::uint32_t ordinal = 0;
    FS_TRY_ASSIGN(ordinal, reader.u32()); runtime.scheduled_ordinal = ordinal;
    FS_TRY_ASSIGN(value, reader.u64()); runtime.scheduled_tick = value;
    FS_TRY_ASSIGN(value, reader.u64()); runtime.last_change_tick = value;
    FS_TRY_ASSIGN(value, reader.u64()); runtime.eligible_since_tick = value;
    FS_TRY_ASSIGN(runtime.ever_ready, reader.boolean());
    FS_TRY_ASSIGN(value, reader.u64()); runtime.min_service_window_start = value;
    FS_TRY_ASSIGN(value, reader.u64()); runtime.min_service_delivered = value;
    FS_TRY_ASSIGN(value, reader.u64()); runtime.virtual_finish = value;
    FS_TRY_ASSIGN(runtime.deadline_missed_counted, reader.boolean());
    FS_TRY_ASSIGN(value, reader.u64()); runtime.accounting.service_credit = value;
    FS_TRY_ASSIGN(value, reader.u64()); runtime.accounting.dispatches_issued = value;
    FS_TRY_ASSIGN(value, reader.u64()); runtime.accounting.dispatches_started = value;
    FS_TRY_ASSIGN(value, reader.u64()); runtime.accounting.completions_committed = value;
    FS_TRY_ASSIGN(value, reader.u64()); runtime.accounting.duplicate_completions = value;
    FS_TRY_ASSIGN(value, reader.u64()); runtime.accounting.rejected_completions = value;
    FS_TRY_ASSIGN(value, reader.u64()); runtime.accounting.preemptions = value;
    FS_TRY_ASSIGN(value, reader.u64()); runtime.accounting.deadline_misses = value;
    FS_TRY_ASSIGN(value, reader.u64()); runtime.accounting.wait_ticks_total = value;
    FS_TRY_ASSIGN(value, reader.u64()); runtime.accounting.starvation_boosts = value;
    flows_[runtime.descriptor.flow] = std::move(runtime);
  }

  FS_TRY_ASSIGN(count, reader.u64());
  if (count > static_cast<std::uint64_t>(policy_.retained_schedule_history) + 1) {
    return Status::failure(ErrorCode::CorruptData, "snapshot attempt count exceeds the bound");
  }
  for (std::uint64_t index = 0; index < count; ++index) {
    AttemptRuntime runtime;
    FS_TRY_ASSIGN(runtime.ticket, read_ticket(reader));
    std::uint8_t state = 0;
    FS_TRY_ASSIGN(state, reader.u8());
    if (state > static_cast<std::uint8_t>(AttemptState::Rejected)) {
      return Status::failure(ErrorCode::CorruptData, "snapshot attempt state is out of range");
    }
    runtime.state = static_cast<AttemptState>(state);
    FS_TRY_ASSIGN(value, reader.u64()); runtime.opened_tick = value;
    FS_TRY_ASSIGN(value, reader.u64()); runtime.closed_tick = value;
    FS_TRY_ASSIGN(runtime.has_evidence, reader.boolean());
    FS_TRY_ASSIGN(value, reader.u64()); runtime.evidence_digest = value;
    FS_TRY_ASSIGN(value, reader.u64()); runtime.credited = value;
    attempts_[runtime.ticket.attempt] = std::move(runtime);
  }

  FS_TRY_ASSIGN(count, reader.u64());
  if (count > static_cast<std::uint64_t>(policy_.retained_schedule_history) + 1) {
    return Status::failure(ErrorCode::CorruptData, "snapshot schedule count exceeds the bound");
  }
  for (std::uint64_t index = 0; index < count; ++index) {
    Schedule schedule;
    FS_TRY_ASSIGN(schedule, read_schedule(reader));
    ScheduleRuntime runtime;
    runtime.schedule = schedule;
    std::uint64_t consumed_count = 0;
    FS_TRY_ASSIGN(consumed_count, reader.u64());
    if (consumed_count != schedule.entries.size()) {
      return Status::failure(ErrorCode::CorruptData,
                             "snapshot schedule consumption vector has the wrong length");
    }
    runtime.consumed.assign(static_cast<std::size_t>(consumed_count), 0);
    for (std::uint64_t slot = 0; slot < consumed_count; ++slot) {
      std::uint8_t consumed = 0;
      FS_TRY_ASSIGN(consumed, reader.u8());
      if (consumed > 2u) {
        return Status::failure(ErrorCode::CorruptData, "snapshot consumption marker is invalid");
      }
      runtime.consumed[static_cast<std::size_t>(slot)] = consumed;
    }
    FS_TRY_ASSIGN(value, reader.u64()); runtime.open_attempts = value;
    FS_TRY_ASSIGN(value, reader.u64()); runtime.pending_entries = value;
    schedules_[schedule.schedule] = std::move(runtime);
    schedule_order_.push_back(schedule.schedule);
  }
  for (const auto& entry : attempts_) {
    schedule_attempts_[entry.second.ticket.schedule].push_back(entry.first);
  }

  FS_TRY_ASSIGN(count, reader.u64());
  if (count > policy_.max_flows) {
    return Status::failure(ErrorCode::CorruptData, "snapshot fairness group count exceeds the bound");
  }
  for (std::uint64_t index = 0; index < count; ++index) {
    FS_TRY_ASSIGN(value, reader.u64());
    const FairnessGroupId group(value);
    FS_TRY_ASSIGN(value, reader.u64());
    virtual_time_[group] = value;
  }

  FS_TRY_ASSIGN(value, reader.u64()); next_attempt_ = DispatchAttemptId(value);
  FS_TRY_ASSIGN(value, reader.u64()); next_schedule_ = ScheduleId(value);
  FS_TRY_ASSIGN(value, reader.u64()); next_schedule_generation_ = Generation(value);
  FS_TRY_ASSIGN(value, reader.u64()); arbitration_rounds_ = value;
  FS_TRY_ASSIGN(value, reader.u64()); schedules_issued_ = value;
  FS_TRY_ASSIGN(value, reader.u64()); schedule_entries_issued_ = value;
  FS_TRY_ASSIGN(value, reader.u64()); completion_reports_ = value;
  FS_TRY_ASSIGN(value, reader.u64()); completions_applied_ = value;
  FS_TRY_ASSIGN(value, reader.u64()); completions_duplicate_ = value;
  FS_TRY_ASSIGN(value, reader.u64()); completions_rejected_ = value;
  FS_TRY_ASSIGN(value, reader.u64()); deadline_misses_ = value;
  FS_TRY_ASSIGN(value, reader.u64()); preemptions_issued_ = value;
  FS_TRY_ASSIGN(value, reader.u64()); reservations_retired_ = value;
  if (!reader.exhausted()) {
    return Status::failure(ErrorCode::CorruptData, "snapshot has trailing bytes");
  }
  reindex_all();
  recovery_.resources_restored = resources_.size();
  recovery_.reservations_restored = reservations_.size();
  recovery_.priority_classes_restored = priority_classes_.size();
  recovery_.qos_classes_restored = qos_classes_.size();
  recovery_.flows_restored = flows_.size();
  return Status::success();
}

Status Scheduler::Impl::apply_record(JournalRecordType type, std::string_view payload) {
  RecordReader reader(payload);
  std::uint64_t value = 0;
  switch (type) {
    case JournalRecordType::PolicyInstalled: {
      SchedulingPolicy policy;
      FS_TRY_ASSIGN(policy, read_policy(reader));
      FS_RETURN_IF_ERROR(validate(policy));
      apply_policy(policy);
      return require_exhausted(reader);
    }
    case JournalRecordType::ResourceRegistered: {
      ResourceDescriptor descriptor;
      FS_TRY_ASSIGN(descriptor, read_resource(reader));
      FS_RETURN_IF_ERROR(validate(descriptor));
      apply_resource(descriptor);
      return require_exhausted(reader);
    }
    case JournalRecordType::ReservationRegistered: {
      ReservationDescriptor descriptor;
      FS_TRY_ASSIGN(descriptor, read_reservation(reader));
      FS_RETURN_IF_ERROR(validate(descriptor));
      apply_reservation(descriptor);
      return require_exhausted(reader);
    }
    case JournalRecordType::ReservationRetired: {
      ReservationId reservation;
      Generation generation;
      FS_TRY_ASSIGN(value, reader.u64()); reservation = ReservationId(value);
      FS_TRY_ASSIGN(value, reader.u64()); generation = Generation(value);
      apply_retire_reservation(reservation, generation);
      return require_exhausted(reader);
    }
    case JournalRecordType::PriorityClassRegistered: {
      PriorityClassDescriptor descriptor;
      FS_TRY_ASSIGN(descriptor, read_priority_class(reader));
      FS_RETURN_IF_ERROR(validate(descriptor));
      apply_priority_class(descriptor);
      return require_exhausted(reader);
    }
    case JournalRecordType::QoSClassRegistered: {
      QoSClassDescriptor descriptor;
      FS_TRY_ASSIGN(descriptor, read_qos_class(reader));
      FS_RETURN_IF_ERROR(validate(descriptor));
      apply_qos_class(descriptor);
      return require_exhausted(reader);
    }
    case JournalRecordType::EpochAdvanced: {
      FabricEpoch previous;
      FabricEpoch next;
      EpochReason reason = EpochReason::Recovery;
      Ticks tick = 0;
      FS_TRY_ASSIGN(value, reader.u64()); previous = FabricEpoch(value);
      FS_TRY_ASSIGN(value, reader.u64()); next = FabricEpoch(value);
      std::uint8_t raw_reason = 0;
      FS_TRY_ASSIGN(raw_reason, reader.u8());
      if (raw_reason > 4u) {
        return Status::failure(ErrorCode::CorruptData, "durable epoch reason is out of range");
      }
      reason = static_cast<EpochReason>(raw_reason);
      FS_TRY_ASSIGN(tick, reader.u64());
      apply_epoch_advance(previous, next, reason, tick);
      return require_exhausted(reader);
    }
    case JournalRecordType::FlowAdmitted:
    case JournalRecordType::FlowUpdated: {
      FlowDescriptor descriptor;
      FS_TRY_ASSIGN(descriptor, read_flow_descriptor(reader));
      FS_RETURN_IF_ERROR(validate(descriptor));
      FS_RETURN_IF_ERROR(type == JournalRecordType::FlowAdmitted ? apply_admit(descriptor)
                                                                  : apply_update(descriptor));
      return require_exhausted(reader);
    }
    case JournalRecordType::FlowReadinessChanged: {
      FlowId flow;
      Generation generation;
      std::uint8_t signal = 0;
      Ticks tick = 0;
      FS_TRY_ASSIGN(value, reader.u64()); flow = FlowId(value);
      FS_TRY_ASSIGN(value, reader.u64()); generation = Generation(value);
      FS_TRY_ASSIGN(signal, reader.u8());
      if (signal > 2u) {
        return Status::failure(ErrorCode::CorruptData, "durable readiness signal is out of range");
      }
      FS_TRY_ASSIGN(tick, reader.u64());
      apply_readiness(flow, generation, static_cast<ReadinessSignal>(signal), tick);
      return require_exhausted(reader);
    }
    case JournalRecordType::FlowCancelled: {
      FlowId flow;
      Generation generation;
      Ticks tick = 0;
      FS_TRY_ASSIGN(value, reader.u64()); flow = FlowId(value);
      FS_TRY_ASSIGN(value, reader.u64()); generation = Generation(value);
      FS_TRY_ASSIGN(tick, reader.u64());
      apply_cancel(flow, generation, tick);
      normalize_transient_states(tick);
      return require_exhausted(reader);
    }
    case JournalRecordType::FlowRetired: {
      FlowId flow;
      Generation generation;
      FS_TRY_ASSIGN(value, reader.u64()); flow = FlowId(value);
      FS_TRY_ASSIGN(value, reader.u64()); generation = Generation(value);
      apply_retire_flow(flow, generation);
      return require_exhausted(reader);
    }
    case JournalRecordType::ScheduleIssued: {
      Schedule schedule;
      FS_TRY_ASSIGN(schedule, read_schedule(reader));
      apply_schedule(schedule);
      return require_exhausted(reader);
    }
    case JournalRecordType::ScheduleReleased: {
      ScheduleId schedule;
      Ticks tick = 0;
      FS_TRY_ASSIGN(value, reader.u64()); schedule = ScheduleId(value);
      FS_TRY_ASSIGN(tick, reader.u64());
      apply_release_schedule(schedule, tick);
      return require_exhausted(reader);
    }
    case JournalRecordType::AttemptOpened: {
      DispatchTicket ticket;
      FS_TRY_ASSIGN(ticket, read_ticket(reader));
      std::uint64_t nominal = 0;
      Ticks tick = 0;
      FS_TRY_ASSIGN(nominal, reader.u64());
      FS_TRY_ASSIGN(tick, reader.u64());
      apply_attempt_opened(ticket, nominal, tick);
      return require_exhausted(reader);
    }
    case JournalRecordType::AttemptStarted: {
      DispatchAttemptId attempt;
      Ticks tick = 0;
      FS_TRY_ASSIGN(value, reader.u64()); attempt = DispatchAttemptId(value);
      FS_TRY_ASSIGN(tick, reader.u64());
      apply_attempt_started(attempt, tick);
      return require_exhausted(reader);
    }
    case JournalRecordType::AttemptClosed: {
      DispatchAttemptId attempt;
      std::uint8_t state = 0;
      Ticks tick = 0;
      FS_TRY_ASSIGN(value, reader.u64()); attempt = DispatchAttemptId(value);
      FS_TRY_ASSIGN(state, reader.u8());
      if (state > static_cast<std::uint8_t>(AttemptState::Rejected)) {
        return Status::failure(ErrorCode::CorruptData, "durable attempt state is out of range");
      }
      FS_TRY_ASSIGN(tick, reader.u64());
      std::string_view detail;
      FS_TRY_ASSIGN(detail, reader.text());
      apply_attempt_closed(attempt, static_cast<AttemptState>(state), detail, tick);
      normalize_transient_states(tick);
      return require_exhausted(reader);
    }
    case JournalRecordType::CompletionCommitted: {
      CompletionEvidence evidence;
      FS_TRY_ASSIGN(evidence, read_evidence(reader));
      std::uint64_t credited = 0;
      std::uint8_t lifecycle = 0;
      Ticks tick = 0;
      FS_TRY_ASSIGN(credited, reader.u64());
      FS_TRY_ASSIGN(lifecycle, reader.u8());
      if (lifecycle > static_cast<std::uint8_t>(FlowLifecycle::Ambiguous)) {
        return Status::failure(ErrorCode::CorruptData, "durable completion lifecycle is invalid");
      }
      FS_TRY_ASSIGN(tick, reader.u64());
      apply_completion(evidence, credited, static_cast<FlowLifecycle>(lifecycle), tick);
      return require_exhausted(reader);
    }
    case JournalRecordType::AmbiguityResolved: {
      FlowId flow;
      Generation generation;
      std::uint8_t resolution = 0;
      Ticks tick = 0;
      FS_TRY_ASSIGN(value, reader.u64()); flow = FlowId(value);
      FS_TRY_ASSIGN(value, reader.u64()); generation = Generation(value);
      FS_TRY_ASSIGN(resolution, reader.u8());
      if (resolution > 1u) {
        return Status::failure(ErrorCode::CorruptData, "durable ambiguity resolution is invalid");
      }
      FS_TRY_ASSIGN(tick, reader.u64());
      apply_ambiguity_resolved(flow, generation, static_cast<AmbiguityResolution>(resolution), tick);
      return require_exhausted(reader);
    }
    case JournalRecordType::Checkpoint:
    case JournalRecordType::Unknown:
      return Status::success();
  }
  return Status::failure(ErrorCode::Unsupported, "journal record type is not supported");
}

Status Scheduler::Impl::recover() {
  const Result<std::string> snapshot = store_->read_snapshot();
  bool have_snapshot = false;
  if (snapshot.ok()) {
    FS_RETURN_IF_ERROR(apply_state_snapshot(snapshot.value()));
    have_snapshot = true;
    recovery_.snapshot_records = 1;
  } else if (snapshot.code() != ErrorCode::NotFound) {
    return snapshot.error();
  }

  JournalScanReport report;
  FS_RETURN_IF_ERROR(store_->replay(true, report,
                                    [this](const JournalRecordView& view) {
                                      return apply_record(view.type, view.payload);
                                    }));
  recovery_.journal_records_replayed = report.records;
  recovery_.journal_bytes_replayed = report.bytes;
  // A torn tail may already have been observed when the store was opened;
  // both sources describe the same crash artifact.
  recovery_.tail_truncated = report.tail_truncated || store_->open_tail_truncated();
  recovery_.recovered = have_snapshot || report.records > 0;
  recovery_.epoch_before = epoch_;
  recovery_.flows_restored = flows_.size();
  recovery_.resources_restored = resources_.size();
  recovery_.reservations_restored = reservations_.size();
  recovery_.priority_classes_restored = priority_classes_.size();
  recovery_.qos_classes_restored = qos_classes_.size();

  // Opening durable state always creates a new coordinator incarnation. The
  // epoch therefore advances unconditionally: every dispatch authority issued
  // by the previous incarnation is retired before this one accepts any work.
  const FabricEpoch previous = epoch_.valid() ? epoch_ : FabricEpoch(0);
  FS_TRY_ASSIGN(const FabricEpoch next, advance(previous));
  {
    RecordWriter writer;
    writer.u64(previous.value());
    writer.u64(next.value());
    writer.u8(static_cast<std::uint8_t>(EpochReason::Recovery));
    writer.u64(last_tick_);
    FS_RETURN_IF_ERROR(journal(JournalRecordType::EpochAdvanced, writer.take()));
  }
  apply_epoch_advance(previous, next, EpochReason::Recovery, last_tick_);
  recovery_.epoch_after = epoch_;
  recovery_.detail = recovery_.recovered ? "recovered durable state; epoch advanced"
                                         : "fresh durable state; epoch initialized";
  return Status::success();
}

Status Scheduler::Impl::checkpoint() {
  std::lock_guard<std::mutex> guard(mutex_);
  if (!store_) {
    return Status::success();
  }
  const std::string payload = encode_state_snapshot();
  if (payload.empty()) {
    return Status::failure(ErrorCode::Bounded,
                           "state image exceeds the snapshot bound; checkpoint refused");
  }
  return store_->compact(payload);
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

Status Scheduler::Impl::register_resource(const ResourceDescriptor& descriptor) {
  std::lock_guard<std::mutex> guard(mutex_);
  FS_RETURN_IF_ERROR(validate(descriptor));
  const auto found = resources_.find(descriptor.resource);
  if (found == resources_.end()) {
    if (resources_.size() >= policy_.max_resources) {
      return Status::failure(ErrorCode::Bounded, "resource table bound reached");
    }
    if (descriptor.generation.value() != 1) {
      return Status::failure(ErrorCode::InvalidArgument,
                             "a newly registered resource must start at generation 1");
    }
  } else {
    FS_TRY_ASSIGN(const Generation expected, advance(found->second.descriptor.generation));
    if (descriptor.generation != expected) {
      return Status::failure(ErrorCode::InvalidArgument,
                             "resource update must advance the generation by exactly one");
    }
  }
  RecordWriter writer;
  write_resource(writer, descriptor);
  FS_RETURN_IF_ERROR(journal(JournalRecordType::ResourceRegistered, writer.take()));
  apply_resource(descriptor);
  return Status::success();
}

Status Scheduler::Impl::register_reservation(const ReservationDescriptor& descriptor) {
  std::lock_guard<std::mutex> guard(mutex_);
  FS_RETURN_IF_ERROR(validate(descriptor));
  const auto resource = resources_.find(descriptor.resource);
  if (resource == resources_.end()) {
    return Status::failure(ErrorCode::UnknownResource,
                           "reservation references a resource that is not registered");
  }
  if (resource->second.descriptor.generation != descriptor.resource_generation) {
    return Status::failure(ErrorCode::StaleResourceGeneration,
                           "reservation binds a stale resource generation");
  }
  const auto found = reservations_.find(descriptor.reservation);
  if (found == reservations_.end()) {
    if (reservations_.size() >= policy_.max_reservations) {
      return Status::failure(ErrorCode::Bounded, "reservation table bound reached");
    }
    if (descriptor.generation.value() != 1) {
      return Status::failure(ErrorCode::InvalidArgument,
                             "a newly registered reservation must start at generation 1");
    }
  } else {
    FS_TRY_ASSIGN(const Generation expected, advance(found->second.descriptor.generation));
    if (descriptor.generation != expected) {
      return Status::failure(ErrorCode::InvalidArgument,
                             "reservation update must advance the generation by exactly one");
    }
  }
  RecordWriter writer;
  write_reservation(writer, descriptor);
  FS_RETURN_IF_ERROR(journal(JournalRecordType::ReservationRegistered, writer.take()));
  apply_reservation(descriptor);
  return Status::success();
}

Status Scheduler::Impl::retire_reservation(ReservationId reservation, Generation generation) {
  std::lock_guard<std::mutex> guard(mutex_);
  const auto found = reservations_.find(reservation);
  if (found == reservations_.end()) {
    return Status::failure(ErrorCode::UnknownReservation, "reservation is not registered");
  }
  if (found->second.descriptor.generation != generation) {
    return Status::failure(ErrorCode::StaleReservationGeneration,
                           "reservation generation does not match");
  }
  if (found->second.retired) {
    return Status::success();  // Idempotent retirement.
  }
  RecordWriter writer;
  writer.u64(reservation.value());
  writer.u64(generation.value());
  FS_RETURN_IF_ERROR(journal(JournalRecordType::ReservationRetired, writer.take()));
  apply_retire_reservation(reservation, generation);
  return Status::success();
}

Status Scheduler::Impl::register_priority_class(const PriorityClassDescriptor& descriptor) {
  std::lock_guard<std::mutex> guard(mutex_);
  FS_RETURN_IF_ERROR(validate(descriptor));
  const auto found = priority_classes_.find(descriptor.priority);
  if (found == priority_classes_.end()) {
    if (priority_classes_.size() >= kMaxPriorityClasses) {
      return Status::failure(ErrorCode::Bounded, "priority class table bound reached");
    }
    if (descriptor.generation.value() != 1) {
      return Status::failure(ErrorCode::InvalidArgument,
                             "a newly registered priority class must start at generation 1");
    }
  } else {
    FS_TRY_ASSIGN(const Generation expected, advance(found->second.generation));
    if (descriptor.generation != expected) {
      return Status::failure(ErrorCode::InvalidArgument,
                             "priority class update must advance the generation by exactly one");
    }
  }
  RecordWriter writer;
  write_priority_class(writer, descriptor);
  FS_RETURN_IF_ERROR(journal(JournalRecordType::PriorityClassRegistered, writer.take()));
  apply_priority_class(descriptor);
  return Status::success();
}

Status Scheduler::Impl::register_qos_class(const QoSClassDescriptor& descriptor) {
  std::lock_guard<std::mutex> guard(mutex_);
  FS_RETURN_IF_ERROR(validate(descriptor));
  const auto found = qos_classes_.find(descriptor.qos);
  if (found == qos_classes_.end()) {
    if (qos_classes_.size() >= kMaxQoSClasses) {
      return Status::failure(ErrorCode::Bounded, "qos class table bound reached");
    }
    if (descriptor.generation.value() != 1) {
      return Status::failure(ErrorCode::InvalidArgument,
                             "a newly registered qos class must start at generation 1");
    }
  } else {
    FS_TRY_ASSIGN(const Generation expected, advance(found->second.generation));
    if (descriptor.generation != expected) {
      return Status::failure(ErrorCode::InvalidArgument,
                             "qos class update must advance the generation by exactly one");
    }
  }
  RecordWriter writer;
  write_qos_class(writer, descriptor);
  FS_RETURN_IF_ERROR(journal(JournalRecordType::QoSClassRegistered, writer.take()));
  apply_qos_class(descriptor);
  return Status::success();
}

// ---------------------------------------------------------------------------
// Flow lifecycle
// ---------------------------------------------------------------------------

Status Scheduler::Impl::admit_flow(const FlowDescriptor& descriptor) {
  std::lock_guard<std::mutex> guard(mutex_);
  FS_RETURN_IF_ERROR(validate(descriptor));
  if (flows_.find(descriptor.flow) != flows_.end()) {
    return Status::failure(ErrorCode::AlreadyExists, "flow identity is already registered");
  }
  const auto resource = resources_.find(descriptor.resource.resource);
  if (resource == resources_.end()) {
    return Status::failure(ErrorCode::UnknownResource,
                           "flow binds a resource that is not registered");
  }
  if (resource->second.descriptor.generation != descriptor.resource.generation) {
    return Status::failure(ErrorCode::StaleResourceGeneration,
                           "flow binds a stale resource generation");
  }
  const auto priority = priority_classes_.find(descriptor.priority.priority);
  if (priority == priority_classes_.end()) {
    return Status::failure(ErrorCode::UnknownPolicy,
                           "flow binds a priority class that is not registered");
  }
  if (priority->second.generation != descriptor.priority.generation) {
    return Status::failure(ErrorCode::StalePriorityGeneration,
                           "flow binds a stale priority class generation");
  }
  const auto qos = qos_classes_.find(descriptor.qos.qos);
  if (qos == qos_classes_.end()) {
    return Status::failure(ErrorCode::UnknownPolicy, "flow binds a qos class that is not registered");
  }
  if (qos->second.generation != descriptor.qos.generation) {
    return Status::failure(ErrorCode::StaleQoSGeneration,
                           "flow binds a stale qos class generation");
  }
  if (descriptor.reservation.bound()) {
    const auto reservation = reservations_.find(descriptor.reservation.reservation);
    if (reservation == reservations_.end() || reservation->second.retired) {
      return Status::failure(ErrorCode::UnknownReservation,
                             "flow binds a reservation that is not registered");
    }
    if (reservation->second.descriptor.generation != descriptor.reservation.generation) {
      return Status::failure(ErrorCode::StaleReservationGeneration,
                             "flow binds a stale reservation generation");
    }
    if (reservation->second.descriptor.resource != descriptor.resource.resource) {
      return Status::failure(ErrorCode::InvalidArgument,
                             "flow reservation and resource bindings disagree");
    }
  }
  RecordWriter writer;
  write_flow_descriptor(writer, descriptor);
  FS_RETURN_IF_ERROR(journal(JournalRecordType::FlowAdmitted, writer.take()));
  return apply_admit(descriptor);
}

Status Scheduler::Impl::update_flow(const FlowDescriptor& descriptor) {
  std::lock_guard<std::mutex> guard(mutex_);
  FS_RETURN_IF_ERROR(validate(descriptor));
  const auto found = flows_.find(descriptor.flow);
  if (found == flows_.end()) {
    return Status::failure(ErrorCode::UnknownFlow, "cannot update an unregistered flow");
  }
  FS_TRY_ASSIGN(const Generation expected, advance(found->second.descriptor.generation));
  if (descriptor.generation != expected) {
    return Status::failure(ErrorCode::InvalidArgument,
                           "flow update must advance the generation by exactly one");
  }
  if (!found->second.descriptor.dependencies.empty() ||
      !descriptor.dependencies.empty() ||
      found->second.descriptor.estimated_work != descriptor.estimated_work) {
    // Dependency and work-load changes alter readiness semantics; refusing
    // them keeps "no flow before ready" decidable without re-deriving history.
    if (found->second.lifecycle != FlowLifecycle::Waiting &&
        found->second.served_work > 0) {
      return Status::failure(ErrorCode::InvalidState,
                             "cannot change dependencies or estimated work of a flow with progress");
    }
  }
  const auto resource = resources_.find(descriptor.resource.resource);
  if (resource == resources_.end()) {
    return Status::failure(ErrorCode::UnknownResource,
                           "flow binds a resource that is not registered");
  }
  if (resource->second.descriptor.generation != descriptor.resource.generation) {
    return Status::failure(ErrorCode::StaleResourceGeneration,
                           "flow binds a stale resource generation");
  }
  RecordWriter writer;
  write_flow_descriptor(writer, descriptor);
  FS_RETURN_IF_ERROR(journal(JournalRecordType::FlowUpdated, writer.take()));
  FS_RETURN_IF_ERROR(apply_update(descriptor));
  // A generation advance retires any schedule entry issued under the old
  // generation: the new generation must be arbitrated afresh.
  FlowRuntime& flow = flows_[descriptor.flow];
  if (flow.lifecycle == FlowLifecycle::Scheduled) {
    ScheduleRuntime* scheduled = find_schedule(flow.scheduled_schedule);
    const auto index = static_cast<std::size_t>(flow.scheduled_ordinal);
    if (scheduled != nullptr && index < scheduled->consumed.size()) {
      release_run_entry(*scheduled, index, last_tick_);
    }
  }
  return Status::success();
}

Status Scheduler::Impl::cancel_flow(FlowId flow, Generation generation, std::string_view reason,
                                    Ticks now) {
  std::lock_guard<std::mutex> guard(mutex_);
  last_tick_ = std::max(last_tick_, now);
  const auto found = flows_.find(flow);
  if (found == flows_.end()) {
    return Status::failure(ErrorCode::UnknownFlow, "cannot cancel an unregistered flow");
  }
  if (found->second.descriptor.generation != generation) {
    return Status::failure(ErrorCode::StaleFlowGeneration,
                           "cancellation names a stale flow generation");
  }
  if (found->second.lifecycle == FlowLifecycle::Cancelled) {
    return Status::success();  // Idempotent.
  }
  if (is_terminal(found->second.lifecycle)) {
    return Status::failure(ErrorCode::InvalidState, "flow already reached a terminal state");
  }
  RecordWriter writer;
  writer.u64(flow.value());
  writer.u64(generation.value());
  writer.u64(now);
  writer.text(bounded_detail(reason));
  FS_RETURN_IF_ERROR(journal(JournalRecordType::FlowCancelled, writer.take()));
  apply_cancel(flow, generation, now);
  normalize_transient_states(now);
  return Status::success();
}

Status Scheduler::Impl::notify_readiness(FlowId flow, Generation generation,
                                         ReadinessSignal signal, Ticks now) {
  std::lock_guard<std::mutex> guard(mutex_);
  last_tick_ = std::max(last_tick_, now);
  const auto found = flows_.find(flow);
  if (found == flows_.end()) {
    return Status::failure(ErrorCode::UnknownFlow, "cannot signal an unregistered flow");
  }
  if (found->second.descriptor.generation != generation) {
    return Status::failure(ErrorCode::StaleFlowGeneration, "readiness names a stale generation");
  }
  if (is_terminal(found->second.lifecycle)) {
    return Status::failure(ErrorCode::InvalidState, "flow already reached a terminal state");
  }
  if (found->second.descriptor.readiness_signal == static_cast<std::uint8_t>(signal)) {
    return Status::success();
  }
  RecordWriter writer;
  writer.u64(flow.value());
  writer.u64(generation.value());
  writer.u8(static_cast<std::uint8_t>(signal));
  writer.u64(now);
  FS_RETURN_IF_ERROR(journal(JournalRecordType::FlowReadinessChanged, writer.take()));
  apply_readiness(flow, generation, signal, now);
  return Status::success();
}

Status Scheduler::Impl::resolve_ambiguous(FlowId flow, Generation generation,
                                          AmbiguityResolution resolution, Ticks now) {
  std::lock_guard<std::mutex> guard(mutex_);
  last_tick_ = std::max(last_tick_, now);
  const auto found = flows_.find(flow);
  if (found == flows_.end()) {
    return Status::failure(ErrorCode::UnknownFlow, "cannot resolve an unregistered flow");
  }
  if (found->second.descriptor.generation != generation) {
    return Status::failure(ErrorCode::StaleFlowGeneration, "resolution names a stale generation");
  }
  if (found->second.lifecycle != FlowLifecycle::Ambiguous) {
    return Status::failure(ErrorCode::InvalidState, "flow is not in an ambiguous state");
  }
  RecordWriter writer;
  writer.u64(flow.value());
  writer.u64(generation.value());
  writer.u8(static_cast<std::uint8_t>(resolution));
  writer.u64(now);
  FS_RETURN_IF_ERROR(journal(JournalRecordType::AmbiguityResolved, writer.take()));
  apply_ambiguity_resolved(flow, generation, resolution, now);
  return Status::success();
}

Status Scheduler::Impl::retire_flow(FlowId flow, Generation generation) {
  std::lock_guard<std::mutex> guard(mutex_);
  const auto found = flows_.find(flow);
  if (found == flows_.end()) {
    return Status::failure(ErrorCode::UnknownFlow, "cannot retire an unregistered flow");
  }
  if (found->second.descriptor.generation != generation) {
    return Status::failure(ErrorCode::StaleFlowGeneration, "retirement names a stale generation");
  }
  if (!is_terminal(found->second.lifecycle)) {
    return Status::failure(ErrorCode::InvalidState,
                           "only a terminal flow can be retired from the table");
  }
  RecordWriter writer;
  writer.u64(flow.value());
  writer.u64(generation.value());
  FS_RETURN_IF_ERROR(journal(JournalRecordType::FlowRetired, writer.take()));
  apply_retire_flow(flow, generation);
  return Status::success();
}

// ---------------------------------------------------------------------------
// Policy and epoch
// ---------------------------------------------------------------------------

Status Scheduler::Impl::install_policy(const SchedulingPolicy& policy) {
  std::lock_guard<std::mutex> guard(mutex_);
  FS_RETURN_IF_ERROR(validate(policy));
  if (policy_ .policy.valid() && policy.policy == policy_.policy) {
    FS_TRY_ASSIGN(const Generation expected, advance(policy_.generation));
    if (policy.generation != expected) {
      return Status::failure(ErrorCode::InvalidArgument,
                             "policy update must advance the generation by exactly one");
    }
  } else if (policy.generation.value() != 1) {
    return Status::failure(ErrorCode::InvalidArgument,
                           "a newly installed policy must start at generation 1");
  }
  if (policy.max_flows < flows_.size()) {
    return Status::failure(ErrorCode::Bounded,
                           "new policy flow bound is below the currently registered flow count");
  }
  RecordWriter writer;
  write_policy(writer, policy);
  FS_RETURN_IF_ERROR(journal(JournalRecordType::PolicyInstalled, writer.take()));
  apply_policy(policy);
  return Status::success();
}

Result<FabricEpoch> Scheduler::Impl::advance_epoch(EpochReason reason, Ticks now) {
  std::lock_guard<std::mutex> guard(mutex_);
  last_tick_ = std::max(last_tick_, now);
  const FabricEpoch previous = epoch_.valid() ? epoch_ : FabricEpoch(0);
  FabricEpoch next;
  FS_TRY_ASSIGN(next, advance(previous));
  RecordWriter writer;
  writer.u64(previous.value());
  writer.u64(next.value());
  writer.u8(static_cast<std::uint8_t>(reason));
  writer.u64(now);
  FS_RETURN_IF_ERROR(journal(JournalRecordType::EpochAdvanced, writer.take()));
  apply_epoch_advance(previous, next, reason, now);
  return next;
}

// ---------------------------------------------------------------------------
// Arbitration
// ---------------------------------------------------------------------------

void Scheduler::Impl::reclaim_expired_deliveries(Ticks now, ArbitrationOutcome& outcome) {
  const std::deque<ScheduleId> order(schedule_order_);
  for (const ScheduleId id : order) {
    ScheduleRuntime* runtime = find_schedule(id);
    if (runtime == nullptr || runtime->pending_entries == 0) {
      continue;
    }
    const std::uint64_t expires_at =
        saturating_add_u64(runtime->schedule.at_tick, policy_.dispatch_delivery_horizon);
    if (expires_at > now) {
      break;  // Schedules are ordered by issue tick, so later ones expire later.
    }
    RecordWriter writer;
    writer.u64(id.value());
    writer.u64(now);
    const Status appended = journal(JournalRecordType::ScheduleReleased, writer.take());
    if (!appended.ok()) {
      outcome.deferred_truncated = true;
      return;
    }
    for (std::size_t index = 0; index < runtime->schedule.entries.size(); ++index) {
      if (runtime->consumed[index] != 0) {
        continue;
      }
      const ScheduleEntry& entry = runtime->schedule.entries[index];
      if (entry.kind == DecisionKind::Run && outcome.delivery_reclaims.size() < kMaxDeferredPerRound) {
        outcome.delivery_reclaims.push_back(entry.flow);
      }
    }
    apply_release_schedule(id, now);
  }
}

void Scheduler::Impl::refresh_lifecycles(Ticks now, ArbitrationOutcome& outcome) {
  const std::vector<FlowId> waiting(waiting_index_.begin(), waiting_index_.end());
  for (const FlowId id : waiting) {
    const auto found = flows_.find(id);
    if (found == flows_.end()) {
      continue;
    }
    FlowRuntime& flow = found->second;
    if (flow.lifecycle == FlowLifecycle::Ambiguous) {
      defer(outcome, flow, DecisionReason::AmbiguousRequiresResolution,
            "recovered outcome requires explicit resolution");
      continue;
    }
    if (now < flow.descriptor.release_tick) {
      // Readiness requires release, so nothing about this flow can change this
      // round: report it and move on without re-deriving its full eligibility.
      // The deferral list is itself bounded, so a large not-yet-released
      // population costs one comparison per flow instead of a full evaluation.
      defer(outcome, flow, DecisionReason::AwaitingRelease, "not released");
      continue;
    }
    bool ready = false;
    const DecisionReason reason = derived_readiness(flow, now, ready);
    if (ready) {
      set_lifecycle(flow, FlowLifecycle::Ready, now);
      record_step(flow, now, FlowLifecycle::Ready, DecisionReason::Selected, ScheduleId{},
                  DispatchAttemptId{}, "ready");
    } else if (reason == DecisionReason::DeadlineMissed) {
      handle_deadline_miss(flow, now, outcome);
    } else if (reason == DecisionReason::DependencyFailed) {
      set_lifecycle(flow, FlowLifecycle::Failed, now);
      record_step(flow, now, FlowLifecycle::Failed, DecisionReason::DependencyFailed, ScheduleId{},
                  DispatchAttemptId{}, "a dependency reached a terminal state without completing");
      defer(outcome, flow, DecisionReason::DependencyFailed, to_string(reason));
    } else {
      defer(outcome, flow, reason, to_string(reason));
    }
  }

  const std::vector<FlowId> inflight(inflight_index_.begin(), inflight_index_.end());
  for (const FlowId id : inflight) {
    const auto found = flows_.find(id);
    if (found == flows_.end()) {
      continue;
    }
    FlowRuntime& flow = found->second;
    if (!has_deadline(flow.descriptor.deadline_tick)) {
      continue;
    }
    if (now < flow.descriptor.deadline_tick) {
      continue;
    }
    if (!flow.deadline_missed_counted) {
      flow.deadline_missed_counted = true;
      flow.accounting.deadline_misses += 1;
      ++deadline_misses_;
      if (outcome.deadline_misses.size() < kMaxDeferredPerRound) {
        outcome.deadline_misses.push_back(flow.descriptor.flow);
      }
    }
  }
}

Result<ArbitrationOutcome> Scheduler::Impl::arbitrate(Ticks now) {
  std::lock_guard<std::mutex> guard(mutex_);
  if (now < last_tick_) {
    return Error(ErrorCode::InvalidArgument, "arbitration tick moved backwards");
  }
  last_tick_ = now;
  ++arbitration_rounds_;

  ArbitrationOutcome outcome;
  reclaim_expired_deliveries(now, outcome);
  refresh_lifecycles(now, outcome);

  // Step 1: a running flow that has crossed its deadline must be stopped. This
  // is an invariant, not a policy: execution after the window is illegal even
  // when opportunistic preemption is disabled.
  {
    std::vector<DispatchAttemptId> overdue;
    for (const FlowId id : inflight_index_) {
      const auto found = flows_.find(id);
      if (found == flows_.end()) {
        continue;
      }
      const FlowRuntime& flow = found->second;
      if (!has_deadline(flow.descriptor.deadline_tick) || now < flow.descriptor.deadline_tick) {
        continue;
      }
      if (flow.open_attempt.valid()) {
        overdue.push_back(flow.open_attempt);
      }
    }
    for (const DispatchAttemptId attempt : overdue) {
      FS_RETURN_IF_ERROR(
          close_attempt(attempt, AttemptState::Preempted, "deadline crossed while running", now));
      ++preemptions_issued_;
    }
  }

  // Step 2: rank every ready flow with a strict total order.
  struct Candidate {
    FlowId flow{};
    OrderKey key{};
  };
  std::vector<Candidate> candidates;
  candidates.reserve(ready_index_.size());
  for (const FlowId id : ready_index_) {
    const auto found = flows_.find(id);
    if (found == flows_.end() || found->second.lifecycle != FlowLifecycle::Ready) {
      continue;
    }
    Candidate candidate;
    candidate.flow = id;
    candidate.key = compute_key(found->second, now);
    if (candidate.key.tier == kTierStarvation && !found->second.starvation_counted) {
      found->second.starvation_counted = true;
      found->second.accounting.starvation_boosts += 1;
    }
    candidates.push_back(candidate);
  }
  std::sort(candidates.begin(), candidates.end(),
            [](const Candidate& a, const Candidate& b) { return a.key < b.key; });

  // Step 3: greedily assign legal service windows.
  const std::size_t hard_entry_limit =
      static_cast<std::size_t>(kMaxJournalRecordPayload) / kScheduleEntryBudgetBytes;
  std::size_t entry_limit = policy_.max_entries_per_schedule;
  if (entry_limit > hard_entry_limit) {
    entry_limit = hard_entry_limit;
  }

  std::vector<ScheduleEntry> entries;
  entries.reserve(std::min<std::size_t>(candidates.size(), entry_limit));
  std::set<FlowId> decided;
  std::uint32_t next_ordinal = 0;

  // Capacity and overlap committed by entries already placed in THIS round.
  // The resource tables are only updated once the schedule is applied, so the
  // loop must account for its own grants or it would over-commit the interval,
  // the concurrency ceiling, and the reservation grant.
  std::map<ResourceId, std::uint64_t> pending_in_flight;
  std::map<ResourceId, std::uint64_t> pending_capacity;
  std::map<ReservationId, std::uint64_t> pending_reservation;

  const std::uint64_t preemption_window =
      policy_.preemption_interval_ticks == 0 ? 0 : now / policy_.preemption_interval_ticks;
  if (preemption_window != preemption_window_index_) {
    preemption_window_index_ = preemption_window;
    preemptions_in_window_ = 0;
  }

  for (const Candidate& candidate : candidates) {
    const auto found = flows_.find(candidate.flow);
    if (found == flows_.end()) {
      continue;
    }
    FlowRuntime& flow = found->second;
    if (flow.lifecycle != FlowLifecycle::Ready) {
      continue;
    }
    if (decided.find(candidate.flow) != decided.end()) {
      continue;
    }
    // Readiness can lapse without the flow ever leaving the ready set: a
    // reservation window can close, a deadline can cross, or an explicit
    // not-ready signal can arrive. Re-deriving it here is what keeps "no flow
    // before ready" true for the whole round, not just at the round boundary.
    {
      bool still_ready = false;
      const DecisionReason current = derived_readiness(flow, now, still_ready);
      if (!still_ready) {
        if (current == DecisionReason::DeadlineMissed) {
          handle_deadline_miss(flow, now, outcome);
        } else if (current == DecisionReason::DependencyFailed) {
          set_lifecycle(flow, FlowLifecycle::Failed, now);
          record_step(flow, now, FlowLifecycle::Failed, current, ScheduleId{},
                      DispatchAttemptId{},
                      "a dependency reached a terminal state without completing");
          defer(outcome, flow, current, to_string(current));
        } else {
          defer(outcome, flow, current, to_string(current));
        }
        continue;
      }
    }
    if (entries.size() >= entry_limit) {
      defer(outcome, flow, DecisionReason::ScheduleEntryLimitReached,
            "round entry bound reached");
      continue;
    }
    const auto resource = resources_.find(flow.descriptor.resource.resource);
    if (resource == resources_.end() ||
        resource->second.descriptor.generation != flow.descriptor.resource.generation) {
      defer(outcome, flow, DecisionReason::ResourceUnavailable,
            "bound resource is missing or its generation advanced");
      continue;
    }
    ResourceRuntime& resource_runtime = resource->second;
    roll_interval(resource_runtime, now);
    // Live value plus this round's grants. Closing an incumbent attempt during
    // the round updates the resource table immediately, so the two never
    // double count.
    const auto effective_in_flight = [&resource_runtime, &pending_in_flight,
                                      &flow]() -> std::uint64_t {
      return resource_runtime.in_flight + pending_in_flight[flow.descriptor.resource.resource];
    };

    // Displacement, when the concurrency ceiling blocks the candidate.
    if (effective_in_flight() >= resource_runtime.descriptor.max_overlap) {
      bool found_incumbent = false;
      const DispatchAttemptId victim = worst_incumbent(flow.descriptor.resource.resource, now,
                                                       found_incumbent);
      if (!found_incumbent) {
        defer(outcome, flow, DecisionReason::ConcurrencyLimitReached,
              "resource concurrency ceiling reached with no displaceable incumbent");
        continue;
      }
      const auto victim_attempt = attempts_.find(victim);
      if (victim_attempt == attempts_.end()) {
        defer(outcome, flow, DecisionReason::ConcurrencyLimitReached,
              "resource concurrency ceiling reached");
        continue;
      }
      const auto victim_flow = flows_.find(victim_attempt->second.ticket.flow);
      if (victim_flow == flows_.end()) {
        defer(outcome, flow, DecisionReason::ConcurrencyLimitReached,
              "resource concurrency ceiling reached");
        continue;
      }
      DecisionReason refusal = DecisionReason::PreemptionDisabled;
      if (!preemption_allowed(candidate.key, victim_flow->second, now, refusal)) {
        defer(outcome, flow, refusal, to_string(refusal));
        continue;
      }
      ScheduleEntry preempt;
      preempt.ordinal = next_ordinal++;
      preempt.kind = DecisionKind::Preempt;
      preempt.flow = victim_flow->first;
      preempt.flow_generation = victim_flow->second.descriptor.generation;
      preempt.attempt = victim;
      preempt.path = victim_attempt->second.ticket.path;
      preempt.path_generation = victim_attempt->second.ticket.path_generation;
      preempt.resource = victim_attempt->second.ticket.resource;
      preempt.resource_generation = victim_attempt->second.ticket.resource_generation;
      preempt.reservation = victim_attempt->second.ticket.reservation;
      preempt.reservation_generation = victim_attempt->second.ticket.reservation_generation;
      preempt.qos = victim_attempt->second.ticket.qos;
      preempt.qos_generation = victim_attempt->second.ticket.qos_generation;
      preempt.priority = victim_attempt->second.ticket.priority;
      preempt.priority_generation = victim_attempt->second.ticket.priority_generation;
      preempt.quantum = victim_attempt->second.ticket.quantum;
      preempt.start_tick = now;
      preempt.deadline_tick = victim_attempt->second.ticket.deadline_tick;
      preempt.reservation_end = victim_attempt->second.ticket.reservation_end;
      preempt.tier = candidate.key.tier;
      preempt.reason = DecisionReason::DisplacedByHigherAuthority;
      preempt.ordering_digest = digest_of(candidate.key);
      preempt.detail = bounded_detail("displaced by " + to_string(flow.descriptor.flow));
      entries.push_back(std::move(preempt));
      FS_RETURN_IF_ERROR(close_attempt(victim, AttemptState::Preempted,
                                       "displaced by higher authority", now));
      ++preemptions_issued_;
      ++preemptions_in_window_;
      decided.insert(victim_flow->first);
      defer(outcome, victim_flow->second, DecisionReason::DisplacedByHigherAuthority,
            "displaced by higher authority");
      roll_interval(resource_runtime, now);
    }

    // Compute the largest legal service window.
    const std::uint64_t remaining_work =
        flow.descriptor.estimated_work > flow.served_work
            ? flow.descriptor.estimated_work - flow.served_work
            : 0;
    std::uint64_t quantum = std::min(flow.descriptor.service_quantum, remaining_work);
    quantum = std::min(quantum, policy_.max_quantum);
    const auto qos = qos_classes_.find(flow.descriptor.qos.qos);
    if (qos != qos_classes_.end() && qos->second.generation == flow.descriptor.qos.generation &&
        qos->second.max_service_per_dispatch > 0) {
      quantum = std::min(quantum, qos->second.max_service_per_dispatch);
    }
    const std::uint64_t raw_capacity = capacity_remaining(resource_runtime, now);
    const std::uint64_t charged =
        pending_capacity[flow.descriptor.resource.resource];
    const std::uint64_t capacity = raw_capacity > charged ? raw_capacity - charged : 0;
    quantum = std::min(quantum, capacity);
    const std::uint64_t raw_reservation = reservation_remaining(flow, now);
    std::uint64_t reservation_slack = raw_reservation;
    if (flow.descriptor.reservation.bound()) {
      const std::uint64_t reserved_here =
          pending_reservation[flow.descriptor.reservation.reservation];
      reservation_slack =
          raw_reservation > reserved_here ? raw_reservation - reserved_here : 0;
    }
    quantum = std::min(quantum, reservation_slack);
    if (has_deadline(flow.descriptor.deadline_tick) && policy_.strict_deadlines) {
      const Ticks slack = flow.descriptor.deadline_tick > now ? flow.descriptor.deadline_tick - now
                                                              : 0;
      quantum = std::min<std::uint64_t>(quantum, slack);
    }
    if (quantum == 0) {
      DecisionReason reason = DecisionReason::NoLegalQuantum;
      if (remaining_work == 0) {
        reason = DecisionReason::NoLegalQuantum;
      } else if (capacity == 0) {
        reason = DecisionReason::ResourceCapacityExhausted;
      } else if (reservation_slack == 0) {
        reason = DecisionReason::ReservationExhausted;
      }
      defer(outcome, flow, reason, to_string(reason));
      continue;
    }

    ScheduleEntry entry;
    entry.ordinal = next_ordinal++;
    entry.kind = DecisionKind::Run;
    entry.flow = flow.descriptor.flow;
    entry.flow_generation = flow.descriptor.generation;
    entry.path = flow.descriptor.path.path;
    entry.path_generation = flow.descriptor.path.generation;
    entry.resource = flow.descriptor.resource.resource;
    entry.resource_generation = flow.descriptor.resource.generation;
    entry.reservation = flow.descriptor.reservation.reservation;
    entry.reservation_generation = flow.descriptor.reservation.generation;
    entry.qos = flow.descriptor.qos.qos;
    entry.qos_generation = flow.descriptor.qos.generation;
    entry.priority = flow.descriptor.priority.priority;
    entry.priority_generation = flow.descriptor.priority.generation;
    entry.quantum = quantum;
    entry.start_tick = now;
    entry.deadline_tick = flow.descriptor.deadline_tick;
    entry.reservation_end = flow.descriptor.reservation.bound()
                                ? reservation_window_end(flow.descriptor.reservation.reservation)
                                : kNoWindowBound;
    entry.tier = candidate.key.tier;
    entry.reason = DecisionReason::Selected;
    entry.ordering_digest = digest_of(candidate.key);
    entry.detail = bounded_detail(std::string("tier ") + std::to_string(candidate.key.tier) +
                                  " priority " + std::to_string(candidate.key.priority_rank));
    const ResourceId assigned_resource = entry.resource;
    const std::uint64_t assigned_quantum = entry.quantum;
    const ReservationId assigned_reservation = entry.reservation;
    entries.push_back(std::move(entry));
    pending_in_flight[assigned_resource] += 1;
    pending_capacity[assigned_resource] =
        saturating_add_u64(pending_capacity[assigned_resource], assigned_quantum);
    if (assigned_reservation.valid()) {
      pending_reservation[assigned_reservation] =
          saturating_add_u64(pending_reservation[assigned_reservation], assigned_quantum);
    }
  }

  if (!entries.empty()) {
    Schedule schedule;
    schedule.schedule = next_schedule_;
    schedule.generation = next_schedule_generation_;
    schedule.epoch = epoch_;
    schedule.policy = policy_.policy;
    schedule.policy_generation = policy_.generation;
    schedule.policy_binding = policy_binding_;
    schedule.at_tick = now;
    schedule.entries = std::move(entries);
    schedule.provenance.origin = ProvenanceOrigin::Internal;
    schedule.provenance.sequence = ++provenance_sequence_;
    schedule.digest = compute_schedule_digest(schedule);

    RecordWriter writer;
    write_schedule(writer, schedule);
    FS_RETURN_IF_ERROR(journal(JournalRecordType::ScheduleIssued, writer.take()));
    apply_schedule(schedule);
    outcome.schedule = schedule;

    FS_TRY_ASSIGN(const ScheduleId next_schedule, advance(next_schedule_));
    FS_TRY_ASSIGN(const Generation next_generation, advance(next_schedule_generation_));
    next_schedule_ = next_schedule;
    next_schedule_generation_ = next_generation;
  } else {
    outcome.schedule.at_tick = now;
    outcome.schedule.epoch = epoch_;
    outcome.schedule.policy = policy_.policy;
    outcome.schedule.policy_generation = policy_.generation;
    outcome.schedule.policy_binding = policy_binding_;
  }

  normalize_transient_states(now);
  maybe_retire_schedules();
  return outcome;
}

Ticks Scheduler::Impl::reservation_window_end(ReservationId reservation) const {
  const auto found = reservations_.find(reservation);
  return found == reservations_.end() ? kNoWindowBound : found->second.descriptor.window_end;
}

DispatchAttemptId Scheduler::Impl::worst_incumbent(ResourceId resource, Ticks now,
                                                   bool& found) const {
  found = false;
  OrderKey worst;
  DispatchAttemptId worst_attempt;
  for (const FlowId id : inflight_index_) {
    const auto flow = flows_.find(id);
    if (flow == flows_.end() || !flow->second.open_attempt.valid()) {
      continue;
    }
    if (flow->second.descriptor.resource.resource != resource) {
      continue;
    }
    const OrderKey key = compute_key(flow->second, now);
    if (!found || worst < key) {
      worst = key;
      worst_attempt = flow->second.open_attempt;
      found = true;
    }
  }
  return worst_attempt;
}

bool Scheduler::Impl::preemption_allowed(const OrderKey& candidate_key,
                                         const FlowRuntime& incumbent, Ticks now,
                                         DecisionReason& reason) const {
  reason = DecisionReason::PreemptionDisabled;
  if (policy_.preemption == PreemptionMode::None) {
    return false;
  }
  const auto incumbent_qos = qos_classes_.find(incumbent.descriptor.qos.qos);
  if (incumbent_qos != qos_classes_.end() &&
      incumbent_qos->second.generation == incumbent.descriptor.qos.generation &&
      incumbent_qos->second.preemption_protected) {
    reason = DecisionReason::IncumbentNotPreemptible;
    return false;
  }
  if (!incumbent.descriptor.preemptible) {
    reason = DecisionReason::IncumbentNotPreemptible;
    return false;
  }
  if (incumbent.served_work < policy_.min_preempt_service) {
    reason = DecisionReason::PreemptionThrottled;
    return false;
  }
  if (preemptions_in_window_ >= policy_.max_preemptions_per_interval) {
    reason = DecisionReason::PreemptionThrottled;
    return false;
  }
  const OrderKey incumbent_key = compute_key(incumbent, now);
  const bool priority_better = candidate_key.priority_rank < incumbent_key.priority_rank;
  const bool tier_better = candidate_key.tier < incumbent_key.tier;
  const bool deadline_urgent = candidate_key.tier == kTierDeadlineUrgent &&
                               incumbent_key.tier != kTierDeadlineUrgent;
  const bool incumbent_overdue = has_deadline(incumbent.descriptor.deadline_tick) &&
                                 now >= incumbent.descriptor.deadline_tick;
  switch (policy_.preemption) {
    case PreemptionMode::None:
      reason = DecisionReason::PreemptionDisabled;
      return false;
    case PreemptionMode::StrictPriority:
      if (priority_better || incumbent_overdue) {
        return true;
      }
      break;
    case PreemptionMode::Deadline:
      if (deadline_urgent || incumbent_overdue) {
        return true;
      }
      break;
    case PreemptionMode::Tiered:
      if (tier_better || priority_better || incumbent_overdue) {
        return true;
      }
      break;
  }
  reason = DecisionReason::PreemptionNotAuthorized;
  return false;
}

Status Scheduler::Impl::release_schedule(ScheduleId schedule, Generation generation, Ticks now) {
  std::lock_guard<std::mutex> guard(mutex_);
  last_tick_ = std::max(last_tick_, now);
  const ScheduleRuntime* runtime = find_schedule(schedule);
  if (runtime == nullptr) {
    return Status::failure(ErrorCode::UnknownSchedule, "schedule is not retained");
  }
  if (runtime->schedule.generation != generation) {
    return Status::failure(ErrorCode::StaleScheduleGeneration,
                           "schedule generation does not match");
  }
  RecordWriter writer;
  writer.u64(schedule.value());
  writer.u64(now);
  FS_RETURN_IF_ERROR(journal(JournalRecordType::ScheduleReleased, writer.take()));
  apply_release_schedule(schedule, now);
  normalize_transient_states(now);
  return Status::success();
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

Status Scheduler::Impl::validate_ticket_fields(
    FlowId flow, Generation flow_generation, ResourceId resource, Generation resource_generation,
    PathId path, Generation path_generation, ReservationId reservation,
    Generation reservation_generation, QoSClassId qos, Generation qos_generation,
    PriorityClassId priority, Generation priority_generation, PolicyId policy,
    Generation policy_generation, FabricEpoch epoch, Ticks deadline, Ticks reservation_end,
    Ticks now) const {
  if (!epoch.valid() || epoch != epoch_) {
    return Status::failure(ErrorCode::StaleEpoch,
                           "dispatch authority was issued under a retired epoch");
  }
  if (policy != policy_binding_.policy || policy_generation != policy_binding_.generation) {
    return Status::failure(ErrorCode::StalePolicyGeneration,
                           "dispatch authority was issued under a retired policy");
  }
  const auto flow_entry = flows_.find(flow);
  if (flow_entry == flows_.end()) {
    return Status::failure(ErrorCode::UnknownFlow, "dispatch names an unregistered flow");
  }
  const FlowRuntime& runtime = flow_entry->second;
  if (runtime.descriptor.generation != flow_generation) {
    return Status::failure(ErrorCode::StaleFlowGeneration,
                           "dispatch authority names a retired flow generation");
  }
  if (runtime.descriptor.path.path != path ||
      runtime.descriptor.path.generation != path_generation) {
    return Status::failure(ErrorCode::StalePathGeneration,
                           "dispatch authority names a retired path binding");
  }
  const auto resource_entry = resources_.find(resource);
  if (resource_entry == resources_.end()) {
    return Status::failure(ErrorCode::UnknownResource, "dispatch names an unregistered resource");
  }
  if (resource_entry->second.descriptor.generation != resource_generation) {
    return Status::failure(ErrorCode::StaleResourceGeneration,
                           "dispatch authority names a retired resource generation");
  }
  const auto priority_entry = priority_classes_.find(priority);
  if (priority_entry == priority_classes_.end()) {
    return Status::failure(ErrorCode::UnknownPolicy, "dispatch names an unregistered priority class");
  }
  if (priority_entry->second.generation != priority_generation) {
    return Status::failure(ErrorCode::StalePriorityGeneration,
                           "dispatch authority names a retired priority class generation");
  }
  const auto qos_entry = qos_classes_.find(qos);
  if (qos_entry == qos_classes_.end()) {
    return Status::failure(ErrorCode::UnknownPolicy, "dispatch names an unregistered qos class");
  }
  if (qos_entry->second.generation != qos_generation) {
    return Status::failure(ErrorCode::StaleQoSGeneration,
                           "dispatch authority names a retired qos class generation");
  }
  if (reservation.valid()) {
    const auto reservation_entry = reservations_.find(reservation);
    if (reservation_entry == reservations_.end() || reservation_entry->second.retired) {
      return Status::failure(ErrorCode::ReservationClosed,
                             "dispatch names a reservation that is no longer available");
    }
    if (reservation_entry->second.descriptor.generation != reservation_generation) {
      return Status::failure(ErrorCode::StaleReservationGeneration,
                             "dispatch authority names a retired reservation generation");
    }
    if (!window_contains(reservation_entry->second.descriptor, now)) {
      return Status::failure(ErrorCode::ReservationClosed,
                             "the reservation window is not open at the dispatch tick");
    }
  } else if (reservation_generation.valid()) {
    return Status::failure(ErrorCode::InvalidArgument,
                           "dispatch authority carries a reservation generation without a "
                           "reservation identity");
  }
  if (has_deadline(deadline) && now >= deadline) {
    return Status::failure(ErrorCode::DeadlinePassed,
                           "the dispatch tick is at or past the flow deadline");
  }
  if (reservation_end != kNoWindowBound && now >= reservation_end) {
    return Status::failure(ErrorCode::ReservationClosed,
                           "the dispatch tick is at or past the reservation window end");
  }
  return Status::success();
}

Result<DispatchTicket> Scheduler::Impl::begin_dispatch(ScheduleId schedule, Generation generation,
                                                       std::uint32_t ordinal, WorkerId worker,
                                                       BootId boot, Ticks now) {
  std::lock_guard<std::mutex> guard(mutex_);
  last_tick_ = std::max(last_tick_, now);
  if (!worker.valid() || !boot.valid()) {
    return Error(ErrorCode::InvalidArgument, "dispatch requires a worker identity and boot id");
  }
  ScheduleRuntime* runtime = find_schedule(schedule);
  if (runtime == nullptr) {
    return Error(ErrorCode::UnknownSchedule, "schedule is not retained");
  }
  if (runtime->schedule.generation != generation) {
    return Error(ErrorCode::StaleScheduleGeneration, "schedule generation does not match");
  }
  if (runtime->schedule.epoch != epoch_) {
    return Error(ErrorCode::StaleEpoch, "schedule was issued under a retired epoch");
  }
  if (ordinal >= runtime->schedule.entries.size()) {
    return Error(ErrorCode::OutOfRange, "schedule entry ordinal is out of range");
  }
  const ScheduleEntry& entry = runtime->schedule.entries[static_cast<std::size_t>(ordinal)];
  if (entry.kind != DecisionKind::Run) {
    return Error(ErrorCode::InvalidArgument, "only Run entries can be dispatched");
  }
  if (runtime->consumed[ordinal] != 0) {
    return Error(ErrorCode::InvalidState, "schedule entry has already been consumed");
  }
  const auto flow_entry = flows_.find(entry.flow);
  if (flow_entry == flows_.end()) {
    return Error(ErrorCode::UnknownFlow, "schedule entry names an unregistered flow");
  }
  FlowRuntime& flow = flow_entry->second;
  if (flow.lifecycle != FlowLifecycle::Scheduled || flow.scheduled_schedule != schedule) {
    return Error(ErrorCode::InvalidState,
                 "flow is no longer holding the scheduled service window");
  }
  if (flow.descriptor.generation != entry.flow_generation) {
    return Error(ErrorCode::StaleFlowGeneration,
                 "flow generation advanced after the schedule was issued");
  }
  const Status authority = validate_ticket_fields(
      entry.flow, entry.flow_generation, entry.resource, entry.resource_generation, entry.path,
      entry.path_generation, entry.reservation, entry.reservation_generation, entry.qos,
      entry.qos_generation, entry.priority, entry.priority_generation, policy_.policy,
      policy_.generation, epoch_, entry.deadline_tick, entry.reservation_end, now);
  if (!authority.ok()) {
    return authority.error();
  }

  DispatchTicket ticket;
  ticket.attempt = next_attempt_;
  ticket.epoch = epoch_;
  ticket.schedule = schedule;
  ticket.schedule_generation = generation;
  ticket.flow = entry.flow;
  ticket.flow_generation = entry.flow_generation;
  ticket.path = entry.path;
  ticket.path_generation = entry.path_generation;
  ticket.resource = entry.resource;
  ticket.resource_generation = entry.resource_generation;
  ticket.reservation = entry.reservation;
  ticket.reservation_generation = entry.reservation_generation;
  ticket.qos = entry.qos;
  ticket.qos_generation = entry.qos_generation;
  ticket.priority = entry.priority;
  ticket.priority_generation = entry.priority_generation;
  ticket.policy = policy_.policy;
  ticket.policy_generation = policy_.generation;
  ticket.worker = worker;
  ticket.boot = boot;
  ticket.start_tick = now;
  ticket.deadline_tick = entry.deadline_tick;
  ticket.reservation_end = entry.reservation_end;
  ticket.quantum = entry.quantum;
  ticket.state = AttemptState::Open;
  {
    Digest64 digest;
    digest.add_u64(ticket.attempt.value());
    digest.add_u64(ticket.epoch.value());
    digest.add_u64(ticket.schedule.value());
    digest.add_u64(ticket.schedule_generation.value());
    digest.add_u64(ticket.flow.value());
    digest.add_u64(ticket.flow_generation.value());
    digest.add_u64(ticket.path.value());
    digest.add_u64(ticket.path_generation.value());
    digest.add_u64(ticket.resource.value());
    digest.add_u64(ticket.resource_generation.value());
    digest.add_u64(ticket.reservation.value());
    digest.add_u64(ticket.reservation_generation.value());
    digest.add_u64(ticket.qos.value());
    digest.add_u64(ticket.qos_generation.value());
    digest.add_u64(ticket.priority.value());
    digest.add_u64(ticket.priority_generation.value());
    digest.add_u64(ticket.policy.value());
    digest.add_u64(ticket.policy_generation.value());
    digest.add_u64(ticket.worker.value());
    digest.add_u64(ticket.boot.value());
    digest.add_u64(ticket.start_tick);
    digest.add_u64(ticket.deadline_tick);
    digest.add_u64(ticket.reservation_end);
    digest.add_u64(ticket.quantum);
    ticket.digest = digest.value();
  }

  RecordWriter writer;
  write_ticket(writer, ticket);
  writer.u64(flow.descriptor.estimated_work > flow.served_work
                 ? flow.descriptor.estimated_work - flow.served_work
                 : 0);
  writer.u64(now);
  FS_RETURN_IF_ERROR(journal(JournalRecordType::AttemptOpened, writer.take()));
  apply_attempt_opened(ticket, ticket.quantum, now);
  runtime->consumed[ordinal] = 1;
  if (runtime->pending_entries > 0) {
    runtime->pending_entries -= 1;
  }
  FS_TRY_ASSIGN(const DispatchAttemptId next, advance(next_attempt_));
  next_attempt_ = next;
  return ticket;
}

Status Scheduler::Impl::revalidate(const DispatchTicket& ticket, Ticks now) const {
  std::lock_guard<std::mutex> guard(mutex_);
  return revalidate_unlocked(ticket, now);
}

Status Scheduler::Impl::revalidate_unlocked(const DispatchTicket& ticket, Ticks now) const {
  if (!ticket.digest) {
    return Status::failure(ErrorCode::InvalidArgument, "dispatch ticket has no integrity digest");
  }
  Digest64 digest;
  digest.add_u64(ticket.attempt.value());
  digest.add_u64(ticket.epoch.value());
  digest.add_u64(ticket.schedule.value());
  digest.add_u64(ticket.schedule_generation.value());
  digest.add_u64(ticket.flow.value());
  digest.add_u64(ticket.flow_generation.value());
  digest.add_u64(ticket.path.value());
  digest.add_u64(ticket.path_generation.value());
  digest.add_u64(ticket.resource.value());
  digest.add_u64(ticket.resource_generation.value());
  digest.add_u64(ticket.reservation.value());
  digest.add_u64(ticket.reservation_generation.value());
  digest.add_u64(ticket.qos.value());
  digest.add_u64(ticket.qos_generation.value());
  digest.add_u64(ticket.priority.value());
  digest.add_u64(ticket.priority_generation.value());
  digest.add_u64(ticket.policy.value());
  digest.add_u64(ticket.policy_generation.value());
  digest.add_u64(ticket.worker.value());
  digest.add_u64(ticket.boot.value());
  digest.add_u64(ticket.start_tick);
  digest.add_u64(ticket.deadline_tick);
  digest.add_u64(ticket.reservation_end);
  digest.add_u64(ticket.quantum);
  if (digest.value() != ticket.digest) {
    return Status::failure(ErrorCode::ContradictoryCompletion,
                           "dispatch ticket digest does not match its fields");
  }
  if (!is_attempt_terminal(ticket.state) && ticket.state != AttemptState::Open &&
      ticket.state != AttemptState::Started) {
    return Status::failure(ErrorCode::InvalidState, "dispatch ticket carries an unknown state");
  }
  return validate_ticket_fields(ticket.flow, ticket.flow_generation, ticket.resource,
                                ticket.resource_generation, ticket.path, ticket.path_generation,
                                ticket.reservation, ticket.reservation_generation, ticket.qos,
                                ticket.qos_generation, ticket.priority, ticket.priority_generation,
                                ticket.policy, ticket.policy_generation, ticket.epoch,
                                ticket.deadline_tick, ticket.reservation_end, now);
}

Status Scheduler::Impl::mark_started(const DispatchTicket& ticket, Ticks now) {
  std::lock_guard<std::mutex> guard(mutex_);
  last_tick_ = std::max(last_tick_, now);
  const auto found = attempts_.find(ticket.attempt);
  if (found == attempts_.end()) {
    return Status::failure(ErrorCode::UnknownAttempt, "attempt is not retained");
  }
  if (is_attempt_terminal(found->second.state)) {
    return Status::failure(ErrorCode::CompletionForClosedAttempt,
                           "attempt already reached a terminal state");
  }
  // The unlocked body is used because this call already holds mutex_. The
  // runtime holds exactly one lock and never re-enters it.
  FS_RETURN_IF_ERROR(revalidate_unlocked(ticket, now));
  if (found->second.state == AttemptState::Started) {
    // Repeated acknowledgement for the same live attempt is idempotent: no
    // second AttemptStarted record, no double counted start.
    return Status::success();
  }
  RecordWriter writer;
  writer.u64(ticket.attempt.value());
  writer.u64(now);
  FS_RETURN_IF_ERROR(journal(JournalRecordType::AttemptStarted, writer.take()));
  apply_attempt_started(ticket.attempt, now);
  return Status::success();
}

Status Scheduler::Impl::abandon_attempt(DispatchAttemptId attempt, std::string_view reason,
                                        Ticks now) {
  std::lock_guard<std::mutex> guard(mutex_);
  last_tick_ = std::max(last_tick_, now);
  const auto found = attempts_.find(attempt);
  if (found == attempts_.end()) {
    return Status::failure(ErrorCode::UnknownAttempt, "attempt is not retained");
  }
  if (is_attempt_terminal(found->second.state)) {
    return Status::success();  // Already closed; abandoning twice is a no-op.
  }
  FS_RETURN_IF_ERROR(close_attempt(attempt, AttemptState::Abandoned, reason, now));
  normalize_transient_states(now);
  return Status::success();
}

Status Scheduler::Impl::preempt_attempt(DispatchAttemptId attempt, std::string_view reason,
                                        Ticks now) {
  std::lock_guard<std::mutex> guard(mutex_);
  last_tick_ = std::max(last_tick_, now);
  const auto found = attempts_.find(attempt);
  if (found == attempts_.end()) {
    return Status::failure(ErrorCode::UnknownAttempt, "attempt is not retained");
  }
  if (is_attempt_terminal(found->second.state)) {
    return Status::failure(ErrorCode::CompletionForClosedAttempt,
                           "attempt already reached a terminal state");
  }
  FS_RETURN_IF_ERROR(close_attempt(attempt, AttemptState::Preempted, reason, now));
  ++preemptions_issued_;
  normalize_transient_states(now);
  return Status::success();
}

// ---------------------------------------------------------------------------
// Completion
// ---------------------------------------------------------------------------

Result<CommitOutcome> Scheduler::Impl::complete(const CompletionEvidence& evidence, Ticks now) {
  std::lock_guard<std::mutex> guard(mutex_);
  last_tick_ = std::max(last_tick_, now);
  ++completion_reports_;

  CommitOutcome outcome;
  const auto reject = [&outcome](ErrorCode code, std::string detail) {
    outcome.disposition = CommitDisposition::Rejected;
    outcome.code = code;
    outcome.detail = std::move(detail);
    return outcome;
  };

  if (!evidence.provenance.established()) {
    ++completions_rejected_;
    return reject(ErrorCode::InvalidArgument, "completion evidence has no established provenance");
  }
  if (!evidence.attempt.valid() || !evidence.schedule.valid() ||
      !evidence.schedule_generation.valid() || !evidence.flow.valid() ||
      !evidence.flow_generation.valid() || !evidence.worker.valid() || !evidence.boot.valid() ||
      !evidence.epoch.valid()) {
    ++completions_rejected_;
    return reject(ErrorCode::InvalidArgument, "completion evidence is missing an authority field");
  }
  if (evidence.served > kMaxServiceUnits) {
    ++completions_rejected_;
    return reject(ErrorCode::OutOfRange, "completion evidence served exceeds the bound");
  }
  if (evidence.epoch != epoch_) {
    ++completions_rejected_;
    return reject(ErrorCode::StaleEpoch,
                  "completion evidence was produced under a retired coordinator epoch");
  }
  const auto flow_entry = flows_.find(evidence.flow);
  if (flow_entry == flows_.end()) {
    ++completions_rejected_;
    return reject(ErrorCode::UnknownFlow, "completion evidence names an unregistered flow");
  }
  FlowRuntime& flow = flow_entry->second;
  if (flow.descriptor.generation != evidence.flow_generation) {
    ++completions_rejected_;
    flow.accounting.rejected_completions += 1;
    return reject(ErrorCode::StaleFlowGeneration,
                  "completion evidence names a retired flow generation");
  }
  const auto attempt_entry = attempts_.find(evidence.attempt);
  if (attempt_entry == attempts_.end()) {
    ++completions_rejected_;
    flow.accounting.rejected_completions += 1;
    return reject(ErrorCode::UnknownAttempt, "completion evidence names a retired attempt");
  }
  AttemptRuntime& attempt = attempt_entry->second;
  if (attempt.ticket.flow != evidence.flow || attempt.ticket.schedule != evidence.schedule) {
    ++completions_rejected_;
    flow.accounting.rejected_completions += 1;
    return reject(ErrorCode::ContradictoryCompletion,
                  "completion evidence contradicts the attempt it names");
  }
  if (attempt.ticket.schedule_generation != evidence.schedule_generation) {
    ++completions_rejected_;
    flow.accounting.rejected_completions += 1;
    return reject(ErrorCode::StaleScheduleGeneration,
                  "completion evidence names a retired schedule generation");
  }
  if (attempt.ticket.worker != evidence.worker || attempt.ticket.boot != evidence.boot) {
    ++completions_rejected_;
    flow.accounting.rejected_completions += 1;
    return reject(ErrorCode::Fenced,
                  "completion evidence comes from a worker incarnation that does not own the "
                  "attempt");
  }

  const std::uint64_t evidence_digest = evidence.digest();
  if (is_attempt_terminal(attempt.state)) {
    if (attempt.has_evidence && attempt.evidence_digest == evidence_digest) {
      if (attempt.state == AttemptState::Committed) {
        // Byte-identical evidence for an already committed attempt. This is
        // the exact-duplicate case: acknowledged, but nothing mutates.
        ++completions_duplicate_;
        flow.accounting.duplicate_completions += 1;
        outcome.disposition = CommitDisposition::IdempotentDuplicate;
        outcome.code = ErrorCode::DuplicateCompletion;
        outcome.credited = attempt.credited;
        outcome.lifecycle_after = flow.lifecycle;
        outcome.flow_completed = flow.lifecycle == FlowLifecycle::Completed;
        outcome.detail = "exact duplicate completion; no state changed";
        return outcome;
      }
      ++completions_rejected_;
      flow.accounting.rejected_completions += 1;
      return reject(ErrorCode::CompletionForClosedAttempt,
                    "attempt was closed as " + std::string(to_string(attempt.state)) +
                        "; completion cannot revive it");
    }
    if (attempt.has_evidence) {
      ++completions_rejected_;
      flow.accounting.rejected_completions += 1;
      return reject(ErrorCode::ContradictoryCompletion,
                    "a different completion was already committed for this attempt");
    }
    ++completions_rejected_;
    flow.accounting.rejected_completions += 1;
    return reject(ErrorCode::CompletionForClosedAttempt,
                  "attempt was closed as " + std::string(to_string(attempt.state)) +
                      "; completion cannot revive it");
  }
  if (flow.open_attempt != evidence.attempt) {
    ++completions_rejected_;
    flow.accounting.rejected_completions += 1;
    return reject(ErrorCode::StaleAttempt,
                  "the flow no longer holds the attempt named by the evidence");
  }
  if (evidence.served > attempt.ticket.quantum) {
    ++completions_rejected_;
    flow.accounting.rejected_completions += 1;
    return reject(ErrorCode::ContradictoryCompletion,
                  "reported service exceeds the authorized quantum");
  }
  if (has_deadline(attempt.ticket.deadline_tick) && now >= attempt.ticket.deadline_tick &&
      evidence.outcome == CompletionOutcome::Served) {
    // Service observed wholly outside the authorized window cannot be credited.
    ++completions_rejected_;
    flow.accounting.rejected_completions += 1;
    return reject(ErrorCode::DeadlinePassed,
                  "completion was observed after the authorized deadline");
  }

  std::uint64_t credited = 0;
  if (evidence.outcome == CompletionOutcome::Served) {
    credited = evidence.served;
  }
  const std::uint64_t projected = saturating_add_u64(flow.served_work, credited);
  FlowLifecycle lifecycle_after = flow.lifecycle;
  if (evidence.outcome == CompletionOutcome::Failed) {
    lifecycle_after = FlowLifecycle::Failed;
  } else if (projected >= flow.descriptor.estimated_work) {
    lifecycle_after = FlowLifecycle::Completed;
  } else {
    lifecycle_after = FlowLifecycle::Waiting;
  }

  // Durable before acknowledged: the commit record is on stable storage
  // before any in-memory authoritative state changes and before the caller is
  // told the completion was applied.
  {
    RecordWriter writer;
    write_evidence(writer, evidence);
    writer.u64(credited);
    writer.u8(static_cast<std::uint8_t>(lifecycle_after));
    writer.u64(now);
    FS_RETURN_IF_ERROR(journal(JournalRecordType::CompletionCommitted, writer.take()));
  }
  apply_completion(evidence, credited, lifecycle_after, now);
  if (lifecycle_after == FlowLifecycle::Waiting) {
    // Remaining work. Readiness is re-derived immediately rather than left for
    // the next round: a flow that just finished a service window is normally
    // eligible again at once, and re-deriving here keeps the waiting set
    // containing only flows that are genuinely blocked.
    FlowRuntime& remaining = flows_[evidence.flow];
    bool ready_again = false;
    const DecisionReason next_reason = derived_readiness(remaining, now, ready_again);
    if (ready_again) {
      set_lifecycle(remaining, FlowLifecycle::Ready, now);
      record_step(remaining, now, FlowLifecycle::Ready, DecisionReason::Selected, ScheduleId{},
                  DispatchAttemptId{}, "eligible again after " + std::to_string(credited) +
                                           " units of service");
    } else {
      set_lifecycle(remaining, FlowLifecycle::Waiting, now);
      (void)next_reason;
    }
  }
  normalize_transient_states(now);

  outcome.disposition = CommitDisposition::Applied;
  outcome.code = ErrorCode::Ok;
  outcome.credited = credited;
  outcome.lifecycle_after = flows_[evidence.flow].lifecycle;
  outcome.flow_completed = outcome.lifecycle_after == FlowLifecycle::Completed;
  outcome.detail = "completion committed";
  return outcome;
}

// ---------------------------------------------------------------------------
// Observation
// ---------------------------------------------------------------------------

Result<FlowSnapshot> Scheduler::Impl::flow(FlowId id) const {
  std::lock_guard<std::mutex> guard(mutex_);
  const auto found = flows_.find(id);
  if (found == flows_.end()) {
    return Error(ErrorCode::UnknownFlow, "flow is not registered");
  }
  const FlowRuntime& runtime = found->second;
  FlowSnapshot snapshot;
  snapshot.flow = runtime.descriptor.flow;
  snapshot.generation = runtime.descriptor.generation;
  snapshot.lifecycle = runtime.lifecycle;
  snapshot.estimated_work = runtime.descriptor.estimated_work;
  snapshot.served_work = runtime.served_work;
  snapshot.outstanding_reserved = runtime.outstanding_reserved;
  snapshot.release_tick = runtime.descriptor.release_tick;
  snapshot.deadline_tick = runtime.descriptor.deadline_tick;
  snapshot.last_change_tick = runtime.last_change_tick;
  snapshot.eligible_since_tick = runtime.eligible_since_tick;
  snapshot.open_attempt = runtime.open_attempt;
  snapshot.accounting = runtime.accounting;
  snapshot.provenance = runtime.descriptor.provenance;
  return snapshot;
}

Result<FlowExplanation> Scheduler::Impl::explain_flow(FlowId id) const {
  std::lock_guard<std::mutex> guard(mutex_);
  const auto found = flows_.find(id);
  if (found == flows_.end()) {
    return Error(ErrorCode::UnknownFlow, "flow is not registered");
  }
  const FlowRuntime& runtime = found->second;
  FlowExplanation explanation;
  explanation.flow = runtime.descriptor.flow;
  explanation.generation = runtime.descriptor.generation;
  explanation.lifecycle = runtime.lifecycle;
  explanation.last_reason = runtime.history.empty() ? DecisionReason::Selected
                                                    : runtime.history.back().reason;
  explanation.tier = tier_of(runtime, last_tick_);
  explanation.ordering_digest = digest_of(compute_key(runtime, last_tick_));
  explanation.accounting = runtime.accounting;
  explanation.history = runtime.history;
  explanation.history_truncated = runtime.history_truncated;
  return explanation;
}

Result<Schedule> Scheduler::Impl::schedule(ScheduleId id) const {
  std::lock_guard<std::mutex> guard(mutex_);
  const ScheduleRuntime* runtime = find_schedule(id);
  if (runtime == nullptr) {
    return Error(ErrorCode::UnknownSchedule, "schedule is not retained");
  }
  return runtime->schedule;
}

Result<ScheduleExplanation> Scheduler::Impl::explain_schedule(ScheduleId id) const {
  std::lock_guard<std::mutex> guard(mutex_);
  const ScheduleRuntime* runtime = find_schedule(id);
  if (runtime == nullptr) {
    return Error(ErrorCode::UnknownSchedule, "schedule is not retained");
  }
  ScheduleExplanation explanation;
  explanation.schedule = runtime->schedule.schedule;
  explanation.generation = runtime->schedule.generation;
  explanation.epoch = runtime->schedule.epoch;
  explanation.at_tick = runtime->schedule.at_tick;
  explanation.digest = runtime->schedule.digest;
  for (std::size_t index = 0; index < runtime->schedule.entries.size(); ++index) {
    const ScheduleEntry& entry = runtime->schedule.entries[index];
    std::string line = to_string(entry.kind);
    line += " ";
    line += to_string(entry.flow);
    line += " quantum=";
    line += std::to_string(entry.quantum);
    line += " tier=";
    line += std::to_string(entry.tier);
    line += " consumed=";
    line += std::to_string(runtime->consumed[index]);
    line += " (";
    line += to_string(entry.reason);
    line += ")";
    explanation.entries.push_back(std::move(line));
  }
  return explanation;
}

Result<DispatchTicket> Scheduler::Impl::attempt(DispatchAttemptId id) const {
  std::lock_guard<std::mutex> guard(mutex_);
  const auto found = attempts_.find(id);
  if (found == attempts_.end()) {
    return Error(ErrorCode::UnknownAttempt, "attempt is not retained");
  }
  DispatchTicket ticket = found->second.ticket;
  ticket.state = found->second.state;
  return ticket;
}

AccountingReport Scheduler::Impl::accounting() const {
  std::lock_guard<std::mutex> guard(mutex_);
  AccountingReport report;
  for (const auto& entry : flows_) {
    const FlowRuntime& runtime = entry.second;
    ++report.total_flows;
    switch (runtime.lifecycle) {
      case FlowLifecycle::Waiting: ++report.waiting; break;
      case FlowLifecycle::Ready: ++report.ready; break;
      case FlowLifecycle::Scheduled: ++report.scheduled; break;
      case FlowLifecycle::Dispatched: ++report.dispatched; break;
      case FlowLifecycle::Running: ++report.running; break;
      case FlowLifecycle::CompletionReported: ++report.completion_reported; break;
      case FlowLifecycle::Cancelling: ++report.cancelling; break;
      case FlowLifecycle::Preempting: ++report.preempting; break;
      case FlowLifecycle::Completed: ++report.completed; break;
      case FlowLifecycle::Cancelled: ++report.cancelled; break;
      case FlowLifecycle::Failed: ++report.failed; break;
      case FlowLifecycle::Ambiguous: ++report.ambiguous; break;
    }
    report.service_credit_total = saturating_add_u64(report.service_credit_total,
                                                     runtime.accounting.service_credit);
    report.estimated_work_total =
        saturating_add_u64(report.estimated_work_total, runtime.descriptor.estimated_work);
    report.starvation_boosts =
        saturating_add_u64(report.starvation_boosts, runtime.accounting.starvation_boosts);
    // Authorized but unresolved service: a flow holding a scheduled window has
    // reserved capacity even before any dispatch attempt exists for it.
    report.outstanding_reserved_units =
        saturating_add_u64(report.outstanding_reserved_units, runtime.outstanding_reserved);
  }
  const AccountSnapshot attempts = account_unlocked();
  report.attempts_total = attempts.attempts_total;
  report.attempts_open = attempts.attempts_open;
  report.attempts_started = attempts.attempts_started;
  report.attempts_committed = attempts.attempts_committed;
  report.attempts_preempted = attempts.attempts_preempted;
  report.attempts_cancelled = attempts.attempts_cancelled;
  report.attempts_abandoned = attempts.attempts_abandoned;
  report.attempts_rejected = attempts.attempts_rejected;
  report.completion_reports_received = completion_reports_;
  report.completions_applied = completions_applied_;
  report.completions_duplicate = completions_duplicate_;
  report.completions_rejected = completions_rejected_;
  report.schedules_issued = schedules_issued_;
  report.schedule_entries_issued = schedule_entries_issued_;
  report.preemptions_issued = preemptions_issued_;
  report.arbitration_rounds = arbitration_rounds_;
  report.deadline_misses = deadline_misses_;
  report.reservations_retired = reservations_retired_;
  report.epoch = epoch_.value();
  report.recovered_flows = recovery_.flows_restored;
  report.recovered_ambiguous_attempts = recovery_.attempts_abandoned;
  return report;
}

std::size_t Scheduler::Impl::flow_count() const {
  std::lock_guard<std::mutex> guard(mutex_);
  return flows_.size();
}

std::size_t Scheduler::Impl::attempt_count() const {
  std::lock_guard<std::mutex> guard(mutex_);
  return attempts_.size();
}

std::uint64_t Scheduler::Impl::journal_position() const {
  std::lock_guard<std::mutex> guard(mutex_);
  return store_ ? store_->last_sequence() : 0;
}

Result<SchedulingPolicy> Scheduler::Impl::policy_copy() const {
  std::lock_guard<std::mutex> guard(mutex_);
  return policy_;
}

Result<FabricEpoch> Scheduler::Impl::epoch() const {
  std::lock_guard<std::mutex> guard(mutex_);
  return epoch_;
}

Result<PolicyBinding> Scheduler::Impl::policy_binding() const {
  std::lock_guard<std::mutex> guard(mutex_);
  return policy_binding_;
}

Ticks Scheduler::Impl::current_tick() const {
  std::lock_guard<std::mutex> guard(mutex_);
  return last_tick_;
}

// ---------------------------------------------------------------------------
// Scheduler facade
// ---------------------------------------------------------------------------

Scheduler::Scheduler(std::unique_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {}

Scheduler::~Scheduler() = default;

Result<std::unique_ptr<Scheduler>> Scheduler::create(const SchedulerOptions& options) {
  FS_RETURN_IF_ERROR(validate(options.policy));
  auto impl = std::make_unique<Impl>(options);
  FS_RETURN_IF_ERROR(impl->initialize());
  return std::unique_ptr<Scheduler>(new Scheduler(std::move(impl)));
}

Status Scheduler::register_resource(const ResourceDescriptor& descriptor) {
  return impl_->register_resource(descriptor);
}

Status Scheduler::register_reservation(const ReservationDescriptor& descriptor) {
  return impl_->register_reservation(descriptor);
}

Status Scheduler::retire_reservation(ReservationId reservation, Generation generation) {
  return impl_->retire_reservation(reservation, generation);
}

Status Scheduler::register_priority_class(const PriorityClassDescriptor& descriptor) {
  return impl_->register_priority_class(descriptor);
}

Status Scheduler::register_qos_class(const QoSClassDescriptor& descriptor) {
  return impl_->register_qos_class(descriptor);
}

Status Scheduler::admit_flow(const FlowDescriptor& descriptor) {
  return impl_->admit_flow(descriptor);
}

Status Scheduler::update_flow(const FlowDescriptor& descriptor) {
  return impl_->update_flow(descriptor);
}

Status Scheduler::cancel_flow(FlowId flow, Generation generation, std::string_view reason) {
  return impl_->cancel_flow(flow, generation, reason, impl_->current_tick());
}

Status Scheduler::notify_readiness(FlowId flow, Generation generation, ReadinessSignal signal,
                                   Ticks now) {
  return impl_->notify_readiness(flow, generation, signal, now);
}

Status Scheduler::resolve_ambiguous(FlowId flow, Generation generation,
                                    AmbiguityResolution resolution, Ticks now) {
  return impl_->resolve_ambiguous(flow, generation, resolution, now);
}

Status Scheduler::retire_flow(FlowId flow, Generation generation) {
  return impl_->retire_flow(flow, generation);
}

Status Scheduler::install_policy(const SchedulingPolicy& policy) {
  return impl_->install_policy(policy);
}

Result<FabricEpoch> Scheduler::advance_epoch(EpochReason reason) {
  return impl_->advance_epoch(reason, impl_->current_tick());
}

Result<FabricEpoch> Scheduler::epoch() const { return impl_->epoch(); }

Result<PolicyBinding> Scheduler::policy_binding() const { return impl_->policy_binding(); }

Result<ArbitrationOutcome> Scheduler::arbitrate(Ticks now) { return impl_->arbitrate(now); }

Status Scheduler::release_schedule(ScheduleId schedule, Generation generation) {
  return impl_->release_schedule(schedule, generation, impl_->current_tick());
}

Result<DispatchTicket> Scheduler::begin_dispatch(ScheduleId schedule, Generation generation,
                                                 std::uint32_t ordinal, WorkerId worker, BootId boot,
                                                 Ticks now) {
  return impl_->begin_dispatch(schedule, generation, ordinal, worker, boot, now);
}

Status Scheduler::revalidate(const DispatchTicket& ticket, Ticks now) const {
  return impl_->revalidate(ticket, now);
}

Status Scheduler::mark_started(const DispatchTicket& ticket, Ticks now) {
  return impl_->mark_started(ticket, now);
}

Status Scheduler::abandon_attempt(DispatchAttemptId attempt, std::string_view reason, Ticks now) {
  return impl_->abandon_attempt(attempt, reason, now);
}

Status Scheduler::preempt_attempt(DispatchAttemptId attempt, std::string_view reason, Ticks now) {
  return impl_->preempt_attempt(attempt, reason, now);
}

Result<CommitOutcome> Scheduler::complete(const CompletionEvidence& evidence, Ticks now) {
  return impl_->complete(evidence, now);
}

Result<FlowSnapshot> Scheduler::flow(FlowId id) const { return impl_->flow(id); }

Result<FlowExplanation> Scheduler::explain_flow(FlowId id) const { return impl_->explain_flow(id); }

Result<Schedule> Scheduler::schedule(ScheduleId id) const { return impl_->schedule(id); }

Result<ScheduleExplanation> Scheduler::explain_schedule(ScheduleId id) const {
  return impl_->explain_schedule(id);
}

Result<DispatchTicket> Scheduler::attempt(DispatchAttemptId id) const {
  return impl_->attempt(id);
}

AccountingReport Scheduler::accounting() const { return impl_->accounting(); }

const RecoveryReport& Scheduler::recovery_report() const { return impl_->recovery_report(); }

Result<SchedulingPolicy> Scheduler::policy() const { return impl_->policy_copy(); }

std::size_t Scheduler::flow_count() const { return impl_->flow_count(); }

std::size_t Scheduler::attempt_count() const { return impl_->attempt_count(); }

std::uint64_t Scheduler::journal_position() const { return impl_->journal_position(); }

Status Scheduler::checkpoint() { return impl_->checkpoint(); }

}  // namespace flow_scheduler

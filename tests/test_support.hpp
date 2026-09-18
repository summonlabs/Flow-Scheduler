// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.
//
// Shared helpers for the test suite. Everything here is deterministic: no
// wall-clock time, no random seeds other than explicit ones, and no reliance on
// container iteration order.

#ifndef FLOW_SCHEDULER_TEST_SUPPORT_HPP
#define FLOW_SCHEDULER_TEST_SUPPORT_HPP

#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "flow_scheduler/bench.hpp"
#include "flow_scheduler/framing.hpp"
#include "flow_scheduler/hash.hpp"
#include "flow_scheduler/journal.hpp"
#include "flow_scheduler/protocol.hpp"
#include "flow_scheduler/scheduler.hpp"
#include "flow_scheduler/transport.hpp"
#include "test_framework.hpp"

namespace fls_test {

using namespace flow_scheduler;

/// Printable renderings for the library's identity and enumeration types, so
/// that a failed equality check shows something meaningful.
template <class Tag, class Rep>
std::string show(const flow_scheduler::Id<Tag, Rep>& id) {
  return flow_scheduler::to_string(id);
}

inline std::string show(flow_scheduler::FlowLifecycle value) {
  return flow_scheduler::to_string(value);
}
inline std::string show(flow_scheduler::DecisionKind value) {
  return flow_scheduler::to_string(value);
}
inline std::string show(flow_scheduler::DecisionReason value) {
  return flow_scheduler::to_string(value);
}
inline std::string show(flow_scheduler::AttemptState value) {
  return flow_scheduler::to_string(value);
}
inline std::string show(flow_scheduler::CommitDisposition value) {
  return flow_scheduler::to_string(value);
}
inline std::string show(flow_scheduler::CompletionOutcome value) {
  return flow_scheduler::to_string(value);
}
inline std::string show(flow_scheduler::ErrorCode value) {
  return flow_scheduler::to_string(value);
}
inline std::string show(flow_scheduler::ProvenanceOrigin value) {
  return flow_scheduler::to_string(value);
}
/// Adapts any Status or Result<T> expression for CHECK_ERROR / CHECK_OK.
inline Status as_status(const Status& status) { return status; }

template <class T>
Status as_status(const Result<T>& result) {
  return result.ok() ? Status::success() : Status(result.error());
}

inline Provenance external_provenance(std::uint64_t sequence) {
  Provenance provenance;
  provenance.origin = ProvenanceOrigin::External;
  provenance.sequence = sequence;
  provenance.digest = mix64(sequence);
  return provenance;
}

inline Provenance internal_provenance(std::uint64_t sequence) {
  Provenance provenance;
  provenance.origin = ProvenanceOrigin::Internal;
  provenance.sequence = sequence;
  provenance.digest = mix64(sequence);
  return provenance;
}

/// Options for the standard single-resource scenario. Capacity is deliberately
/// generous so that arbitration ordering, not capacity, decides the outcome
/// unless a test changes it.
inline SchedulerOptions standard_options(bool durable = false,
                                         const std::string& directory = std::string()) {
  SchedulerOptions options;
  options.policy.policy = PolicyId(1);
  options.policy.generation = Generation(1);
  options.policy.preemption = PreemptionMode::Tiered;
  options.policy.deadline_ordering = true;
  options.policy.weighted_fairness = true;
  options.policy.min_service_guarantee = true;
  options.policy.strict_deadlines = true;
  options.policy.starvation_bound_ticks = 1000;
  options.policy.deadline_urgency_ticks = 100;
  options.policy.min_preempt_service = 0;
  options.policy.max_preemptions_per_interval = 64;
  options.policy.preemption_interval_ticks = 1000;
  options.policy.dispatch_delivery_horizon = 100000;
  options.policy.default_max_overlap = 1;
  options.policy.retained_schedule_history = 4096;
  options.policy.provenance = external_provenance(1);
  options.policy.provenance.digest = digest_of(options.policy);
  if (durable) {
    options.state_directory = directory;
  }
  return options;
}

inline ResourceDescriptor make_resource(ResourceId id = ResourceId(1),
                                        Generation generation = Generation(1),
                                        std::uint64_t capacity = 1'000'000,
                                        Ticks interval = 1000,
                                        std::uint64_t max_overlap = 1) {
  ResourceDescriptor descriptor;
  descriptor.resource = id;
  descriptor.generation = generation;
  descriptor.capacity = capacity;
  descriptor.interval = interval;
  descriptor.max_overlap = max_overlap;
  descriptor.provenance = external_provenance(id.value());
  return descriptor;
}

inline PriorityClassDescriptor make_priority(PriorityClassId id, std::uint32_t rank,
                                             std::uint64_t weight = 1,
                                             Generation generation = Generation(1)) {
  PriorityClassDescriptor descriptor;
  descriptor.priority = id;
  descriptor.generation = generation;
  descriptor.rank = rank;
  descriptor.weight = weight;
  descriptor.provenance = external_provenance(id.value());
  return descriptor;
}

inline QoSClassDescriptor make_qos(QoSClassId id = QoSClassId(1), bool protected_class = false,
                                   std::uint64_t max_service = 0,
                                   Generation generation = Generation(1)) {
  QoSClassDescriptor descriptor;
  descriptor.qos = id;
  descriptor.generation = generation;
  descriptor.preemption_protected = protected_class;
  descriptor.max_service_per_dispatch = max_service;
  descriptor.provenance = external_provenance(id.value());
  return descriptor;
}

struct FlowSpec {
  FlowId flow{FlowId(1)};
  Generation generation{Generation(1)};
  PriorityClassId priority{PriorityClassId(1)};
  Generation priority_generation{Generation(1)};
  QoSClassId qos{QoSClassId(1)};
  Generation qos_generation{Generation(1)};
  FairnessGroupId fairness_group{FairnessGroupId(1)};
  ResourceId resource{ResourceId(1)};
  Generation resource_generation{Generation(1)};
  std::uint64_t estimated_work{4};
  std::uint64_t service_quantum{1};
  std::uint64_t min_service{0};
  Ticks min_service_window{0};
  Ticks release_tick{0};
  Ticks deadline_tick{kNoDeadline};
  std::uint64_t weight{1};
  bool preemptible{true};
  bool auto_ready{true};
  std::vector<FlowId> dependencies{};
};

inline FlowDescriptor make_flow(const FlowSpec& spec = FlowSpec{}) {
  FlowDescriptor descriptor;
  descriptor.flow = spec.flow;
  descriptor.generation = spec.generation;
  descriptor.path.path = PathId(spec.flow.value());
  descriptor.path.generation = Generation(1);
  descriptor.resource = {spec.resource, spec.resource_generation};
  descriptor.priority = {spec.priority, spec.priority_generation};
  descriptor.qos = {spec.qos, spec.qos_generation};
  descriptor.fairness_group = spec.fairness_group;
  descriptor.estimated_work = spec.estimated_work;
  descriptor.service_quantum = spec.service_quantum;
  descriptor.min_service = spec.min_service;
  descriptor.min_service_window = spec.min_service_window;
  descriptor.release_tick = spec.release_tick;
  descriptor.deadline_tick = spec.deadline_tick;
  descriptor.weight = spec.weight;
  descriptor.preemptible = spec.preemptible;
  descriptor.auto_ready = spec.auto_ready;
  descriptor.dependencies = spec.dependencies;
  descriptor.trace = TraceId(spec.flow.value());
  descriptor.provenance = external_provenance(spec.flow.value());
  return descriptor;
}

/// Builds the standard scenario: one resource, four priority classes, one qos
/// class. Returns the scheduler or the first failure encountered.
inline Result<std::unique_ptr<Scheduler>> make_standard_scheduler(
    const SchedulerOptions& options = standard_options(),
    std::uint64_t capacity = 1'000'000, std::uint64_t max_overlap = 1) {
  std::unique_ptr<Scheduler> scheduler;
  FS_TRY_ASSIGN(scheduler, Scheduler::create(options));
  FS_RETURN_IF_ERROR(scheduler->register_resource(make_resource(ResourceId(1), Generation(1),
                                                                capacity, 1000, max_overlap)));
  for (std::uint32_t rank = 0; rank < 4; ++rank) {
    FS_RETURN_IF_ERROR(scheduler->register_priority_class(
        make_priority(PriorityClassId(rank + 1), rank)));
  }
  FS_RETURN_IF_ERROR(scheduler->register_qos_class(make_qos()));
  return scheduler;
}

inline Status admit(Scheduler& scheduler, const FlowSpec& spec) {
  return scheduler.admit_flow(make_flow(spec));
}

/// Worker incarnations used by the tests. Boot ids distinguish process
/// incarnations; a new boot id fences the previous one.
inline WorkerId test_worker(std::uint64_t index = 1) { return WorkerId(index); }
inline BootId test_boot(std::uint64_t index = 1) { return BootId(index); }

struct DispatchCycle {
  bool dispatched{false};
  DispatchTicket ticket{};
  CommitOutcome outcome{};
};

/// Drives one Run entry through dispatch, start and completion. Returns the
/// commit outcome so tests can assert on dispositions.
inline Result<DispatchCycle> run_entry(Scheduler& scheduler, const Schedule& schedule,
                                       const ScheduleEntry& entry, Ticks now,
                                       WorkerId worker = test_worker(),
                                       BootId boot = test_boot(),
                                       std::int64_t served_override = -1,
                                       CompletionOutcome outcome = CompletionOutcome::Served) {
  DispatchCycle cycle;
  FS_TRY_ASSIGN(cycle.ticket, scheduler.begin_dispatch(schedule.schedule, schedule.generation,
                                                       entry.ordinal, worker, boot, now));
  cycle.dispatched = true;
  FS_RETURN_IF_ERROR(scheduler.mark_started(cycle.ticket, now));
  CompletionEvidence evidence;
  evidence.schedule = cycle.ticket.schedule;
  evidence.schedule_generation = cycle.ticket.schedule_generation;
  evidence.attempt = cycle.ticket.attempt;
  evidence.epoch = cycle.ticket.epoch;
  evidence.flow = cycle.ticket.flow;
  evidence.flow_generation = cycle.ticket.flow_generation;
  evidence.worker = cycle.ticket.worker;
  evidence.boot = cycle.ticket.boot;
  evidence.outcome = outcome;
  evidence.served = served_override < 0 ? cycle.ticket.quantum
                                        : static_cast<std::uint64_t>(served_override);
  evidence.effect_code = 1;
  evidence.effect_digest = mix64(cycle.ticket.attempt.value());
  evidence.observed_tick = now;
  evidence.provenance = external_provenance(cycle.ticket.attempt.value());
  FS_TRY_ASSIGN(cycle.outcome, scheduler.complete(evidence, now));
  return cycle;
}

inline std::string unique_temp_directory(const std::string& tag) {
  const auto base = std::filesystem::temp_directory_path();
  const std::uint64_t nonce = mix64(static_cast<std::uint64_t>(
      std::chrono::steady_clock::now().time_since_epoch().count()));
  std::filesystem::path path = base / ("flow_scheduler_" + tag + "_" + std::to_string(nonce));
  std::error_code error;
  std::filesystem::remove_all(path, error);
  return path.string();
}

}  // namespace fls_test

#endif  // FLOW_SCHEDULER_TEST_SUPPORT_HPP

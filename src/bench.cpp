// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#include "flow_scheduler/bench.hpp"

#include <algorithm>
#include <chrono>
#include <vector>

#include "flow_scheduler/hash.hpp"

namespace flow_scheduler {
namespace {

Status validate_parameters(const BenchmarkParameters& parameters) {
  if (parameters.flow_count == 0 || parameters.flow_count > kMaxFlows) {
    return Status::failure(ErrorCode::OutOfRange, "flow_count outside [1, kMaxFlows]");
  }
  if (parameters.priority_classes == 0 || parameters.priority_classes > kMaxPriorityClasses) {
    return Status::failure(ErrorCode::OutOfRange, "priority_classes outside [1, kMaxPriorityClasses]");
  }
  if (parameters.fairness_groups == 0 ||
      parameters.fairness_groups > static_cast<std::uint64_t>(kMaxFlows)) {
    return Status::failure(ErrorCode::OutOfRange, "fairness_groups is out of range");
  }
  if (!(parameters.deadline_density >= 0.0 && parameters.deadline_density <= 1.0)) {
    return Status::failure(ErrorCode::InvalidArgument, "deadline_density outside [0, 1]");
  }
  if (!(parameters.dependency_density >= 0.0 && parameters.dependency_density <= 1.0)) {
    return Status::failure(ErrorCode::InvalidArgument, "dependency_density outside [0, 1]");
  }
  if (parameters.concurrency == 0 || parameters.concurrency > kMaxServiceUnits) {
    return Status::failure(ErrorCode::OutOfRange, "concurrency is out of range");
  }
  if (parameters.capacity_per_interval == 0 ||
      parameters.capacity_per_interval > kMaxServiceUnits) {
    return Status::failure(ErrorCode::OutOfRange, "capacity_per_interval is out of range");
  }
  if (parameters.interval_ticks == 0 || parameters.interval_ticks > kMaxTickHorizon) {
    return Status::failure(ErrorCode::OutOfRange, "interval_ticks is out of range");
  }
  if (parameters.service_quantum == 0 || parameters.service_quantum > kMaxQuantum) {
    return Status::failure(ErrorCode::OutOfRange, "service_quantum is out of range");
  }
  if (parameters.estimated_work == 0 || parameters.estimated_work > kMaxServiceUnits) {
    return Status::failure(ErrorCode::OutOfRange, "estimated_work is out of range");
  }
  if (parameters.round_advance_ticks == 0 || parameters.round_advance_ticks > kMaxTickHorizon) {
    return Status::failure(ErrorCode::OutOfRange, "round_advance_ticks is out of range");
  }
  if (parameters.max_rounds == 0) {
    return Status::failure(ErrorCode::OutOfRange, "max_rounds must be positive");
  }
  return Status::success();
}

std::uint64_t saturating_mul(std::uint64_t a, std::uint64_t b) {
  if (a == 0 || b == 0) {
    return 0;
  }
  const std::uint64_t max = std::numeric_limits<std::uint64_t>::max();
  return a > max / b ? max : a * b;
}

std::vector<std::uint64_t> percentile_input(const std::vector<std::uint64_t>& values) {
  std::vector<std::uint64_t> sorted(values);
  std::sort(sorted.begin(), sorted.end());
  return sorted;
}

std::uint64_t percentile(const std::vector<std::uint64_t>& sorted, double fraction) {
  if (sorted.empty()) {
    return 0;
  }
  if (fraction <= 0.0) {
    return sorted.front();
  }
  if (fraction >= 1.0) {
    return sorted.back();
  }
  const auto index = static_cast<std::size_t>(
      fraction * static_cast<double>(sorted.size() - 1) + 0.5);
  return sorted[std::min(index, sorted.size() - 1)];
}

}  // namespace

Result<std::vector<SyntheticFlow>> generate_synthetic_population(
    const BenchmarkParameters& parameters) {
  FS_RETURN_IF_ERROR(validate_parameters(parameters));
  Rng rng(mix64(parameters.seed));
  std::vector<SyntheticFlow> population;
  population.reserve(static_cast<std::size_t>(parameters.flow_count));
  const std::uint64_t wave = parameters.concurrency == 0 ? 1 : parameters.concurrency;
  // Number of intervals needed to serve the whole population once, derived from
  // the supplied capacity. Deadlines are drawn relative to it so that a
  // deadline-carrying flow has a genuine but achievable window instead of a
  // window no scheduler could meet. Without this the deadline dimensions would
  // measure nothing but the population size.
  const std::uint64_t dispatches_per_interval =
      (parameters.capacity_per_interval / parameters.service_quantum) == 0
          ? 1
          : parameters.capacity_per_interval / parameters.service_quantum;
  const std::uint64_t service_intervals =
      ((parameters.flow_count * 8) / dispatches_per_interval) == 0
          ? 1
          : (parameters.flow_count * 8) / dispatches_per_interval;
  for (std::uint64_t index = 0; index < parameters.flow_count; ++index) {
    SyntheticFlow flow;
    flow.flow = FlowId(index + 1);
    flow.generation = Generation(1);
    flow.resource = ResourceId(1);
    flow.resource_generation = Generation(1);
    flow.priority = PriorityClassId(rng.next_below(parameters.priority_classes) + 1);
    flow.priority_generation = Generation(1);
    flow.qos = QoSClassId(1);
    flow.qos_generation = Generation(1);
    flow.fairness_group = FairnessGroupId(rng.next_below(parameters.fairness_groups) + 1);
    flow.release_tick = (index / wave) * parameters.interval_ticks;
    if (rng.next_bool(parameters.deadline_density)) {
      const std::uint64_t slack_windows =
          service_intervals + rng.next_below(service_intervals + 1);
      const std::uint64_t span = saturating_mul(slack_windows, parameters.interval_ticks);
      const std::uint64_t deadline =
          flow.release_tick > kMaxTickHorizon - span ? kMaxTickHorizon : flow.release_tick + span;
      flow.deadline_tick = deadline > flow.release_tick ? deadline : kNoDeadline;
    } else {
      flow.deadline_tick = kNoDeadline;
    }
    flow.estimated_work = parameters.estimated_work * (1 + rng.next_below(4));
    flow.service_quantum = parameters.service_quantum;
    flow.weight = 1 + rng.next_below(4);
    if (index > 0 && rng.next_bool(parameters.dependency_density)) {
      flow.dependency = FlowId(index);
      flow.has_dependency = true;
    }
    population.push_back(flow);
  }
  return population;
}

Result<BenchmarkResult> run_synthetic_benchmark(const BenchmarkParameters& parameters) {
  FS_RETURN_IF_ERROR(validate_parameters(parameters));
  std::vector<SyntheticFlow> population;
  FS_TRY_ASSIGN(population, generate_synthetic_population(parameters));

  SchedulerOptions options;
  options.policy.policy = PolicyId(1);
  options.policy.generation = Generation(1);
  options.policy.provenance.origin = ProvenanceOrigin::Synthetic;
  options.policy.provenance.sequence = 1;
  options.policy.starvation_bound_ticks = parameters.interval_ticks * 16;
  options.policy.deadline_urgency_ticks = parameters.interval_ticks * 2;
  options.policy.dispatch_delivery_horizon = parameters.interval_ticks * 4;
  options.policy.max_entries_per_schedule = 4096;
  options.policy.default_max_overlap = parameters.concurrency;
  options.policy.provenance.digest = digest_of(options.policy);
  if (parameters.durable) {
    if (parameters.durability_directory.empty()) {
      return Error(ErrorCode::InvalidArgument,
                   "a durable benchmark requires a durability directory");
    }
    options.state_directory = parameters.durability_directory;
  }

  std::unique_ptr<Scheduler> scheduler;
  FS_TRY_ASSIGN(scheduler, Scheduler::create(options));

  ResourceDescriptor resource;
  resource.resource = ResourceId(1);
  resource.generation = Generation(1);
  resource.capacity = parameters.capacity_per_interval;
  resource.interval = parameters.interval_ticks;
  resource.max_overlap = parameters.concurrency;
  resource.provenance.origin = ProvenanceOrigin::Synthetic;
  resource.provenance.sequence = 1;
  FS_RETURN_IF_ERROR(scheduler->register_resource(resource));

  for (std::uint64_t index = 0; index < parameters.priority_classes; ++index) {
    PriorityClassDescriptor priority;
    priority.priority = PriorityClassId(index + 1);
    priority.generation = Generation(1);
    priority.rank = static_cast<std::uint32_t>(index);
    priority.weight = 1 + index;
    priority.provenance.origin = ProvenanceOrigin::Synthetic;
    priority.provenance.sequence = index + 1;
    FS_RETURN_IF_ERROR(scheduler->register_priority_class(priority));
  }

  QoSClassDescriptor qos;
  qos.qos = QoSClassId(1);
  qos.generation = Generation(1);
  qos.provenance.origin = ProvenanceOrigin::Synthetic;
  qos.provenance.sequence = 1;
  FS_RETURN_IF_ERROR(scheduler->register_qos_class(qos));

  for (const SyntheticFlow& synthetic : population) {
    FlowDescriptor descriptor;
    descriptor.flow = synthetic.flow;
    descriptor.generation = synthetic.generation;
    descriptor.path.path = PathId(synthetic.flow.value());
    descriptor.path.generation = Generation(1);
    descriptor.resource = {synthetic.resource, synthetic.resource_generation};
    descriptor.priority = {synthetic.priority, synthetic.priority_generation};
    descriptor.qos = {synthetic.qos, synthetic.qos_generation};
    descriptor.fairness_group = synthetic.fairness_group;
    descriptor.estimated_work = synthetic.estimated_work;
    descriptor.service_quantum = synthetic.service_quantum;
    descriptor.release_tick = synthetic.release_tick;
    descriptor.deadline_tick = synthetic.deadline_tick;
    descriptor.weight = synthetic.weight;
    descriptor.preemptible = true;
    descriptor.auto_ready = true;
    descriptor.trace = TraceId(synthetic.flow.value());
    descriptor.provenance.origin = ProvenanceOrigin::Synthetic;
    descriptor.provenance.sequence = synthetic.flow.value();
    if (synthetic.has_dependency) {
      descriptor.dependencies.push_back(synthetic.dependency);
    }
    FS_RETURN_IF_ERROR(scheduler->admit_flow(descriptor));
  }

  BenchmarkResult result;
  result.label = "SYNTHETIC";
  result.flows_admitted = parameters.flow_count;

  std::vector<std::uint64_t> waits;
  waits.reserve(static_cast<std::size_t>(parameters.flow_count));
  std::uint64_t max_continuous_wait = 0;

  const auto started = std::chrono::steady_clock::now();
  Ticks now = 0;
  std::uint64_t rounds = 0;
  bool converged = false;
  std::uint64_t stalled_rounds = 0;
  std::uint64_t last_terminal = 0;
  std::uint64_t last_dispatches = 0;
  // A workload that stops making progress is reported as unconverged rather
  // than being allowed to spin until the round budget runs out.
  constexpr std::uint64_t kStallRounds = 4096;
  while (rounds < parameters.max_rounds) {
    ++rounds;
    ArbitrationOutcome outcome;
    FS_TRY_ASSIGN(outcome, scheduler->arbitrate(now));
    for (const ScheduleEntry& entry : outcome.schedule.entries) {
      if (entry.kind != DecisionKind::Run) {
        continue;
      }
      const WorkerId worker(1 + (entry.flow.value() % parameters.concurrency));
      const BootId boot(1);
      DispatchTicket ticket;
      const Result<DispatchTicket> dispatch = scheduler->begin_dispatch(
          outcome.schedule.schedule, outcome.schedule.generation, entry.ordinal, worker, boot, now);
      if (!dispatch.ok()) {
        continue;
      }
      ticket = dispatch.value();
      const Result<FlowSnapshot> snapshot = scheduler->flow(entry.flow);
      if (snapshot.ok()) {
        const Ticks eligible = snapshot.value().eligible_since_tick;
        const Ticks waited = now >= eligible ? now - eligible : 0;
        waits.push_back(waited);
        max_continuous_wait = std::max(max_continuous_wait, waited);
      }
      FS_RETURN_IF_ERROR(scheduler->mark_started(ticket, now));
      CompletionEvidence evidence;
      evidence.schedule = ticket.schedule;
      evidence.schedule_generation = ticket.schedule_generation;
      evidence.attempt = ticket.attempt;
      evidence.epoch = ticket.epoch;
      evidence.flow = ticket.flow;
      evidence.flow_generation = ticket.flow_generation;
      evidence.worker = ticket.worker;
      evidence.boot = ticket.boot;
      evidence.outcome = CompletionOutcome::Served;
      evidence.served = ticket.quantum;
      evidence.effect_code = 1;
      evidence.effect_digest = mix64(ticket.attempt.value());
      evidence.observed_tick = now;
      evidence.provenance.origin = ProvenanceOrigin::Synthetic;
      evidence.provenance.sequence = ticket.attempt.value();
      const Result<CommitOutcome> committed = scheduler->complete(evidence, now);
      if (!committed.ok()) {
        return committed.error();
      }
      ++result.dispatches;
      result.service_units_total += committed.value().credited;
    }
    now += parameters.round_advance_ticks;
    // Convergence is polled periodically rather than every round: accounting is
    // a full projection of the flow table and polling it per round would make
    // the benchmark measure its own instrumentation.
    if ((rounds % 16) != 0) {
      continue;
    }
    const AccountingReport report = scheduler->accounting();
    const std::uint64_t terminal =
        report.completed + report.cancelled + report.failed + report.ambiguous;
    if (terminal == result.flows_admitted) {
      converged = true;
      break;
    }
    if (terminal == last_terminal && result.dispatches == last_dispatches) {
      ++stalled_rounds;
      if (stalled_rounds >= kStallRounds) {
        break;
      }
    } else {
      stalled_rounds = 0;
      last_terminal = terminal;
      last_dispatches = result.dispatches;
    }
    if (report.ready == 0 && report.waiting == 0 && report.scheduled == 0 &&
        report.dispatched == 0 && report.running == 0) {
      // Nothing can progress: every remaining flow is terminal or ambiguous.
      converged = terminal == result.flows_admitted;
      break;
    }
  }
  const auto finished = std::chrono::steady_clock::now();
  result.wall_seconds = std::chrono::duration<double>(finished - started).count();
  result.logical_ticks = now;
  result.arbitration_rounds = rounds;
  result.converged = converged;

  const AccountingReport report = scheduler->accounting();
  FS_RETURN_IF_ERROR(report.validate());
  result.flows_completed = report.completed;
  result.flows_cancelled = report.cancelled;
  result.flows_failed = report.failed;
  result.flows_ambiguous = report.ambiguous;
  result.schedules_issued = report.schedules_issued;
  result.schedule_entries = report.schedule_entries_issued;
  result.preemptions = report.preemptions_issued;
  result.starvation_boosts = report.starvation_boosts;
  result.deadline_misses = report.deadline_misses;
  result.max_continuous_wait_ticks = max_continuous_wait;

  const std::vector<std::uint64_t> sorted = percentile_input(waits);
  result.wait_p50_ticks = percentile(sorted, 0.50);
  result.wait_p99_ticks = percentile(sorted, 0.99);
  result.wait_max_ticks = sorted.empty() ? 0 : sorted.back();

  if (result.wall_seconds > 0.0) {
    result.completions_per_second =
        static_cast<double>(result.flows_completed) / result.wall_seconds;
    result.service_units_per_second =
        static_cast<double>(result.service_units_total) / result.wall_seconds;
    result.rounds_per_second = static_cast<double>(rounds) / result.wall_seconds;
  }
  result.detail = converged ? "all admitted flows reached a terminal state"
                            : "round budget exhausted before convergence";
  return result;
}

}  // namespace flow_scheduler

// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#ifndef FLOW_SCHEDULER_BENCH_HPP
#define FLOW_SCHEDULER_BENCH_HPP

#include <cstdint>
#include <string>
#include <vector>

#include "flow_scheduler/error.hpp"
#include "flow_scheduler/scheduler.hpp"

namespace flow_scheduler {

/// Parameters of the synthetic scheduling workload. Every parameter is a
/// dimension the release standard requires coverage for: flow count, priority
/// classes, fairness groups, deadline density, dependency density, and
/// concurrency.
struct BenchmarkParameters {
  std::uint64_t flow_count{10'000};
  std::uint64_t priority_classes{4};
  std::uint64_t fairness_groups{4};
  /// Fraction of flows that carry a deadline, in [0, 1].
  double deadline_density{0.5};
  /// Fraction of flows that depend on an earlier flow, in [0, 1].
  double dependency_density{0.25};
  /// Resource concurrency ceiling (max_overlap).
  std::uint64_t concurrency{8};
  /// Service units available per resource interval.
  std::uint64_t capacity_per_interval{64};
  Ticks interval_ticks{1'000};
  std::uint64_t service_quantum{4};
  std::uint64_t estimated_work{16};
  Ticks schedule_delivery_horizon{5'000};
  std::uint64_t seed{0x5EED1234ull};
  /// When true the workload also exercises the durable journal and therefore
  /// reports durable throughput rather than in-memory throughput.
  bool durable{false};
  std::string durability_directory{};
  /// Upper bound on arbitration rounds so a misconfigured workload cannot spin
  /// forever. A timeout here is a defect, not a result.
  std::uint64_t max_rounds{5'000'000};
  Ticks round_advance_ticks{1'000};
  /// Populate per-flow explanation history during the run. Off by default
  /// because it inflates memory; the explanation path is covered by tests.
  bool collect_histories{false};
};

/// Honest measurement of completed work. Everything here counts flows that
/// reached authoritative completion, never enqueue or admission counts.
struct BenchmarkResult {
  std::string label{"SYNTHETIC"};
  std::uint64_t flows_admitted{0};
  std::uint64_t flows_completed{0};
  std::uint64_t flows_cancelled{0};
  std::uint64_t flows_failed{0};
  std::uint64_t flows_ambiguous{0};
  std::uint64_t dispatches{0};
  std::uint64_t arbitration_rounds{0};
  std::uint64_t schedules_issued{0};
  std::uint64_t schedule_entries{0};
  std::uint64_t preemptions{0};
  std::uint64_t starvation_boosts{0};
  std::uint64_t deadline_misses{0};
  std::uint64_t service_units_total{0};
  std::uint64_t logical_ticks{0};
  double wall_seconds{0.0};
  double completions_per_second{0.0};
  double service_units_per_second{0.0};
  double rounds_per_second{0.0};
  std::uint64_t wait_p50_ticks{0};
  std::uint64_t wait_p99_ticks{0};
  std::uint64_t wait_max_ticks{0};
  /// Longest observed delay between a flow becoming continuously eligible and
  /// being dispatched. Must stay within the configured starvation bound.
  std::uint64_t max_continuous_wait_ticks{0};
  bool converged{false};
  std::string detail{};
};

[[nodiscard]] Result<BenchmarkResult> run_synthetic_benchmark(
    const BenchmarkParameters& parameters);

/// Deterministic population generator, exposed so tests can assert on the
/// exact same population the benchmark uses.
struct SyntheticFlow {
  FlowId flow{};
  Generation generation{};
  ResourceId resource{};
  Generation resource_generation{};
  PriorityClassId priority{};
  Generation priority_generation{};
  QoSClassId qos{};
  Generation qos_generation{};
  FairnessGroupId fairness_group{};
  Ticks release_tick{0};
  Ticks deadline_tick{kNoDeadline};
  std::uint64_t estimated_work{0};
  std::uint64_t service_quantum{0};
  std::uint64_t weight{1};
  FlowId dependency{};
  bool has_dependency{false};
};

[[nodiscard]] Result<std::vector<SyntheticFlow>> generate_synthetic_population(
    const BenchmarkParameters& parameters);

}  // namespace flow_scheduler

#endif  // FLOW_SCHEDULER_BENCH_HPP

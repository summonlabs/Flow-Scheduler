// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#include "test_support.hpp"

namespace {

using namespace flow_scheduler;
using namespace fls_test;

BenchmarkParameters small_parameters() {
  BenchmarkParameters parameters;
  parameters.flow_count = 64;
  parameters.priority_classes = 4;
  parameters.fairness_groups = 3;
  parameters.deadline_density = 0.5;
  parameters.dependency_density = 0.25;
  parameters.concurrency = 4;
  parameters.capacity_per_interval = 64;
  parameters.interval_ticks = 1'000;
  parameters.service_quantum = 2;
  parameters.estimated_work = 8;
  parameters.seed = 0x1234;
  return parameters;
}

}  // namespace

FLOW_TEST(bench, population_generation_is_deterministic_and_shaped_as_requested) {
  const BenchmarkParameters parameters = small_parameters();
  const Result<std::vector<SyntheticFlow>> first = generate_synthetic_population(parameters);
  REQUIRE(first.ok());
  const Result<std::vector<SyntheticFlow>> second = generate_synthetic_population(parameters);
  REQUIRE(second.ok());
  REQUIRE_EQ(first.value().size(), 64u);
  CHECK_EQ(first.value().size(), second.value().size());
  for (std::size_t index = 0; index < first.value().size(); ++index) {
    CHECK_EQ(first.value()[index].flow, second.value()[index].flow);
    CHECK_EQ(first.value()[index].priority, second.value()[index].priority);
    CHECK_EQ(first.value()[index].fairness_group, second.value()[index].fairness_group);
    CHECK_EQ(first.value()[index].release_tick, second.value()[index].release_tick);
    CHECK_EQ(first.value()[index].deadline_tick, second.value()[index].deadline_tick);
    CHECK_EQ(first.value()[index].estimated_work, second.value()[index].estimated_work);
    CHECK_EQ(first.value()[index].has_dependency, second.value()[index].has_dependency);
  }
  std::uint64_t with_deadlines = 0;
  std::uint64_t with_dependencies = 0;
  for (const SyntheticFlow& flow : first.value()) {
    if (has_deadline(flow.deadline_tick)) {
      ++with_deadlines;
    }
    if (flow.has_dependency) {
      ++with_dependencies;
    }
  }
  CHECK(with_deadlines > 0);
  CHECK(with_dependencies > 0);

  BenchmarkParameters different = parameters;
  different.seed = parameters.seed + 1;
  const Result<std::vector<SyntheticFlow>> third = generate_synthetic_population(different);
  REQUIRE(third.ok());
  bool differs = false;
  for (std::size_t index = 0; index < third.value().size(); ++index) {
    if (third.value()[index].priority != first.value()[index].priority) {
      differs = true;
    }
  }
  CHECK(differs);
}

FLOW_TEST(bench, invalid_parameters_are_rejected) {
  BenchmarkParameters parameters = small_parameters();
  parameters.flow_count = 0;
  CHECK_ERROR(as_status(generate_synthetic_population(parameters)), ErrorCode::OutOfRange);

  parameters = small_parameters();
  parameters.deadline_density = 1.5;
  CHECK_ERROR(as_status(generate_synthetic_population(parameters)), ErrorCode::InvalidArgument);

  parameters = small_parameters();
  parameters.concurrency = 0;
  CHECK_ERROR(as_status(generate_synthetic_population(parameters)), ErrorCode::OutOfRange);

  parameters = small_parameters();
  parameters.service_quantum = 0;
  CHECK_ERROR(as_status(generate_synthetic_population(parameters)), ErrorCode::OutOfRange);

  parameters = small_parameters();
  parameters.max_rounds = 0;
  CHECK_ERROR(as_status(generate_synthetic_population(parameters)), ErrorCode::OutOfRange);
}

FLOW_TEST(bench, synthetic_workload_converges_and_accounting_closes) {
  BenchmarkParameters parameters = small_parameters();
  const Result<BenchmarkResult> result = run_synthetic_benchmark(parameters);
  REQUIRE(result.ok());
  CHECK_EQ(result.value().label, std::string("SYNTHETIC"));
  CHECK(result.value().converged);
  CHECK_EQ(result.value().flows_admitted, 64u);
  CHECK_EQ(result.value().flows_completed + result.value().flows_cancelled +
               result.value().flows_failed + result.value().flows_ambiguous,
           64u);
  CHECK(result.value().dispatches > 0);
  CHECK(result.value().service_units_total > 0);
  CHECK(result.value().wall_seconds >= 0.0);
  CHECK(result.value().wait_max_ticks >= result.value().wait_p50_ticks);
  CHECK(result.value().wait_p99_ticks >= result.value().wait_p50_ticks);
  CHECK(!result.value().detail.empty());
}

FLOW_TEST(bench, deadlock_free_scaling_over_the_required_dimensions) {
  struct Sweep {
    std::uint64_t flows;
    std::uint64_t priority_classes;
    std::uint64_t fairness_groups;
    double deadline_density;
    double dependency_density;
    std::uint64_t concurrency;
  };
  const Sweep sweeps[] = {
      {32, 1, 1, 0.0, 0.0, 1},
      {64, 8, 8, 1.0, 0.0, 8},
      {96, 4, 2, 0.5, 0.75, 3},
      {48, 16, 16, 0.25, 0.5, 16},
  };
  for (const Sweep& sweep : sweeps) {
    BenchmarkParameters parameters = small_parameters();
    parameters.flow_count = sweep.flows;
    parameters.priority_classes = sweep.priority_classes;
    parameters.fairness_groups = sweep.fairness_groups;
    parameters.deadline_density = sweep.deadline_density;
    parameters.dependency_density = sweep.dependency_density;
    parameters.concurrency = sweep.concurrency;
    parameters.capacity_per_interval = 128;
    const Result<BenchmarkResult> result = run_synthetic_benchmark(parameters);
    REQUIRE(result.ok());
    CHECK_EQ(result.value().flows_admitted, sweep.flows);
    CHECK(result.value().flows_completed + result.value().flows_cancelled +
              result.value().flows_failed + result.value().flows_ambiguous ==
          sweep.flows);
    CHECK(result.value().converged);
  }
}

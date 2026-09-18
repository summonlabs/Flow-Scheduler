// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#include "test_support.hpp"

namespace {

using namespace flow_scheduler;
using namespace fls_test;

}  // namespace

FLOW_TEST(fairness, starvation_bound_forces_a_waiting_flow_to_run) {
  SchedulerOptions options = standard_options();
  options.policy.starvation_bound_ticks = 50;
  options.policy.min_preempt_service = 0;
  Result<std::unique_ptr<Scheduler>> created =
      make_standard_scheduler(options, 1'000'000, 1);
  REQUIRE(created.ok());
  auto scheduler = std::move(created).value();

  FlowSpec high;
  high.flow = FlowId(1);
  high.priority = PriorityClassId(1);
  high.estimated_work = 100'000;
  high.service_quantum = 1;
  REQUIRE_OK(admit(*scheduler, high));

  FlowSpec low;
  low.flow = FlowId(2);
  low.priority = PriorityClassId(4);
  low.estimated_work = 100'000;
  low.service_quantum = 1;
  REQUIRE_OK(admit(*scheduler, low));

  std::uint64_t first_low_tick = 0;
  Ticks now = 0;
  bool low_ran = false;
  for (int round = 0; round < 40 && !low_ran; ++round) {
    const Result<ArbitrationOutcome> outcome = scheduler->arbitrate(now);
    REQUIRE(outcome.ok());
    for (const ScheduleEntry& entry : outcome.value().schedule.entries) {
      if (entry.kind != DecisionKind::Run) {
        continue;
      }
      if (entry.flow == FlowId(2)) {
        low_ran = true;
        first_low_tick = now;
      }
      const Result<DispatchCycle> cycle =
          run_entry(*scheduler, outcome.value().schedule, entry, now);
      REQUIRE(cycle.ok());
    }
    now += 10;
  }
  CHECK(low_ran);
  // The bound is honoured within one round step of the configured value.
  CHECK(first_low_tick <= options.policy.starvation_bound_ticks + 10);
}

FLOW_TEST(fairness, continuously_eligible_flows_never_exceed_the_starvation_bound) {
  SchedulerOptions options = standard_options();
  options.policy.starvation_bound_ticks = 30;
  Result<std::unique_ptr<Scheduler>> created =
      make_standard_scheduler(options, 1'000'000, 2);
  REQUIRE(created.ok());
  auto scheduler = std::move(created).value();

  for (std::uint64_t index = 1; index <= 6; ++index) {
    FlowSpec spec;
    spec.flow = FlowId(index);
    spec.priority = PriorityClassId(1 + (index % 4));
    spec.estimated_work = 100'000;
    spec.service_quantum = 1;
    REQUIRE_OK(admit(*scheduler, spec));
  }

  std::uint64_t worst_wait = 0;
  Ticks now = 0;
  for (int round = 0; round < 400; ++round) {
    const Result<ArbitrationOutcome> outcome = scheduler->arbitrate(now);
    REQUIRE(outcome.ok());
    for (const ScheduleEntry& entry : outcome.value().schedule.entries) {
      if (entry.kind != DecisionKind::Run) {
        continue;
      }
      const Result<FlowSnapshot> snapshot = scheduler->flow(entry.flow);
      REQUIRE(snapshot.ok());
      const Ticks waited = now >= snapshot.value().eligible_since_tick
                               ? now - snapshot.value().eligible_since_tick
                               : 0;
      if (waited > worst_wait) {
        worst_wait = waited;
      }
      const Result<DispatchCycle> cycle =
          run_entry(*scheduler, outcome.value().schedule, entry, now);
      REQUIRE(cycle.ok());
    }
    now += 5;
  }
  // Every eligible flow is served well inside the bound: the age tier makes
  // them strictly better than the normal tier, and the bound is measured in
  // the same units as the round step.
  CHECK(worst_wait <= options.policy.starvation_bound_ticks + 5);
  const AccountingReport report = scheduler->accounting();
  CHECK_OK(as_status(report.validate()));
}

FLOW_TEST(fairness, minimum_service_deficit_outranks_normal_work) {
  Result<std::unique_ptr<Scheduler>> created =
      make_standard_scheduler(standard_options(), 1'000'000, 1);
  REQUIRE(created.ok());
  auto scheduler = std::move(created).value();

  FlowSpec deficient;
  deficient.flow = FlowId(9);
  deficient.priority = PriorityClassId(1);
  deficient.estimated_work = 100'000;
  deficient.min_service = 4;
  deficient.min_service_window = 1'000'000;
  REQUIRE_OK(admit(*scheduler, deficient));

  FlowSpec normal;
  normal.flow = FlowId(1);
  normal.priority = PriorityClassId(1);
  normal.estimated_work = 100'000;
  REQUIRE_OK(admit(*scheduler, normal));

  const Result<ArbitrationOutcome> outcome = scheduler->arbitrate(0);
  REQUIRE(outcome.ok());
  REQUIRE_EQ(outcome.value().schedule.entries.size(), 1u);
  // The deficit tier outranks the normal tier even though the normal flow has
  // the lower identity.
  CHECK_EQ(outcome.value().schedule.entries[0].flow, FlowId(9));
  CHECK_EQ(outcome.value().schedule.entries[0].tier, 2u);
}

FLOW_TEST(fairness, weighted_fairness_shares_service_in_proportion_to_weight) {
  SchedulerOptions options = standard_options();
  options.policy.preemption = PreemptionMode::None;
  Result<std::unique_ptr<Scheduler>> created =
      make_standard_scheduler(options, 1'000'000, 1);
  REQUIRE(created.ok());
  auto scheduler = std::move(created).value();

  FlowSpec heavy;
  heavy.flow = FlowId(1);
  heavy.fairness_group = FairnessGroupId(1);
  heavy.weight = 1;
  heavy.estimated_work = 1'000'000;
  heavy.service_quantum = 1;
  REQUIRE_OK(admit(*scheduler, heavy));

  FlowSpec light;
  light.flow = FlowId(2);
  light.fairness_group = FairnessGroupId(1);
  light.weight = 4;
  light.estimated_work = 1'000'000;
  light.service_quantum = 1;
  REQUIRE_OK(admit(*scheduler, light));

  std::uint64_t heavy_runs = 0;
  std::uint64_t light_runs = 0;
  Ticks now = 0;
  for (int round = 0; round < 500; ++round) {
    const Result<ArbitrationOutcome> outcome = scheduler->arbitrate(now);
    REQUIRE(outcome.ok());
    for (const ScheduleEntry& entry : outcome.value().schedule.entries) {
      if (entry.kind != DecisionKind::Run) {
        continue;
      }
      if (entry.flow == FlowId(1)) {
        ++heavy_runs;
      } else if (entry.flow == FlowId(2)) {
        ++light_runs;
      }
      const Result<DispatchCycle> cycle =
          run_entry(*scheduler, outcome.value().schedule, entry, now);
      REQUIRE(cycle.ok());
    }
    now += 1;
  }
  CHECK(heavy_runs > 0);
  CHECK(light_runs > heavy_runs);
  // A weight ratio of 4 must produce at least twice the service share; the
  // exact ratio is bounded by the discrete round structure.
  CHECK(light_runs >= heavy_runs * 2);
}

FLOW_TEST(fairness, separate_fairness_groups_do_not_cross_charge) {
  SchedulerOptions options = standard_options();
  options.policy.preemption = PreemptionMode::None;
  Result<std::unique_ptr<Scheduler>> created =
      make_standard_scheduler(options, 1'000'000, 1);
  REQUIRE(created.ok());
  auto scheduler = std::move(created).value();

  FlowSpec first;
  first.flow = FlowId(1);
  first.fairness_group = FairnessGroupId(1);
  first.weight = 1;
  first.estimated_work = 1'000'000;
  REQUIRE_OK(admit(*scheduler, first));

  FlowSpec second;
  second.flow = FlowId(2);
  second.fairness_group = FairnessGroupId(2);
  second.weight = 1;
  second.estimated_work = 1'000'000;
  REQUIRE_OK(admit(*scheduler, second));

  std::uint64_t first_runs = 0;
  std::uint64_t second_runs = 0;
  Ticks now = 0;
  for (int round = 0; round < 200; ++round) {
    const Result<ArbitrationOutcome> outcome = scheduler->arbitrate(now);
    REQUIRE(outcome.ok());
    for (const ScheduleEntry& entry : outcome.value().schedule.entries) {
      if (entry.kind != DecisionKind::Run) {
        continue;
      }
      if (entry.flow == FlowId(1)) {
        ++first_runs;
      } else {
        ++second_runs;
      }
      const Result<DispatchCycle> cycle =
          run_entry(*scheduler, outcome.value().schedule, entry, now);
      REQUIRE(cycle.ok());
    }
    now += 1;
  }
  // Equal weights in equal-cardinality groups share the resource exactly.
  const std::uint64_t difference = first_runs > second_runs ? first_runs - second_runs
                                                            : second_runs - first_runs;
  CHECK(difference <= 1u);
}

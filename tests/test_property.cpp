// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.
//
// Seeded randomized property tests. Every scenario is reproducible from its
// seed, which is printed with any failure, and every run must satisfy the same
// invariants as the hand written suites.

#include <algorithm>

#include "test_support.hpp"

namespace {

using namespace flow_scheduler;
using namespace fls_test;

struct Scenario {
  std::uint64_t seed{1};
  std::uint64_t flow_count{24};
  std::uint64_t priority_classes{4};
  std::uint64_t fairness_groups{3};
  double deadline_density{0.5};
  double dependency_density{0.3};
  std::uint64_t concurrency{3};
  std::uint64_t capacity{64};
  Ticks interval{1'000};
  std::uint64_t quantum{2};
  std::uint64_t max_rounds{400};
};

Result<std::unique_ptr<Scheduler>> build(const Scenario& scenario) {
  SchedulerOptions options = standard_options();
  options.policy.starvation_bound_ticks = scenario.interval * 20;
  options.policy.deadline_urgency_ticks = scenario.interval * 2;
  options.policy.dispatch_delivery_horizon = scenario.interval * 4;
  options.policy.default_max_overlap = scenario.concurrency;
  options.policy.provenance.digest = digest_of(options.policy);
  std::unique_ptr<Scheduler> scheduler;
  FS_TRY_ASSIGN(scheduler, Scheduler::create(options));
  FS_RETURN_IF_ERROR(scheduler->register_resource(
      make_resource(ResourceId(1), Generation(1), scenario.capacity, scenario.interval,
                    scenario.concurrency)));
  for (std::uint64_t rank = 0; rank < scenario.priority_classes; ++rank) {
    FS_RETURN_IF_ERROR(scheduler->register_priority_class(
        make_priority(PriorityClassId(rank + 1), static_cast<std::uint32_t>(rank), 1 + rank)));
  }
  FS_RETURN_IF_ERROR(scheduler->register_qos_class(make_qos()));

  Rng rng(mix64(scenario.seed));
  for (std::uint64_t index = 1; index <= scenario.flow_count; ++index) {
    FlowSpec spec;
    spec.flow = FlowId(index);
    spec.priority = PriorityClassId(rng.next_below(scenario.priority_classes) + 1);
    spec.fairness_group = FairnessGroupId(rng.next_below(scenario.fairness_groups) + 1);
    spec.weight = 1 + rng.next_below(4);
    spec.service_quantum = scenario.quantum;
    spec.estimated_work = scenario.quantum * (1 + rng.next_below(6));
    spec.release_tick = (index / (scenario.concurrency + 1)) * scenario.interval;
    if (rng.next_bool(scenario.deadline_density)) {
      spec.deadline_tick = spec.release_tick + scenario.interval * (2 + rng.next_below(6));
    }
    if (index > 2 && rng.next_bool(scenario.dependency_density)) {
      spec.dependencies.push_back(FlowId(1 + rng.next_below(index - 1)));
      std::sort(spec.dependencies.begin(), spec.dependencies.end());
    }
    FS_RETURN_IF_ERROR(scheduler->admit_flow(make_flow(spec)));
  }
  return scheduler;
}

struct RunResult {
  std::vector<std::uint64_t> schedule_digests;
  std::uint64_t applied{0};
  std::uint64_t duplicates{0};
  std::uint64_t rejected{0};
  std::uint64_t dispatched{0};
  std::uint64_t completed{0};
};

RunResult drive(const Scenario& scenario) {
  RunResult result;
  Result<std::unique_ptr<Scheduler>> created = build(scenario);
  REQUIRE(created.ok());
  auto scheduler = std::move(created).value();

  Ticks now = 0;
  for (std::uint64_t round = 0; round < scenario.max_rounds; ++round) {
    const Result<ArbitrationOutcome> outcome = scheduler->arbitrate(now);
    REQUIRE(outcome.ok());
    result.schedule_digests.push_back(outcome.value().schedule.digest);
    for (const ScheduleEntry& entry : outcome.value().schedule.entries) {
      if (entry.kind != DecisionKind::Run) {
        continue;
      }
      const Result<DispatchCycle> cycle =
          run_entry(*scheduler, outcome.value().schedule, entry, now);
      if (!cycle.ok()) {
        ++result.rejected;
        continue;
      }
      ++result.dispatched;
      switch (cycle.value().outcome.disposition) {
        case CommitDisposition::Applied: ++result.applied; break;
        case CommitDisposition::IdempotentDuplicate: ++result.duplicates; break;
        case CommitDisposition::Rejected: ++result.rejected; break;
      }
    }
    const AccountingReport report = scheduler->accounting();
    if (!report.validate().ok()) {
      REQUIRE(report.validate().ok());
    }
    now += scenario.interval;
  }
  const AccountingReport report = scheduler->accounting();
  result.completed = report.completed;
  return result;
}

}  // namespace

FLOW_TEST(property, seeded_scenarios_preserve_every_invariant) {
  for (std::uint64_t seed = 1; seed <= 8; ++seed) {
    Scenario scenario;
    scenario.seed = seed;
    scenario.flow_count = 16 + seed * 4;
    scenario.priority_classes = 1 + (seed % 4);
    scenario.fairness_groups = 1 + (seed % 3);
    scenario.deadline_density = static_cast<double>(seed % 5) / 4.0;
    scenario.dependency_density = static_cast<double>(seed % 3) / 4.0;
    scenario.concurrency = 1 + (seed % 4);
    scenario.max_rounds = 300;

    const RunResult run = drive(scenario);
    if (run.rejected != 0) {
      // A rejection inside this driver can only come from a flow that was
      // cancelled or replaced, which the scenario never does.
      CHECK_EQ(run.rejected, 0u);
    }
    CHECK(run.applied > 0);
    CHECK(run.completed > 0);
    CHECK_EQ(run.schedule_digests.size(), scenario.max_rounds);
  }
}

FLOW_TEST(property, identical_seeds_produce_identical_schedule_sequences) {
  for (std::uint64_t seed = 11; seed <= 14; ++seed) {
    Scenario scenario;
    scenario.seed = seed;
    scenario.flow_count = 20;
    scenario.max_rounds = 120;
    const RunResult first = drive(scenario);
    const RunResult second = drive(scenario);
    CHECK_EQ(first.schedule_digests.size(), second.schedule_digests.size());
    CHECK(first.schedule_digests == second.schedule_digests);
    CHECK_EQ(first.applied, second.applied);
    CHECK_EQ(first.dispatched, second.dispatched);
    CHECK_EQ(first.completed, second.completed);
  }
}

FLOW_TEST(property, randomized_descriptors_are_validated_without_crashing) {
  for (std::uint64_t seed = 1; seed <= 64; ++seed) {
    Result<std::unique_ptr<Scheduler>> created =
        make_standard_scheduler(standard_options(), 1'000'000, 4);
    REQUIRE(created.ok());
    auto scheduler = std::move(created).value();
    Rng rng(mix64(seed * 7919));

    for (int attempt = 0; attempt < 32; ++attempt) {
      FlowDescriptor descriptor = make_flow();
      descriptor.flow = FlowId(1 + rng.next_below(64));
      descriptor.generation = Generation(1 + rng.next_below(4));
      descriptor.estimated_work = rng.next_below(1ull << 40);
      descriptor.service_quantum = rng.next_below(1ull << 32);
      descriptor.min_service = rng.next_below(1ull << 20);
      descriptor.min_service_window = rng.next_below(1ull << 20);
      descriptor.release_tick = rng.next_below(1ull << 30);
      descriptor.deadline_tick = rng.next_bool(0.5) ? rng.next_below(1ull << 30) : kNoDeadline;
      descriptor.weight = rng.next_below(8);
      descriptor.readiness_signal = static_cast<std::uint8_t>(rng.next_below(9));
      descriptor.resource.generation = Generation(1 + rng.next_below(3));
      descriptor.priority.generation = Generation(1 + rng.next_below(3));
      descriptor.qos.generation = Generation(1 + rng.next_below(3));
      descriptor.fairness_group = FairnessGroupId(rng.next_below(4));
      const std::uint64_t dependencies = rng.next_below(3);
      for (std::uint64_t index = 0; index < dependencies; ++index) {
        descriptor.dependencies.push_back(FlowId(1 + rng.next_below(8)));
      }
      if (rng.next_bool(0.3)) {
        descriptor.provenance = Provenance{};
      }
      if (rng.next_bool(0.2)) {
        descriptor.flow = FlowId{};
      }

      const AccountingReport before = scheduler->accounting();
      const Status admitted = scheduler->admit_flow(descriptor);
      const AccountingReport after = scheduler->accounting();
      if (!admitted.ok()) {
        // A rejected descriptor must not change anything at all.
        CHECK_EQ(before.total_flows, after.total_flows);
        CHECK_EQ(before.waiting, after.waiting);
      } else {
        CHECK_EQ(after.total_flows, before.total_flows + 1);
      }
      CHECK_OK(as_status(after.validate()));
    }
  }
}

FLOW_TEST(property, every_run_entry_is_bounded_and_unique_per_round) {
  for (std::uint64_t seed = 31; seed <= 34; ++seed) {
    Scenario scenario;
    scenario.seed = seed;
    scenario.flow_count = 24;
    scenario.max_rounds = 120;
    Result<std::unique_ptr<Scheduler>> created = build(scenario);
    REQUIRE(created.ok());
    auto scheduler = std::move(created).value();

    Ticks now = 0;
    std::uint64_t total_runs = 0;
    for (std::uint64_t round = 0; round < scenario.max_rounds; ++round) {
      const Result<ArbitrationOutcome> outcome = scheduler->arbitrate(now);
      REQUIRE(outcome.ok());
      std::vector<FlowId> seen;
      for (const ScheduleEntry& entry : outcome.value().schedule.entries) {
        // A flow may hold at most one decision per round, so no flow can be
        // double booked inside a single schedule.
        CHECK(std::find(seen.begin(), seen.end(), entry.flow) == seen.end());
        seen.push_back(entry.flow);
        if (entry.kind != DecisionKind::Run) {
          continue;
        }
        ++total_runs;
        CHECK(entry.quantum >= 1);
        CHECK(entry.quantum <= scenario.quantum);
        CHECK_EQ(entry.start_tick, now);
        const Result<FlowSnapshot> snapshot = scheduler->flow(entry.flow);
        REQUIRE(snapshot.ok());
        CHECK(snapshot.value().served_work + entry.quantum <= snapshot.value().estimated_work);
      }
      for (const ScheduleEntry& entry : outcome.value().schedule.entries) {
        if (entry.kind != DecisionKind::Run) {
          continue;
        }
        const Result<DispatchCycle> cycle =
            run_entry(*scheduler, outcome.value().schedule, entry, now);
        REQUIRE(cycle.ok());
      }
      CHECK_OK(as_status(scheduler->accounting().validate()));
      now += scenario.interval;
    }
    CHECK(total_runs > 0);
  }
}
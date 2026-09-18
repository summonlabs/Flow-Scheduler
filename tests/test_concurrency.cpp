// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.
//
// Concurrency and race coverage. Every test here relies on the scheduler's
// single internal mutex: the runtime never invokes caller callbacks, never
// emits events, and never takes a second lock while holding the first, so the
// only observable behaviour under contention is serialization.

#include <atomic>
#include <thread>

#include "test_support.hpp"

namespace {

using namespace flow_scheduler;
using namespace fls_test;

std::unique_ptr<Scheduler> standard(std::uint64_t max_overlap = 2) {
  Result<std::unique_ptr<Scheduler>> created =
      make_standard_scheduler(standard_options(), 1'000'000, max_overlap);
  REQUIRE(created.ok());
  return std::move(created).value();
}

CompletionEvidence evidence_for(const DispatchTicket& ticket, std::uint64_t served) {
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
  evidence.served = served;
  evidence.effect_code = 1;
  evidence.effect_digest = mix64(ticket.attempt.value());
  evidence.observed_tick = ticket.start_tick;
  evidence.provenance = external_provenance(ticket.attempt.value());
  return evidence;
}

}  // namespace

FLOW_TEST(concurrency, duplicate_completions_race_to_exactly_one_commit) {
  for (int iteration = 0; iteration < 40; ++iteration) {
    auto scheduler = standard();
    FlowSpec spec;
    spec.flow = FlowId(1);
    spec.estimated_work = 64;
    spec.service_quantum = 4;
    REQUIRE_OK(admit(*scheduler, spec));
    const Result<ArbitrationOutcome> issued = scheduler->arbitrate(0);
    REQUIRE(issued.ok());
    REQUIRE_EQ(issued.value().schedule.entries.size(), 1u);
    const Result<DispatchTicket> ticket = scheduler->begin_dispatch(
        issued.value().schedule.schedule, issued.value().schedule.generation,
        issued.value().schedule.entries[0].ordinal, test_worker(), test_boot(), 0);
    REQUIRE(ticket.ok());
    REQUIRE_OK(scheduler->mark_started(ticket.value(), 0));
    const CompletionEvidence evidence = evidence_for(ticket.value(), 4);

    constexpr int kThreads = 8;
    std::atomic<int> applied{0};
    std::atomic<int> duplicates{0};
    std::atomic<int> rejected{0};
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int index = 0; index < kThreads; ++index) {
      threads.emplace_back([&scheduler, &evidence, &applied, &duplicates, &rejected]() {
        const Result<CommitOutcome> outcome = scheduler->complete(evidence, 1);
        if (!outcome.ok()) {
          rejected.fetch_add(1);
          return;
        }
        switch (outcome.value().disposition) {
          case CommitDisposition::Applied: applied.fetch_add(1); break;
          case CommitDisposition::IdempotentDuplicate: duplicates.fetch_add(1); break;
          case CommitDisposition::Rejected: rejected.fetch_add(1); break;
        }
      });
    }
    for (std::thread& thread : threads) {
      thread.join();
    }
    CHECK_EQ(applied.load(), 1);
    CHECK_EQ(duplicates.load() + rejected.load(), kThreads - 1);
    const AccountingReport report = scheduler->accounting();
    CHECK_EQ(report.completions_applied, 1u);
    CHECK_EQ(report.service_credit_total, 4u);
    CHECK_OK(as_status(report.validate()));
  }
}

FLOW_TEST(concurrency, cancellation_racing_completion_has_exactly_one_winner) {
  for (int iteration = 0; iteration < 120; ++iteration) {
    auto scheduler = standard();
    FlowSpec spec;
    spec.flow = FlowId(1);
    spec.estimated_work = 4;
    spec.service_quantum = 4;
    REQUIRE_OK(admit(*scheduler, spec));
    const Result<ArbitrationOutcome> issued = scheduler->arbitrate(0);
    REQUIRE(issued.ok());
    REQUIRE_EQ(issued.value().schedule.entries.size(), 1u);
    const Result<DispatchTicket> ticket = scheduler->begin_dispatch(
        issued.value().schedule.schedule, issued.value().schedule.generation,
        issued.value().schedule.entries[0].ordinal, test_worker(), test_boot(), 0);
    REQUIRE(ticket.ok());
    REQUIRE_OK(scheduler->mark_started(ticket.value(), 0));
    const CompletionEvidence evidence = evidence_for(ticket.value(), 4);

    std::thread completer([&scheduler, &evidence]() {
      const Result<CommitOutcome> outcome = scheduler->complete(evidence, 1);
      (void)outcome;
    });
    std::thread canceller([&scheduler]() {
      const Status status = scheduler->cancel_flow(FlowId(1), Generation(1), "race");
      (void)status;
    });
    completer.join();
    canceller.join();

    const Result<FlowSnapshot> snapshot = scheduler->flow(FlowId(1));
    REQUIRE(snapshot.ok());
    const FlowLifecycle lifecycle = snapshot.value().lifecycle;
    CHECK(lifecycle == FlowLifecycle::Completed || lifecycle == FlowLifecycle::Cancelled);
    if (lifecycle == FlowLifecycle::Cancelled) {
      // Cancellation always wins outright: nothing is credited afterwards.
      CHECK_EQ(snapshot.value().served_work, 0u);
    } else {
      CHECK_EQ(snapshot.value().served_work, 4u);
    }
    const AccountingReport report = scheduler->accounting();
    CHECK_OK(as_status(report.validate()));
    CHECK_EQ(report.service_credit_total, snapshot.value().served_work);
  }
}

FLOW_TEST(concurrency, concurrent_arbitration_never_corrupts_state) {
  auto scheduler = standard();
  for (std::uint64_t index = 1; index <= 16; ++index) {
    FlowSpec spec;
    spec.flow = FlowId(index);
    spec.estimated_work = 32;
    spec.service_quantum = 1;
    REQUIRE_OK(admit(*scheduler, spec));
  }

  constexpr int kThreads = 4;
  std::atomic<int> accepted{0};
  std::atomic<int> refused{0};
  std::vector<std::thread> threads;
  for (int index = 0; index < kThreads; ++index) {
    threads.emplace_back([index, &scheduler, &accepted, &refused]() {
      for (int round = 0; round < 64; ++round) {
        const Ticks now = static_cast<Ticks>(round * 1000 + index);
        const Result<ArbitrationOutcome> outcome = scheduler->arbitrate(now);
        if (outcome.ok()) {
          accepted.fetch_add(1);
        } else {
          // Ticks supplied by concurrent callers may move backwards relative
          // to the timeline established by another thread; that is a refusal,
          // not a corruption.
          if (outcome.code() != ErrorCode::InvalidArgument) {
            refused.fetch_add(1);
          }
        }
      }
    });
  }
  for (std::thread& thread : threads) {
    thread.join();
  }
  CHECK(accepted.load() > 0);
  CHECK_EQ(refused.load(), 0);
  const AccountingReport report = scheduler->accounting();
  CHECK_OK(as_status(report.validate()));
  CHECK_EQ(report.total_flows, 16u);
}

FLOW_TEST(concurrency, readers_observe_consistent_snapshots_while_writers_run) {
  auto scheduler = standard();
  for (std::uint64_t index = 1; index <= 8; ++index) {
    FlowSpec spec;
    spec.flow = FlowId(index);
    spec.estimated_work = 8;
    spec.service_quantum = 1;
    REQUIRE_OK(admit(*scheduler, spec));
  }

  std::atomic<bool> stop{false};
  std::atomic<int> reader_failures{0};
  std::thread reader([&scheduler, &stop, &reader_failures]() {
    while (!stop.load()) {
      const AccountingReport report = scheduler->accounting();
      if (!report.validate().ok()) {
        reader_failures.fetch_add(1);
      }
      for (std::uint64_t index = 1; index <= 8; ++index) {
        const Result<FlowSnapshot> snapshot = scheduler->flow(FlowId(index));
        if (!snapshot.ok()) {
          reader_failures.fetch_add(1);
        }
      }
      const Status checkpoint = scheduler->checkpoint();
      if (!checkpoint.ok()) {
        reader_failures.fetch_add(1);
      }
    }
  });

  Ticks now = 0;
  for (int round = 0; round < 120; ++round) {
    const Result<ArbitrationOutcome> outcome = scheduler->arbitrate(now);
    REQUIRE(outcome.ok());
    for (const ScheduleEntry& entry : outcome.value().schedule.entries) {
      if (entry.kind != DecisionKind::Run) {
        continue;
      }
      const Result<DispatchCycle> cycle =
          run_entry(*scheduler, outcome.value().schedule, entry, now);
      REQUIRE(cycle.ok());
    }
    now += 1'000;
  }
  stop.store(true);
  reader.join();
  CHECK_EQ(reader_failures.load(), 0);
  const AccountingReport report = scheduler->accounting();
  CHECK_OK(as_status(report.validate()));
}

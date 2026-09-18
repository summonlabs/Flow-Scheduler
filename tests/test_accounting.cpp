// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#include "test_support.hpp"

namespace {

using namespace flow_scheduler;
using namespace fls_test;

std::unique_ptr<Scheduler> standard(std::uint64_t capacity = 1'000'000,
                                    std::uint64_t max_overlap = 2) {
  Result<std::unique_ptr<Scheduler>> created =
      make_standard_scheduler(standard_options(), capacity, max_overlap);
  REQUIRE(created.ok());
  return std::move(created).value();
}

}  // namespace

FLOW_TEST(accounting, books_close_over_a_mixed_workload) {
  auto scheduler = standard();
  for (std::uint64_t index = 1; index <= 12; ++index) {
    FlowSpec spec;
    spec.flow = FlowId(index);
    spec.estimated_work = index <= 6 ? 2 : 100;
    spec.service_quantum = 1;
    REQUIRE_OK(admit(*scheduler, spec));
  }

  Ticks now = 0;
  for (int round = 0; round < 30; ++round) {
    const Result<ArbitrationOutcome> outcome = scheduler->arbitrate(now);
    REQUIRE(outcome.ok());
    for (const ScheduleEntry& entry : outcome.value().schedule.entries) {
      if (entry.kind == DecisionKind::Preempt) {
        continue;
      }
      // Alternate between completing, refusing to dispatch, and double
      // reporting to exercise every disposition.
      if (entry.flow.value() == 3) {
        continue;  // Leave the window undelivered until it is reclaimed.
      }
      const Result<DispatchCycle> cycle =
          run_entry(*scheduler, outcome.value().schedule, entry, now);
      REQUIRE(cycle.ok());
      if (round == 0 && entry.flow.value() == 2) {
        // Re-report the same evidence: must be an idempotent duplicate.
        CompletionEvidence evidence;
        evidence.schedule = cycle.value().ticket.schedule;
        evidence.schedule_generation = cycle.value().ticket.schedule_generation;
        evidence.attempt = cycle.value().ticket.attempt;
        evidence.epoch = cycle.value().ticket.epoch;
        evidence.flow = cycle.value().ticket.flow;
        evidence.flow_generation = cycle.value().ticket.flow_generation;
        evidence.worker = cycle.value().ticket.worker;
        evidence.boot = cycle.value().ticket.boot;
        evidence.outcome = CompletionOutcome::Served;
        evidence.served = cycle.value().ticket.quantum;
        evidence.effect_code = 1;
        evidence.effect_digest = mix64(cycle.value().ticket.attempt.value());
        evidence.observed_tick = now;
        evidence.provenance = external_provenance(cycle.value().ticket.attempt.value());
        const Result<CommitOutcome> duplicate = scheduler->complete(evidence, now);
        REQUIRE(duplicate.ok());
        CHECK_EQ(duplicate.value().disposition, CommitDisposition::IdempotentDuplicate);
      }
    }
    const AccountingReport report = scheduler->accounting();
    CHECK_OK(as_status(report.validate()));
    now += 1'000;
  }

  REQUIRE_OK(scheduler->cancel_flow(FlowId(7), Generation(1), "test"));
  const AccountingReport report = scheduler->accounting();
  CHECK_OK(as_status(report.validate()));
  CHECK_EQ(report.total_flows, 12u);
  CHECK(report.completed >= 5u);
  CHECK_EQ(report.cancelled, 1u);
  CHECK_EQ(report.cancelling, 0u);
  CHECK_EQ(report.preempting, 0u);
  CHECK(report.completions_duplicate >= 1u);
  CHECK(report.outstanding_reserved_units > 0u);
  CHECK(!report.to_string().empty());
}

FLOW_TEST(accounting, outstanding_reservation_tracks_open_attempts) {
  auto scheduler = standard();
  FlowSpec spec;
  spec.flow = FlowId(1);
  spec.estimated_work = 64;
  spec.service_quantum = 8;
  REQUIRE_OK(admit(*scheduler, spec));

  const Result<ArbitrationOutcome> issued = scheduler->arbitrate(0);
  REQUIRE(issued.ok());
  REQUIRE_EQ(issued.value().schedule.entries.size(), 1u);
  CHECK_EQ(scheduler->accounting().outstanding_reserved_units, 8u);

  const Result<DispatchTicket> ticket = scheduler->begin_dispatch(
      issued.value().schedule.schedule, issued.value().schedule.generation,
      issued.value().schedule.entries[0].ordinal, test_worker(), test_boot(), 0);
  REQUIRE(ticket.ok());
  REQUIRE_OK(scheduler->mark_started(ticket.value(), 0));
  const AccountingReport running = scheduler->accounting();
  CHECK_EQ(running.outstanding_reserved_units, 8u);
  CHECK_EQ(running.running, 1u);
  CHECK_OK(as_status(running.validate()));

  REQUIRE_OK(scheduler->abandon_attempt(ticket.value().attempt, "worker died", 1));
  const AccountingReport abandoned = scheduler->accounting();
  CHECK_EQ(abandoned.outstanding_reserved_units, 0u);
  CHECK_EQ(abandoned.attempts_abandoned, 1u);
  CHECK_EQ(abandoned.ambiguous, 1u);
  CHECK_OK(as_status(abandoned.validate()));

  // An unresolved ambiguous flow is never dispatched again.
  const Result<ArbitrationOutcome> next = scheduler->arbitrate(2);
  REQUIRE(next.ok());
  CHECK(next.value().schedule.empty());
  bool needs_resolution = false;
  for (const DeferredFlow& deferred : next.value().deferred) {
    if (deferred.flow == FlowId(1) &&
        deferred.reason == DecisionReason::AmbiguousRequiresResolution) {
      needs_resolution = true;
    }
  }
  CHECK(needs_resolution);

  REQUIRE_OK(scheduler->resolve_ambiguous(FlowId(1), Generation(1), AmbiguityResolution::Retry, 3));
  const Result<ArbitrationOutcome> resumed = scheduler->arbitrate(4);
  REQUIRE(resumed.ok());
  REQUIRE_EQ(resumed.value().schedule.entries.size(), 1u);
  CHECK_OK(as_status(scheduler->accounting().validate()));
}

FLOW_TEST(accounting, epoch_advance_abandons_in_flight_work_and_closes_the_books) {
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

  const Result<FabricEpoch> advanced = scheduler->advance_epoch(EpochReason::CoordinatorRestart);
  REQUIRE(advanced.ok());
  CHECK(advanced.value().value() > ticket.value().epoch.value());

  const AccountingReport report = scheduler->accounting();
  CHECK_EQ(report.epoch, advanced.value().value());
  CHECK_EQ(report.ambiguous, 1u);
  CHECK_EQ(report.outstanding_reserved_units, 0u);
  CHECK_EQ(report.running, 0u);
  CHECK_OK(as_status(report.validate()));

  // The retired attempt can never be completed: its authority is gone.
  CompletionEvidence evidence;
  evidence.schedule = ticket.value().schedule;
  evidence.schedule_generation = ticket.value().schedule_generation;
  evidence.attempt = ticket.value().attempt;
  evidence.epoch = ticket.value().epoch;
  evidence.flow = ticket.value().flow;
  evidence.flow_generation = ticket.value().flow_generation;
  evidence.worker = ticket.value().worker;
  evidence.boot = ticket.value().boot;
  evidence.outcome = CompletionOutcome::Served;
  evidence.served = 4;
  evidence.provenance = external_provenance(1);
  const Result<CommitOutcome> rejected = scheduler->complete(evidence, 1);
  REQUIRE(rejected.ok());
  CHECK_EQ(rejected.value().disposition, CommitDisposition::Rejected);
  CHECK_EQ(rejected.value().code, ErrorCode::StaleEpoch);
  CHECK_OK(as_status(scheduler->accounting().validate()));
}

FLOW_TEST(accounting, cancellation_releases_the_reservation) {
  auto scheduler = standard();
  FlowSpec spec;
  spec.flow = FlowId(1);
  spec.estimated_work = 64;
  spec.service_quantum = 4;
  REQUIRE_OK(admit(*scheduler, spec));
  const Result<ArbitrationOutcome> issued = scheduler->arbitrate(0);
  REQUIRE(issued.ok());
  REQUIRE_EQ(issued.value().schedule.entries.size(), 1u);
  CHECK_EQ(scheduler->accounting().outstanding_reserved_units, 4u);

  REQUIRE_OK(scheduler->cancel_flow(FlowId(1), Generation(1), "test"));
  const AccountingReport report = scheduler->accounting();
  CHECK_EQ(report.outstanding_reserved_units, 0u);
  CHECK_EQ(report.cancelled, 1u);
  CHECK_EQ(report.scheduled, 0u);
  CHECK_OK(as_status(report.validate()));
}

FLOW_TEST(accounting, retirement_removes_only_terminal_flows) {
  auto scheduler = standard();
  FlowSpec spec;
  spec.flow = FlowId(1);
  spec.estimated_work = 1;
  REQUIRE_OK(admit(*scheduler, spec));

  CHECK_ERROR(as_status(scheduler->retire_flow(FlowId(1), Generation(1))),
              ErrorCode::InvalidState);

  const Result<ArbitrationOutcome> issued = scheduler->arbitrate(0);
  REQUIRE(issued.ok());
  REQUIRE_EQ(issued.value().schedule.entries.size(), 1u);
  const Result<DispatchCycle> cycle =
      run_entry(*scheduler, issued.value().schedule, issued.value().schedule.entries[0], 0);
  REQUIRE(cycle.ok());
  CHECK_EQ(cycle.value().outcome.lifecycle_after, FlowLifecycle::Completed);

  REQUIRE_OK(scheduler->retire_flow(FlowId(1), Generation(1)));
  const AccountingReport report = scheduler->accounting();
  CHECK_EQ(report.total_flows, 0u);
  CHECK_EQ(report.completed, 0u);
  CHECK_OK(as_status(report.validate()));
  CHECK_ERROR(as_status(scheduler->retire_flow(FlowId(1), Generation(1))),
              ErrorCode::UnknownFlow);
}

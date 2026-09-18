// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#include "test_support.hpp"

namespace {

using namespace flow_scheduler;
using namespace fls_test;

std::unique_ptr<Scheduler> standard(std::uint64_t capacity = 1'000'000,
                                    std::uint64_t max_overlap = 1) {
  Result<std::unique_ptr<Scheduler>> created =
      make_standard_scheduler(standard_options(), capacity, max_overlap);
  REQUIRE(created.ok());
  return std::move(created).value();
}

struct Issued {
  ArbitrationOutcome outcome{};
  DispatchTicket ticket{};
};

/// Admits one flow, arbitrates once, and opens the dispatch attempt.
Issued open_attempt(Scheduler& scheduler, const FlowSpec& spec, Ticks now,
                    WorkerId worker = test_worker(), BootId boot = test_boot()) {
  REQUIRE_OK(admit(scheduler, spec));
  const Result<ArbitrationOutcome> issued = scheduler.arbitrate(now);
  REQUIRE(issued.ok());
  REQUIRE_EQ(issued.value().schedule.entries.size(), 1u);
  Issued result;
  result.outcome = issued.value();
  const Result<DispatchTicket> ticket = scheduler.begin_dispatch(
      result.outcome.schedule.schedule, result.outcome.schedule.generation,
      result.outcome.schedule.entries[0].ordinal, worker, boot, now);
  REQUIRE(ticket.ok());
  result.ticket = ticket.value();
  return result;
}

CompletionEvidence evidence_for(const DispatchTicket& ticket, std::uint64_t served,
                                CompletionOutcome outcome = CompletionOutcome::Served) {
  CompletionEvidence evidence;
  evidence.schedule = ticket.schedule;
  evidence.schedule_generation = ticket.schedule_generation;
  evidence.attempt = ticket.attempt;
  evidence.epoch = ticket.epoch;
  evidence.flow = ticket.flow;
  evidence.flow_generation = ticket.flow_generation;
  evidence.worker = ticket.worker;
  evidence.boot = ticket.boot;
  evidence.outcome = outcome;
  evidence.served = served;
  evidence.effect_code = 7;
  evidence.effect_digest = mix64(ticket.attempt.value());
  evidence.observed_tick = ticket.start_tick;
  evidence.provenance = external_provenance(ticket.attempt.value());
  return evidence;
}

}  // namespace

FLOW_TEST(completion, exact_duplicate_is_idempotent) {
  auto scheduler = standard();
  FlowSpec spec;
  spec.flow = FlowId(1);
  spec.estimated_work = 8;
  spec.service_quantum = 4;
  const Issued issued = open_attempt(*scheduler, spec, 0);
  REQUIRE_OK(scheduler->mark_started(issued.ticket, 0));

  const CompletionEvidence evidence = evidence_for(issued.ticket, 4);
  const Result<CommitOutcome> first = scheduler->complete(evidence, 0);
  REQUIRE(first.ok());
  CHECK_EQ(first.value().disposition, CommitDisposition::Applied);
  CHECK_EQ(first.value().credited, 4u);
  // The flow still has work, and it is re-derived as eligible immediately, so
  // the post-commit lifecycle is Ready rather than Waiting.
  CHECK_EQ(first.value().lifecycle_after, FlowLifecycle::Ready);

  const AccountingReport after_first = scheduler->accounting();
  const Result<CommitOutcome> duplicate = scheduler->complete(evidence, 0);
  REQUIRE(duplicate.ok());
  CHECK_EQ(duplicate.value().disposition, CommitDisposition::IdempotentDuplicate);
  CHECK_EQ(duplicate.value().code, ErrorCode::DuplicateCompletion);
  CHECK_EQ(duplicate.value().credited, 4u);

  const AccountingReport after_duplicate = scheduler->accounting();
  CHECK_EQ(after_first.service_credit_total, after_duplicate.service_credit_total);
  CHECK_EQ(after_first.completions_applied, after_duplicate.completions_applied);
  CHECK_EQ(after_duplicate.completions_duplicate, after_first.completions_duplicate + 1);
  CHECK_OK(as_status(after_duplicate.validate()));

  const Result<FlowSnapshot> snapshot = scheduler->flow(FlowId(1));
  REQUIRE(snapshot.ok());
  CHECK_EQ(snapshot.value().served_work, 4u);
}

FLOW_TEST(completion, contradictory_evidence_for_a_committed_attempt_is_rejected) {
  auto scheduler = standard();
  FlowSpec spec;
  spec.flow = FlowId(1);
  spec.estimated_work = 8;
  spec.service_quantum = 4;
  const Issued issued = open_attempt(*scheduler, spec, 0);
  REQUIRE_OK(scheduler->mark_started(issued.ticket, 0));
  REQUIRE_OK(scheduler->complete(evidence_for(issued.ticket, 4), 0));

  const AccountingReport before = scheduler->accounting();
  CompletionEvidence contradictory = evidence_for(issued.ticket, 2);
  contradictory.effect_digest += 1;  // Same authority, different observed effect.
  const Result<CommitOutcome> rejected = scheduler->complete(contradictory, 0);
  REQUIRE(rejected.ok());
  CHECK_EQ(rejected.value().disposition, CommitDisposition::Rejected);
  CHECK_EQ(rejected.value().code, ErrorCode::ContradictoryCompletion);
  const AccountingReport after = scheduler->accounting();
  CHECK_EQ(before.service_credit_total, after.service_credit_total);
  CHECK_EQ(before.completions_applied, after.completions_applied);
  CHECK_OK(as_status(after.validate()));
}

FLOW_TEST(completion, served_service_cannot_exceed_the_authorized_quantum) {
  auto scheduler = standard();
  FlowSpec spec;
  spec.flow = FlowId(1);
  spec.estimated_work = 16;
  spec.service_quantum = 4;
  const Issued issued = open_attempt(*scheduler, spec, 0);
  REQUIRE_OK(scheduler->mark_started(issued.ticket, 0));

  const Result<CommitOutcome> rejected =
      scheduler->complete(evidence_for(issued.ticket, 5), 0);
  REQUIRE(rejected.ok());
  CHECK_EQ(rejected.value().disposition, CommitDisposition::Rejected);
  CHECK_EQ(rejected.value().code, ErrorCode::ContradictoryCompletion);
  const Result<FlowSnapshot> snapshot = scheduler->flow(FlowId(1));
  REQUIRE(snapshot.ok());
  CHECK_EQ(snapshot.value().served_work, 0u);
  CHECK_EQ(snapshot.value().lifecycle, FlowLifecycle::Running);
}

FLOW_TEST(completion, preempted_work_cannot_complete_under_stale_authority) {
  auto scheduler = standard();
  FlowSpec spec;
  spec.flow = FlowId(1);
  spec.estimated_work = 16;
  spec.service_quantum = 4;
  const Issued issued = open_attempt(*scheduler, spec, 0);
  REQUIRE_OK(scheduler->mark_started(issued.ticket, 0));
  REQUIRE_OK(scheduler->preempt_attempt(issued.ticket.attempt, "test preemption", 1));

  const AccountingReport before = scheduler->accounting();
  const Result<CommitOutcome> rejected =
      scheduler->complete(evidence_for(issued.ticket, 4), 2);
  REQUIRE(rejected.ok());
  CHECK_EQ(rejected.value().disposition, CommitDisposition::Rejected);
  CHECK_EQ(rejected.value().code, ErrorCode::CompletionForClosedAttempt);
  const AccountingReport after = scheduler->accounting();
  CHECK_EQ(before.service_credit_total, after.service_credit_total);
  CHECK_EQ(before.completions_applied, after.completions_applied);
  CHECK_OK(as_status(after.validate()));

  const Result<FlowSnapshot> snapshot = scheduler->flow(FlowId(1));
  REQUIRE(snapshot.ok());
  CHECK_EQ(snapshot.value().served_work, 0u);
  CHECK_EQ(snapshot.value().lifecycle, FlowLifecycle::Ready);
}

FLOW_TEST(completion, cancelled_work_never_reports_success) {
  auto scheduler = standard();
  FlowSpec spec;
  spec.flow = FlowId(1);
  spec.estimated_work = 16;
  spec.service_quantum = 4;
  const Issued issued = open_attempt(*scheduler, spec, 0);
  REQUIRE_OK(scheduler->mark_started(issued.ticket, 0));
  REQUIRE_OK(scheduler->cancel_flow(FlowId(1), Generation(1), "operator cancelled"));

  const Result<CommitOutcome> rejected =
      scheduler->complete(evidence_for(issued.ticket, 4), 1);
  REQUIRE(rejected.ok());
  CHECK_EQ(rejected.value().disposition, CommitDisposition::Rejected);
  CHECK_EQ(rejected.value().code, ErrorCode::CompletionForClosedAttempt);

  const Result<FlowSnapshot> snapshot = scheduler->flow(FlowId(1));
  REQUIRE(snapshot.ok());
  CHECK_EQ(snapshot.value().lifecycle, FlowLifecycle::Cancelled);
  CHECK_EQ(snapshot.value().served_work, 0u);

  // Cancellation is idempotent and never revives the flow.
  REQUIRE_OK(scheduler->cancel_flow(FlowId(1), Generation(1), "again"));
  const Result<FlowSnapshot> again = scheduler->flow(FlowId(1));
  REQUIRE(again.ok());
  CHECK_EQ(again.value().lifecycle, FlowLifecycle::Cancelled);
}

FLOW_TEST(completion, stale_epoch_is_rejected_without_mutation) {
  auto scheduler = standard();
  FlowSpec spec;
  spec.flow = FlowId(1);
  spec.estimated_work = 16;
  spec.service_quantum = 4;
  const Issued issued = open_attempt(*scheduler, spec, 0);
  REQUIRE_OK(scheduler->mark_started(issued.ticket, 0));

  CompletionEvidence evidence = evidence_for(issued.ticket, 4);
  evidence.epoch = FabricEpoch(issued.ticket.epoch.value() + 1);
  const Result<CommitOutcome> rejected = scheduler->complete(evidence, 1);
  REQUIRE(rejected.ok());
  CHECK_EQ(rejected.value().disposition, CommitDisposition::Rejected);
  CHECK_EQ(rejected.value().code, ErrorCode::StaleEpoch);

  // The genuine evidence still applies, and the epoch is unchanged.
  const Result<CommitOutcome> applied =
      scheduler->complete(evidence_for(issued.ticket, 4), 1);
  REQUIRE(applied.ok());
  CHECK_EQ(applied.value().disposition, CommitDisposition::Applied);
  const Result<FabricEpoch> epoch = scheduler->epoch();
  REQUIRE(epoch.ok());
  CHECK_EQ(epoch.value(), issued.ticket.epoch);
}

FLOW_TEST(completion, evidence_from_a_fenced_worker_incarnation_is_rejected) {
  auto scheduler = standard();
  FlowSpec spec;
  spec.flow = FlowId(1);
  spec.estimated_work = 16;
  spec.service_quantum = 4;
  const Issued issued = open_attempt(*scheduler, spec, 0, test_worker(1), test_boot(1));
  REQUIRE_OK(scheduler->mark_started(issued.ticket, 0));

  CompletionEvidence wrong_boot = evidence_for(issued.ticket, 4);
  wrong_boot.boot = test_boot(2);
  const Result<CommitOutcome> rejected_boot = scheduler->complete(wrong_boot, 1);
  REQUIRE(rejected_boot.ok());
  CHECK_EQ(rejected_boot.value().disposition, CommitDisposition::Rejected);
  CHECK_EQ(rejected_boot.value().code, ErrorCode::Fenced);

  CompletionEvidence wrong_worker = evidence_for(issued.ticket, 4);
  wrong_worker.worker = test_worker(9);
  const Result<CommitOutcome> rejected_worker = scheduler->complete(wrong_worker, 1);
  REQUIRE(rejected_worker.ok());
  CHECK_EQ(rejected_worker.value().code, ErrorCode::Fenced);
}

FLOW_TEST(completion, evidence_without_provenance_is_rejected) {
  auto scheduler = standard();
  FlowSpec spec;
  spec.flow = FlowId(1);
  spec.estimated_work = 16;
  spec.service_quantum = 4;
  const Issued issued = open_attempt(*scheduler, spec, 0);
  REQUIRE_OK(scheduler->mark_started(issued.ticket, 0));

  CompletionEvidence evidence = evidence_for(issued.ticket, 4);
  evidence.provenance = Provenance{};
  const Result<CommitOutcome> rejected = scheduler->complete(evidence, 1);
  REQUIRE(rejected.ok());
  CHECK_EQ(rejected.value().disposition, CommitDisposition::Rejected);
  CHECK_EQ(rejected.value().code, ErrorCode::InvalidArgument);
}

FLOW_TEST(completion, reported_failure_fails_the_flow) {
  auto scheduler = standard();
  FlowSpec spec;
  spec.flow = FlowId(1);
  spec.estimated_work = 16;
  spec.service_quantum = 4;
  const Issued issued = open_attempt(*scheduler, spec, 0);
  REQUIRE_OK(scheduler->mark_started(issued.ticket, 0));
  const Result<CommitOutcome> failed = scheduler->complete(
      evidence_for(issued.ticket, 0, CompletionOutcome::Failed), 1);
  REQUIRE(failed.ok());
  CHECK_EQ(failed.value().disposition, CommitDisposition::Applied);
  CHECK_EQ(failed.value().lifecycle_after, FlowLifecycle::Failed);
  CHECK_EQ(failed.value().credited, 0u);
  const Result<FlowSnapshot> snapshot = scheduler->flow(FlowId(1));
  REQUIRE(snapshot.ok());
  CHECK_EQ(snapshot.value().lifecycle, FlowLifecycle::Failed);
}

FLOW_TEST(completion, no_service_returns_the_flow_to_the_ready_path) {
  auto scheduler = standard();
  FlowSpec spec;
  spec.flow = FlowId(1);
  spec.estimated_work = 16;
  spec.service_quantum = 4;
  const Issued issued = open_attempt(*scheduler, spec, 0);
  REQUIRE_OK(scheduler->mark_started(issued.ticket, 0));
  const Result<CommitOutcome> idle = scheduler->complete(
      evidence_for(issued.ticket, 0, CompletionOutcome::NoService), 1);
  REQUIRE(idle.ok());
  CHECK_EQ(idle.value().disposition, CommitDisposition::Applied);
  CHECK_EQ(idle.value().lifecycle_after, FlowLifecycle::Ready);
  CHECK_EQ(idle.value().credited, 0u);

  const Result<ArbitrationOutcome> next = scheduler->arbitrate(2);
  REQUIRE(next.ok());
  REQUIRE_EQ(next.value().schedule.entries.size(), 1u);
  CHECK_EQ(next.value().schedule.entries[0].flow, FlowId(1));
}

FLOW_TEST(completion, several_rounds_accumulate_until_authoritative_completion) {
  auto scheduler = standard();
  FlowSpec spec;
  spec.flow = FlowId(1);
  spec.estimated_work = 6;
  spec.service_quantum = 2;
  REQUIRE_OK(admit(*scheduler, spec));

  Ticks now = 0;
  std::uint64_t total_credit = 0;
  bool completed = false;
  for (int round = 0; round < 8 && !completed; ++round) {
    const Result<ArbitrationOutcome> issued = scheduler->arbitrate(now);
    REQUIRE(issued.ok());
    if (issued.value().schedule.empty()) {
      now += 1;
      continue;
    }
    REQUIRE_EQ(issued.value().schedule.entries.size(), 1u);
    const Result<DispatchCycle> cycle = run_entry(*scheduler, issued.value().schedule,
                                                  issued.value().schedule.entries[0], now);
    REQUIRE(cycle.ok());
    CHECK_EQ(cycle.value().outcome.disposition, CommitDisposition::Applied);
    total_credit += cycle.value().outcome.credited;
    completed = cycle.value().outcome.flow_completed;
    now += 1;
  }
  CHECK(completed);
  CHECK_EQ(total_credit, 6u);

  const Result<FlowSnapshot> snapshot = scheduler->flow(FlowId(1));
  REQUIRE(snapshot.ok());
  CHECK_EQ(snapshot.value().lifecycle, FlowLifecycle::Completed);
  CHECK_EQ(snapshot.value().served_work, 6u);
  CHECK_EQ(snapshot.value().outstanding_reserved, 0u);

  const AccountingReport report = scheduler->accounting();
  CHECK_EQ(report.completed, 1u);
  CHECK_EQ(report.outstanding_reserved_units, 0u);
  CHECK_EQ(report.service_credit_total, 6u);
  CHECK_OK(as_status(report.validate()));

  // A completed flow is never scheduled again.
  const Result<ArbitrationOutcome> after = scheduler->arbitrate(now + 1'000);
  REQUIRE(after.ok());
  CHECK(after.value().schedule.empty());
}

FLOW_TEST(completion, unknown_attempt_and_flow_are_rejected) {
  auto scheduler = standard();
  CompletionEvidence evidence;
  evidence.schedule = ScheduleId(1);
  evidence.schedule_generation = Generation(1);
  evidence.attempt = DispatchAttemptId(99);
  evidence.epoch = FabricEpoch(1);
  evidence.flow = FlowId(42);
  evidence.flow_generation = Generation(1);
  evidence.worker = test_worker();
  evidence.boot = test_boot();
  evidence.outcome = CompletionOutcome::Served;
  evidence.served = 1;
  evidence.provenance = external_provenance(1);
  const Result<CommitOutcome> rejected = scheduler->complete(evidence, 0);
  REQUIRE(rejected.ok());
  CHECK_EQ(rejected.value().disposition, CommitDisposition::Rejected);
  CHECK_EQ(rejected.value().code, ErrorCode::UnknownFlow);
}

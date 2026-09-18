// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#include "test_support.hpp"

namespace {

using namespace flow_scheduler;
using namespace fls_test;

std::unique_ptr<Scheduler> with_policy(const SchedulingPolicy& policy,
                                       std::uint64_t max_overlap = 1) {
  SchedulerOptions options = standard_options();
  options.policy = policy;
  options.policy.provenance.digest = digest_of(options.policy);
  Result<std::unique_ptr<Scheduler>> created =
      make_standard_scheduler(options, 1'000'000, max_overlap);
  REQUIRE(created.ok());
  return std::move(created).value();
}

}  // namespace

FLOW_TEST(deadline, urgency_outranks_priority) {
  auto scheduler = with_policy(standard_options().policy);
  FlowSpec urgent;
  urgent.flow = FlowId(1);
  urgent.priority = PriorityClassId(4);  // lowest priority rank
  urgent.deadline_tick = 50;             // inside the urgency horizon of 100
  urgent.estimated_work = 64;
  REQUIRE_OK(admit(*scheduler, urgent));

  FlowSpec plain;
  plain.flow = FlowId(2);
  plain.priority = PriorityClassId(1);  // highest priority rank
  plain.estimated_work = 64;
  REQUIRE_OK(admit(*scheduler, plain));

  const Result<ArbitrationOutcome> outcome = scheduler->arbitrate(0);
  REQUIRE(outcome.ok());
  REQUIRE_EQ(outcome.value().schedule.entries.size(), 1u);
  CHECK_EQ(outcome.value().schedule.entries[0].flow, FlowId(1));
  CHECK_EQ(outcome.value().schedule.entries[0].tier, 0u);
}

FLOW_TEST(deadline, strict_policy_fails_a_flow_that_misses_its_window) {
  auto scheduler = with_policy(standard_options().policy);
  FlowSpec spec;
  spec.flow = FlowId(1);
  spec.release_tick = 0;
  spec.deadline_tick = 100;
  spec.estimated_work = 64;
  REQUIRE_OK(admit(*scheduler, spec));

  const Result<ArbitrationOutcome> before = scheduler->arbitrate(50);
  REQUIRE(before.ok());
  REQUIRE_EQ(before.value().schedule.entries.size(), 1u);
  REQUIRE_OK(scheduler->release_schedule(before.value().schedule.schedule,
                                         before.value().schedule.generation));

  const Result<ArbitrationOutcome> miss = scheduler->arbitrate(100);
  REQUIRE(miss.ok());
  CHECK(miss.value().schedule.empty());
  REQUIRE_EQ(miss.value().deadline_misses.size(), 1u);
  CHECK_EQ(miss.value().deadline_misses[0], FlowId(1));

  const Result<FlowSnapshot> snapshot = scheduler->flow(FlowId(1));
  REQUIRE(snapshot.ok());
  CHECK_EQ(snapshot.value().lifecycle, FlowLifecycle::Failed);
  CHECK_EQ(snapshot.value().accounting.deadline_misses, 1u);

  // The miss is counted exactly once, and a failed flow is never scheduled.
  const Result<ArbitrationOutcome> later = scheduler->arbitrate(500);
  REQUIRE(later.ok());
  CHECK(later.value().schedule.empty());
  const AccountingReport report = scheduler->accounting();
  CHECK_EQ(report.deadline_misses, 1u);
  CHECK_EQ(report.failed, 1u);
  CHECK_OK(as_status(report.validate()));
}

FLOW_TEST(deadline, soft_policy_records_the_miss_and_stops_dispatching) {
  SchedulingPolicy policy = standard_options().policy;
  policy.strict_deadlines = false;
  auto scheduler = with_policy(policy);
  FlowSpec spec;
  spec.flow = FlowId(1);
  spec.release_tick = 0;
  spec.deadline_tick = 100;
  spec.estimated_work = 64;
  REQUIRE_OK(admit(*scheduler, spec));

  const Result<ArbitrationOutcome> miss = scheduler->arbitrate(150);
  REQUIRE(miss.ok());
  CHECK(miss.value().schedule.empty());
  REQUIRE_EQ(miss.value().deadline_misses.size(), 1u);
  const Result<FlowSnapshot> snapshot = scheduler->flow(FlowId(1));
  REQUIRE(snapshot.ok());
  CHECK_EQ(snapshot.value().lifecycle, FlowLifecycle::Waiting);

  const Result<ArbitrationOutcome> later = scheduler->arbitrate(400);
  REQUIRE(later.ok());
  CHECK(later.value().schedule.empty());
  const AccountingReport report = scheduler->accounting();
  CHECK_EQ(report.deadline_misses, 1u);
  CHECK_EQ(report.waiting, 1u);
  CHECK_OK(as_status(report.validate()));
}

FLOW_TEST(deadline, dispatch_is_refused_at_or_after_the_deadline) {
  auto scheduler = with_policy(standard_options().policy);
  FlowSpec spec;
  spec.flow = FlowId(1);
  spec.release_tick = 0;
  spec.deadline_tick = 100;
  spec.estimated_work = 64;
  spec.service_quantum = 4;
  REQUIRE_OK(admit(*scheduler, spec));

  const Result<ArbitrationOutcome> issued = scheduler->arbitrate(0);
  REQUIRE(issued.ok());
  REQUIRE_EQ(issued.value().schedule.entries.size(), 1u);
  CHECK_EQ(issued.value().schedule.entries[0].deadline_tick, 100u);

  const Result<DispatchTicket> late = scheduler->begin_dispatch(
      issued.value().schedule.schedule, issued.value().schedule.generation,
      issued.value().schedule.entries[0].ordinal, test_worker(), test_boot(), 100);
  CHECK(!late.ok());
  CHECK_EQ(late.code(), ErrorCode::DeadlinePassed);

  const Result<DispatchTicket> on_time = scheduler->begin_dispatch(
      issued.value().schedule.schedule, issued.value().schedule.generation,
      issued.value().schedule.entries[0].ordinal, test_worker(), test_boot(), 99);
  CHECK(on_time.ok());
}

FLOW_TEST(deadline, completion_observed_after_the_window_is_rejected) {
  auto scheduler = with_policy(standard_options().policy);
  FlowSpec spec;
  spec.flow = FlowId(1);
  spec.release_tick = 0;
  spec.deadline_tick = 100;
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

  const AccountingReport before = scheduler->accounting();
  const Result<CommitOutcome> rejected = scheduler->complete(evidence, 100);
  REQUIRE(rejected.ok());
  CHECK_EQ(rejected.value().disposition, CommitDisposition::Rejected);
  CHECK_EQ(rejected.value().code, ErrorCode::DeadlinePassed);
  const AccountingReport after = scheduler->accounting();
  CHECK_EQ(before.service_credit_total, after.service_credit_total);
  CHECK_EQ(before.completions_applied, after.completions_applied);
  CHECK_OK(as_status(after.validate()));

  // The same evidence observed inside the window is accepted.
  evidence.observed_tick = 99;
  const Result<CommitOutcome> accepted = scheduler->complete(evidence, 99);
  REQUIRE(accepted.ok());
  CHECK_EQ(accepted.value().disposition, CommitDisposition::Applied);
  CHECK_EQ(accepted.value().credited, 4u);
}

FLOW_TEST(deadline, bounded_slack_clamps_the_granted_quantum) {
  auto scheduler = with_policy(standard_options().policy);
  FlowSpec spec;
  spec.flow = FlowId(1);
  spec.release_tick = 0;
  spec.deadline_tick = 20;
  spec.estimated_work = 64;
  spec.service_quantum = 64;
  REQUIRE_OK(admit(*scheduler, spec));

  const Result<ArbitrationOutcome> issued = scheduler->arbitrate(0);
  REQUIRE(issued.ok());
  REQUIRE_EQ(issued.value().schedule.entries.size(), 1u);
  // Strict deadlines clamp the window to the remaining slack.
  CHECK_EQ(issued.value().schedule.entries[0].quantum, 20u);
}

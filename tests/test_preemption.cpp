// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#include "test_support.hpp"

namespace {

using namespace flow_scheduler;
using namespace fls_test;

/// Admits a flow, schedules it, opens the attempt and marks it started so it
/// is a genuine incumbent rather than merely a reserved window.
DispatchTicket start_running(Scheduler& scheduler, const FlowSpec& spec, Ticks now,
                             WorkerId worker = test_worker(), BootId boot = test_boot()) {
  REQUIRE_OK(admit(scheduler, spec));
  const Result<ArbitrationOutcome> issued = scheduler.arbitrate(now);
  REQUIRE(issued.ok());
  REQUIRE_EQ(issued.value().schedule.entries.size(), 1u);
  REQUIRE_EQ(issued.value().schedule.entries[0].kind, DecisionKind::Run);
  const Result<DispatchTicket> ticket = scheduler.begin_dispatch(
      issued.value().schedule.schedule, issued.value().schedule.generation,
      issued.value().schedule.entries[0].ordinal, worker, boot, now);
  REQUIRE(ticket.ok());
  REQUIRE_OK(scheduler.mark_started(ticket.value(), now));
  return ticket.value();
}

std::unique_ptr<Scheduler> with_policy(const SchedulingPolicy& policy, std::uint64_t max_overlap = 1) {
  SchedulerOptions options = standard_options();
  options.policy = policy;
  options.policy.provenance.digest = digest_of(options.policy);
  Result<std::unique_ptr<Scheduler>> created =
      make_standard_scheduler(options, 1'000'000, max_overlap);
  REQUIRE(created.ok());
  return std::move(created).value();
}

}  // namespace

FLOW_TEST(preemption, higher_priority_displaces_a_running_incumbent) {
  auto scheduler = with_policy(standard_options().policy);
  FlowSpec incumbent;
  incumbent.flow = FlowId(1);
  incumbent.priority = PriorityClassId(4);
  incumbent.estimated_work = 64;
  incumbent.service_quantum = 4;
  const DispatchTicket running = start_running(*scheduler, incumbent, 0);

  FlowSpec challenger;
  challenger.flow = FlowId(2);
  challenger.priority = PriorityClassId(1);
  challenger.estimated_work = 64;
  challenger.service_quantum = 4;
  REQUIRE_OK(admit(*scheduler, challenger));

  const Result<ArbitrationOutcome> round = scheduler->arbitrate(1);
  REQUIRE(round.ok());
  REQUIRE_EQ(round.value().schedule.entries.size(), 2u);
  CHECK_EQ(round.value().schedule.entries[0].kind, DecisionKind::Preempt);
  CHECK_EQ(round.value().schedule.entries[0].flow, FlowId(1));
  CHECK_EQ(round.value().schedule.entries[0].attempt, running.attempt);
  CHECK_EQ(round.value().schedule.entries[0].reason,
           DecisionReason::DisplacedByHigherAuthority);
  CHECK_EQ(round.value().schedule.entries[1].kind, DecisionKind::Run);
  CHECK_EQ(round.value().schedule.entries[1].flow, FlowId(2));

  const Result<FlowSnapshot> displaced = scheduler->flow(FlowId(1));
  REQUIRE(displaced.ok());
  CHECK_EQ(displaced.value().lifecycle, FlowLifecycle::Ready);
  CHECK_EQ(displaced.value().accounting.preemptions, 1u);

  const Result<DispatchTicket> closed = scheduler->attempt(running.attempt);
  REQUIRE(closed.ok());
  CHECK_EQ(closed.value().state, AttemptState::Preempted);

  const AccountingReport report = scheduler->accounting();
  CHECK_EQ(report.preemptions_issued, 1u);
  CHECK_OK(as_status(report.validate()));
}

FLOW_TEST(preemption, disabled_mode_never_displaces) {
  SchedulingPolicy policy = standard_options().policy;
  policy.preemption = PreemptionMode::None;
  auto scheduler = with_policy(policy);
  FlowSpec incumbent;
  incumbent.flow = FlowId(1);
  incumbent.priority = PriorityClassId(4);
  incumbent.estimated_work = 64;
  REQUIRE_OK(admit(*scheduler, incumbent));
  FlowSpec challenger;
  challenger.flow = FlowId(2);
  challenger.priority = PriorityClassId(1);
  challenger.estimated_work = 64;
  REQUIRE_OK(admit(*scheduler, challenger));

  // Give the challenger first place in the ordering by admitting it first.
  const Result<ArbitrationOutcome> first = scheduler->arbitrate(0);
  REQUIRE(first.ok());
  REQUIRE_EQ(first.value().schedule.entries.size(), 1u);
  CHECK_EQ(first.value().schedule.entries[0].flow, FlowId(2));
  REQUIRE_OK(scheduler->begin_dispatch(first.value().schedule.schedule,
                                       first.value().schedule.generation,
                                       first.value().schedule.entries[0].ordinal, test_worker(),
                                       test_boot(), 0));

  const Result<ArbitrationOutcome> second = scheduler->arbitrate(1);
  REQUIRE(second.ok());
  CHECK(second.value().schedule.empty());
  bool throttled = false;
  for (const DeferredFlow& deferred : second.value().deferred) {
    if (deferred.flow == FlowId(1) && deferred.reason == DecisionReason::PreemptionDisabled) {
      throttled = true;
    }
  }
  CHECK(throttled);
}

FLOW_TEST(preemption, non_preemptible_incumbent_is_protected) {
  auto scheduler = with_policy(standard_options().policy);
  FlowSpec incumbent;
  incumbent.flow = FlowId(1);
  incumbent.priority = PriorityClassId(4);
  incumbent.estimated_work = 64;
  incumbent.preemptible = false;
  const DispatchTicket running = start_running(*scheduler, incumbent, 0);

  FlowSpec challenger;
  challenger.flow = FlowId(2);
  challenger.priority = PriorityClassId(1);
  challenger.estimated_work = 64;
  REQUIRE_OK(admit(*scheduler, challenger));

  const Result<ArbitrationOutcome> round = scheduler->arbitrate(1);
  REQUIRE(round.ok());
  CHECK(round.value().schedule.empty());
  bool refused = false;
  for (const DeferredFlow& deferred : round.value().deferred) {
    if (deferred.flow == FlowId(2) &&
        deferred.reason == DecisionReason::IncumbentNotPreemptible) {
      refused = true;
    }
  }
  CHECK(refused);
  const Result<DispatchTicket> still_open = scheduler->attempt(running.attempt);
  REQUIRE(still_open.ok());
  CHECK_EQ(still_open.value().state, AttemptState::Started);
}

FLOW_TEST(preemption, qos_protected_class_blocks_displacement) {
  SchedulingPolicy policy = standard_options().policy;
  auto scheduler = with_policy(policy);
  REQUIRE_OK(scheduler->register_qos_class(make_qos(QoSClassId(2), true, 0)));

  FlowSpec incumbent;
  incumbent.flow = FlowId(1);
  incumbent.priority = PriorityClassId(4);
  incumbent.estimated_work = 64;
  incumbent.qos = QoSClassId(2);
  const DispatchTicket running = start_running(*scheduler, incumbent, 0);

  FlowSpec challenger;
  challenger.flow = FlowId(2);
  challenger.priority = PriorityClassId(1);
  challenger.estimated_work = 64;
  REQUIRE_OK(admit(*scheduler, challenger));

  const Result<ArbitrationOutcome> round = scheduler->arbitrate(1);
  REQUIRE(round.ok());
  CHECK(round.value().schedule.empty());
  const Result<DispatchTicket> still_open = scheduler->attempt(running.attempt);
  REQUIRE(still_open.ok());
  CHECK_EQ(still_open.value().state, AttemptState::Started);
}

FLOW_TEST(preemption, minimum_service_before_displacement_throttles_thrash) {
  SchedulingPolicy policy = standard_options().policy;
  policy.min_preempt_service = 5;
  auto scheduler = with_policy(policy);
  FlowSpec incumbent;
  incumbent.flow = FlowId(1);
  incumbent.priority = PriorityClassId(4);
  incumbent.estimated_work = 64;
  const DispatchTicket running = start_running(*scheduler, incumbent, 0);

  FlowSpec challenger;
  challenger.flow = FlowId(2);
  challenger.priority = PriorityClassId(1);
  challenger.estimated_work = 64;
  REQUIRE_OK(admit(*scheduler, challenger));

  const Result<ArbitrationOutcome> round = scheduler->arbitrate(1);
  REQUIRE(round.ok());
  CHECK(round.value().schedule.empty());
  bool throttled = false;
  for (const DeferredFlow& deferred : round.value().deferred) {
    if (deferred.flow == FlowId(2) && deferred.reason == DecisionReason::PreemptionThrottled) {
      throttled = true;
    }
  }
  CHECK(throttled);
  const Result<DispatchTicket> still_open = scheduler->attempt(running.attempt);
  REQUIRE(still_open.ok());
  CHECK_EQ(still_open.value().state, AttemptState::Started);
}

FLOW_TEST(preemption, preemption_throttle_bounds_displacement_rate) {
  SchedulingPolicy policy = standard_options().policy;
  policy.max_preemptions_per_interval = 1;
  policy.preemption_interval_ticks = 1'000'000;
  auto scheduler = with_policy(policy, 1);

  FlowSpec first;
  first.flow = FlowId(1);
  first.priority = PriorityClassId(4);
  first.estimated_work = 64;
  const DispatchTicket running = start_running(*scheduler, first, 0);

  FlowSpec challenger;
  challenger.flow = FlowId(2);
  challenger.priority = PriorityClassId(1);
  challenger.estimated_work = 64;
  REQUIRE_OK(admit(*scheduler, challenger));

  const Result<ArbitrationOutcome> first_round = scheduler->arbitrate(1);
  REQUIRE(first_round.ok());
  REQUIRE_EQ(first_round.value().schedule.entries.size(), 2u);
  CHECK_EQ(first_round.value().schedule.entries[0].kind, DecisionKind::Preempt);

  // Dispatch the challenger so the resource is busy again, then attempt a
  // second displacement which the throttle must refuse.
  REQUIRE_OK(scheduler->begin_dispatch(first_round.value().schedule.schedule,
                                       first_round.value().schedule.generation,
                                       first_round.value().schedule.entries[1].ordinal,
                                       test_worker(), test_boot(), 1));
  FlowSpec third;
  third.flow = FlowId(3);
  third.priority = PriorityClassId(1);
  third.estimated_work = 64;
  REQUIRE_OK(admit(*scheduler, third));
  const Result<ArbitrationOutcome> second_round = scheduler->arbitrate(2);
  REQUIRE(second_round.ok());
  bool throttled = false;
  for (const DeferredFlow& deferred : second_round.value().deferred) {
    if (deferred.reason == DecisionReason::PreemptionThrottled) {
      throttled = true;
    }
  }
  CHECK(throttled);
  const AccountingReport report = scheduler->accounting();
  CHECK_EQ(report.preemptions_issued, 1u);
}

FLOW_TEST(preemption, overdue_running_work_is_stopped_even_without_preemption_policy) {
  SchedulingPolicy policy = standard_options().policy;
  policy.preemption = PreemptionMode::None;
  auto scheduler = with_policy(policy);

  FlowSpec spec;
  spec.flow = FlowId(1);
  spec.estimated_work = 64;
  spec.service_quantum = 4;
  spec.release_tick = 0;
  spec.deadline_tick = 100;
  const DispatchTicket running = start_running(*scheduler, spec, 0);

  const Result<ArbitrationOutcome> round = scheduler->arbitrate(100);
  REQUIRE(round.ok());
  const Result<DispatchTicket> closed = scheduler->attempt(running.attempt);
  REQUIRE(closed.ok());
  CHECK_EQ(closed.value().state, AttemptState::Preempted);

  // Service observed after the authorized window can never be credited.
  CompletionEvidence evidence;
  evidence.schedule = running.schedule;
  evidence.schedule_generation = running.schedule_generation;
  evidence.attempt = running.attempt;
  evidence.epoch = running.epoch;
  evidence.flow = running.flow;
  evidence.flow_generation = running.flow_generation;
  evidence.worker = running.worker;
  evidence.boot = running.boot;
  evidence.outcome = CompletionOutcome::Served;
  evidence.served = 4;
  evidence.provenance = external_provenance(1);
  const Result<CommitOutcome> rejected = scheduler->complete(evidence, 100);
  REQUIRE(rejected.ok());
  CHECK_EQ(rejected.value().disposition, CommitDisposition::Rejected);
  const AccountingReport report = scheduler->accounting();
  CHECK_EQ(report.preemptions_issued, 1u);
  CHECK_OK(as_status(report.validate()));
}

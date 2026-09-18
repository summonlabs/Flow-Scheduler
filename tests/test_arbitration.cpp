// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#include "test_support.hpp"

namespace {

using namespace flow_scheduler;
using namespace fls_test;

/// Standard scheduler with one generous resource unless the test overrides it.
std::unique_ptr<Scheduler> standard(std::uint64_t capacity = 1'000'000,
                                    std::uint64_t max_overlap = 1) {
  Result<std::unique_ptr<Scheduler>> created =
      make_standard_scheduler(standard_options(), capacity, max_overlap);
  REQUIRE(created.ok());
  return std::move(created).value();
}

}  // namespace

FLOW_TEST(arbitration, no_flow_is_scheduled_before_it_is_ready) {
  auto scheduler = standard();
  FlowSpec spec;
  spec.flow = FlowId(1);
  spec.release_tick = 500;
  REQUIRE_OK(admit(*scheduler, spec));

  const Result<ArbitrationOutcome> early = scheduler->arbitrate(0);
  REQUIRE(early.ok());
  CHECK(early.value().schedule.empty());
  CHECK_EQ(early.value().schedule.schedule, ScheduleId{});
  REQUIRE_EQ(early.value().deferred.size(), 1u);
  CHECK_EQ(early.value().deferred[0].reason, DecisionReason::AwaitingRelease);

  const Result<FlowSnapshot> snapshot = scheduler->flow(FlowId(1));
  REQUIRE(snapshot.ok());
  CHECK_EQ(snapshot.value().lifecycle, FlowLifecycle::Waiting);

  const Result<ArbitrationOutcome> on_time = scheduler->arbitrate(500);
  REQUIRE(on_time.ok());
  REQUIRE_EQ(on_time.value().schedule.entries.size(), 1u);
  CHECK_EQ(on_time.value().schedule.entries[0].flow, FlowId(1));
  CHECK_EQ(on_time.value().schedule.entries[0].kind, DecisionKind::Run);
  CHECK_EQ(on_time.value().schedule.entries[0].start_tick, 500u);

  // Releasing without dispatching returns the flow to the ready set; it is
  // still never scheduled before its release tick.
  REQUIRE_OK(
      scheduler->release_schedule(on_time.value().schedule.schedule,
                                  on_time.value().schedule.generation));
  const Result<FlowSnapshot> released = scheduler->flow(FlowId(1));
  REQUIRE(released.ok());
  CHECK_EQ(released.value().lifecycle, FlowLifecycle::Ready);
}

FLOW_TEST(arbitration, explicit_readiness_signal_gates_arbitration) {
  auto scheduler = standard();
  FlowSpec spec;
  spec.flow = FlowId(1);
  spec.auto_ready = false;
  REQUIRE_OK(admit(*scheduler, spec));

  const Result<ArbitrationOutcome> gated = scheduler->arbitrate(10);
  REQUIRE(gated.ok());
  CHECK(gated.value().schedule.empty());
  REQUIRE_EQ(gated.value().deferred.size(), 1u);
  CHECK_EQ(gated.value().deferred[0].reason, DecisionReason::AwaitingReadinessSignal);

  REQUIRE_OK(scheduler->notify_readiness(FlowId(1), Generation(1), ReadinessSignal::Ready, 20));
  const Result<ArbitrationOutcome> admitted = scheduler->arbitrate(20);
  REQUIRE(admitted.ok());
  REQUIRE_EQ(admitted.value().schedule.entries.size(), 1u);
  REQUIRE_OK(scheduler->release_schedule(admitted.value().schedule.schedule,
                                         admitted.value().schedule.generation));

  REQUIRE_OK(scheduler->notify_readiness(FlowId(1), Generation(1), ReadinessSignal::NotReady, 30));
  const Result<ArbitrationOutcome> revoked = scheduler->arbitrate(30);
  REQUIRE(revoked.ok());
  CHECK(revoked.value().schedule.empty());
  REQUIRE_EQ(revoked.value().deferred.size(), 1u);
  CHECK_EQ(revoked.value().deferred[0].reason, DecisionReason::AwaitingReadinessSignal);
}

FLOW_TEST(arbitration, dependencies_gate_readiness_until_authoritative_completion) {
  auto scheduler = standard();
  FlowSpec first;
  first.flow = FlowId(1);
  first.estimated_work = 1;
  REQUIRE_OK(admit(*scheduler, first));

  FlowSpec dependent;
  dependent.flow = FlowId(2);
  dependent.dependencies = {FlowId(1)};
  REQUIRE_OK(admit(*scheduler, dependent));

  const Result<ArbitrationOutcome> first_round = scheduler->arbitrate(0);
  REQUIRE(first_round.ok());
  REQUIRE_EQ(first_round.value().schedule.entries.size(), 1u);
  CHECK_EQ(first_round.value().schedule.entries[0].flow, FlowId(1));
  bool saw_dependency_defer = false;
  for (const DeferredFlow& deferred : first_round.value().deferred) {
    if (deferred.flow == FlowId(2) && deferred.reason == DecisionReason::AwaitingDependency) {
      saw_dependency_defer = true;
    }
  }
  CHECK(saw_dependency_defer);

  const Result<DispatchCycle> cycle =
      run_entry(*scheduler, first_round.value().schedule, first_round.value().schedule.entries[0], 0);
  REQUIRE(cycle.ok());
  CHECK_EQ(cycle.value().outcome.disposition, CommitDisposition::Applied);
  CHECK_EQ(cycle.value().outcome.lifecycle_after, FlowLifecycle::Completed);

  const Result<ArbitrationOutcome> second_round = scheduler->arbitrate(1'000);
  REQUIRE(second_round.ok());
  REQUIRE_EQ(second_round.value().schedule.entries.size(), 1u);
  CHECK_EQ(second_round.value().schedule.entries[0].flow, FlowId(2));
}

FLOW_TEST(arbitration, identical_state_produces_byte_identical_schedules) {
  auto left = standard(1'000'000, 8);
  auto right = standard(1'000'000, 8);
  for (std::uint64_t index = 1; index <= 16; ++index) {
    FlowSpec spec;
    spec.flow = FlowId(index);
    spec.priority = PriorityClassId((index % 4) + 1);
    spec.fairness_group = FairnessGroupId((index % 3) + 1);
    spec.estimated_work = 4 + index;
    spec.weight = 1 + (index % 3);
    REQUIRE_OK(admit(*left, spec));
  }
  // Admission order on the right hand side is deliberately reversed.
  for (std::uint64_t index = 16; index >= 1; --index) {
    FlowSpec spec;
    spec.flow = FlowId(index);
    spec.priority = PriorityClassId((index % 4) + 1);
    spec.fairness_group = FairnessGroupId((index % 3) + 1);
    spec.estimated_work = 4 + index;
    spec.weight = 1 + (index % 3);
    REQUIRE_OK(admit(*right, spec));
    if (index == 1) {
      break;
    }
  }

  const Result<ArbitrationOutcome> left_result = left->arbitrate(0);
  REQUIRE(left_result.ok());
  const Result<ArbitrationOutcome> right_result = right->arbitrate(0);
  REQUIRE(right_result.ok());

  REQUIRE_EQ(left_result.value().schedule.entries.size(), 8u);
  CHECK_EQ(left_result.value().schedule.entries.size(), right_result.value().schedule.entries.size());
  CHECK_EQ(left_result.value().schedule.digest, right_result.value().schedule.digest);
  CHECK_EQ(left_result.value().schedule.canonical_form(),
           right_result.value().schedule.canonical_form());
  CHECK_EQ(compute_schedule_digest(left_result.value().schedule),
           compute_schedule_digest(right_result.value().schedule));
  for (std::size_t index = 0; index < left_result.value().schedule.entries.size(); ++index) {
    CHECK_EQ(left_result.value().schedule.entries[index].flow,
             right_result.value().schedule.entries[index].flow);
    CHECK_EQ(left_result.value().schedule.entries[index].ordering_digest,
             right_result.value().schedule.entries[index].ordering_digest);
  }
}

FLOW_TEST(arbitration, saturating_concurrency_does_not_double_book_a_round) {
  auto scheduler = standard(1'000'000, 4);
  for (std::uint64_t index = 1; index <= 12; ++index) {
    FlowSpec spec;
    spec.flow = FlowId(index);
    spec.priority = PriorityClassId((index % 4) + 1);
    spec.estimated_work = 64;
    REQUIRE_OK(admit(*scheduler, spec));
  }
  const Result<ArbitrationOutcome> first = scheduler->arbitrate(0);
  REQUIRE(first.ok());
  REQUIRE_EQ(first.value().schedule.entries.size(), 4u);

  const Result<ArbitrationOutcome> second = scheduler->arbitrate(0);
  REQUIRE(second.ok());
  CHECK(second.value().schedule.empty());
  for (const ScheduleEntry& entry : first.value().schedule.entries) {
    const Result<FlowSnapshot> snapshot = scheduler->flow(entry.flow);
    REQUIRE(snapshot.ok());
    CHECK_EQ(snapshot.value().lifecycle, FlowLifecycle::Scheduled);
    CHECK_EQ(snapshot.value().outstanding_reserved, entry.quantum);
  }
}

FLOW_TEST(arbitration, strict_priority_orders_entries_deterministically) {
  auto scheduler = standard(1'000'000, 8);
  for (std::uint64_t index = 4; index >= 1; --index) {
    FlowSpec spec;
    spec.flow = FlowId(index);
    spec.priority = PriorityClassId(index);
    REQUIRE_OK(admit(*scheduler, spec));
    if (index == 1) {
      break;
    }
  }
  const Result<ArbitrationOutcome> result = scheduler->arbitrate(0);
  REQUIRE(result.ok());
  REQUIRE_EQ(result.value().schedule.entries.size(), 4u);
  CHECK_EQ(result.value().schedule.entries[0].priority, PriorityClassId(1));
  CHECK_EQ(result.value().schedule.entries[1].priority, PriorityClassId(2));
  CHECK_EQ(result.value().schedule.entries[2].priority, PriorityClassId(3));
  CHECK_EQ(result.value().schedule.entries[3].priority, PriorityClassId(4));
  for (std::uint32_t ordinal = 0; ordinal < 4; ++ordinal) {
    CHECK_EQ(result.value().schedule.entries[ordinal].ordinal, ordinal);
    CHECK_EQ(result.value().schedule.entries[ordinal].tier, 3u);
  }
}

FLOW_TEST(arbitration, equal_priority_breaks_ties_by_flow_identity) {
  auto scheduler = standard(1'000'000, 8);
  for (std::uint64_t index = 5; index >= 1; --index) {
    FlowSpec spec;
    spec.flow = FlowId(index * 10);
    REQUIRE_OK(admit(*scheduler, spec));
    if (index == 1) {
      break;
    }
  }
  const Result<ArbitrationOutcome> result = scheduler->arbitrate(0);
  REQUIRE(result.ok());
  REQUIRE_EQ(result.value().schedule.entries.size(), 5u);
  for (std::size_t index = 1; index < result.value().schedule.entries.size(); ++index) {
    CHECK(result.value().schedule.entries[index - 1].flow <
          result.value().schedule.entries[index].flow);
  }
}

FLOW_TEST(arbitration, resource_capacity_bounds_the_service_window) {
  auto scheduler = standard(4, 4);
  FlowSpec first;
  first.flow = FlowId(1);
  first.service_quantum = 4;
  first.estimated_work = 16;
  REQUIRE_OK(admit(*scheduler, first));
  FlowSpec second;
  second.flow = FlowId(2);
  second.service_quantum = 4;
  second.estimated_work = 16;
  REQUIRE_OK(admit(*scheduler, second));

  const Result<ArbitrationOutcome> saturated = scheduler->arbitrate(0);
  REQUIRE(saturated.ok());
  REQUIRE_EQ(saturated.value().schedule.entries.size(), 1u);
  CHECK_EQ(saturated.value().schedule.entries[0].quantum, 4u);
  bool exhausted = false;
  for (const DeferredFlow& deferred : saturated.value().deferred) {
    if (deferred.flow == FlowId(2) &&
        deferred.reason == DecisionReason::ResourceCapacityExhausted) {
      exhausted = true;
    }
  }
  CHECK(exhausted);

  // Capacity refills in the next interval.
  const Result<ArbitrationOutcome> refilled = scheduler->arbitrate(1'000);
  REQUIRE(refilled.ok());
  REQUIRE_EQ(refilled.value().schedule.entries.size(), 1u);
  CHECK_EQ(refilled.value().schedule.entries[0].flow, FlowId(2));
  CHECK_EQ(refilled.value().schedule.entries[0].quantum, 4u);
}

FLOW_TEST(arbitration, concurrency_ceiling_defers_without_preemption) {
  SchedulerOptions options = standard_options();
  options.policy.preemption = PreemptionMode::None;
  Result<std::unique_ptr<Scheduler>> created = make_standard_scheduler(options, 1'000'000, 1);
  REQUIRE(created.ok());
  auto scheduler = std::move(created).value();

  FlowSpec first;
  first.flow = FlowId(1);
  first.estimated_work = 64;
  REQUIRE_OK(admit(*scheduler, first));
  FlowSpec second;
  second.flow = FlowId(2);
  second.estimated_work = 64;
  REQUIRE_OK(admit(*scheduler, second));

  const Result<ArbitrationOutcome> result = scheduler->arbitrate(0);
  REQUIRE(result.ok());
  REQUIRE_EQ(result.value().schedule.entries.size(), 1u);
  CHECK_EQ(result.value().schedule.entries[0].flow, FlowId(1));
  bool deferred = false;
  for (const DeferredFlow& entry : result.value().deferred) {
    if (entry.flow == FlowId(2) && entry.reason == DecisionReason::ConcurrencyLimitReached) {
      deferred = true;
    }
  }
  CHECK(deferred);
}

FLOW_TEST(arbitration, reservation_window_gates_and_then_closes) {
  auto scheduler = standard();
  ReservationDescriptor reservation;
  reservation.reservation = ReservationId(1);
  reservation.generation = Generation(1);
  reservation.resource = ResourceId(1);
  reservation.resource_generation = Generation(1);
  reservation.window_begin = 100;
  reservation.window_end = 200;
  reservation.capacity = 8;
  reservation.provenance = external_provenance(1);
  REQUIRE_OK(scheduler->register_reservation(reservation));

  FlowSpec spec;
  spec.flow = FlowId(1);
  spec.estimated_work = 8;
  spec.service_quantum = 2;
  REQUIRE_OK(admit(*scheduler, spec));
  FlowDescriptor bound = make_flow(spec);
  bound.generation = Generation(2);
  bound.reservation = {ReservationId(1), Generation(1)};
  REQUIRE_OK(scheduler->update_flow(bound));

  const Result<ArbitrationOutcome> before = scheduler->arbitrate(0);
  REQUIRE(before.ok());
  CHECK(before.value().schedule.empty());
  bool waiting_window = false;
  for (const DeferredFlow& entry : before.value().deferred) {
    if (entry.flow == FlowId(1) &&
        entry.reason == DecisionReason::AwaitingReservationWindow) {
      waiting_window = true;
    }
  }
  CHECK(waiting_window);

  const Result<ArbitrationOutcome> inside = scheduler->arbitrate(150);
  REQUIRE(inside.ok());
  REQUIRE_EQ(inside.value().schedule.entries.size(), 1u);
  CHECK_EQ(inside.value().schedule.entries[0].reservation, ReservationId(1));
  CHECK_EQ(inside.value().schedule.entries[0].reservation_end, 200u);
  REQUIRE_OK(scheduler->release_schedule(inside.value().schedule.schedule,
                                         inside.value().schedule.generation));

  const Result<ArbitrationOutcome> after = scheduler->arbitrate(200);
  REQUIRE(after.ok());
  CHECK(after.value().schedule.empty());
  bool closed = false;
  for (const DeferredFlow& entry : after.value().deferred) {
    if (entry.flow == FlowId(1) &&
        entry.reason == DecisionReason::ReservationWindowClosed) {
      closed = true;
    }
  }
  CHECK(closed);
}

FLOW_TEST(arbitration, exclusive_reservation_excludes_unbound_flows) {
  auto scheduler = standard();
  ReservationDescriptor reservation;
  reservation.reservation = ReservationId(1);
  reservation.generation = Generation(1);
  reservation.resource = ResourceId(1);
  reservation.resource_generation = Generation(1);
  reservation.window_begin = 100;
  reservation.window_end = 200;
  reservation.capacity = 8;
  reservation.exclusive = true;
  reservation.provenance = external_provenance(1);
  REQUIRE_OK(scheduler->register_reservation(reservation));

  FlowSpec spec;
  spec.flow = FlowId(1);
  REQUIRE_OK(admit(*scheduler, spec));

  const Result<ArbitrationOutcome> inside = scheduler->arbitrate(150);
  REQUIRE(inside.ok());
  CHECK(inside.value().schedule.empty());

  const Result<ArbitrationOutcome> outside = scheduler->arbitrate(900);
  REQUIRE(outside.ok());
  REQUIRE_EQ(outside.value().schedule.entries.size(), 1u);
}

FLOW_TEST(arbitration, tick_must_move_forward) {
  auto scheduler = standard();
  FlowSpec spec;
  spec.flow = FlowId(1);
  REQUIRE_OK(admit(*scheduler, spec));
  const Result<ArbitrationOutcome> forward = scheduler->arbitrate(100);
  REQUIRE(forward.ok());
  const AccountingReport before = scheduler->accounting();
  const Result<ArbitrationOutcome> backwards = scheduler->arbitrate(99);
  CHECK(!backwards.ok());
  CHECK_EQ(backwards.code(), ErrorCode::InvalidArgument);
  const AccountingReport after = scheduler->accounting();
  CHECK_EQ(before.arbitration_rounds, after.arbitration_rounds);
  CHECK_OK(as_status(after.validate()));
}

FLOW_TEST(arbitration, undelivered_entries_are_reclaimed_after_the_delivery_horizon) {
  SchedulerOptions options = standard_options();
  options.policy.dispatch_delivery_horizon = 100;
  Result<std::unique_ptr<Scheduler>> created = make_standard_scheduler(options);
  REQUIRE(created.ok());
  auto scheduler = std::move(created).value();

  FlowSpec spec;
  spec.flow = FlowId(1);
  spec.estimated_work = 64;
  REQUIRE_OK(admit(*scheduler, spec));

  const Result<ArbitrationOutcome> issued = scheduler->arbitrate(0);
  REQUIRE(issued.ok());
  REQUIRE_EQ(issued.value().schedule.entries.size(), 1u);
  const Result<FlowSnapshot> scheduled = scheduler->flow(FlowId(1));
  REQUIRE(scheduled.ok());
  CHECK_EQ(scheduled.value().lifecycle, FlowLifecycle::Scheduled);

  const Result<ArbitrationOutcome> reclaimed = scheduler->arbitrate(101);
  REQUIRE(reclaimed.ok());
  REQUIRE_EQ(reclaimed.value().delivery_reclaims.size(), 1u);
  CHECK_EQ(reclaimed.value().delivery_reclaims[0], FlowId(1));
}

FLOW_TEST(arbitration, dispatch_is_refused_after_the_flow_generation_moves_on) {
  auto scheduler = standard();
  FlowSpec spec;
  spec.flow = FlowId(1);
  spec.estimated_work = 64;
  REQUIRE_OK(admit(*scheduler, spec));
  const Result<ArbitrationOutcome> issued = scheduler->arbitrate(0);
  REQUIRE(issued.ok());
  REQUIRE_EQ(issued.value().schedule.entries.size(), 1u);

  FlowDescriptor updated = make_flow(spec);
  updated.generation = Generation(2);
  updated.service_quantum = 2;
  REQUIRE_OK(scheduler->update_flow(updated));

  const Result<DispatchTicket> ticket = scheduler->begin_dispatch(
      issued.value().schedule.schedule, issued.value().schedule.generation,
      issued.value().schedule.entries[0].ordinal, test_worker(), test_boot(), 0);
  CHECK(!ticket.ok());
  CHECK(ticket.code() == ErrorCode::StaleFlowGeneration || ticket.code() == ErrorCode::InvalidState);
}

FLOW_TEST(arbitration, issued_ticket_revalidates_and_detects_policy_change) {
  auto scheduler = standard();
  FlowSpec spec;
  spec.flow = FlowId(1);
  spec.estimated_work = 64;
  REQUIRE_OK(admit(*scheduler, spec));
  const Result<ArbitrationOutcome> issued = scheduler->arbitrate(0);
  REQUIRE(issued.ok());
  REQUIRE_EQ(issued.value().schedule.entries.size(), 1u);

  const Result<DispatchTicket> ticket = scheduler->begin_dispatch(
      issued.value().schedule.schedule, issued.value().schedule.generation,
      issued.value().schedule.entries[0].ordinal, test_worker(), test_boot(), 0);
  REQUIRE(ticket.ok());
  CHECK_OK(as_status(scheduler->revalidate(ticket.value(), 0)));

  const Result<DispatchTicket> second = scheduler->begin_dispatch(
      issued.value().schedule.schedule, issued.value().schedule.generation,
      issued.value().schedule.entries[0].ordinal, test_worker(), test_boot(), 0);
  CHECK(!second.ok());
  CHECK_EQ(second.code(), ErrorCode::InvalidState);

  SchedulingPolicy replacement = standard_options().policy;
  replacement.generation = Generation(2);
  REQUIRE_OK(scheduler->install_policy(replacement));
  CHECK_ERROR(as_status(scheduler->revalidate(ticket.value(), 0)),
              ErrorCode::StalePolicyGeneration);
}

FLOW_TEST(arbitration, explanation_reports_decision_history) {
  auto scheduler = standard();
  FlowSpec spec;
  spec.flow = FlowId(1);
  spec.estimated_work = 4;
  spec.service_quantum = 1;
  REQUIRE_OK(admit(*scheduler, spec));
  const Result<ArbitrationOutcome> issued = scheduler->arbitrate(0);
  REQUIRE(issued.ok());
  REQUIRE_EQ(issued.value().schedule.entries.size(), 1u);
  const Result<DispatchCycle> cycle =
      run_entry(*scheduler, issued.value().schedule, issued.value().schedule.entries[0], 0);
  REQUIRE(cycle.ok());
  CHECK_EQ(cycle.value().outcome.disposition, CommitDisposition::Applied);

  const Result<FlowExplanation> explanation = scheduler->explain_flow(FlowId(1));
  REQUIRE(explanation.ok());
  CHECK_EQ(explanation.value().flow, FlowId(1));
  CHECK(!explanation.value().history.empty());
  CHECK(explanation.value().history.size() <= 32u);
  for (const ExplanationStep& step : explanation.value().history) {
    CHECK(step.detail.size() <= kMaxExplanationBytes);
  }

  const Result<ScheduleExplanation> schedule_explanation =
      scheduler->explain_schedule(issued.value().schedule.schedule);
  REQUIRE(schedule_explanation.ok());
  REQUIRE_EQ(schedule_explanation.value().entries.size(), 1u);
  CHECK_EQ(schedule_explanation.value().digest, issued.value().schedule.digest);

  const Result<Schedule> round_trip = scheduler->schedule(issued.value().schedule.schedule);
  REQUIRE(round_trip.ok());
  CHECK(round_trip.value() == issued.value().schedule);
}

FLOW_TEST(arbitration, release_schedule_returns_flows_to_ready) {
  auto scheduler = standard();
  FlowSpec spec;
  spec.flow = FlowId(1);
  spec.estimated_work = 64;
  REQUIRE_OK(admit(*scheduler, spec));
  const Result<ArbitrationOutcome> issued = scheduler->arbitrate(0);
  REQUIRE(issued.ok());
  REQUIRE_EQ(issued.value().schedule.entries.size(), 1u);
  REQUIRE_OK(scheduler->release_schedule(issued.value().schedule.schedule,
                                         issued.value().schedule.generation));
  const Result<FlowSnapshot> snapshot = scheduler->flow(FlowId(1));
  REQUIRE(snapshot.ok());
  CHECK_EQ(snapshot.value().lifecycle, FlowLifecycle::Ready);
  CHECK_EQ(snapshot.value().outstanding_reserved, 0u);
  CHECK_OK(as_status(scheduler->accounting().validate()));
}

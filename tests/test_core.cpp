// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#include "test_support.hpp"

namespace {

using namespace flow_scheduler;
using namespace fls_test;

}  // namespace

FLOW_TEST(core, identity_types_are_distinct_and_ordered) {
  const FlowId flow(7);
  const ScheduleId schedule(7);
  CHECK_EQ(flow.value(), 7u);
  CHECK_EQ(schedule.value(), 7u);
  CHECK(flow.valid());
  CHECK(!FlowId{}.valid());
  CHECK(FlowId(3) < FlowId(4));
  CHECK(FlowId(4) != FlowId(3));
  CHECK_EQ(std::string(to_string(flow)), std::string("flow#7"));
  CHECK_EQ(std::string(to_string(schedule)), std::string("schedule#7"));
  CHECK_EQ(std::string(to_string(FabricEpoch(2))), std::string("epoch#2"));
}

FLOW_TEST(core, generation_advance_fails_at_the_numeric_limit) {
  Generation generation(1);
  REQUIRE_OK(advance(generation));
  const Result<Generation> next = advance(generation);
  CHECK_EQ(next.value().value(), 2u);

  const Generation last(std::numeric_limits<std::uint64_t>::max());
  CHECK_ERROR(as_status(advance(last)), ErrorCode::ArithmeticOverflow);
}

FLOW_TEST(core, checked_arithmetic_rejects_overflow_and_underflow) {
  CHECK_EQ(checked_add(1, 2).value(), 3u);
  CHECK_ERROR(as_status(checked_add(std::numeric_limits<std::uint64_t>::max(), 1)),
              ErrorCode::ArithmeticOverflow);
  CHECK_ERROR(as_status(checked_sub(0, 1)), ErrorCode::ArithmeticOverflow);
  CHECK_ERROR(as_status(checked_mul(std::numeric_limits<std::uint64_t>::max(), 2)),
              ErrorCode::ArithmeticOverflow);
  CHECK_EQ(checked_mul(0, std::numeric_limits<std::uint64_t>::max()).value(), 0u);
  CHECK_EQ(checked_add(1, 2, 3).value(), 6u);
  CHECK_EQ(checked_mul(2, 3, 4).value(), 24u);
  CHECK_EQ(checked_narrow<std::uint16_t>(65535).value(), 65535u);
  CHECK_ERROR(as_status(checked_narrow<std::uint16_t>(65536)), ErrorCode::OutOfRange);
  CHECK_ERROR(as_status(checked_narrow<std::uint16_t>(-1)), ErrorCode::OutOfRange);
  CHECK_EQ(saturating_add(std::numeric_limits<std::uint64_t>::max(), 1),
           std::numeric_limits<std::uint64_t>::max());
  CHECK_EQ(saturating_sub(0, 5), 0u);
}

FLOW_TEST(core, error_messages_are_bounded) {
  const std::string huge(4096, 'x');
  const Error error(ErrorCode::Internal, huge);
  CHECK_EQ(error.message().size(), kMaxErrorMessageBytes);
  CHECK_EQ(std::string(to_string(ErrorCode::StaleEpoch)), std::string("stale-epoch"));
  CHECK(is_staleness(ErrorCode::StaleEpoch));
  CHECK(!is_staleness(ErrorCode::NotReady));
  CHECK(is_transient(ErrorCode::NotReady));
  CHECK(!is_transient(ErrorCode::StaleEpoch));
}

FLOW_TEST(core, result_carries_value_or_error) {
  const Result<std::uint64_t> value(42);
  CHECK(value.ok());
  CHECK_EQ(value.value(), 42u);
  const Result<std::uint64_t> failure(Error(ErrorCode::NotFound, "missing"));
  CHECK(!failure.ok());
  CHECK_EQ(failure.code(), ErrorCode::NotFound);
  CHECK_EQ(failure.value_or(9), 9u);
}

FLOW_TEST(core, crc32c_matches_known_vectors) {
  // CRC-32C of the ASCII string "123456789" is 0xE3069283.
  CHECK_EQ(crc32c(std::string_view("123456789")), 0xE3069283u);
  CHECK_EQ(crc32c(std::string_view("")), 0u);
  const std::uint32_t partial = crc32c(std::string_view("12345"));
  CHECK_EQ(crc32c_extend(partial, "6789", 4), 0xE3069283u);
}

FLOW_TEST(core, digest_and_rng_are_deterministic) {
  Digest64 first;
  first.add_u64(1);
  first.add_string("abc");
  Digest64 second;
  second.add_u64(1);
  second.add_string("abc");
  CHECK_EQ(first.value(), second.value());

  Digest64 different;
  different.add_u64(2);
  different.add_string("abc");
  CHECK_NE(first.value(), different.value());

  Rng left(12345);
  Rng right(12345);
  for (int index = 0; index < 64; ++index) {
    CHECK_EQ(left.next_u64(), right.next_u64());
  }
  Rng bounded(7);
  for (int index = 0; index < 512; ++index) {
    CHECK(bounded.next_below(10) < 10);
  }
  CHECK_EQ(bounded.next_below(0), 0u);
  CHECK_EQ(bounded.next_range(5, 5), 5u);
}

FLOW_TEST(core, manual_clock_is_explicit) {
  ManualClock clock;
  CHECK_EQ(clock.now(), 0u);
  clock.advance(500);
  CHECK_EQ(clock.now(), 500u);
  clock.set(7);
  CHECK_EQ(clock.now(), 7u);
  CHECK_EQ(ticks_between(10, 3), 0u);
  CHECK_EQ(ticks_between(3, 10), 7u);
}

FLOW_TEST(core, descriptor_validation_rejects_malformed_input) {
  FlowSpec spec;
  spec.flow = FlowId(1);
  CHECK_OK(as_status(validate(make_flow(spec))));

  FlowDescriptor zero_work = make_flow(spec);
  zero_work.estimated_work = 0;
  CHECK_ERROR(as_status(validate(zero_work)), ErrorCode::OutOfRange);

  FlowDescriptor zero_quantum = make_flow(spec);
  zero_quantum.service_quantum = 0;
  CHECK_ERROR(as_status(validate(zero_quantum)), ErrorCode::OutOfRange);

  FlowDescriptor bad_deadline = make_flow(spec);
  bad_deadline.deadline_tick = bad_deadline.release_tick;
  CHECK_ERROR(as_status(validate(bad_deadline)), ErrorCode::InvalidArgument);

  FlowDescriptor no_window = make_flow(spec);
  no_window.min_service = 1;
  no_window.min_service_window = 0;
  CHECK_ERROR(as_status(validate(no_window)), ErrorCode::InvalidArgument);

  FlowDescriptor self_dependency = make_flow(spec);
  self_dependency.dependencies.push_back(self_dependency.flow);
  CHECK_ERROR(as_status(validate(self_dependency)), ErrorCode::InvalidArgument);

  FlowDescriptor duplicate_dependency = make_flow(spec);
  duplicate_dependency.dependencies.push_back(FlowId(2));
  duplicate_dependency.dependencies.push_back(FlowId(2));
  CHECK_ERROR(as_status(validate(duplicate_dependency)), ErrorCode::InvalidArgument);

  FlowDescriptor too_many = make_flow(spec);
  too_many.dependencies.assign(kMaxDependenciesPerFlow + 1, FlowId(2));
  CHECK_ERROR(as_status(validate(too_many)), ErrorCode::Bounded);

  FlowDescriptor unbound = make_flow(spec);
  unbound.flow = FlowId{};
  CHECK_ERROR(as_status(validate(unbound)), ErrorCode::InvalidArgument);

  FlowDescriptor partial_reservation = make_flow(spec);
  partial_reservation.reservation.reservation = ReservationId(1);
  CHECK_ERROR(as_status(validate(partial_reservation)), ErrorCode::InvalidArgument);

  FlowDescriptor zero_weight = make_flow(spec);
  zero_weight.weight = 0;
  CHECK_ERROR(as_status(validate(zero_weight)), ErrorCode::OutOfRange);

  FlowDescriptor unknown_signal = make_flow(spec);
  unknown_signal.readiness_signal = 7;
  CHECK_ERROR(as_status(validate(unknown_signal)), ErrorCode::InvalidArgument);

  FlowDescriptor no_provenance = make_flow(spec);
  no_provenance.provenance = Provenance{};
  CHECK_ERROR(as_status(validate(no_provenance)), ErrorCode::InvalidArgument);
}

FLOW_TEST(core, descriptor_digest_is_order_independent_for_dependencies) {
  FlowDescriptor first = make_flow();
  first.dependencies = {FlowId(3), FlowId(2)};
  FlowDescriptor second = make_flow();
  second.dependencies = {FlowId(2), FlowId(3)};
  CHECK_EQ(digest_of(first), digest_of(second));
}

FLOW_TEST(core, policy_validation_bounds) {
  SchedulingPolicy policy = standard_options().policy;
  CHECK_OK(as_status(validate(policy)));

  SchedulingPolicy no_starvation_bound = policy;
  no_starvation_bound.starvation_bound_ticks = 0;
  CHECK_ERROR(as_status(validate(no_starvation_bound)), ErrorCode::OutOfRange);

  SchedulingPolicy tiny_history = policy;
  tiny_history.retained_schedule_history = 1;
  CHECK_ERROR(as_status(validate(tiny_history)), ErrorCode::OutOfRange);

  SchedulingPolicy zero_horizon = policy;
  zero_horizon.dispatch_delivery_horizon = 0;
  CHECK_ERROR(as_status(validate(zero_horizon)), ErrorCode::OutOfRange);

  SchedulingPolicy zero_flows = policy;
  zero_flows.max_flows = 0;
  CHECK_ERROR(as_status(validate(zero_flows)), ErrorCode::OutOfRange);

  SchedulingPolicy no_provenance = policy;
  no_provenance.provenance = Provenance{};
  CHECK_ERROR(as_status(validate(no_provenance)), ErrorCode::InvalidArgument);

  CHECK_NE(digest_of(policy), digest_of(no_starvation_bound));
}

FLOW_TEST(core, reservation_window_semantics) {
  ReservationDescriptor reservation;
  reservation.reservation = ReservationId(1);
  reservation.generation = Generation(1);
  reservation.resource = ResourceId(1);
  reservation.resource_generation = Generation(1);
  reservation.window_begin = 100;
  reservation.window_end = 200;
  reservation.capacity = 8;
  reservation.provenance = external_provenance(1);
  CHECK_OK(as_status(validate(reservation)));
  CHECK(!window_contains(reservation, 99));
  CHECK(window_contains(reservation, 100));
  CHECK(window_contains(reservation, 199));
  CHECK(!window_contains(reservation, 200));
  CHECK_EQ(reservation_grant_at(reservation, 150), 8u);
  CHECK_EQ(reservation_grant_at(reservation, 200), 0u);

  ReservationDescriptor inverted = reservation;
  inverted.window_end = inverted.window_begin;
  CHECK_ERROR(as_status(validate(inverted)), ErrorCode::InvalidArgument);
}

FLOW_TEST(core, accounting_buckets_partition_every_lifecycle) {
  CHECK_EQ(bucket_of(FlowLifecycle::Waiting), AccountingBucket::Waiting);
  CHECK_EQ(bucket_of(FlowLifecycle::Ready), AccountingBucket::Ready);
  CHECK_EQ(bucket_of(FlowLifecycle::Scheduled), AccountingBucket::Scheduled);
  CHECK_EQ(bucket_of(FlowLifecycle::Dispatched), AccountingBucket::InFlight);
  CHECK_EQ(bucket_of(FlowLifecycle::Running), AccountingBucket::InFlight);
  CHECK_EQ(bucket_of(FlowLifecycle::CompletionReported), AccountingBucket::InFlight);
  CHECK_EQ(bucket_of(FlowLifecycle::Cancelling), AccountingBucket::InFlight);
  CHECK_EQ(bucket_of(FlowLifecycle::Preempting), AccountingBucket::InFlight);
  CHECK_EQ(bucket_of(FlowLifecycle::Completed), AccountingBucket::Completed);
  CHECK_EQ(bucket_of(FlowLifecycle::Cancelled), AccountingBucket::Cancelled);
  CHECK_EQ(bucket_of(FlowLifecycle::Failed), AccountingBucket::Failed);
  CHECK_EQ(bucket_of(FlowLifecycle::Ambiguous), AccountingBucket::Ambiguous);
  CHECK(is_terminal(FlowLifecycle::Completed));
  CHECK(!is_terminal(FlowLifecycle::Ambiguous));
  CHECK(holds_open_work(FlowLifecycle::Running));
  CHECK(!holds_open_work(FlowLifecycle::Scheduled));
}

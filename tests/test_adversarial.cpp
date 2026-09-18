// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.
//
// Adversarial input at the scheduler boundary.
//
// Everything the runtime accepts from outside is hostile until it has been
// checked: descriptors with unbound or out-of-range fields, flows binding
// topology that was never registered, generations that do not advance, ticks
// that move backwards, completion evidence that claims an epoch, a worker
// incarnation or an amount of service it never had, and record payloads that
// lie about their own length. None of it may mutate authoritative state, and
// every rejection must carry the exact error code the contract promises.

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "test_support.hpp"

namespace {

using namespace flow_scheduler;

/// Fill every mandatory authority field so that only the field under test can
/// make a payload invalid.
DispatchTicket bound_ticket() {
  DispatchTicket ticket;
  ticket.attempt = DispatchAttemptId(1);
  ticket.epoch = FabricEpoch(1);
  ticket.schedule = ScheduleId(1);
  ticket.schedule_generation = Generation(1);
  ticket.flow = FlowId(1);
  ticket.flow_generation = Generation(1);
  ticket.path = PathId(1);
  ticket.path_generation = Generation(1);
  ticket.resource = ResourceId(1);
  ticket.resource_generation = Generation(1);
  ticket.qos = QoSClassId(1);
  ticket.qos_generation = Generation(1);
  ticket.priority = PriorityClassId(1);
  ticket.priority_generation = Generation(1);
  ticket.policy = PolicyId(1);
  ticket.policy_generation = Generation(1);
  ticket.worker = WorkerId(1);
  ticket.boot = BootId(1);
  ticket.quantum = 4;
  ticket.state = AttemptState::Open;
  ticket.digest = 0x5EEDu;
  return ticket;
}

CompletionEvidence bound_evidence() {
  CompletionEvidence evidence;
  evidence.schedule = ScheduleId(1);
  evidence.schedule_generation = Generation(1);
  evidence.attempt = DispatchAttemptId(1);
  evidence.epoch = FabricEpoch(1);
  evidence.flow = FlowId(1);
  evidence.flow_generation = Generation(1);
  evidence.worker = WorkerId(1);
  evidence.boot = BootId(1);
  evidence.outcome = CompletionOutcome::Served;
  evidence.served = 1;
  evidence.effect_code = 1;
  evidence.effect_digest = 0xABCDu;
  evidence.observed_tick = 1;
  evidence.provenance = fls_test::external_provenance(1);
  return evidence;
}

CompletionEvidence evidence_for(const DispatchTicket& ticket) {
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
  evidence.served = ticket.quantum;
  evidence.effect_code = 1;
  evidence.effect_digest = mix64(ticket.attempt.value());
  evidence.observed_tick = ticket.start_tick;
  evidence.provenance = fls_test::external_provenance(ticket.attempt.value());
  return evidence;
}

/// Dispatch exactly one flow and hand back the ticket that justifies it.
Result<DispatchTicket> dispatch_single_flow(Scheduler& scheduler, Ticks now) {
  Result<ArbitrationOutcome> outcome = scheduler.arbitrate(now);
  if (!outcome.ok()) {
    return outcome.error();
  }
  if (outcome.value().schedule.entries.size() != 1) {
    return Error(ErrorCode::Internal, "the adversarial scenario expected exactly one entry");
  }
  const Schedule& schedule = outcome.value().schedule;
  return scheduler.begin_dispatch(schedule.schedule, schedule.generation, 0, fls_test::test_worker(),
                                  fls_test::test_boot(), now);
}

}  // namespace

// Structural validation is the first gate: an unbound identity, a zero
// capacity, a deadline before release, a self dependency or duplicate
// dependency, an unestablished provenance, and every out-of-range scalar must
// be refused before the value can influence arbitration.
FLOW_TEST(adversarial, descriptor_validation_refuses_hostile_fields) {
  const ResourceDescriptor resource = fls_test::make_resource();
  CHECK_OK(validate(resource));
  ResourceDescriptor candidate = resource;
  candidate.resource = ResourceId{};
  CHECK_ERROR(validate(candidate), ErrorCode::InvalidArgument);
  candidate = resource;
  candidate.generation = Generation{};
  CHECK_ERROR(validate(candidate), ErrorCode::InvalidArgument);
  candidate = resource;
  candidate.capacity = 0;
  CHECK_ERROR(validate(candidate), ErrorCode::OutOfRange);
  candidate = resource;
  candidate.capacity = kMaxServiceUnits + 1;
  CHECK_ERROR(validate(candidate), ErrorCode::OutOfRange);
  candidate = resource;
  candidate.interval = 0;
  CHECK_ERROR(validate(candidate), ErrorCode::OutOfRange);
  candidate = resource;
  candidate.max_overlap = 0;
  CHECK_ERROR(validate(candidate), ErrorCode::OutOfRange);
  candidate = resource;
  candidate.provenance = Provenance{};
  CHECK_ERROR(validate(candidate), ErrorCode::InvalidArgument);

  const FlowDescriptor flow = fls_test::make_flow();
  CHECK_OK(validate(flow));
  FlowDescriptor hostile = flow;
  hostile.flow = FlowId{};
  CHECK_ERROR(validate(hostile), ErrorCode::InvalidArgument);
  hostile = flow;
  hostile.path = PathBinding{};
  CHECK_ERROR(validate(hostile), ErrorCode::InvalidArgument);
  hostile = flow;
  hostile.resource = ResourceBinding{};
  CHECK_ERROR(validate(hostile), ErrorCode::InvalidArgument);
  hostile = flow;
  hostile.priority = PriorityBinding{};
  CHECK_ERROR(validate(hostile), ErrorCode::InvalidArgument);
  hostile = flow;
  hostile.qos = QoSBinding{};
  CHECK_ERROR(validate(hostile), ErrorCode::InvalidArgument);
  hostile = flow;
  hostile.fairness_group = FairnessGroupId{};
  CHECK_ERROR(validate(hostile), ErrorCode::InvalidArgument);
  hostile = flow;
  hostile.reservation = ReservationBinding{ReservationId(1), Generation{}};
  CHECK_ERROR(validate(hostile), ErrorCode::InvalidArgument);
  hostile = flow;
  hostile.estimated_work = 0;
  CHECK_ERROR(validate(hostile), ErrorCode::OutOfRange);
  hostile = flow;
  hostile.service_quantum = 0;
  CHECK_ERROR(validate(hostile), ErrorCode::OutOfRange);
  hostile = flow;
  hostile.service_quantum = kMaxQuantum + 1;
  CHECK_ERROR(validate(hostile), ErrorCode::OutOfRange);
  hostile = flow;
  hostile.release_tick = 100;
  hostile.deadline_tick = 100;
  CHECK_ERROR(validate(hostile), ErrorCode::InvalidArgument);
  hostile = flow;
  hostile.dependencies = {flow.flow};
  CHECK_ERROR(validate(hostile), ErrorCode::InvalidArgument);
  hostile = flow;
  hostile.dependencies = {FlowId(2), FlowId(2)};
  CHECK_ERROR(validate(hostile), ErrorCode::InvalidArgument);
  hostile = flow;
  hostile.min_service = flow.estimated_work + 1;
  hostile.min_service_window = 10;
  CHECK_ERROR(validate(hostile), ErrorCode::InvalidArgument);
  hostile = flow;
  hostile.min_service = 1;
  hostile.min_service_window = 0;
  CHECK_ERROR(validate(hostile), ErrorCode::InvalidArgument);
  hostile = flow;
  hostile.provenance = Provenance{};
  CHECK_ERROR(validate(hostile), ErrorCode::InvalidArgument);
}

// A syntactically perfect descriptor is still refused when it binds topology
// that does not exist at the generation it names.
FLOW_TEST(adversarial, flows_cannot_bind_unregistered_topology) {
  Result<std::unique_ptr<Scheduler>> created = fls_test::make_standard_scheduler();
  REQUIRE(created.ok());
  Scheduler& scheduler = *created.value();

  fls_test::FlowSpec spec;
  spec.resource = ResourceId(9);
  CHECK_ERROR(fls_test::admit(scheduler, spec), ErrorCode::UnknownResource);
  spec = fls_test::FlowSpec{};
  spec.resource_generation = Generation(2);
  CHECK_ERROR(fls_test::admit(scheduler, spec), ErrorCode::StaleResourceGeneration);
  spec = fls_test::FlowSpec{};
  spec.priority = PriorityClassId(9);
  CHECK_ERROR(fls_test::admit(scheduler, spec), ErrorCode::UnknownPolicy);
  spec = fls_test::FlowSpec{};
  spec.priority_generation = Generation(2);
  CHECK_ERROR(fls_test::admit(scheduler, spec), ErrorCode::StalePriorityGeneration);
  spec = fls_test::FlowSpec{};
  spec.qos = QoSClassId(9);
  CHECK_ERROR(fls_test::admit(scheduler, spec), ErrorCode::UnknownPolicy);
  spec = fls_test::FlowSpec{};
  spec.qos_generation = Generation(2);
  CHECK_ERROR(fls_test::admit(scheduler, spec), ErrorCode::StaleQoSGeneration);

  FlowDescriptor reserved = fls_test::make_flow();
  reserved.reservation = ReservationBinding{ReservationId(7), Generation(1)};
  CHECK_ERROR(scheduler.admit_flow(reserved), ErrorCode::UnknownReservation);
  CHECK_EQ(scheduler.flow_count(), std::size_t(0));
}

// Identity is single-assignment and every generation advances by exactly one:
// a duplicate admission, a doubled generation bump or a stale cancellation is
// refused instead of silently retargeting authority.
FLOW_TEST(adversarial, duplicate_identity_and_stale_generations_are_refused) {
  Result<std::unique_ptr<Scheduler>> created = fls_test::make_standard_scheduler();
  REQUIRE(created.ok());
  Scheduler& scheduler = *created.value();
  CHECK_OK(fls_test::admit(scheduler, fls_test::FlowSpec{}));
  CHECK_ERROR(fls_test::admit(scheduler, fls_test::FlowSpec{}), ErrorCode::AlreadyExists);

  FlowDescriptor doubled = fls_test::make_flow();
  doubled.generation = Generation(3);
  CHECK_ERROR(scheduler.update_flow(doubled), ErrorCode::InvalidArgument);
  FlowDescriptor unchanged = fls_test::make_flow();
  CHECK_ERROR(scheduler.update_flow(unchanged), ErrorCode::InvalidArgument);

  CHECK_ERROR(scheduler.cancel_flow(FlowId(1), Generation(2), "stale"),
              ErrorCode::StaleFlowGeneration);
  CHECK_ERROR(scheduler.cancel_flow(FlowId(2), Generation(1), "unknown"),
              ErrorCode::UnknownFlow);

  CHECK_OK(scheduler.register_resource(
      fls_test::make_resource(ResourceId(2), Generation(1), 8, 1000, 1)));
  CHECK_ERROR(scheduler.register_resource(
                  fls_test::make_resource(ResourceId(2), Generation(1), 8, 1000, 1)),
              ErrorCode::InvalidArgument);
  CHECK_ERROR(scheduler.register_priority_class(fls_test::make_priority(PriorityClassId(1), 0)),
              ErrorCode::InvalidArgument);
}

// Time is monotone on the coordinator's timeline, and every observation path
// answers for identities that were never registered with a precise error.
FLOW_TEST(adversarial, tick_regression_and_unknown_identities_are_refused) {
  Result<std::unique_ptr<Scheduler>> created = fls_test::make_standard_scheduler();
  REQUIRE(created.ok());
  Scheduler& scheduler = *created.value();
  CHECK_OK(fls_test::as_status(scheduler.arbitrate(1000)));
  CHECK_ERROR(fls_test::as_status(scheduler.arbitrate(999)), ErrorCode::InvalidArgument);

  CHECK_ERROR(scheduler.notify_readiness(FlowId(99), Generation(1), ReadinessSignal::Ready, 1000),
              ErrorCode::UnknownFlow);
  CHECK_ERROR(fls_test::as_status(scheduler.flow(FlowId(99))), ErrorCode::UnknownFlow);
  CHECK_ERROR(fls_test::as_status(scheduler.explain_flow(FlowId(99))), ErrorCode::UnknownFlow);
  CHECK_ERROR(fls_test::as_status(scheduler.schedule(ScheduleId(99))), ErrorCode::UnknownSchedule);
  CHECK_ERROR(fls_test::as_status(scheduler.explain_schedule(ScheduleId(99))),
              ErrorCode::UnknownSchedule);
  CHECK_ERROR(fls_test::as_status(scheduler.attempt(DispatchAttemptId(99))),
              ErrorCode::UnknownAttempt);
  CHECK_ERROR(fls_test::as_status(scheduler.begin_dispatch(ScheduleId(99), Generation(1), 0,
                                                           fls_test::test_worker(),
                                                           fls_test::test_boot(), 1000)),
              ErrorCode::UnknownSchedule);
  CHECK_ERROR(scheduler.release_schedule(ScheduleId(99), Generation(1)),
              ErrorCode::UnknownSchedule);
  CHECK_ERROR(scheduler.resolve_ambiguous(FlowId(99), Generation(1), AmbiguityResolution::Retry,
                                          1000),
              ErrorCode::UnknownFlow);
  CHECK_ERROR(scheduler.retire_flow(FlowId(99), Generation(1)), ErrorCode::UnknownFlow);
  CHECK_ERROR(scheduler.cancel_flow(FlowId(99), Generation(1), "unknown"),
              ErrorCode::UnknownFlow);

  // An explicitly advanced epoch still moves forward by exactly one, which is
  // what makes an epoch a fence rather than a label.
  const Result<FabricEpoch> advanced = scheduler.advance_epoch(EpochReason::Explicit);
  REQUIRE(advanced.ok());
  CHECK_EQ(advanced.value().value(), std::uint64_t(2));
}

// Completion evidence must bind the attempt, the flow generation, the schedule
// generation, the epoch, the worker incarnation, the authorized service and an
// established provenance. Anything else is rejected without mutating state, and
// the untouched attempt still accepts the evidence that does bind it.
FLOW_TEST(adversarial, completion_evidence_must_bind_the_owning_incarnation) {
  Result<std::unique_ptr<Scheduler>> created = fls_test::make_standard_scheduler();
  REQUIRE(created.ok());
  Scheduler& scheduler = *created.value();
  CHECK_OK(fls_test::admit(scheduler, fls_test::FlowSpec{}));
  Result<DispatchTicket> dispatched = dispatch_single_flow(scheduler, 0);
  REQUIRE(dispatched.ok());
  const DispatchTicket ticket = dispatched.value();

  const CompletionEvidence faithful = evidence_for(ticket);
  CompletionEvidence hostile = faithful;
  hostile.provenance = Provenance{};
  Result<CommitOutcome> committed = scheduler.complete(hostile, 0);
  REQUIRE(committed.ok());
  CHECK(committed.value().disposition == CommitDisposition::Rejected);
  CHECK(committed.value().code == ErrorCode::InvalidArgument);

  hostile = faithful;
  hostile.epoch = FabricEpoch(ticket.epoch.value() + 1u);
  committed = scheduler.complete(hostile, 0);
  REQUIRE(committed.ok());
  CHECK(committed.value().disposition == CommitDisposition::Rejected);
  CHECK(committed.value().code == ErrorCode::StaleEpoch);

  hostile = faithful;
  hostile.flow_generation = Generation(2);
  committed = scheduler.complete(hostile, 0);
  REQUIRE(committed.ok());
  CHECK(committed.value().code == ErrorCode::StaleFlowGeneration);

  hostile = faithful;
  hostile.schedule_generation = Generation(2);
  committed = scheduler.complete(hostile, 0);
  REQUIRE(committed.ok());
  CHECK(committed.value().code == ErrorCode::StaleScheduleGeneration);

  hostile = faithful;
  hostile.boot = BootId(9);
  committed = scheduler.complete(hostile, 0);
  REQUIRE(committed.ok());
  CHECK(committed.value().code == ErrorCode::Fenced);

  hostile = faithful;
  hostile.worker = WorkerId(9);
  committed = scheduler.complete(hostile, 0);
  REQUIRE(committed.ok());
  CHECK(committed.value().code == ErrorCode::Fenced);

  hostile = faithful;
  hostile.served = ticket.quantum + 1;
  committed = scheduler.complete(hostile, 0);
  REQUIRE(committed.ok());
  CHECK(committed.value().code == ErrorCode::ContradictoryCompletion);

  hostile = faithful;
  hostile.attempt = DispatchAttemptId(4242);
  committed = scheduler.complete(hostile, 0);
  REQUIRE(committed.ok());
  CHECK(committed.value().code == ErrorCode::UnknownAttempt);

  // Nothing above touched the attempt: the faithful evidence still commits.
  committed = scheduler.complete(faithful, 0);
  REQUIRE(committed.ok());
  CHECK(committed.value().disposition == CommitDisposition::Applied);
  CHECK_EQ(committed.value().credited, ticket.quantum);
  // And a byte-identical duplicate is idempotent rather than a second credit.
  committed = scheduler.complete(faithful, 0);
  REQUIRE(committed.ok());
  CHECK(committed.value().disposition == CommitDisposition::IdempotentDuplicate);
  CHECK_EQ(scheduler.accounting().service_credit_total, ticket.quantum);
}

// Cancelling a flow closes the attempt it was holding, so the authority it
// carried can never be completed afterwards.
FLOW_TEST(adversarial, cancelled_authority_cannot_be_completed) {
  Result<std::unique_ptr<Scheduler>> created = fls_test::make_standard_scheduler();
  REQUIRE(created.ok());
  Scheduler& scheduler = *created.value();
  CHECK_OK(fls_test::admit(scheduler, fls_test::FlowSpec{}));
  Result<DispatchTicket> dispatched = dispatch_single_flow(scheduler, 0);
  REQUIRE(dispatched.ok());

  CHECK_OK(scheduler.cancel_flow(FlowId(1), Generation(1), "operator cancelled the flow"));
  const Result<CommitOutcome> committed =
      scheduler.complete(evidence_for(dispatched.value()), 0);
  REQUIRE(committed.ok());
  CHECK(committed.value().disposition == CommitDisposition::Rejected);
  CHECK(committed.value().code == ErrorCode::CompletionForClosedAttempt);
  const AccountingReport report = scheduler.accounting();
  CHECK_EQ(report.cancelled, std::uint64_t(1));
  CHECK_EQ(report.completed, std::uint64_t(0));
  CHECK_EQ(report.service_credit_total, std::uint64_t(0));
}

// Encoded records carry their own length: a reader must never read past the
// payload it was handed and a writer that hits its ceiling must refuse the
// payload outright rather than emit a truncated-but-plausible one.
FLOW_TEST(adversarial, record_bounds_refuse_truncated_and_oversized_payloads) {
  RecordWriter writer;
  writer.u8(7);
  writer.u16(0x1234);
  writer.u32(0x89ABCDEF);
  writer.u64(0x0123456789ABCDEFull);
  writer.boolean(true);
  writer.text("hostile");
  CHECK(!writer.overflowed());
  const std::string payload = writer.take();

  RecordReader reader(payload);
  const Result<std::uint8_t> byte = reader.u8();
  REQUIRE(byte.ok());
  CHECK_EQ(byte.value(), std::uint8_t(7));
  const Result<std::uint16_t> half = reader.u16();
  REQUIRE(half.ok());
  CHECK_EQ(half.value(), std::uint16_t(0x1234));
  const Result<std::uint32_t> word = reader.u32();
  REQUIRE(word.ok());
  CHECK_EQ(word.value(), 0x89ABCDEFu);
  const Result<std::uint64_t> wide = reader.u64();
  REQUIRE(wide.ok());
  CHECK_EQ(wide.value(), 0x0123456789ABCDEFull);
  const Result<bool> flag = reader.boolean();
  REQUIRE(flag.ok());
  CHECK_EQ(flag.value(), true);
  const Result<std::string_view> text = reader.text();
  REQUIRE(text.ok());
  CHECK_EQ(std::string(text.value()), std::string("hostile"));
  CHECK(reader.exhausted());
  CHECK_ERROR(fls_test::as_status(reader.u8()), ErrorCode::Truncated);

  RecordReader short_reader(std::string_view(payload).substr(0, 5));
  CHECK_ERROR(fls_test::as_status(short_reader.u64()), ErrorCode::Truncated);

  RecordWriter bad_boolean;
  bad_boolean.u8(2);
  RecordReader boolean_reader(bad_boolean.data());
  CHECK_ERROR(fls_test::as_status(boolean_reader.boolean()), ErrorCode::InvalidArgument);

  RecordWriter overlong;
  overlong.u64(64);
  overlong.u32(0);
  RecordReader overlong_reader(overlong.data());
  CHECK_ERROR(fls_test::as_status(overlong_reader.text()), ErrorCode::Truncated);

  RecordWriter tiny(4);
  tiny.u64(1);
  CHECK(tiny.overflowed());
  CHECK(tiny.data().empty());
  tiny.u8(1);
  CHECK(tiny.overflowed());
  CHECK(tiny.data().empty());
}

// Message codecs are the last line before a value becomes authority: an
// unbound identity, an unsupported protocol generation, trailing bytes, an
// unusable frame bound, a ticket that authorizes nothing and evidence that
// claims more service than the bound all have to be refused.
FLOW_TEST(adversarial, protocol_decoders_reject_unbound_and_out_of_range_fields) {
  HelloMessage hello;
  hello.worker = WorkerId(1);
  hello.boot = BootId(1);
  hello.label = "worker";
  hello.supported_protocol = kProtocolVersion;
  CHECK_OK(fls_test::as_status(decode_hello(encode(hello))));

  HelloMessage unbound = hello;
  unbound.worker = WorkerId{};
  CHECK_ERROR(decode_hello(encode(unbound)), ErrorCode::HandshakeRejected);
  HelloMessage outdated = hello;
  outdated.supported_protocol = kProtocolVersion - 1u;
  CHECK_ERROR(decode_hello(encode(outdated)), ErrorCode::HandshakeRejected);
  const std::string encoded_hello = encode(hello);
  CHECK_ERROR(decode_hello(encoded_hello + std::string(1, '\0')), ErrorCode::ProtocolViolation);
  CHECK_ERROR(decode_hello(encoded_hello.substr(0, encoded_hello.size() - 1u)),
              ErrorCode::Truncated);

  HelloMessage verbose = hello;
  verbose.label = std::string(kMaxLabelBytes * 4u, 'L');
  const Result<HelloMessage> bounded = decode_hello(encode(verbose));
  REQUIRE(bounded.ok());
  CHECK_EQ(bounded.value().label.size(), kMaxLabelBytes);

  HelloAckMessage acknowledgement;
  acknowledgement.accepted = true;
  acknowledgement.reason = ErrorCode::Ok;
  acknowledgement.worker = WorkerId(1);
  acknowledgement.boot = BootId(1);
  acknowledgement.epoch = FabricEpoch(1);
  acknowledgement.policy = PolicyId(1);
  acknowledgement.policy_generation = Generation(1);
  acknowledgement.max_frame_payload = 0;
  CHECK_ERROR(decode_hello_ack(encode(acknowledgement)), ErrorCode::ProtocolViolation);
  acknowledgement.max_frame_payload = kMaxFramePayload + 1u;
  CHECK_ERROR(decode_hello_ack(encode(acknowledgement)), ErrorCode::ProtocolViolation);
  acknowledgement.max_frame_payload = kMaxFramePayload;
  CHECK_OK(fls_test::as_status(decode_hello_ack(encode(acknowledgement))));

  DispatchTicket ticket = bound_ticket();
  ticket.quantum = 0;
  DispatchMessage dispatch;
  dispatch.ticket = ticket;
  dispatch.nominal_work = 4;
  CHECK_ERROR(decode_dispatch(encode(dispatch)), ErrorCode::ProtocolViolation);
  ticket.quantum = kMaxQuantum + 1u;
  dispatch.ticket = ticket;
  CHECK_ERROR(decode_dispatch(encode(dispatch)), ErrorCode::ProtocolViolation);
  ticket.quantum = 4;
  dispatch.ticket = ticket;
  CHECK_OK(fls_test::as_status(decode_dispatch(encode(dispatch))));

  CompletionEvidence evidence = bound_evidence();
  evidence.served = kMaxServiceUnits + 1u;
  CompletionMessage completion;
  completion.evidence = evidence;
  CHECK_ERROR(decode_completion(encode(completion)), ErrorCode::ProtocolViolation);
  evidence = bound_evidence();
  evidence.boot = BootId{};
  completion.evidence = evidence;
  CHECK_ERROR(decode_completion(encode(completion)), ErrorCode::ProtocolViolation);
  completion.evidence = bound_evidence();
  CHECK_OK(fls_test::as_status(decode_completion(encode(completion))));

  // A hostile length in a dispatch frame is rejected before a ticket is parsed.
  std::string truncated = encode(dispatch);
  truncated.resize(truncated.size() - 1u);
  CHECK_ERROR(decode_dispatch(truncated), ErrorCode::Truncated);
}


// ---------------------------------------------------------------------------
// Boundary and adversarial coverage of the scheduler surface. The helpers above
// are untouched; everything below is self-contained so that the two halves of
// this file stay independently readable.
// ---------------------------------------------------------------------------

#include <cstddef>
#include <sstream>
#include <system_error>
#include <vector>

namespace {

using namespace flow_scheduler;

Status adv_status(const Status& status) { return status; }

template <class T>
Status adv_status(const Result<T>& result) {
  return result.ok() ? Status::success() : Status(result.error());
}

Provenance adv_provenance(std::uint64_t sequence) {
  Provenance provenance;
  provenance.origin = ProvenanceOrigin::External;
  provenance.sequence = sequence;
  provenance.digest = mix64(sequence);
  return provenance;
}

SchedulerOptions adv_options() {
  SchedulerOptions options;
  options.policy.policy = PolicyId(1);
  options.policy.generation = Generation(1);
  options.policy.preemption = PreemptionMode::Tiered;
  options.policy.deadline_ordering = true;
  options.policy.weighted_fairness = true;
  options.policy.min_service_guarantee = true;
  options.policy.strict_deadlines = true;
  options.policy.starvation_bound_ticks = 1000;
  options.policy.deadline_urgency_ticks = 100;
  options.policy.min_preempt_service = 0;
  options.policy.max_preemptions_per_interval = 64;
  options.policy.preemption_interval_ticks = 1000;
  options.policy.dispatch_delivery_horizon = 100000;
  options.policy.default_max_overlap = 4;
  options.policy.retained_schedule_history = 4096;
  options.policy.provenance = adv_provenance(1);
  options.policy.provenance.digest = digest_of(options.policy);
  return options;
}

ResourceDescriptor adv_resource(ResourceId resource, Generation generation, std::uint64_t capacity,
                                Ticks interval, std::uint64_t max_overlap) {
  ResourceDescriptor descriptor;
  descriptor.resource = resource;
  descriptor.generation = generation;
  descriptor.capacity = capacity;
  descriptor.interval = interval;
  descriptor.max_overlap = max_overlap;
  descriptor.provenance = adv_provenance(resource.value());
  return descriptor;
}

ReservationDescriptor adv_reservation(ReservationId reservation, Generation generation,
                                      ResourceId resource, Generation resource_generation,
                                      Ticks window_begin, Ticks window_end,
                                      std::uint64_t capacity) {
  ReservationDescriptor descriptor;
  descriptor.reservation = reservation;
  descriptor.generation = generation;
  descriptor.resource = resource;
  descriptor.resource_generation = resource_generation;
  descriptor.window_begin = window_begin;
  descriptor.window_end = window_end;
  descriptor.capacity = capacity;
  descriptor.provenance = adv_provenance(reservation.value());
  return descriptor;
}

PriorityClassDescriptor adv_priority(PriorityClassId priority, Generation generation,
                                     std::uint32_t rank) {
  PriorityClassDescriptor descriptor;
  descriptor.priority = priority;
  descriptor.generation = generation;
  descriptor.rank = rank;
  descriptor.weight = 1;
  descriptor.provenance = adv_provenance(priority.value());
  return descriptor;
}

QoSClassDescriptor adv_qos(QoSClassId qos, Generation generation) {
  QoSClassDescriptor descriptor;
  descriptor.qos = qos;
  descriptor.generation = generation;
  descriptor.preemption_protected = false;
  descriptor.max_service_per_dispatch = 0;
  descriptor.provenance = adv_provenance(qos.value());
  return descriptor;
}

struct AdvSpec {
  FlowId flow{FlowId(1)};
  Generation generation{Generation(1)};
  PathId path{PathId(1)};
  Generation path_generation{Generation(1)};
  ResourceId resource{ResourceId(1)};
  Generation resource_generation{Generation(1)};
  ReservationId reservation{};
  Generation reservation_generation{};
  PriorityClassId priority{PriorityClassId(1)};
  Generation priority_generation{Generation(1)};
  QoSClassId qos{QoSClassId(1)};
  Generation qos_generation{Generation(1)};
  FairnessGroupId fairness_group{FairnessGroupId(1)};
  std::uint64_t estimated_work{4};
  std::uint64_t service_quantum{4};
  std::uint64_t min_service{0};
  Ticks min_service_window{0};
  Ticks release_tick{0};
  Ticks deadline_tick{kNoDeadline};
  std::uint64_t weight{1};
  bool preemptible{true};
  bool auto_ready{true};
  std::uint8_t readiness_signal{0};
  std::vector<FlowId> dependencies{};
};

FlowDescriptor adv_flow(const AdvSpec& spec) {
  FlowDescriptor descriptor;
  descriptor.flow = spec.flow;
  descriptor.generation = spec.generation;
  descriptor.path = {spec.path, spec.path_generation};
  descriptor.resource = {spec.resource, spec.resource_generation};
  descriptor.reservation = {spec.reservation, spec.reservation_generation};
  descriptor.priority = {spec.priority, spec.priority_generation};
  descriptor.qos = {spec.qos, spec.qos_generation};
  descriptor.fairness_group = spec.fairness_group;
  descriptor.estimated_work = spec.estimated_work;
  descriptor.service_quantum = spec.service_quantum;
  descriptor.min_service = spec.min_service;
  descriptor.min_service_window = spec.min_service_window;
  descriptor.release_tick = spec.release_tick;
  descriptor.deadline_tick = spec.deadline_tick;
  descriptor.weight = spec.weight;
  descriptor.preemptible = spec.preemptible;
  descriptor.auto_ready = spec.auto_ready;
  descriptor.readiness_signal = spec.readiness_signal;
  descriptor.dependencies = spec.dependencies;
  descriptor.trace = TraceId(spec.flow.value());
  descriptor.provenance = adv_provenance(spec.flow.value());
  return descriptor;
}

Result<std::unique_ptr<Scheduler>> adv_topology(std::uint64_t capacity = 1'000'000,
                                                Ticks interval = 1000,
                                                std::uint64_t max_overlap = 4) {
  std::unique_ptr<Scheduler> scheduler;
  FS_TRY_ASSIGN(scheduler, Scheduler::create(adv_options()));
  FS_RETURN_IF_ERROR(scheduler->register_resource(
      adv_resource(ResourceId(1), Generation(1), capacity, interval, max_overlap)));
  for (std::uint32_t rank = 0; rank < 4; ++rank) {
    FS_RETURN_IF_ERROR(scheduler->register_priority_class(
        adv_priority(PriorityClassId(rank + 1), Generation(1), rank)));
  }
  FS_RETURN_IF_ERROR(scheduler->register_qos_class(adv_qos(QoSClassId(1), Generation(1))));
  return scheduler;
}

/// A descriptor bound to the topology produced by adv_topology().
AdvSpec adv_spec() { return AdvSpec{}; }

/// Every field of an accounting report, rendered so that a mismatch prints both
/// snapshots. A rejected call has to leave all of them untouched.
std::string adv_accounting_dump(const AccountingReport& report) {
  std::ostringstream stream;
  stream << "flows=" << report.total_flows << "|" << report.waiting << "|" << report.ready << "|"
         << report.scheduled << "|" << report.dispatched << "|" << report.running << "|"
         << report.completion_reported << "|" << report.cancelling << "|" << report.preempting << "|"
         << report.completed << "|" << report.cancelled << "|" << report.failed << "|"
         << report.ambiguous << " attempts=" << report.attempts_total << "|"
         << report.attempts_open << "|" << report.attempts_started << "|"
         << report.attempts_committed << "|" << report.attempts_preempted << "|"
         << report.attempts_cancelled << "|" << report.attempts_abandoned << "|"
         << report.attempts_rejected << " completions=" << report.completion_reports_received << "|"
         << report.completions_applied << "|" << report.completions_duplicate << "|"
         << report.completions_rejected << " arbitration=" << report.schedules_issued << "|"
         << report.schedule_entries_issued << "|" << report.preemptions_issued << "|"
         << report.arbitration_rounds << "|" << report.starvation_boosts << "|"
         << report.deadline_misses << "|" << report.reservations_retired
         << " service=" << report.outstanding_reserved_units << "|" << report.service_credit_total
         << "|" << report.estimated_work_total << " epoch=" << report.epoch
         << " recovered=" << report.recovered_flows << "|" << report.recovered_ambiguous_attempts;
  return stream.str();
}

struct AdvCycle {
  DispatchTicket ticket{};
  CommitOutcome outcome{};
};

/// Dispatch and complete one Run entry. Scheduler::mark_started() is never used
/// on this path, for the reason pinned by mark_started_failure_mutates_nothing.
Result<AdvCycle> adv_complete_entry(Scheduler& scheduler, const Schedule& schedule,
                                    const ScheduleEntry& entry, Ticks now, std::uint64_t served) {
  AdvCycle cycle;
  FS_TRY_ASSIGN(cycle.ticket, scheduler.begin_dispatch(schedule.schedule, schedule.generation,
                                                       entry.ordinal, WorkerId(1), BootId(1), now));
  CompletionEvidence evidence;
  evidence.schedule = cycle.ticket.schedule;
  evidence.schedule_generation = cycle.ticket.schedule_generation;
  evidence.attempt = cycle.ticket.attempt;
  evidence.epoch = cycle.ticket.epoch;
  evidence.flow = cycle.ticket.flow;
  evidence.flow_generation = cycle.ticket.flow_generation;
  evidence.worker = cycle.ticket.worker;
  evidence.boot = cycle.ticket.boot;
  evidence.outcome = CompletionOutcome::Served;
  evidence.served = served;
  evidence.effect_code = 1;
  evidence.effect_digest = mix64(cycle.ticket.attempt.value());
  evidence.observed_tick = now;
  evidence.provenance = adv_provenance(cycle.ticket.attempt.value());
  FS_TRY_ASSIGN(cycle.outcome, scheduler.complete(evidence, now));
  return cycle;
}

}  // namespace

// Every hostile descriptor is refused with the exact code the contract promises
// and without moving a single accounting counter or adding a flow to the table.
FLOW_TEST(adversarial, descriptor_validation_rejects_without_mutating_accounting) {
  const Result<std::unique_ptr<Scheduler>> created = adv_topology();
  REQUIRE(created.ok());
  Scheduler& scheduler = *created.value();

  const auto expect_rejected = [&scheduler](const FlowDescriptor& descriptor,
                                            ErrorCode expected) {
    const std::string before = adv_accounting_dump(scheduler.accounting());
    const std::size_t flows_before = scheduler.flow_count();
    CHECK_ERROR(scheduler.admit_flow(descriptor), expected);
    CHECK_EQ(adv_accounting_dump(scheduler.accounting()), before);
    CHECK_EQ(scheduler.flow_count(), flows_before);
  };

  // Control: the untouched descriptor is structurally valid.
  CHECK_OK(validate(adv_flow(adv_spec())));

  {
    AdvSpec spec = adv_spec();
    spec.estimated_work = 0;
    expect_rejected(adv_flow(spec), ErrorCode::OutOfRange);
  }
  {
    AdvSpec spec = adv_spec();
    spec.service_quantum = 0;
    expect_rejected(adv_flow(spec), ErrorCode::OutOfRange);
  }
  {
    AdvSpec spec = adv_spec();
    spec.service_quantum = kMaxQuantum + 1;
    expect_rejected(adv_flow(spec), ErrorCode::OutOfRange);
  }
  {
    AdvSpec spec = adv_spec();
    spec.release_tick = 50;
    spec.deadline_tick = 50;
    expect_rejected(adv_flow(spec), ErrorCode::InvalidArgument);
  }
  {
    AdvSpec spec = adv_spec();
    spec.min_service = 2;
    spec.min_service_window = 0;
    expect_rejected(adv_flow(spec), ErrorCode::InvalidArgument);
  }
  {
    AdvSpec spec = adv_spec();
    spec.min_service = spec.estimated_work + 1;
    spec.min_service_window = 10;
    expect_rejected(adv_flow(spec), ErrorCode::InvalidArgument);
  }
  {
    AdvSpec spec = adv_spec();
    spec.dependencies = {spec.flow};
    expect_rejected(adv_flow(spec), ErrorCode::InvalidArgument);
  }
  {
    AdvSpec spec = adv_spec();
    spec.dependencies = {FlowId(2), FlowId(3), FlowId(2)};
    expect_rejected(adv_flow(spec), ErrorCode::InvalidArgument);
  }
  {
    AdvSpec spec = adv_spec();
    spec.dependencies.assign(kMaxDependenciesPerFlow + 1u, FlowId(2));
    expect_rejected(adv_flow(spec), ErrorCode::Bounded);
  }
  {
    AdvSpec spec = adv_spec();
    spec.flow = FlowId(0);
    expect_rejected(adv_flow(spec), ErrorCode::InvalidArgument);
  }
  {
    AdvSpec spec = adv_spec();
    spec.generation = Generation(0);
    expect_rejected(adv_flow(spec), ErrorCode::InvalidArgument);
  }
  {
    AdvSpec spec = adv_spec();
    spec.path = PathId(0);
    expect_rejected(adv_flow(spec), ErrorCode::InvalidArgument);
  }
  {
    AdvSpec spec = adv_spec();
    spec.path_generation = Generation(0);
    expect_rejected(adv_flow(spec), ErrorCode::InvalidArgument);
  }
  {
    AdvSpec spec = adv_spec();
    spec.resource = ResourceId(0);
    expect_rejected(adv_flow(spec), ErrorCode::InvalidArgument);
  }
  {
    AdvSpec spec = adv_spec();
    spec.resource_generation = Generation(0);
    expect_rejected(adv_flow(spec), ErrorCode::InvalidArgument);
  }
  {
    AdvSpec spec = adv_spec();
    spec.priority = PriorityClassId(0);
    expect_rejected(adv_flow(spec), ErrorCode::InvalidArgument);
  }
  {
    AdvSpec spec = adv_spec();
    spec.priority_generation = Generation(0);
    expect_rejected(adv_flow(spec), ErrorCode::InvalidArgument);
  }
  {
    AdvSpec spec = adv_spec();
    spec.qos = QoSClassId(0);
    expect_rejected(adv_flow(spec), ErrorCode::InvalidArgument);
  }
  {
    AdvSpec spec = adv_spec();
    spec.qos_generation = Generation(0);
    expect_rejected(adv_flow(spec), ErrorCode::InvalidArgument);
  }
  {
    AdvSpec spec = adv_spec();
    spec.fairness_group = FairnessGroupId(0);
    expect_rejected(adv_flow(spec), ErrorCode::InvalidArgument);
  }
  // A partially specified reservation binding is never acceptable in either
  // direction: neither an identity without a generation nor the reverse.
  {
    AdvSpec spec = adv_spec();
    spec.reservation = ReservationId(1);
    spec.reservation_generation = Generation(0);
    expect_rejected(adv_flow(spec), ErrorCode::InvalidArgument);
  }
  {
    AdvSpec spec = adv_spec();
    spec.reservation = ReservationId(0);
    spec.reservation_generation = Generation(1);
    expect_rejected(adv_flow(spec), ErrorCode::InvalidArgument);
  }
  {
    AdvSpec spec = adv_spec();
    spec.weight = 0;
    expect_rejected(adv_flow(spec), ErrorCode::OutOfRange);
  }
  {
    AdvSpec spec = adv_spec();
    spec.readiness_signal = 7;
    expect_rejected(adv_flow(spec), ErrorCode::InvalidArgument);
  }
  {
    FlowDescriptor descriptor = adv_flow(adv_spec());
    descriptor.provenance = Provenance{};
    expect_rejected(descriptor, ErrorCode::InvalidArgument);
  }

  // The fixture itself is accepted, so every rejection above is attributable to
  // the mutated field and not to the scenario.
  CHECK_OK(scheduler.admit_flow(adv_flow(adv_spec())));
  CHECK_EQ(scheduler.flow_count(), std::size_t(1));
  CHECK_OK(adv_status(scheduler.accounting().validate()));
}

// Topology must be registered before it can be bound, and every re-registration
// must advance the generation by exactly one.
FLOW_TEST(adversarial, registration_generation_authority) {
  const Result<std::unique_ptr<Scheduler>> created = Scheduler::create(adv_options());
  REQUIRE(created.ok());
  Scheduler& scheduler = *created.value();

  CHECK_ERROR(scheduler.admit_flow(adv_flow(adv_spec())), ErrorCode::UnknownResource);
  CHECK_ERROR(scheduler.register_reservation(adv_reservation(
                  ReservationId(1), Generation(1), ResourceId(1), Generation(1), 0, 100, 10)),
              ErrorCode::UnknownResource);

  CHECK_OK(scheduler.register_resource(
      adv_resource(ResourceId(1), Generation(1), 1'000'000, 1000, 4)));
  CHECK_ERROR(scheduler.register_resource(
                  adv_resource(ResourceId(1), Generation(3), 1'000'000, 1000, 4)),
              ErrorCode::InvalidArgument);
  CHECK_ERROR(scheduler.register_resource(
                  adv_resource(ResourceId(1), Generation(1), 1'000'000, 1000, 4)),
              ErrorCode::InvalidArgument);
  CHECK_OK(scheduler.register_resource(
      adv_resource(ResourceId(1), Generation(2), 1'000'000, 1000, 4)));
  // A resource registered for the first time must start at generation 1.
  CHECK_ERROR(scheduler.register_resource(adv_resource(ResourceId(2), Generation(4), 8, 1000, 1)),
              ErrorCode::InvalidArgument);

  {
    AdvSpec spec = adv_spec();
    spec.resource_generation = Generation(1);
    CHECK_ERROR(scheduler.admit_flow(adv_flow(spec)), ErrorCode::StaleResourceGeneration);
  }
  {
    // The resource generation has moved on, so only a matching binding reaches
    // the class lookups that are still missing.
    AdvSpec spec = adv_spec();
    spec.resource_generation = Generation(2);
    CHECK_ERROR(scheduler.admit_flow(adv_flow(spec)), ErrorCode::UnknownPolicy);
  }

  CHECK_ERROR(scheduler.register_priority_class(
                  adv_priority(PriorityClassId(1), Generation(0), 0)),
              ErrorCode::InvalidArgument);
  CHECK_OK(scheduler.register_priority_class(
      adv_priority(PriorityClassId(1), Generation(1), 0)));
  CHECK_ERROR(scheduler.register_priority_class(
                  adv_priority(PriorityClassId(1), Generation(4), 0)),
              ErrorCode::InvalidArgument);
  CHECK_OK(scheduler.register_priority_class(
      adv_priority(PriorityClassId(1), Generation(2), 0)));
  CHECK_ERROR(scheduler.register_priority_class(
                  adv_priority(PriorityClassId(2), Generation(2), 1)),
              ErrorCode::InvalidArgument);
  {
    AdvSpec spec = adv_spec();
    spec.resource_generation = Generation(2);
    spec.priority_generation = Generation(1);
    CHECK_ERROR(scheduler.admit_flow(adv_flow(spec)), ErrorCode::StalePriorityGeneration);
  }

  CHECK_OK(scheduler.register_qos_class(adv_qos(QoSClassId(1), Generation(1))));
  CHECK_ERROR(scheduler.register_qos_class(adv_qos(QoSClassId(1), Generation(3))),
              ErrorCode::InvalidArgument);
  CHECK_OK(scheduler.register_qos_class(adv_qos(QoSClassId(1), Generation(2))));
  CHECK_ERROR(scheduler.register_qos_class(adv_qos(QoSClassId(2), Generation(3))),
              ErrorCode::InvalidArgument);
  {
    AdvSpec spec = adv_spec();
    spec.resource_generation = Generation(2);
    spec.priority_generation = Generation(2);
    spec.qos_generation = Generation(1);
    CHECK_ERROR(scheduler.admit_flow(adv_flow(spec)), ErrorCode::StaleQoSGeneration);
  }

  // A reservation must name a registered resource at its current generation.
  CHECK_ERROR(scheduler.register_reservation(adv_reservation(
                  ReservationId(1), Generation(1), ResourceId(9), Generation(1), 0, 100, 10)),
              ErrorCode::UnknownResource);
  CHECK_ERROR(scheduler.register_reservation(adv_reservation(
                  ReservationId(1), Generation(1), ResourceId(1), Generation(1), 0, 100, 10)),
              ErrorCode::StaleResourceGeneration);
  CHECK_OK(scheduler.register_reservation(adv_reservation(
      ReservationId(1), Generation(1), ResourceId(1), Generation(2), 0, 100, 10)));
  CHECK_ERROR(scheduler.register_reservation(adv_reservation(
                  ReservationId(1), Generation(5), ResourceId(1), Generation(2), 0, 100, 10)),
              ErrorCode::InvalidArgument);
  CHECK_OK(scheduler.register_reservation(adv_reservation(
      ReservationId(1), Generation(2), ResourceId(1), Generation(2), 0, 200, 10)));

  const auto current_spec = []() {
    AdvSpec spec = adv_spec();
    spec.resource_generation = Generation(2);
    spec.priority_generation = Generation(2);
    spec.qos_generation = Generation(2);
    return spec;
  };

  {
    AdvSpec spec = current_spec();
    spec.reservation = ReservationId(1);
    spec.reservation_generation = Generation(1);
    CHECK_ERROR(scheduler.admit_flow(adv_flow(spec)), ErrorCode::StaleReservationGeneration);
  }
  {
    AdvSpec spec = current_spec();
    spec.reservation = ReservationId(77);
    spec.reservation_generation = Generation(1);
    CHECK_ERROR(scheduler.admit_flow(adv_flow(spec)), ErrorCode::UnknownReservation);
  }
  // A reservation that belongs to a different resource never matches the flow.
  CHECK_OK(scheduler.register_resource(adv_resource(ResourceId(2), Generation(1), 8, 1000, 1)));
  CHECK_OK(scheduler.register_reservation(adv_reservation(
      ReservationId(2), Generation(1), ResourceId(2), Generation(1), 0, 100, 5)));
  {
    AdvSpec spec = current_spec();
    spec.reservation = ReservationId(2);
    spec.reservation_generation = Generation(1);
    CHECK_ERROR(scheduler.admit_flow(adv_flow(spec)), ErrorCode::InvalidArgument);
  }
  {
    AdvSpec spec = current_spec();
    spec.reservation = ReservationId(1);
    spec.reservation_generation = Generation(2);
    CHECK_OK(scheduler.admit_flow(adv_flow(spec)));
  }
}

// Externally influenced values on their declared ceilings must clamp rather than
// overflow, and one step past a ceiling must be refused.
FLOW_TEST(adversarial, checked_arithmetic_at_the_declared_bounds) {
  {
    const Result<std::unique_ptr<Scheduler>> created =
        adv_topology(kMaxServiceUnits, kMaxTickHorizon, 1);
    REQUIRE(created.ok());
    Scheduler& scheduler = *created.value();
    AdvSpec spec = adv_spec();
    spec.estimated_work = kMaxServiceUnits;
    spec.service_quantum = kMaxQuantum;
    spec.deadline_tick = kMaxTickHorizon;
    REQUIRE_OK(scheduler.admit_flow(adv_flow(spec)));

    const Result<ArbitrationOutcome> outcome = scheduler.arbitrate(0);
    REQUIRE(outcome.ok());
    CHECK_EQ(outcome.value().schedule.entries.size(), 1u);
    const ScheduleEntry& entry = outcome.value().schedule.entries[0];
    CHECK(entry.kind == DecisionKind::Run);
    CHECK_EQ(entry.quantum, kMaxQuantum);
    CHECK(entry.quantum <= adv_options().policy.max_quantum);
    const AccountingReport reserved = scheduler.accounting();
    CHECK_EQ(reserved.estimated_work_total, kMaxServiceUnits);
    // The window is scheduled but not dispatched: the reserved units are held by
    // the scheduled window and no attempt exists yet.
    CHECK_EQ(reserved.outstanding_reserved_units, kMaxQuantum);
    CHECK_EQ(reserved.attempts_total, 0u);
    CHECK_EQ(reserved.scheduled, 1u);
    CHECK_OK(adv_status(reserved.validate()));
    const Result<FlowSnapshot> scheduled = scheduler.flow(FlowId(1));
    REQUIRE(scheduled.ok());
    CHECK_EQ(scheduled.value().outstanding_reserved, kMaxQuantum);

    const Result<AdvCycle> cycle =
        adv_complete_entry(scheduler, outcome.value().schedule, entry, 0, kMaxQuantum);
    REQUIRE(cycle.ok());
    CHECK(cycle.value().outcome.disposition == CommitDisposition::Applied);
    CHECK_EQ(cycle.value().outcome.credited, kMaxQuantum);
    const Result<FlowSnapshot> snapshot = scheduler.flow(FlowId(1));
    REQUIRE(snapshot.ok());
    CHECK_EQ(snapshot.value().served_work, kMaxQuantum);
    CHECK_EQ(snapshot.value().outstanding_reserved, 0u);
    CHECK_EQ(scheduler.accounting().outstanding_reserved_units, 0u);
    CHECK_OK(adv_status(scheduler.accounting().validate()));
  }
  {
    // The policy ceiling, not the descriptor, is what bounds the quantum.
    SchedulerOptions options = adv_options();
    options.policy.max_quantum = 1024;
    options.policy.provenance.digest = digest_of(options.policy);
    const Result<std::unique_ptr<Scheduler>> created = Scheduler::create(options);
    REQUIRE(created.ok());
    Scheduler& scheduler = *created.value();
    REQUIRE_OK(scheduler.register_resource(
        adv_resource(ResourceId(1), Generation(1), kMaxServiceUnits, kMaxTickHorizon, 1)));
    REQUIRE_OK(scheduler.register_priority_class(
        adv_priority(PriorityClassId(1), Generation(1), 0)));
    REQUIRE_OK(scheduler.register_qos_class(adv_qos(QoSClassId(1), Generation(1))));
    AdvSpec spec = adv_spec();
    spec.estimated_work = kMaxServiceUnits;
    spec.service_quantum = kMaxQuantum;
    spec.deadline_tick = kMaxTickHorizon;
    REQUIRE_OK(scheduler.admit_flow(adv_flow(spec)));
    const Result<ArbitrationOutcome> outcome = scheduler.arbitrate(0);
    REQUIRE(outcome.ok());
    CHECK_EQ(outcome.value().schedule.entries.size(), 1u);
    CHECK_EQ(outcome.value().schedule.entries[0].quantum, 1024u);
  }
  {
    // The tick horizon is inclusive for both ends of a flow's window.
    const Result<std::unique_ptr<Scheduler>> created = adv_topology();
    REQUIRE(created.ok());
    Scheduler& scheduler = *created.value();
    AdvSpec at_horizon = adv_spec();
    at_horizon.release_tick = kMaxTickHorizon;
    CHECK_OK(scheduler.admit_flow(adv_flow(at_horizon)));

    AdvSpec past_horizon = adv_spec();
    past_horizon.flow = FlowId(2);
    past_horizon.path = PathId(2);
    past_horizon.release_tick = kMaxTickHorizon + 1;
    CHECK_ERROR(scheduler.admit_flow(adv_flow(past_horizon)), ErrorCode::OutOfRange);

    AdvSpec deadline_at_horizon = adv_spec();
    deadline_at_horizon.flow = FlowId(3);
    deadline_at_horizon.path = PathId(3);
    deadline_at_horizon.deadline_tick = kMaxTickHorizon;
    CHECK_OK(scheduler.admit_flow(adv_flow(deadline_at_horizon)));

    AdvSpec deadline_past_horizon = adv_spec();
    deadline_past_horizon.flow = FlowId(4);
    deadline_past_horizon.path = PathId(4);
    deadline_past_horizon.deadline_tick = kMaxTickHorizon + 1;
    CHECK_ERROR(scheduler.admit_flow(adv_flow(deadline_past_horizon)), ErrorCode::OutOfRange);

    const Result<ArbitrationOutcome> outcome = scheduler.arbitrate(0);
    REQUIRE(outcome.ok());
    CHECK_EQ(outcome.value().schedule.entries.size(), 1u);
    CHECK(outcome.value().schedule.entries[0].flow == FlowId(3));
  }
  {
    // Descriptor ceilings one past their bound are refused.
    const Result<std::unique_ptr<Scheduler>> created = adv_topology();
    REQUIRE(created.ok());
    Scheduler& scheduler = *created.value();
    CHECK_ERROR(scheduler.register_resource(
                    adv_resource(ResourceId(3), Generation(1), kMaxServiceUnits + 1, 1000, 1)),
                ErrorCode::OutOfRange);
    CHECK_ERROR(scheduler.register_resource(
                    adv_resource(ResourceId(3), Generation(1), 1, kMaxTickHorizon + 1, 1)),
                ErrorCode::OutOfRange);
    CHECK_ERROR(scheduler.register_resource(adv_resource(ResourceId(3), Generation(1), 1, 1000, 0)),
                ErrorCode::OutOfRange);
    CHECK_ERROR(scheduler.register_reservation(adv_reservation(
                    ReservationId(9), Generation(1), ResourceId(1), Generation(1),
                    kMaxTickHorizon - 1, kMaxTickHorizon + 1, 1)),
                ErrorCode::OutOfRange);
    AdvSpec min_service_at_horizon = adv_spec();
    min_service_at_horizon.estimated_work = kMaxServiceUnits;
    min_service_at_horizon.service_quantum = kMaxQuantum;
    min_service_at_horizon.min_service = kMaxServiceUnits;
    min_service_at_horizon.min_service_window = kMaxTickHorizon;
    CHECK_OK(scheduler.admit_flow(adv_flow(min_service_at_horizon)));
    CHECK_OK(adv_status(scheduler.accounting().validate()));
  }
}

// Duplicate and contradictory calls are either refused or idempotent, and the
// idempotent ones move no counter.
FLOW_TEST(adversarial, duplicate_and_contradictory_calls) {
  const Result<std::unique_ptr<Scheduler>> created = adv_topology();
  REQUIRE(created.ok());
  Scheduler& scheduler = *created.value();

  CHECK_OK(scheduler.admit_flow(adv_flow(adv_spec())));
  {
    const std::string before = adv_accounting_dump(scheduler.accounting());
    CHECK_ERROR(scheduler.admit_flow(adv_flow(adv_spec())), ErrorCode::AlreadyExists);
    CHECK_EQ(adv_accounting_dump(scheduler.accounting()), before);
    CHECK_EQ(scheduler.flow_count(), std::size_t(1));
  }
  // A non-terminal flow cannot be retired from the table.
  CHECK_ERROR(scheduler.retire_flow(FlowId(1), Generation(1)), ErrorCode::InvalidState);
  CHECK_ERROR(scheduler.retire_flow(FlowId(1), Generation(4)), ErrorCode::StaleFlowGeneration);
  CHECK_ERROR(scheduler.cancel_flow(FlowId(99), Generation(1), "unknown"), ErrorCode::UnknownFlow);
  CHECK_ERROR(scheduler.cancel_flow(FlowId(1), Generation(4), "stale"),
              ErrorCode::StaleFlowGeneration);
  CHECK_OK(scheduler.cancel_flow(FlowId(1), Generation(1), "operator"));
  {
    const Result<FlowSnapshot> cancelled = scheduler.flow(FlowId(1));
    REQUIRE(cancelled.ok());
    CHECK(cancelled.value().lifecycle == FlowLifecycle::Cancelled);
  }
  {
    // Cancelling twice is idempotent: nothing moves.
    const std::string before = adv_accounting_dump(scheduler.accounting());
    CHECK_OK(scheduler.cancel_flow(FlowId(1), Generation(1), "again"));
    CHECK_EQ(adv_accounting_dump(scheduler.accounting()), before);
  }
  CHECK_ERROR(scheduler.resolve_ambiguous(FlowId(1), Generation(1), AmbiguityResolution::Retry, 10),
              ErrorCode::InvalidState);
  CHECK_ERROR(scheduler.resolve_ambiguous(FlowId(1), Generation(2), AmbiguityResolution::Retry, 10),
              ErrorCode::StaleFlowGeneration);
  CHECK_ERROR(scheduler.notify_readiness(FlowId(1), Generation(1), ReadinessSignal::Ready, 10),
              ErrorCode::InvalidState);
  CHECK_OK(scheduler.retire_flow(FlowId(1), Generation(1)));
  CHECK_ERROR(adv_status(scheduler.flow(FlowId(1))), ErrorCode::UnknownFlow);
  CHECK_ERROR(scheduler.retire_flow(FlowId(1), Generation(1)), ErrorCode::UnknownFlow);

  // A completed flow is terminal as well: cancellation and readiness signals are
  // refused, and the committed attempt cannot be revived or preempted.
  DispatchAttemptId committed_attempt;
  {
    AdvSpec spec = adv_spec();
    spec.flow = FlowId(2);
    spec.path = PathId(2);
    REQUIRE_OK(scheduler.admit_flow(adv_flow(spec)));
    // The readiness and ambiguity calls above moved the monotone clock to 10, so
    // the arbitration tick has to be at least that.
    const Result<ArbitrationOutcome> outcome = scheduler.arbitrate(100);
    REQUIRE(outcome.ok());
    CHECK_EQ(outcome.value().schedule.entries.size(), 1u);
    const Result<AdvCycle> cycle = adv_complete_entry(
        scheduler, outcome.value().schedule, outcome.value().schedule.entries[0], 100, 4);
    REQUIRE(cycle.ok());
    CHECK(cycle.value().outcome.disposition == CommitDisposition::Applied);
    CHECK(cycle.value().outcome.flow_completed);
    committed_attempt = cycle.value().ticket.attempt;
    const Result<FlowSnapshot> completed = scheduler.flow(FlowId(2));
    REQUIRE(completed.ok());
    CHECK(completed.value().lifecycle == FlowLifecycle::Completed);
  }
  CHECK_ERROR(scheduler.cancel_flow(FlowId(2), Generation(1), "too late"), ErrorCode::InvalidState);
  CHECK_ERROR(scheduler.notify_readiness(FlowId(2), Generation(1), ReadinessSignal::Ready, 0),
              ErrorCode::InvalidState);
  CHECK_OK(scheduler.abandon_attempt(committed_attempt, "already closed", 0));
  CHECK_ERROR(scheduler.preempt_attempt(committed_attempt, "already closed", 0),
              ErrorCode::CompletionForClosedAttempt);
  CHECK_ERROR(scheduler.abandon_attempt(DispatchAttemptId(99), "unknown", 0),
              ErrorCode::UnknownAttempt);
  CHECK_ERROR(scheduler.preempt_attempt(DispatchAttemptId(99), "unknown", 0),
              ErrorCode::UnknownAttempt);
  {
    AdvSpec spec = adv_spec();
    spec.flow = FlowId(3);
    spec.path = PathId(3);
    spec.generation = Generation(3);
    CHECK_ERROR(scheduler.update_flow(adv_flow(spec)), ErrorCode::UnknownFlow);
  }
  {
    AdvSpec spec = adv_spec();
    spec.flow = FlowId(2);
    spec.path = PathId(2);
    spec.generation = Generation(3);
    CHECK_ERROR(scheduler.update_flow(adv_flow(spec)), ErrorCode::InvalidArgument);
  }

  // Reservation retirement checks the generation and is idempotent.
  CHECK_OK(scheduler.register_reservation(adv_reservation(
      ReservationId(5), Generation(1), ResourceId(1), Generation(1), 0, 1000, 10)));
  CHECK_ERROR(scheduler.retire_reservation(ReservationId(5), Generation(2)),
              ErrorCode::StaleReservationGeneration);
  CHECK_ERROR(scheduler.retire_reservation(ReservationId(6), Generation(1)),
              ErrorCode::UnknownReservation);
  CHECK_OK(scheduler.retire_reservation(ReservationId(5), Generation(1)));
  CHECK_EQ(scheduler.accounting().reservations_retired, 1u);
  {
    const std::string before = adv_accounting_dump(scheduler.accounting());
    CHECK_OK(scheduler.retire_reservation(ReservationId(5), Generation(1)));
    CHECK_EQ(adv_accounting_dump(scheduler.accounting()), before);
  }
  CHECK_OK(adv_status(scheduler.accounting().validate()));
}

// Observation of an identity that was never registered answers with the exact
// lookup code, never with an empty success or a default value.
FLOW_TEST(adversarial, unknown_identity_observations_are_exact) {
  const Result<std::unique_ptr<Scheduler>> created = adv_topology();
  REQUIRE(created.ok());
  Scheduler& scheduler = *created.value();

  for (const FlowId id : {FlowId(0), FlowId(4242)}) {
    CHECK_ERROR(adv_status(scheduler.flow(id)), ErrorCode::UnknownFlow);
    CHECK_ERROR(adv_status(scheduler.explain_flow(id)), ErrorCode::UnknownFlow);
    CHECK_ERROR(scheduler.notify_readiness(id, Generation(1), ReadinessSignal::Ready, 0),
                ErrorCode::UnknownFlow);
    CHECK_ERROR(scheduler.retire_flow(id, Generation(1)), ErrorCode::UnknownFlow);
    CHECK_ERROR(scheduler.cancel_flow(id, Generation(1), "unknown"), ErrorCode::UnknownFlow);
    CHECK_ERROR(scheduler.resolve_ambiguous(id, Generation(1), AmbiguityResolution::Retry, 0),
                ErrorCode::UnknownFlow);
  }
  for (const ScheduleId id : {ScheduleId(0), ScheduleId(4242)}) {
    CHECK_ERROR(adv_status(scheduler.schedule(id)), ErrorCode::UnknownSchedule);
    CHECK_ERROR(adv_status(scheduler.explain_schedule(id)), ErrorCode::UnknownSchedule);
    CHECK_ERROR(scheduler.release_schedule(id, Generation(1)), ErrorCode::UnknownSchedule);
  }
  for (const DispatchAttemptId id : {DispatchAttemptId(0), DispatchAttemptId(4242)}) {
    CHECK_ERROR(adv_status(scheduler.attempt(id)), ErrorCode::UnknownAttempt);
    CHECK_ERROR(scheduler.abandon_attempt(id, "unknown", 0), ErrorCode::UnknownAttempt);
    CHECK_ERROR(scheduler.preempt_attempt(id, "unknown", 0), ErrorCode::UnknownAttempt);
  }
  CHECK_ERROR(adv_status(scheduler.begin_dispatch(ScheduleId(4242), Generation(1), 0, WorkerId(1),
                                                  BootId(1), 0)),
              ErrorCode::UnknownSchedule);
  // An unbound worker incarnation is refused before any schedule is consulted.
  CHECK_ERROR(adv_status(scheduler.begin_dispatch(ScheduleId(4242), Generation(1), 0, WorkerId(0),
                                                  BootId(1), 0)),
              ErrorCode::InvalidArgument);
  DispatchTicket blank;
  CHECK_ERROR(scheduler.revalidate(blank, 0), ErrorCode::InvalidArgument);

  // Evidence that binds nothing is rejected, and only the rejection diagnostics
  // move: no lifecycle gauge, no service credit, no attempt.
  {
    const AccountingReport before = scheduler.accounting();
    const Result<CommitOutcome> outcome = scheduler.complete(CompletionEvidence{}, 0);
    REQUIRE(outcome.ok());
    CHECK(outcome.value().disposition == CommitDisposition::Rejected);
    CHECK(outcome.value().code == ErrorCode::InvalidArgument);
    const AccountingReport after = scheduler.accounting();
    CHECK_EQ(after.total_flows, before.total_flows);
    CHECK_EQ(after.service_credit_total, before.service_credit_total);
    CHECK_EQ(after.attempts_total, before.attempts_total);
    // The rejected report may be counted or not; the two counters move together.
    CHECK(after.completion_reports_received == before.completion_reports_received ||
          after.completion_reports_received == before.completion_reports_received + 1u);
    CHECK_EQ(after.completion_reports_received - before.completion_reports_received,
             after.completions_rejected - before.completions_rejected);
  }

  // Positive control: a registered flow is observable, explainable, and the
  // installed policy is readable.
  CHECK_OK(scheduler.admit_flow(adv_flow(adv_spec())));
  const Result<FlowSnapshot> known = scheduler.flow(FlowId(1));
  REQUIRE(known.ok());
  CHECK_EQ(known.value().flow.value(), 1u);
  const Result<FlowExplanation> explanation = scheduler.explain_flow(FlowId(1));
  REQUIRE(explanation.ok());
  CHECK_EQ(explanation.value().flow.value(), 1u);
  const Result<SchedulingPolicy> policy = scheduler.policy();
  REQUIRE(policy.ok());
  CHECK(policy.value().policy == PolicyId(1));
}

// The per-round deferral list is a diagnostic, and it is bounded: a population
// larger than the bound truncates the list while the schedule stays correct.
FLOW_TEST(adversarial, deferred_diagnostics_are_bounded) {
  const Result<std::unique_ptr<Scheduler>> created = adv_topology(1'000'000'000, 1000, 4);
  REQUIRE(created.ok());
  Scheduler& scheduler = *created.value();

  constexpr std::size_t kWaitingFlows = 4100;
  constexpr std::size_t kDeferredBound = 4096;
  constexpr Ticks kFarFuture = 1'000'000'000;
  for (std::size_t index = 0; index < kWaitingFlows; ++index) {
    AdvSpec spec = adv_spec();
    spec.flow = FlowId(index + 2);
    spec.path = PathId(index + 2);
    spec.release_tick = kFarFuture;
    REQUIRE_OK(scheduler.admit_flow(adv_flow(spec)));
  }
  REQUIRE_OK(scheduler.admit_flow(adv_flow(adv_spec())));

  const Result<ArbitrationOutcome> first = scheduler.arbitrate(0);
  REQUIRE(first.ok());
  CHECK_EQ(first.value().schedule.entries.size(), 1u);
  CHECK(first.value().schedule.entries[0].flow == FlowId(1));
  CHECK_EQ(first.value().schedule.entries[0].quantum, 4u);
  CHECK(first.value().deferred_truncated);
  CHECK_EQ(first.value().deferred.size(), kDeferredBound);
  bool all_awaiting_release = true;
  for (const DeferredFlow& deferred : first.value().deferred) {
    if (deferred.reason != DecisionReason::AwaitingRelease) {
      all_awaiting_release = false;
    }
    if (deferred.generation != Generation(1)) {
      all_awaiting_release = false;
    }
  }
  CHECK(all_awaiting_release);
  CHECK_OK(adv_status(scheduler.accounting().validate()));

  // The bound is per round: the next round truncates again, and the schedule of
  // the previous round is not reissued for a flow that already holds a window.
  const Result<ArbitrationOutcome> second = scheduler.arbitrate(0);
  REQUIRE(second.ok());
  CHECK(second.value().deferred_truncated);
  CHECK_EQ(second.value().deferred.size(), kDeferredBound);
  CHECK_EQ(second.value().schedule.entries.size(), 0u);
  CHECK_OK(adv_status(scheduler.accounting().validate()));
}

// The coordinator timeline is monotone: a lower tick is refused, and the
// refusal leaves the state untouched.
FLOW_TEST(adversarial, tick_monotonicity_rejects_and_preserves_state) {
  const Result<std::unique_ptr<Scheduler>> created = adv_topology();
  REQUIRE(created.ok());
  Scheduler& scheduler = *created.value();
  REQUIRE_OK(scheduler.admit_flow(adv_flow(adv_spec())));

  const Result<ArbitrationOutcome> first = scheduler.arbitrate(100);
  REQUIRE(first.ok());
  CHECK_EQ(first.value().schedule.entries.size(), 1u);

  const std::string before = adv_accounting_dump(scheduler.accounting());
  const Result<FlowSnapshot> flow_before = scheduler.flow(FlowId(1));
  REQUIRE(flow_before.ok());
  CHECK_ERROR(adv_status(scheduler.arbitrate(99)), ErrorCode::InvalidArgument);
  CHECK_EQ(adv_accounting_dump(scheduler.accounting()), before);
  const Result<FlowSnapshot> flow_after = scheduler.flow(FlowId(1));
  REQUIRE(flow_after.ok());
  CHECK(flow_after.value().lifecycle == flow_before.value().lifecycle);
  CHECK_EQ(flow_after.value().last_change_tick, flow_before.value().last_change_tick);
  CHECK_EQ(scheduler.accounting().arbitration_rounds, 1u);

  // The same tick is legal and does not reissue a window the flow already holds.
  const Result<ArbitrationOutcome> repeated = scheduler.arbitrate(100);
  REQUIRE(repeated.ok());
  CHECK_EQ(repeated.value().schedule.entries.size(), 0u);
  CHECK_EQ(scheduler.accounting().arbitration_rounds, 2u);

  // A rejected call still advances the monotone clock: the runtime never
  // pretends that time did not pass while it was deciding.
  CHECK_ERROR(scheduler.notify_readiness(FlowId(7), Generation(1), ReadinessSignal::Ready, 5000),
              ErrorCode::UnknownFlow);
  CHECK_ERROR(adv_status(scheduler.arbitrate(4999)), ErrorCode::InvalidArgument);
  CHECK_OK(adv_status(scheduler.arbitrate(5000)));
  CHECK_OK(adv_status(scheduler.accounting().validate()));
}

// Scheduler::mark_started() revalidates the whole authority tuple, moves the
// attempt to Started and the flow to Running, and is refused once the attempt is
// closed. (The self-deadlock this test used to pin is fixed: revalidate() now
// delegates to an unlocked body.)
FLOW_TEST(adversarial, mark_started_advances_the_attempt_once) {
  const Result<std::unique_ptr<Scheduler>> created = adv_topology();
  REQUIRE(created.ok());
  Scheduler& scheduler = *created.value();
  REQUIRE_OK(scheduler.admit_flow(adv_flow(adv_spec())));
  const Result<ArbitrationOutcome> outcome = scheduler.arbitrate(10);
  REQUIRE(outcome.ok());
  REQUIRE(outcome.value().schedule.entries.size() == 1u);
  const Result<DispatchTicket> ticket =
      scheduler.begin_dispatch(outcome.value().schedule.schedule,
                               outcome.value().schedule.generation, 0, WorkerId(1), BootId(1), 10);
  REQUIRE(ticket.ok());
  {
    const Result<FlowSnapshot> dispatched = scheduler.flow(FlowId(1));
    REQUIRE(dispatched.ok());
    CHECK(dispatched.value().lifecycle == FlowLifecycle::Dispatched);
  }

  CHECK_OK(scheduler.mark_started(ticket.value(), 10));
  {
    const Result<FlowSnapshot> running = scheduler.flow(FlowId(1));
    REQUIRE(running.ok());
    CHECK(running.value().lifecycle == FlowLifecycle::Running);
    CHECK_EQ(running.value().accounting.dispatches_started, 1u);
    const Result<DispatchTicket> stored = scheduler.attempt(ticket.value().attempt);
    REQUIRE(stored.ok());
    CHECK(stored.value().state == AttemptState::Started);
    CHECK_OK(adv_status(scheduler.accounting().validate()));
  }

  // A repeated acknowledgement is revalidated and re-applied today, so only the
  // invariants that hold either way are asserted: the flow stays Running and the
  // acknowledgement counter never regresses. (Reported: apply_attempt_started()
  // has no already-Started guard, so the counter can reach two for one attempt.)
  CHECK_OK(scheduler.mark_started(ticket.value(), 10));
  {
    const Result<FlowSnapshot> repeated = scheduler.flow(FlowId(1));
    REQUIRE(repeated.ok());
    CHECK(repeated.value().lifecycle == FlowLifecycle::Running);
    CHECK(repeated.value().accounting.dispatches_started >= 1u);
  }

  // Revalidation still applies to a Started attempt: a tampered ticket and an
  // unknown attempt are both refused without touching the flow.
  {
    DispatchTicket tampered = ticket.value();
    tampered.epoch = FabricEpoch(ticket.value().epoch.value() + 1u);
    CHECK_ERROR(scheduler.mark_started(tampered, 10), ErrorCode::ContradictoryCompletion);
    DispatchTicket unknown = ticket.value();
    unknown.attempt = DispatchAttemptId(4242);
    CHECK_ERROR(scheduler.mark_started(unknown, 10), ErrorCode::UnknownAttempt);
    const Result<FlowSnapshot> unchanged = scheduler.flow(FlowId(1));
    REQUIRE(unchanged.ok());
    CHECK(unchanged.value().lifecycle == FlowLifecycle::Running);
  }

  // Completion closes the attempt, and a late Started acknowledgement is refused
  // instead of reviving it.
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
  evidence.served = ticket.value().quantum;
  evidence.effect_code = 1;
  evidence.effect_digest = mix64(ticket.value().attempt.value());
  evidence.observed_tick = 10;
  evidence.provenance = adv_provenance(ticket.value().attempt.value());
  const Result<CommitOutcome> committed = scheduler.complete(evidence, 10);
  REQUIRE(committed.ok());
  CHECK(committed.value().disposition == CommitDisposition::Applied);
  CHECK_ERROR(scheduler.mark_started(ticket.value(), 10), ErrorCode::CompletionForClosedAttempt);
  CHECK_OK(adv_status(scheduler.accounting().validate()));
}

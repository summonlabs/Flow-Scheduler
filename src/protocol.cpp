// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#include "flow_scheduler/protocol.hpp"

#include "flow_scheduler/framing.hpp"

namespace flow_scheduler {
namespace {

Status require_exhausted(const RecordReader& reader) {
  if (!reader.exhausted()) {
    return Status::failure(ErrorCode::ProtocolViolation,
                           "message carries trailing bytes after the declared fields");
  }
  return Status::success();
}

Result<std::string_view> bounded_text(RecordReader& reader, std::size_t bound,
                                      const char* field) {
  std::string_view value;
  FS_TRY_ASSIGN(value, reader.text());
  if (value.size() > bound) {
    return Error(ErrorCode::Oversized, std::string(field) + " exceeds its bound");
  }
  return value;
}

Result<std::uint8_t> bounded_enum(RecordReader& reader, std::uint8_t maximum,
                                  const char* field) {
  std::uint8_t value = 0;
  FS_TRY_ASSIGN(value, reader.u8());
  if (value > maximum) {
    return Error(ErrorCode::ProtocolViolation, std::string(field) + " is not a known value");
  }
  return value;
}

}  // namespace

const char* to_string(MessageType type) noexcept {
  switch (type) {
    case MessageType::Invalid: return "invalid";
    case MessageType::Hello: return "hello";
    case MessageType::HelloAck: return "hello-ack";
    case MessageType::Dispatch: return "dispatch";
    case MessageType::DispatchRejected: return "dispatch-rejected";
    case MessageType::Started: return "started";
    case MessageType::Completion: return "completion";
    case MessageType::Shutdown: return "shutdown";
    case MessageType::Error: return "error";
    case MessageType::Heartbeat: return "heartbeat";
  }
  return "unknown-message";
}

void write_ticket(RecordWriter& writer, const DispatchTicket& ticket) {
  writer.u64(ticket.attempt.value());
  writer.u64(ticket.epoch.value());
  writer.u64(ticket.schedule.value());
  writer.u64(ticket.schedule_generation.value());
  writer.u64(ticket.flow.value());
  writer.u64(ticket.flow_generation.value());
  writer.u64(ticket.path.value());
  writer.u64(ticket.path_generation.value());
  writer.u64(ticket.resource.value());
  writer.u64(ticket.resource_generation.value());
  writer.u64(ticket.reservation.value());
  writer.u64(ticket.reservation_generation.value());
  writer.u64(ticket.qos.value());
  writer.u64(ticket.qos_generation.value());
  writer.u64(ticket.priority.value());
  writer.u64(ticket.priority_generation.value());
  writer.u64(ticket.policy.value());
  writer.u64(ticket.policy_generation.value());
  writer.u64(ticket.worker.value());
  writer.u64(ticket.boot.value());
  writer.u64(ticket.start_tick);
  writer.u64(ticket.deadline_tick);
  writer.u64(ticket.reservation_end);
  writer.u64(ticket.quantum);
  writer.u8(static_cast<std::uint8_t>(ticket.state));
  writer.u64(ticket.digest);
}

Result<DispatchTicket> read_ticket(RecordReader& reader) {
  DispatchTicket ticket;
  std::uint64_t value = 0;
  FS_TRY_ASSIGN(value, reader.u64()); ticket.attempt = DispatchAttemptId(value);
  FS_TRY_ASSIGN(value, reader.u64()); ticket.epoch = FabricEpoch(value);
  FS_TRY_ASSIGN(value, reader.u64()); ticket.schedule = ScheduleId(value);
  FS_TRY_ASSIGN(value, reader.u64()); ticket.schedule_generation = Generation(value);
  FS_TRY_ASSIGN(value, reader.u64()); ticket.flow = FlowId(value);
  FS_TRY_ASSIGN(value, reader.u64()); ticket.flow_generation = Generation(value);
  FS_TRY_ASSIGN(value, reader.u64()); ticket.path = PathId(value);
  FS_TRY_ASSIGN(value, reader.u64()); ticket.path_generation = Generation(value);
  FS_TRY_ASSIGN(value, reader.u64()); ticket.resource = ResourceId(value);
  FS_TRY_ASSIGN(value, reader.u64()); ticket.resource_generation = Generation(value);
  FS_TRY_ASSIGN(value, reader.u64()); ticket.reservation = ReservationId(value);
  FS_TRY_ASSIGN(value, reader.u64()); ticket.reservation_generation = Generation(value);
  FS_TRY_ASSIGN(value, reader.u64()); ticket.qos = QoSClassId(value);
  FS_TRY_ASSIGN(value, reader.u64()); ticket.qos_generation = Generation(value);
  FS_TRY_ASSIGN(value, reader.u64()); ticket.priority = PriorityClassId(value);
  FS_TRY_ASSIGN(value, reader.u64()); ticket.priority_generation = Generation(value);
  FS_TRY_ASSIGN(value, reader.u64()); ticket.policy = PolicyId(value);
  FS_TRY_ASSIGN(value, reader.u64()); ticket.policy_generation = Generation(value);
  FS_TRY_ASSIGN(value, reader.u64()); ticket.worker = WorkerId(value);
  FS_TRY_ASSIGN(value, reader.u64()); ticket.boot = BootId(value);
  FS_TRY_ASSIGN(value, reader.u64()); ticket.start_tick = value;
  FS_TRY_ASSIGN(value, reader.u64()); ticket.deadline_tick = value;
  FS_TRY_ASSIGN(value, reader.u64()); ticket.reservation_end = value;
  FS_TRY_ASSIGN(value, reader.u64()); ticket.quantum = value;
  std::uint8_t state = 0;
  FS_TRY_ASSIGN(state, bounded_enum(reader, 6, "attempt state"));
  ticket.state = static_cast<AttemptState>(state);
  FS_TRY_ASSIGN(value, reader.u64()); ticket.digest = value;

  if (!ticket.attempt.valid() || !ticket.epoch.valid() || !ticket.schedule.valid() ||
      !ticket.schedule_generation.valid() || !ticket.flow.valid() ||
      !ticket.flow_generation.valid() || !ticket.path.valid() || !ticket.path_generation.valid() ||
      !ticket.resource.valid() || !ticket.resource_generation.valid() || !ticket.policy.valid() ||
      !ticket.policy_generation.valid() || !ticket.qos.valid() || !ticket.qos_generation.valid() ||
      !ticket.priority.valid() || !ticket.priority_generation.valid()) {
    return Error(ErrorCode::ProtocolViolation, "dispatch ticket has an unbound authority field");
  }
  if (ticket.quantum == 0) {
    return Error(ErrorCode::ProtocolViolation, "dispatch ticket authorizes a zero quantum");
  }
  if (ticket.quantum > kMaxQuantum) {
    return Error(ErrorCode::ProtocolViolation, "dispatch ticket quantum exceeds kMaxQuantum");
  }
  return ticket;
}

void write_evidence(RecordWriter& writer, const CompletionEvidence& evidence) {
  writer.u64(evidence.schedule.value());
  writer.u64(evidence.schedule_generation.value());
  writer.u64(evidence.attempt.value());
  writer.u64(evidence.epoch.value());
  writer.u64(evidence.flow.value());
  writer.u64(evidence.flow_generation.value());
  writer.u64(evidence.worker.value());
  writer.u64(evidence.boot.value());
  writer.u8(static_cast<std::uint8_t>(evidence.outcome));
  writer.u64(evidence.served);
  writer.u32(evidence.effect_code);
  writer.u64(evidence.effect_digest);
  writer.u64(evidence.observed_tick);
  writer.u8(static_cast<std::uint8_t>(evidence.provenance.origin));
  writer.u64(evidence.provenance.sequence);
  writer.u64(evidence.provenance.digest);
}

Result<CompletionEvidence> read_evidence(RecordReader& reader) {
  CompletionEvidence evidence;
  std::uint64_t value = 0;
  FS_TRY_ASSIGN(value, reader.u64()); evidence.schedule = ScheduleId(value);
  FS_TRY_ASSIGN(value, reader.u64()); evidence.schedule_generation = Generation(value);
  FS_TRY_ASSIGN(value, reader.u64()); evidence.attempt = DispatchAttemptId(value);
  FS_TRY_ASSIGN(value, reader.u64()); evidence.epoch = FabricEpoch(value);
  FS_TRY_ASSIGN(value, reader.u64()); evidence.flow = FlowId(value);
  FS_TRY_ASSIGN(value, reader.u64()); evidence.flow_generation = Generation(value);
  FS_TRY_ASSIGN(value, reader.u64()); evidence.worker = WorkerId(value);
  FS_TRY_ASSIGN(value, reader.u64()); evidence.boot = BootId(value);
  std::uint8_t outcome = 0;
  FS_TRY_ASSIGN(outcome, bounded_enum(reader, 2, "completion outcome"));
  evidence.outcome = static_cast<CompletionOutcome>(outcome);
  FS_TRY_ASSIGN(value, reader.u64()); evidence.served = value;
  std::uint32_t effect_code = 0;
  FS_TRY_ASSIGN(effect_code, reader.u32()); evidence.effect_code = effect_code;
  FS_TRY_ASSIGN(value, reader.u64()); evidence.effect_digest = value;
  FS_TRY_ASSIGN(value, reader.u64()); evidence.observed_tick = value;
  std::uint8_t origin = 0;
  FS_TRY_ASSIGN(origin, bounded_enum(reader, 4, "provenance origin"));
  evidence.provenance.origin = static_cast<ProvenanceOrigin>(origin);
  FS_TRY_ASSIGN(value, reader.u64()); evidence.provenance.sequence = value;
  FS_TRY_ASSIGN(value, reader.u64()); evidence.provenance.digest = value;

  if (!evidence.attempt.valid() || !evidence.epoch.valid() || !evidence.schedule.valid() ||
      !evidence.schedule_generation.valid() || !evidence.flow.valid() ||
      !evidence.flow_generation.valid() || !evidence.worker.valid() || !evidence.boot.valid()) {
    return Error(ErrorCode::ProtocolViolation, "completion evidence has an unbound field");
  }
  if (evidence.served > kMaxServiceUnits) {
    return Error(ErrorCode::ProtocolViolation, "completion evidence served exceeds the bound");
  }
  return evidence;
}

// ---------------------------------------------------------------------------
// Encoders
// ---------------------------------------------------------------------------

std::string encode(const HelloMessage& message) {
  RecordWriter writer;
  writer.u64(message.worker.value());
  writer.u64(message.boot.value());
  writer.text(message.label.substr(0, kMaxLabelBytes));
  writer.u64(message.supported_protocol);
  return writer.take();
}

std::string encode(const HelloAckMessage& message) {
  RecordWriter writer;
  writer.boolean(message.accepted);
  writer.u16(static_cast<std::uint16_t>(message.reason));
  writer.text(message.detail.substr(0, kMaxDetailBytes));
  writer.u64(message.worker.value());
  writer.u64(message.boot.value());
  writer.u64(message.epoch.value());
  writer.u64(message.policy.value());
  writer.u64(message.policy_generation.value());
  writer.u64(message.policy_digest);
  writer.u32(message.max_frame_payload);
  writer.u64(message.heartbeat_ticks);
  return writer.take();
}

std::string encode(const DispatchMessage& message) {
  RecordWriter writer;
  write_ticket(writer, message.ticket);
  writer.u64(message.nominal_work);
  return writer.take();
}

std::string encode(const DispatchRejectedMessage& message) {
  RecordWriter writer;
  writer.u64(message.attempt.value());
  writer.u64(message.epoch.value());
  writer.u16(static_cast<std::uint16_t>(message.code));
  writer.text(message.detail.substr(0, kMaxDetailBytes));
  return writer.take();
}

std::string encode(const StartedMessage& message) {
  RecordWriter writer;
  writer.u64(message.attempt.value());
  writer.u64(message.epoch.value());
  writer.u64(message.at_tick);
  return writer.take();
}

std::string encode(const CompletionMessage& message) {
  RecordWriter writer;
  write_evidence(writer, message.evidence);
  return writer.take();
}

std::string encode(const ShutdownMessage& message) {
  RecordWriter writer;
  writer.text(message.reason.substr(0, kMaxDetailBytes));
  return writer.take();
}

std::string encode(const ErrorMessage& message) {
  RecordWriter writer;
  writer.u16(static_cast<std::uint16_t>(message.code));
  writer.text(message.detail.substr(0, kMaxDetailBytes));
  return writer.take();
}

// ---------------------------------------------------------------------------
// Decoders
// ---------------------------------------------------------------------------

Result<HelloMessage> decode_hello(std::string_view payload) {
  RecordReader reader(payload);
  HelloMessage message;
  std::uint64_t value = 0;
  FS_TRY_ASSIGN(value, reader.u64()); message.worker = WorkerId(value);
  FS_TRY_ASSIGN(value, reader.u64()); message.boot = BootId(value);
  FS_TRY_ASSIGN(message.label, bounded_text(reader, kMaxLabelBytes, "worker label"));
  FS_TRY_ASSIGN(value, reader.u64()); message.supported_protocol = value;
  FS_RETURN_IF_ERROR(require_exhausted(reader));
  if (!message.worker.valid()) {
    return Error(ErrorCode::HandshakeRejected, "worker identity is unbound");
  }
  if (message.supported_protocol != kProtocolVersion) {
    return Error(ErrorCode::HandshakeRejected, "worker protocol version is not supported");
  }
  return message;
}

Result<HelloAckMessage> decode_hello_ack(std::string_view payload) {
  RecordReader reader(payload);
  HelloAckMessage message;
  FS_TRY_ASSIGN(message.accepted, reader.boolean());
  std::uint16_t code = 0;
  FS_TRY_ASSIGN(code, reader.u16());
  message.reason = static_cast<ErrorCode>(code);
  FS_TRY_ASSIGN(message.detail, bounded_text(reader, kMaxDetailBytes, "handshake detail"));
  std::uint64_t value = 0;
  FS_TRY_ASSIGN(value, reader.u64()); message.worker = WorkerId(value);
  FS_TRY_ASSIGN(value, reader.u64()); message.boot = BootId(value);
  FS_TRY_ASSIGN(value, reader.u64()); message.epoch = FabricEpoch(value);
  FS_TRY_ASSIGN(value, reader.u64()); message.policy = PolicyId(value);
  FS_TRY_ASSIGN(value, reader.u64()); message.policy_generation = Generation(value);
  FS_TRY_ASSIGN(value, reader.u64()); message.policy_digest = value;
  std::uint32_t max_frame = 0;
  FS_TRY_ASSIGN(max_frame, reader.u32()); message.max_frame_payload = max_frame;
  FS_TRY_ASSIGN(value, reader.u64()); message.heartbeat_ticks = value;
  FS_RETURN_IF_ERROR(require_exhausted(reader));
  if (message.accepted) {
    if (!message.worker.valid() || !message.boot.valid() || !message.epoch.valid()) {
      return Error(ErrorCode::ProtocolViolation, "accepted handshake is missing authority fields");
    }
    if (message.max_frame_payload == 0 || message.max_frame_payload > kMaxFramePayload) {
      return Error(ErrorCode::ProtocolViolation, "handshake frame bound is outside the contract");
    }
  }
  return message;
}

Result<DispatchMessage> decode_dispatch(std::string_view payload) {
  RecordReader reader(payload);
  DispatchMessage message;
  FS_TRY_ASSIGN(message.ticket, read_ticket(reader));
  std::uint64_t nominal = 0;
  FS_TRY_ASSIGN(nominal, reader.u64());
  message.nominal_work = nominal;
  FS_RETURN_IF_ERROR(require_exhausted(reader));
  return message;
}

Result<DispatchRejectedMessage> decode_dispatch_rejected(std::string_view payload) {
  RecordReader reader(payload);
  DispatchRejectedMessage message;
  std::uint64_t value = 0;
  FS_TRY_ASSIGN(value, reader.u64()); message.attempt = DispatchAttemptId(value);
  FS_TRY_ASSIGN(value, reader.u64()); message.epoch = FabricEpoch(value);
  std::uint16_t code = 0;
  FS_TRY_ASSIGN(code, reader.u16());
  message.code = static_cast<ErrorCode>(code);
  FS_TRY_ASSIGN(message.detail, bounded_text(reader, kMaxDetailBytes, "rejection detail"));
  FS_RETURN_IF_ERROR(require_exhausted(reader));
  return message;
}

Result<StartedMessage> decode_started(std::string_view payload) {
  RecordReader reader(payload);
  StartedMessage message;
  std::uint64_t value = 0;
  FS_TRY_ASSIGN(value, reader.u64()); message.attempt = DispatchAttemptId(value);
  FS_TRY_ASSIGN(value, reader.u64()); message.epoch = FabricEpoch(value);
  FS_TRY_ASSIGN(value, reader.u64()); message.at_tick = value;
  FS_RETURN_IF_ERROR(require_exhausted(reader));
  if (!message.attempt.valid()) {
    return Error(ErrorCode::ProtocolViolation, "started message has an unbound attempt");
  }
  return message;
}

Result<CompletionMessage> decode_completion(std::string_view payload) {
  RecordReader reader(payload);
  CompletionMessage message;
  FS_TRY_ASSIGN(message.evidence, read_evidence(reader));
  FS_RETURN_IF_ERROR(require_exhausted(reader));
  return message;
}

Result<ShutdownMessage> decode_shutdown(std::string_view payload) {
  RecordReader reader(payload);
  ShutdownMessage message;
  FS_TRY_ASSIGN(message.reason, bounded_text(reader, kMaxDetailBytes, "shutdown reason"));
  FS_RETURN_IF_ERROR(require_exhausted(reader));
  return message;
}

Result<ErrorMessage> decode_error(std::string_view payload) {
  RecordReader reader(payload);
  ErrorMessage message;
  std::uint16_t code = 0;
  FS_TRY_ASSIGN(code, reader.u16());
  message.code = static_cast<ErrorCode>(code);
  FS_TRY_ASSIGN(message.detail, bounded_text(reader, kMaxDetailBytes, "error detail"));
  FS_RETURN_IF_ERROR(require_exhausted(reader));
  return message;
}

}  // namespace flow_scheduler

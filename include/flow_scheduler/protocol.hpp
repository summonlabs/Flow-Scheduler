// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#ifndef FLOW_SCHEDULER_PROTOCOL_HPP
#define FLOW_SCHEDULER_PROTOCOL_HPP

#include <cstdint>
#include <string>
#include <string_view>

#include "flow_scheduler/dispatch.hpp"
#include "flow_scheduler/error.hpp"
#include "flow_scheduler/identity.hpp"
#include "flow_scheduler/journal.hpp"
#include "flow_scheduler/time.hpp"

namespace flow_scheduler {

/// Coordinator/worker message types. Values are wire contract.
enum class MessageType : std::uint16_t {
  Invalid = 0,
  Hello = 1,
  HelloAck = 2,
  Dispatch = 3,
  DispatchRejected = 4,
  Started = 5,
  Completion = 6,
  Shutdown = 7,
  Error = 8,
  Heartbeat = 9,
};

[[nodiscard]] const char* to_string(MessageType type) noexcept;

/// Worker identity offered at handshake. The label is diagnostic only and is
/// bounded before it is stored or echoed.
struct HelloMessage {
  WorkerId worker{};
  BootId boot{};
  std::string label{};
  std::uint64_t supported_protocol{0};
};

struct HelloAckMessage {
  bool accepted{false};
  ErrorCode reason{ErrorCode::Ok};
  std::string detail{};
  WorkerId worker{};
  BootId boot{};
  FabricEpoch epoch{};
  PolicyId policy{};
  Generation policy_generation{};
  std::uint64_t policy_digest{0};
  std::uint32_t max_frame_payload{0};
  Ticks heartbeat_ticks{0};
};

struct DispatchMessage {
  DispatchTicket ticket{};
  /// Informational only: the worker must never treat this as authority.
  std::uint64_t nominal_work{0};
};

struct DispatchRejectedMessage {
  DispatchAttemptId attempt{};
  FabricEpoch epoch{};
  ErrorCode code{ErrorCode::Ok};
  std::string detail{};
};

struct StartedMessage {
  DispatchAttemptId attempt{};
  FabricEpoch epoch{};
  Ticks at_tick{0};
};

struct CompletionMessage {
  CompletionEvidence evidence{};
};

struct ShutdownMessage {
  std::string reason{};
};

struct ErrorMessage {
  ErrorCode code{ErrorCode::Ok};
  std::string detail{};
};

inline constexpr std::size_t kMaxLabelBytes = 128;
inline constexpr std::size_t kMaxDetailBytes = 512;

// --- encoders ---------------------------------------------------------------
[[nodiscard]] std::string encode(const HelloMessage& message);
[[nodiscard]] std::string encode(const HelloAckMessage& message);
[[nodiscard]] std::string encode(const DispatchMessage& message);
[[nodiscard]] std::string encode(const DispatchRejectedMessage& message);
[[nodiscard]] std::string encode(const StartedMessage& message);
[[nodiscard]] std::string encode(const CompletionMessage& message);
[[nodiscard]] std::string encode(const ShutdownMessage& message);
[[nodiscard]] std::string encode(const ErrorMessage& message);

// --- decoders (strict: reject trailing, missing, or out-of-range fields) ----
[[nodiscard]] Result<HelloMessage> decode_hello(std::string_view payload);
[[nodiscard]] Result<HelloAckMessage> decode_hello_ack(std::string_view payload);
[[nodiscard]] Result<DispatchMessage> decode_dispatch(std::string_view payload);
[[nodiscard]] Result<DispatchRejectedMessage> decode_dispatch_rejected(std::string_view payload);
[[nodiscard]] Result<StartedMessage> decode_started(std::string_view payload);
[[nodiscard]] Result<CompletionMessage> decode_completion(std::string_view payload);
[[nodiscard]] Result<ShutdownMessage> decode_shutdown(std::string_view payload);
[[nodiscard]] Result<ErrorMessage> decode_error(std::string_view payload);

// --- shared field helpers ---------------------------------------------------
void write_ticket(RecordWriter& writer, const DispatchTicket& ticket);
[[nodiscard]] Result<DispatchTicket> read_ticket(RecordReader& reader);
void write_evidence(RecordWriter& writer, const CompletionEvidence& evidence);
[[nodiscard]] Result<CompletionEvidence> read_evidence(RecordReader& reader);

}  // namespace flow_scheduler

#endif  // FLOW_SCHEDULER_PROTOCOL_HPP

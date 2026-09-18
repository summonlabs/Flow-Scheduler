// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#ifndef FLOW_SCHEDULER_FRAMING_HPP
#define FLOW_SCHEDULER_FRAMING_HPP

#include <cstdint>
#include <string>
#include <string_view>

#include "flow_scheduler/error.hpp"

namespace flow_scheduler {

/// Wire frame layout (little endian on the wire):
///
///   offset  size  field
///   ------  ----  -----------------------------------------------------------
///        0     4  magic 0x31435346 ('FSC1')
///        4     2  protocol version
///        6     2  message type
///       8     4  flags (must be zero; non-zero is a protocol violation)
///       12     4  payload length
///       16     4  CRC-32C over bytes [0,16)
///       20     N  payload
///     20+N     4  CRC-32C over payload bytes
///
/// A frame is accepted only when magic, version, flags, length bound, header
/// CRC, and payload CRC all agree. Anything else is rejected before any
/// message decoder sees the bytes.
inline constexpr std::uint32_t kFrameMagic = 0x31435346u;
inline constexpr std::uint16_t kProtocolVersion = 1;
inline constexpr std::size_t kFrameHeaderBytes = 20;
inline constexpr std::size_t kFrameTrailerBytes = 4;
inline constexpr std::size_t kFrameOverhead = kFrameHeaderBytes + kFrameTrailerBytes;
/// Upper bound on any single frame payload. Both directions enforce it, so a
/// peer cannot make the coordinator allocate without limit.
inline constexpr std::uint32_t kMaxFramePayload = 1u << 20;  // 1 MiB

struct FrameHeader {
  std::uint16_t version{0};
  std::uint16_t type{0};
  std::uint32_t flags{0};
  std::uint32_t payload_length{0};
};

/// Encode a frame into a single contiguous buffer.
[[nodiscard]] Result<std::string> encode_frame(std::uint16_t type, std::string_view payload,
                                               std::uint32_t flags = 0);

/// Validate and split a candidate frame buffer. The payload view aliases the
/// input buffer and is valid only while it does.
[[nodiscard]] Result<FrameHeader> decode_frame(std::string_view buffer,
                                               std::string_view& payload);

/// Incremental decoder for stream transports, where a frame may be split
/// across an arbitrary number of reads and several frames may arrive
/// coalesced. Bounded: a single push may not exceed
/// kMaxFramePayload + kFrameOverhead and the internal buffer never exceeds
/// twice that, which is enough to hold one complete maximal frame plus one
/// push of pending bytes. Exceeding the bound is rejected as Oversized before
/// any allocation happens.
class FrameAccumulator {
 public:
  FrameAccumulator() = default;

  /// Append freshly read bytes. Fails with Oversized when a single frame
  /// cannot fit inside the bound.
  [[nodiscard]] Status push(std::string_view bytes);

  /// Pop the next complete frame. Returns false when more bytes are needed.
  [[nodiscard]] Result<bool> next(FrameHeader& header, std::string& payload);

  [[nodiscard]] std::size_t buffered() const noexcept { return buffer_.size(); }
  [[nodiscard]] std::size_t frames_decoded() const noexcept { return frames_decoded_; }
  [[nodiscard]] std::size_t bytes_discarded() const noexcept { return bytes_discarded_; }

  /// Drop everything buffered. Used when a peer is fenced to avoid carrying a
  /// half frame into the next incarnation.
  void reset();

 private:
  std::string buffer_;
  std::size_t frames_decoded_{0};
  std::size_t bytes_discarded_{0};
};

}  // namespace flow_scheduler

#endif  // FLOW_SCHEDULER_FRAMING_HPP

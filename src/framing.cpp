// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#include "flow_scheduler/framing.hpp"

#include <cstring>

#include "flow_scheduler/hash.hpp"

namespace flow_scheduler {
namespace {

constexpr std::uint16_t kMaxMessageType = 1024;

void encode_u16_le(char* out, std::uint16_t value) noexcept {
  out[0] = static_cast<char>(value & 0xFFu);
  out[1] = static_cast<char>((value >> 8) & 0xFFu);
}

void encode_u32_le(char* out, std::uint32_t value) noexcept {
  out[0] = static_cast<char>(value & 0xFFu);
  out[1] = static_cast<char>((value >> 8) & 0xFFu);
  out[2] = static_cast<char>((value >> 16) & 0xFFu);
  out[3] = static_cast<char>((value >> 24) & 0xFFu);
}

std::uint16_t decode_u16_le(const char* in) noexcept {
  return static_cast<std::uint16_t>(static_cast<unsigned char>(in[0])) |
         static_cast<std::uint16_t>(static_cast<unsigned char>(in[1]) << 8);
}

std::uint32_t decode_u32_le(const char* in) noexcept {
  return static_cast<std::uint32_t>(static_cast<unsigned char>(in[0])) |
         (static_cast<std::uint32_t>(static_cast<unsigned char>(in[1])) << 8) |
         (static_cast<std::uint32_t>(static_cast<unsigned char>(in[2])) << 16) |
         (static_cast<std::uint32_t>(static_cast<unsigned char>(in[3])) << 24);
}

}  // namespace

Result<std::string> encode_frame(std::uint16_t type, std::string_view payload,
                                 std::uint32_t flags) {
  if (payload.size() > kMaxFramePayload) {
    return Error(ErrorCode::Oversized, "frame payload exceeds kMaxFramePayload");
  }
  if (type == 0 || type > kMaxMessageType) {
    return Error(ErrorCode::InvalidArgument, "frame message type is outside the permitted range");
  }
  std::string frame;
  frame.resize(kFrameHeaderBytes);
  encode_u32_le(&frame[0], kFrameMagic);
  encode_u16_le(&frame[4], kProtocolVersion);
  encode_u16_le(&frame[6], type);
  encode_u32_le(&frame[8], flags);
  encode_u32_le(&frame[12], static_cast<std::uint32_t>(payload.size()));
  encode_u32_le(&frame[16], crc32c(frame.data(), 16));
  frame.append(payload.data(), payload.size());
  char trailer[kFrameTrailerBytes];
  encode_u32_le(trailer, crc32c(payload.data(), payload.size()));
  frame.append(trailer, kFrameTrailerBytes);
  return frame;
}

Result<FrameHeader> decode_frame(std::string_view buffer, std::string_view& payload) {
  payload = std::string_view{};
  if (buffer.size() < kFrameOverhead) {
    return Error(ErrorCode::Truncated, "frame shorter than the minimum frame size");
  }
  if (decode_u32_le(buffer.data()) != kFrameMagic) {
    return Error(ErrorCode::ProtocolViolation, "frame magic mismatch");
  }
  FrameHeader header;
  header.version = decode_u16_le(buffer.data() + 4);
  header.type = decode_u16_le(buffer.data() + 6);
  header.flags = decode_u32_le(buffer.data() + 8);
  header.payload_length = decode_u32_le(buffer.data() + 12);
  if (header.version != kProtocolVersion) {
    return Error(ErrorCode::Unsupported, "frame protocol version is not supported");
  }
  if (header.flags != 0) {
    return Error(ErrorCode::ProtocolViolation, "frame flags must be zero");
  }
  if (header.type == 0 || header.type > kMaxMessageType) {
    return Error(ErrorCode::ProtocolViolation, "frame message type is outside the permitted range");
  }
  if (header.payload_length > kMaxFramePayload) {
    return Error(ErrorCode::Oversized, "frame payload exceeds kMaxFramePayload");
  }
  if (decode_u32_le(buffer.data() + 16) != crc32c(buffer.data(), 16)) {
    return Error(ErrorCode::CorruptData, "frame header checksum mismatch");
  }
  const std::size_t total = kFrameOverhead + header.payload_length;
  if (buffer.size() < total) {
    return Error(ErrorCode::Truncated, "frame body is incomplete");
  }
  payload = buffer.substr(kFrameHeaderBytes, header.payload_length);
  if (decode_u32_le(buffer.data() + kFrameHeaderBytes + header.payload_length) !=
      crc32c(payload.data(), payload.size())) {
    return Error(ErrorCode::CorruptData, "frame payload checksum mismatch");
  }
  return header;
}

Status FrameAccumulator::push(std::string_view bytes) {
  if (bytes.size() > (kMaxFramePayload + kFrameOverhead) ||
      buffer_.size() + bytes.size() > 2u * (kMaxFramePayload + kFrameOverhead)) {
    return Status::failure(ErrorCode::Oversized,
                           "frame accumulator bound exceeded; peer is not framing correctly");
  }
  buffer_.append(bytes.data(), bytes.size());
  return Status::success();
}

Result<bool> FrameAccumulator::next(FrameHeader& header, std::string& payload) {
  if (buffer_.size() < kFrameHeaderBytes) {
    return false;
  }
  // Validate the header before trusting the declared length, so a corrupt
  // length can never drive an allocation or an unbounded wait.
  const std::uint32_t declared = decode_u32_le(buffer_.data() + 12);
  if (declared > kMaxFramePayload) {
    return Error(ErrorCode::Oversized, "declared frame payload exceeds kMaxFramePayload");
  }
  const std::size_t total = kFrameOverhead + declared;
  if (buffer_.size() < total) {
    return false;
  }
  std::string_view view;
  FrameHeader parsed;
  FS_TRY_ASSIGN(parsed, decode_frame(std::string_view(buffer_.data(), total), view));
  header = parsed;
  payload.assign(view.data(), view.size());
  buffer_.erase(0, total);
  ++frames_decoded_;
  return true;
}

void FrameAccumulator::reset() {
  bytes_discarded_ += buffer_.size();
  buffer_.clear();
}

}  // namespace flow_scheduler

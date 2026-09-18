// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.
//
// Wire-format hardening. Every byte on the wire is adversarial until magic,
// protocol version, flags, the payload bound, the header checksum and the
// payload checksum all agree: a lying length must be rejected before anything
// is allocated for it, and the message codecs must reject truncated, trailing,
// out-of-range, unbound and oversized input with exact error codes.

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

#include "flow_scheduler/framing.hpp"
#include "flow_scheduler/hash.hpp"
#include "flow_scheduler/journal.hpp"
#include "flow_scheduler/protocol.hpp"
#include "test_framework.hpp"

namespace {

using namespace flow_scheduler;

/// CHECK_OK / CHECK_ERROR / REQUIRE_OK require an expression that evaluates to
/// Status; Result<T> carries a code, so it is adapted without losing it.
Status as_status(const Status& status) { return status; }

template <class T>
Status as_status(const Result<T>& result) {
  return result.ok() ? Status::success() : Status(result.error());
}

void patch_u16(std::string& bytes, std::size_t offset, std::uint16_t value) {
  bytes[offset] = static_cast<char>(value & 0xFFu);
  bytes[offset + 1] = static_cast<char>((value >> 8) & 0xFFu);
}

void patch_u32(std::string& bytes, std::size_t offset, std::uint32_t value) {
  for (std::size_t index = 0; index < 4; ++index) {
    bytes[offset + index] = static_cast<char>((value >> (8u * index)) & 0xFFu);
  }
}

std::uint32_t peek_u32(const std::string& bytes, std::size_t offset) {
  std::uint32_t value = 0;
  for (std::size_t index = 0; index < 4; ++index) {
    value |= static_cast<std::uint32_t>(static_cast<unsigned char>(bytes[offset + index]))
             << (8u * index);
  }
  return value;
}

/// Recompute the header checksum after deliberately corrupting another header
/// field, so that the header checksum is not what rejects the frame.
void reseal_header_crc(std::string& frame) { patch_u32(frame, 16, crc32c(frame.data(), 16)); }

std::string patterned_payload(std::size_t length, std::uint8_t seed) {
  std::string payload(length, '\0');
  for (std::size_t index = 0; index < length; ++index) {
    const std::uint8_t step = static_cast<std::uint8_t>(index % 251u);
    payload[index] = static_cast<char>((seed + step) & 0xFFu);
  }
  return payload;
}

/// Canonical valid frame used as the mutation base for decoder rejection tests.
std::string valid_frame() {
  const Result<std::string> frame =
      encode_frame(static_cast<std::uint16_t>(MessageType::Hello), patterned_payload(64, 0x11u));
  REQUIRE(frame.ok());
  return frame.value();
}

std::string frame_of(std::uint16_t type, const std::string& payload) {
  const Result<std::string> frame = encode_frame(type, payload);
  REQUIRE(frame.ok());
  return frame.value();
}

Status decode_status(const std::string& buffer) {
  std::string_view payload;
  return as_status(decode_frame(buffer, payload));
}

HelloMessage sample_hello() {
  HelloMessage message;
  message.worker = WorkerId(21);
  message.boot = BootId(22);
  message.label = "worker-twenty-one";
  message.supported_protocol = kProtocolVersion;
  return message;
}

HelloAckMessage sample_hello_ack() {
  HelloAckMessage message;
  message.accepted = true;
  message.reason = ErrorCode::Ok;
  message.detail = "welcome";
  message.worker = WorkerId(21);
  message.boot = BootId(22);
  message.epoch = FabricEpoch(3);
  message.policy = PolicyId(1);
  message.policy_generation = Generation(2);
  message.policy_digest = 0x1234u;
  message.max_frame_payload = static_cast<std::uint32_t>(kMaxFramePayload);
  message.heartbeat_ticks = 1000;
  return message;
}

DispatchTicket sample_ticket() {
  DispatchTicket ticket;
  ticket.attempt = DispatchAttemptId(11);
  ticket.epoch = FabricEpoch(3);
  ticket.schedule = ScheduleId(5);
  ticket.schedule_generation = Generation(2);
  ticket.flow = FlowId(7);
  ticket.flow_generation = Generation(4);
  ticket.path = PathId(9);
  ticket.path_generation = Generation(1);
  ticket.resource = ResourceId(2);
  ticket.resource_generation = Generation(6);
  ticket.reservation = ReservationId(0);
  ticket.reservation_generation = Generation(0);
  ticket.qos = QoSClassId(1);
  ticket.qos_generation = Generation(1);
  ticket.priority = PriorityClassId(1);
  ticket.priority_generation = Generation(1);
  ticket.policy = PolicyId(1);
  ticket.policy_generation = Generation(1);
  ticket.worker = WorkerId(21);
  ticket.boot = BootId(22);
  ticket.start_tick = 100;
  ticket.deadline_tick = 1000;
  ticket.reservation_end = kNoWindowBound;
  ticket.quantum = 64;
  ticket.state = AttemptState::Started;
  ticket.digest = 0xABCDEF01u;
  return ticket;
}

CompletionEvidence sample_evidence() {
  CompletionEvidence evidence;
  evidence.schedule = ScheduleId(5);
  evidence.schedule_generation = Generation(2);
  evidence.attempt = DispatchAttemptId(11);
  evidence.epoch = FabricEpoch(3);
  evidence.flow = FlowId(7);
  evidence.flow_generation = Generation(4);
  evidence.worker = WorkerId(21);
  evidence.boot = BootId(22);
  evidence.outcome = CompletionOutcome::Served;
  evidence.served = 17;
  evidence.effect_code = 9;
  evidence.effect_digest = 0x0F0F0F0Fu;
  evidence.observed_tick = 123;
  evidence.provenance.origin = ProvenanceOrigin::External;
  evidence.provenance.sequence = 4;
  evidence.provenance.digest = 0x5555u;
  return evidence;
}

}  // namespace

// ---------------------------------------------------------------------------
// Framing
// ---------------------------------------------------------------------------

FLOW_TEST(Framing, frame_round_trip_empty_and_max_payload) {
  const std::uint16_t hello_type = static_cast<std::uint16_t>(MessageType::Hello);

  const Result<std::string> empty_frame = encode_frame(hello_type, std::string_view{});
  REQUIRE(empty_frame.ok());
  CHECK_EQ(empty_frame.value().size(), kFrameOverhead);

  std::string_view empty_payload;
  const Result<FrameHeader> empty_header = decode_frame(empty_frame.value(), empty_payload);
  REQUIRE(empty_header.ok());
  CHECK_EQ(static_cast<unsigned>(empty_header.value().version),
           static_cast<unsigned>(kProtocolVersion));
  CHECK_EQ(static_cast<unsigned>(empty_header.value().type),
           static_cast<unsigned>(MessageType::Hello));
  CHECK_EQ(empty_header.value().flags, 0u);
  CHECK_EQ(empty_header.value().payload_length, 0u);
  CHECK_EQ(empty_payload.size(), 0u);

  // A maximal payload is the largest frame the contract permits. It is accepted
  // on both directions, and the decoded view aliases the encoded buffer rather
  // than copying it.
  const std::string large_payload = patterned_payload(kMaxFramePayload, 0x5Au);
  const Result<std::string> large_frame =
      encode_frame(static_cast<std::uint16_t>(MessageType::Completion), large_payload);
  REQUIRE(large_frame.ok());
  CHECK_EQ(large_frame.value().size(), kMaxFramePayload + kFrameOverhead);

  std::string_view large_view;
  const Result<FrameHeader> large_header = decode_frame(large_frame.value(), large_view);
  REQUIRE(large_header.ok());
  CHECK_EQ(large_header.value().payload_length, kMaxFramePayload);
  CHECK_EQ(large_view.size(), large_payload.size());
  CHECK(std::memcmp(large_view.data(), large_payload.data(), large_payload.size()) == 0);
  CHECK(large_view.data() >= large_frame.value().data());
  CHECK(large_view.data() + large_view.size() <=
        large_frame.value().data() + large_frame.value().size());

  // One byte past the payload bound is refused by the encoder, as is a message
  // type outside the wire contract.
  const std::string oversized(kMaxFramePayload + 1, 'p');
  CHECK_ERROR(as_status(encode_frame(hello_type, oversized)), ErrorCode::Oversized);
  CHECK_ERROR(as_status(encode_frame(static_cast<std::uint16_t>(0), std::string_view{})),
              ErrorCode::InvalidArgument);
  CHECK_ERROR(as_status(encode_frame(static_cast<std::uint16_t>(1025), std::string_view{})),
              ErrorCode::InvalidArgument);
  CHECK_OK(as_status(encode_frame(static_cast<std::uint16_t>(1024), std::string_view{})));
}

FLOW_TEST(Framing, frame_decoder_rejects_malformed_headers) {
  const std::string valid = valid_frame();
  CHECK_EQ(valid.size(), kFrameOverhead + 64u);
  CHECK_EQ(peek_u32(valid, 0), kFrameMagic);

  // Shorter than the fixed overhead: nothing can be validated yet.
  CHECK_ERROR(decode_status(std::string()), ErrorCode::Truncated);
  CHECK_ERROR(decode_status(valid.substr(0, 1)), ErrorCode::Truncated);
  CHECK_ERROR(decode_status(valid.substr(0, kFrameHeaderBytes - 1)), ErrorCode::Truncated);
  CHECK_ERROR(decode_status(valid.substr(0, kFrameOverhead - 1)), ErrorCode::Truncated);

  std::string corrupt = valid;
  patch_u32(corrupt, 0, 0xDEADBEEFu);
  CHECK_ERROR(decode_status(corrupt), ErrorCode::ProtocolViolation);

  corrupt = valid;
  patch_u16(corrupt, 4, static_cast<std::uint16_t>(kProtocolVersion + 1));
  CHECK_ERROR(decode_status(corrupt), ErrorCode::Unsupported);

  corrupt = valid;
  patch_u32(corrupt, 8, 1u);
  CHECK_ERROR(decode_status(corrupt), ErrorCode::ProtocolViolation);

  corrupt = valid;
  patch_u16(corrupt, 6, static_cast<std::uint16_t>(0));
  CHECK_ERROR(decode_status(corrupt), ErrorCode::ProtocolViolation);

  corrupt = valid;
  patch_u16(corrupt, 6, static_cast<std::uint16_t>(1025));
  CHECK_ERROR(decode_status(corrupt), ErrorCode::ProtocolViolation);

  // A declared length beyond the bound is refused before the body is examined,
  // even though the header checksum still matches.
  corrupt = valid;
  patch_u16(corrupt, 6, static_cast<std::uint16_t>(MessageType::Hello));
  patch_u32(corrupt, 12, kMaxFramePayload + 1u);
  reseal_header_crc(corrupt);
  CHECK_ERROR(decode_status(corrupt), ErrorCode::Oversized);

  // A plausible declared length that simply is not there yet is reported as a
  // truncated body, not as corruption.
  corrupt = valid;
  patch_u32(corrupt, 12, 4096u);
  reseal_header_crc(corrupt);
  CHECK_ERROR(decode_status(corrupt), ErrorCode::Truncated);

  // Header checksum covers exactly the first 16 bytes of the header.
  corrupt = valid;
  patch_u16(corrupt, 6,
           static_cast<std::uint16_t>(static_cast<std::uint16_t>(MessageType::Hello) + 1u));
  CHECK_ERROR(decode_status(corrupt), ErrorCode::CorruptData);

  corrupt = valid;
  patch_u32(corrupt, 16, peek_u32(valid, 16) ^ 0x1u);
  CHECK_ERROR(decode_status(corrupt), ErrorCode::CorruptData);

  // Payload checksum covers the payload bytes only.
  corrupt = valid;
  corrupt[kFrameHeaderBytes + 3] = static_cast<char>(corrupt[kFrameHeaderBytes + 3] ^ 0x40);
  CHECK_ERROR(decode_status(corrupt), ErrorCode::CorruptData);

  corrupt = valid;
  corrupt.back() = static_cast<char>(corrupt.back() ^ 0x01);
  CHECK_ERROR(decode_status(corrupt), ErrorCode::CorruptData);
}

FLOW_TEST(Framing, frame_decoder_accepts_valid_prefix_and_ignores_trailing_bytes) {
  const std::string valid = valid_frame();
  const std::string extended = valid + "trailing-bytes-after-a-complete-frame";

  // Documented behaviour: decode_frame validates the prefix that the header
  // describes and reports its payload; it deliberately does not require the
  // buffer to end at the frame boundary, because stream transports hand it an
  // arbitrary window. Consuming exactly kFrameOverhead + payload_length bytes is
  // the caller's (and FrameAccumulator's) responsibility.
  std::string_view payload;
  const Result<FrameHeader> header = decode_frame(extended, payload);
  REQUIRE(header.ok());
  CHECK_EQ(header.value().payload_length, 64u);
  CHECK_EQ(payload.size(), 64u);
  CHECK(std::memcmp(payload.data(), valid.data() + kFrameHeaderBytes, 64u) == 0);

  // The accumulator consumes exactly one frame and leaves the rest buffered.
  FrameAccumulator accumulator;
  REQUIRE_OK(accumulator.push(extended));
  FrameHeader popped;
  std::string popped_payload;
  const Result<bool> first = accumulator.next(popped, popped_payload);
  REQUIRE(first.ok());
  CHECK(first.value());
  CHECK_EQ(accumulator.buffered(), extended.size() - valid.size());
}

FLOW_TEST(Framing, frame_accumulator_streaming_reassembly) {
  const std::string hello = frame_of(static_cast<std::uint16_t>(MessageType::Hello), std::string());
  const std::string dispatch =
      frame_of(static_cast<std::uint16_t>(MessageType::Dispatch), patterned_payload(17, 0x22u));
  const std::string shutdown =
      frame_of(static_cast<std::uint16_t>(MessageType::Shutdown), patterned_payload(300, 0x33u));
  const std::string stream = hello + dispatch + shutdown;

  // Byte at a time must yield exactly the same frames as one coalesced push.
  FrameAccumulator trickle;
  std::vector<std::string> decoded;
  for (std::size_t index = 0; index < stream.size(); ++index) {
    REQUIRE_OK(trickle.push(std::string_view(stream).substr(index, 1)));
    for (;;) {
      FrameHeader header;
      std::string payload;
      const Result<bool> step = trickle.next(header, payload);
      REQUIRE(step.ok());
      if (!step.value()) {
        break;
      }
      CHECK_EQ(static_cast<std::size_t>(header.payload_length), payload.size());
      const Result<std::string> reencoded = encode_frame(header.type, payload, header.flags);
      REQUIRE(reencoded.ok());
      decoded.push_back(reencoded.value());
    }
  }
  CHECK_EQ(decoded.size(), 3u);
  CHECK_EQ(decoded[0], hello);
  CHECK_EQ(decoded[1], dispatch);
  CHECK_EQ(decoded[2], shutdown);
  CHECK_EQ(trickle.frames_decoded(), 3u);
  CHECK_EQ(trickle.buffered(), 0u);

  // Two frames in a single push.
  FrameAccumulator coalesced;
  REQUIRE_OK(coalesced.push(dispatch + shutdown));
  FrameHeader header;
  std::string payload;
  const Result<bool> first = coalesced.next(header, payload);
  REQUIRE(first.ok());
  CHECK(first.value());
  CHECK_EQ(payload, patterned_payload(17, 0x22u));
  const Result<bool> second = coalesced.next(header, payload);
  REQUIRE(second.ok());
  CHECK(second.value());
  CHECK_EQ(payload, patterned_payload(300, 0x33u));
  const Result<bool> exhausted = coalesced.next(header, payload);
  REQUIRE(exhausted.ok());
  CHECK(!exhausted.value());
  CHECK_EQ(coalesced.frames_decoded(), 2u);
  CHECK_EQ(coalesced.buffered(), 0u);

  // One frame split across three pushes.
  FrameAccumulator split;
  const std::size_t first_cut = 7;
  const std::size_t second_cut = hello.size() + 5;
  REQUIRE_OK(split.push(std::string_view(stream).substr(0, first_cut)));
  const Result<bool> partial = split.next(header, payload);
  REQUIRE(partial.ok());
  CHECK(!partial.value());
  CHECK_EQ(split.buffered(), first_cut);

  REQUIRE_OK(split.push(std::string_view(stream).substr(first_cut, second_cut - first_cut)));
  const Result<bool> first_frame = split.next(header, payload);
  REQUIRE(first_frame.ok());
  CHECK(first_frame.value());
  CHECK_EQ(payload.size(), 0u);
  const Result<bool> needs_more = split.next(header, payload);
  REQUIRE(needs_more.ok());
  CHECK(!needs_more.value());

  REQUIRE_OK(split.push(std::string_view(stream).substr(second_cut)));
  const Result<bool> second_frame = split.next(header, payload);
  REQUIRE(second_frame.ok());
  CHECK(second_frame.value());
  CHECK_EQ(payload, patterned_payload(17, 0x22u));
  const Result<bool> third_frame = split.next(header, payload);
  REQUIRE(third_frame.ok());
  CHECK(third_frame.value());
  CHECK_EQ(payload, patterned_payload(300, 0x33u));
  CHECK_EQ(split.frames_decoded(), 3u);
  CHECK_EQ(split.buffered(), 0u);
}

FLOW_TEST(Framing, frame_accumulator_oversized_bounds) {
  const std::size_t push_bound = kMaxFramePayload + kFrameOverhead;

  // Anti-allocation property: a header declaring more than kMaxFramePayload is
  // rejected as Oversized as soon as the header is buffered, long before the
  // claimed payload arrives.
  std::string lying_header = valid_frame().substr(0, kFrameHeaderBytes);
  patch_u32(lying_header, 12, kMaxFramePayload + 1u);
  FrameAccumulator lying;
  REQUIRE_OK(lying.push(lying_header));
  FrameHeader header;
  std::string payload;
  CHECK_ERROR(as_status(lying.next(header, payload)), ErrorCode::Oversized);
  CHECK_EQ(lying.buffered(), kFrameHeaderBytes);

  // A single push larger than the documented per-push bound is refused before
  // anything is buffered.
  FrameAccumulator single;
  const std::string too_large(push_bound + 1, 'x');
  CHECK_ERROR(single.push(too_large), ErrorCode::Oversized);
  CHECK_EQ(single.buffered(), 0u);

  // The implemented ceiling on buffered bytes is twice the per-push bound (one
  // complete maximal frame plus one maximal pending push): two maximal pushes
  // fit, a single byte more does not.
  FrameAccumulator bounded;
  const std::string maximal(push_bound, 'y');
  CHECK_OK(bounded.push(maximal));
  CHECK_OK(bounded.push(maximal));
  CHECK_EQ(bounded.buffered(), 2u * push_bound);
  CHECK_ERROR(bounded.push("z"), ErrorCode::Oversized);
  CHECK_EQ(bounded.buffered(), 2u * push_bound);

  // The buffered bytes are still usable after the refused push.
  FrameAccumulator recovered;
  CHECK_OK(recovered.push(frame_of(static_cast<std::uint16_t>(MessageType::Heartbeat),
                                  patterned_payload(8, 0x44u))));
  CHECK_ERROR(recovered.push(std::string(push_bound + 1, 'q')), ErrorCode::Oversized);
  const Result<bool> step = recovered.next(header, payload);
  REQUIRE(step.ok());
  CHECK(step.value());
  CHECK_EQ(payload, patterned_payload(8, 0x44u));
}

FLOW_TEST(Framing, frame_accumulator_reset_discards_buffered_bytes) {
  FrameAccumulator accumulator;
  CHECK_EQ(accumulator.bytes_discarded(), 0u);
  accumulator.reset();
  CHECK_EQ(accumulator.bytes_discarded(), 0u);
  CHECK_EQ(accumulator.frames_decoded(), 0u);

  const std::string frame = valid_frame();
  REQUIRE_OK(accumulator.push(std::string_view(frame).substr(0, 10)));
  CHECK_EQ(accumulator.buffered(), 10u);
  accumulator.reset();
  CHECK_EQ(accumulator.buffered(), 0u);
  CHECK_EQ(accumulator.bytes_discarded(), 10u);

  REQUIRE_OK(accumulator.push(std::string_view(frame).substr(0, 5)));
  accumulator.reset();
  CHECK_EQ(accumulator.bytes_discarded(), 15u);
  CHECK_EQ(accumulator.buffered(), 0u);

  // A completed frame leaves nothing buffered, so resetting afterwards discards
  // nothing and never rewinds frames_decoded().
  REQUIRE_OK(accumulator.push(frame));
  FrameHeader header;
  std::string payload;
  const Result<bool> step = accumulator.next(header, payload);
  REQUIRE(step.ok());
  CHECK(step.value());
  CHECK_EQ(accumulator.buffered(), 0u);
  accumulator.reset();
  CHECK_EQ(accumulator.bytes_discarded(), 15u);
  CHECK_EQ(accumulator.frames_decoded(), 1u);
}

FLOW_TEST(Framing, frame_fuzz_deterministic) {
  // Seeded and exactly reproducible: the same 2000 byte strings are generated
  // on every run, so a failure is always diagnosable from the seed.
  constexpr std::size_t kIterations = 2000;
  constexpr std::size_t kMaxLength = 4096;
  Rng rng(0x5EED1234u);
  std::size_t frames_extracted = 0;
  std::size_t largest_buffer = 0;

  for (std::size_t iteration = 0; iteration < kIterations; ++iteration) {
    const std::size_t length = static_cast<std::size_t>(rng.next_below(kMaxLength + 1));
    std::string bytes(length, '\0');
    for (std::size_t index = 0; index < length; ++index) {
      bytes[index] = static_cast<char>(rng.next_below(256u));
    }

    FrameAccumulator accumulator;
    REQUIRE_OK(accumulator.push(bytes));
    std::size_t consumed = 0;
    for (;;) {
      FrameHeader header;
      std::string payload;
      const Result<bool> step = accumulator.next(header, payload);
      if (!step.ok()) {
        // Random noise may be rejected for any of the documented reasons, but
        // never for an undocumented one.
        const ErrorCode code = step.code();
        CHECK(code == ErrorCode::Truncated || code == ErrorCode::ProtocolViolation ||
              code == ErrorCode::Unsupported || code == ErrorCode::Oversized ||
              code == ErrorCode::CorruptData);
        break;
      }
      if (!step.value()) {
        break;
      }
      // Every accepted frame must re-encode to exactly the bytes it was decoded
      // from, which is what makes the framing deterministic.
      const Result<std::string> reencoded = encode_frame(header.type, payload, header.flags);
      REQUIRE(reencoded.ok());
      CHECK(consumed + reencoded.value().size() <= bytes.size());
      CHECK(bytes.compare(consumed, reencoded.value().size(), reencoded.value()) == 0);
      consumed += reencoded.value().size();
      ++frames_extracted;
    }
    largest_buffer = std::max(largest_buffer, accumulator.buffered());
    CHECK(accumulator.buffered() <= 2u * (kMaxFramePayload + kFrameOverhead));
    CHECK(accumulator.frames_decoded() <= bytes.size() / kFrameOverhead);
  }
  // Random noise cannot contain a valid header checksum in practice, and cannot
  // buffer more than the input itself.
  CHECK(frames_extracted <= kIterations);
  CHECK(largest_buffer <= kMaxLength);

  // The same generator also drives a structured stream of valid frames split
  // into random chunks, so that the re-encode invariant is exercised on real
  // frames rather than only on rejected noise.
  constexpr std::size_t kStructuredIterations = 400;
  Rng structured(0x0F1E2D3Cu);
  for (std::size_t iteration = 0; iteration < kStructuredIterations; ++iteration) {
    const std::size_t frame_count = 1 + static_cast<std::size_t>(structured.next_below(3u));
    std::vector<std::string> frames;
    std::string stream;
    for (std::size_t index = 0; index < frame_count; ++index) {
      const std::size_t payload_length = static_cast<std::size_t>(structured.next_below(300u));
      const std::uint8_t seed = static_cast<std::uint8_t>(structured.next_below(256u));
      const std::uint16_t type = static_cast<std::uint16_t>(1u + structured.next_below(1024u));
      const std::string frame = frame_of(type, patterned_payload(payload_length, seed));
      frames.push_back(frame);
      stream += frame;
    }

    FrameAccumulator accumulator;
    std::size_t offset = 0;
    std::size_t decoded = 0;
    while (offset < stream.size()) {
      const std::size_t chunk = 1 + static_cast<std::size_t>(structured.next_below(7u));
      const std::size_t end = std::min(stream.size(), offset + chunk);
      REQUIRE_OK(accumulator.push(std::string_view(stream).substr(offset, end - offset)));
      offset = end;
      for (;;) {
        FrameHeader header;
        std::string payload;
        const Result<bool> step = accumulator.next(header, payload);
        REQUIRE(step.ok());
        if (!step.value()) {
          break;
        }
        REQUIRE(decoded < frames.size());
        const Result<std::string> reencoded = encode_frame(header.type, payload, header.flags);
        REQUIRE(reencoded.ok());
        CHECK_EQ(reencoded.value(), frames[decoded]);
        ++decoded;
      }
    }
    CHECK_EQ(decoded, frames.size());
    CHECK_EQ(accumulator.frames_decoded(), frames.size());
    CHECK_EQ(accumulator.buffered(), 0u);
  }
}

// ---------------------------------------------------------------------------
// Protocol codecs
// ---------------------------------------------------------------------------

FLOW_TEST(Protocol, message_round_trip_is_byte_exact) {
  const HelloMessage hello = sample_hello();
  const std::string hello_bytes = encode(hello);
  const Result<HelloMessage> decoded_hello = decode_hello(hello_bytes);
  REQUIRE(decoded_hello.ok());
  CHECK(decoded_hello.value().worker == hello.worker);
  CHECK(decoded_hello.value().boot == hello.boot);
  CHECK_EQ(decoded_hello.value().label, hello.label);
  CHECK_EQ(decoded_hello.value().supported_protocol, static_cast<std::uint64_t>(kProtocolVersion));
  CHECK_EQ(encode(decoded_hello.value()), hello_bytes);

  const HelloAckMessage ack = sample_hello_ack();
  const std::string ack_bytes = encode(ack);
  const Result<HelloAckMessage> decoded_ack = decode_hello_ack(ack_bytes);
  REQUIRE(decoded_ack.ok());
  CHECK_EQ(decoded_ack.value().accepted, true);
  CHECK_EQ(decoded_ack.value().detail, ack.detail);
  CHECK_EQ(decoded_ack.value().worker.value(), ack.worker.value());
  CHECK_EQ(decoded_ack.value().epoch.value(), ack.epoch.value());
  CHECK_EQ(decoded_ack.value().max_frame_payload, ack.max_frame_payload);
  CHECK_EQ(encode(decoded_ack.value()), ack_bytes);

  const DispatchMessage dispatch = DispatchMessage{sample_ticket(), 4096};
  const std::string dispatch_bytes = encode(dispatch);
  const Result<DispatchMessage> decoded_dispatch = decode_dispatch(dispatch_bytes);
  REQUIRE(decoded_dispatch.ok());
  const DispatchTicket& ticket = decoded_dispatch.value().ticket;
  CHECK(ticket.attempt == dispatch.ticket.attempt);
  CHECK(ticket.epoch == dispatch.ticket.epoch);
  CHECK(ticket.schedule == dispatch.ticket.schedule);
  CHECK(ticket.flow == dispatch.ticket.flow);
  CHECK(ticket.path == dispatch.ticket.path);
  CHECK(ticket.resource == dispatch.ticket.resource);
  CHECK(ticket.qos == dispatch.ticket.qos);
  CHECK(ticket.priority == dispatch.ticket.priority);
  CHECK(ticket.policy == dispatch.ticket.policy);
  CHECK(ticket.worker == dispatch.ticket.worker);
  CHECK(ticket.boot == dispatch.ticket.boot);
  CHECK_EQ(ticket.start_tick, dispatch.ticket.start_tick);
  CHECK_EQ(ticket.deadline_tick, dispatch.ticket.deadline_tick);
  CHECK_EQ(ticket.reservation_end, dispatch.ticket.reservation_end);
  CHECK_EQ(ticket.quantum, dispatch.ticket.quantum);
  CHECK_EQ(ticket.digest, dispatch.ticket.digest);
  CHECK(ticket.state == dispatch.ticket.state);
  CHECK_EQ(decoded_dispatch.value().nominal_work, dispatch.nominal_work);
  CHECK_EQ(encode(decoded_dispatch.value()), dispatch_bytes);

  const CompletionMessage completion = CompletionMessage{sample_evidence()};
  const std::string completion_bytes = encode(completion);
  const Result<CompletionMessage> decoded_completion = decode_completion(completion_bytes);
  REQUIRE(decoded_completion.ok());
  const CompletionEvidence& evidence = decoded_completion.value().evidence;
  CHECK(evidence.schedule == completion.evidence.schedule);
  CHECK(evidence.attempt == completion.evidence.attempt);
  CHECK(evidence.epoch == completion.evidence.epoch);
  CHECK(evidence.flow == completion.evidence.flow);
  CHECK(evidence.worker == completion.evidence.worker);
  CHECK(evidence.outcome == completion.evidence.outcome);
  CHECK_EQ(evidence.served, completion.evidence.served);
  CHECK_EQ(evidence.effect_code, completion.evidence.effect_code);
  CHECK_EQ(evidence.effect_digest, completion.evidence.effect_digest);
  CHECK_EQ(evidence.observed_tick, completion.evidence.observed_tick);
  CHECK(evidence.provenance == completion.evidence.provenance);
  CHECK_EQ(evidence.digest(), completion.evidence.digest());
  CHECK_EQ(encode(decoded_completion.value()), completion_bytes);

  const DispatchRejectedMessage rejected =
      DispatchRejectedMessage{DispatchAttemptId(11), FabricEpoch(3), ErrorCode::StaleEpoch,
                              "retired epoch"};
  const std::string rejected_bytes = encode(rejected);
  const Result<DispatchRejectedMessage> decoded_rejected = decode_dispatch_rejected(rejected_bytes);
  REQUIRE(decoded_rejected.ok());
  CHECK(decoded_rejected.value().attempt == rejected.attempt);
  CHECK(decoded_rejected.value().code == rejected.code);
  CHECK_EQ(decoded_rejected.value().detail, rejected.detail);
  CHECK_EQ(encode(decoded_rejected.value()), rejected_bytes);

  const StartedMessage started = StartedMessage{DispatchAttemptId(11), FabricEpoch(3), 250};
  const std::string started_bytes = encode(started);
  const Result<StartedMessage> decoded_started = decode_started(started_bytes);
  REQUIRE(decoded_started.ok());
  CHECK(decoded_started.value().attempt == started.attempt);
  CHECK_EQ(decoded_started.value().at_tick, started.at_tick);
  CHECK_EQ(encode(decoded_started.value()), started_bytes);

  const ShutdownMessage shutdown = ShutdownMessage{"draining"};
  const std::string shutdown_bytes = encode(shutdown);
  const Result<ShutdownMessage> decoded_shutdown = decode_shutdown(shutdown_bytes);
  REQUIRE(decoded_shutdown.ok());
  CHECK_EQ(decoded_shutdown.value().reason, shutdown.reason);
  CHECK_EQ(encode(decoded_shutdown.value()), shutdown_bytes);

  const ErrorMessage failure = ErrorMessage{ErrorCode::Busy, "coordinator busy"};
  const std::string failure_bytes = encode(failure);
  const Result<ErrorMessage> decoded_failure = decode_error(failure_bytes);
  REQUIRE(decoded_failure.ok());
  CHECK(decoded_failure.value().code == failure.code);
  CHECK_EQ(decoded_failure.value().detail, failure.detail);
  CHECK_EQ(encode(decoded_failure.value()), failure_bytes);
}

FLOW_TEST(Protocol, decoder_rejects_malformed_payloads) {
  const std::string hello_bytes = encode(sample_hello());
  const std::string ack_bytes = encode(sample_hello_ack());
  const std::string dispatch_bytes = encode(DispatchMessage{sample_ticket(), 7});
  const std::string completion_bytes = encode(CompletionMessage{sample_evidence()});
  const std::string rejected_bytes =
      encode(DispatchRejectedMessage{DispatchAttemptId(11), FabricEpoch(3), ErrorCode::Busy, "x"});
  const std::string started_bytes = encode(StartedMessage{DispatchAttemptId(11), FabricEpoch(3), 5});
  const std::string shutdown_bytes = encode(ShutdownMessage{"draining"});
  const std::string error_bytes = encode(ErrorMessage{ErrorCode::Busy, "busy"});

  // Truncation: a payload that ends before its declared fields is Truncated.
  CHECK_ERROR(as_status(decode_hello(hello_bytes.substr(0, hello_bytes.size() - 1))),
              ErrorCode::Truncated);
  CHECK_ERROR(as_status(decode_hello_ack(ack_bytes.substr(0, ack_bytes.size() - 1))),
              ErrorCode::Truncated);
  CHECK_ERROR(as_status(decode_dispatch(dispatch_bytes.substr(0, dispatch_bytes.size() - 1))),
              ErrorCode::Truncated);
  CHECK_ERROR(as_status(decode_completion(completion_bytes.substr(0, completion_bytes.size() - 1))),
              ErrorCode::Truncated);
  CHECK_ERROR(
      as_status(decode_dispatch_rejected(rejected_bytes.substr(0, rejected_bytes.size() - 1))),
      ErrorCode::Truncated);
  CHECK_ERROR(as_status(decode_started(started_bytes.substr(0, started_bytes.size() - 1))),
              ErrorCode::Truncated);
  CHECK_ERROR(as_status(decode_shutdown(shutdown_bytes.substr(0, shutdown_bytes.size() - 1))),
              ErrorCode::Truncated);
  CHECK_ERROR(as_status(decode_error(error_bytes.substr(0, error_bytes.size() - 1))),
              ErrorCode::Truncated);

  // Trailing bytes are never ignored: every decoder is strict.
  CHECK_ERROR(as_status(decode_hello(hello_bytes + std::string(1, '\0'))),
              ErrorCode::ProtocolViolation);
  CHECK_ERROR(as_status(decode_hello_ack(ack_bytes + std::string(1, '\0'))),
              ErrorCode::ProtocolViolation);
  CHECK_ERROR(as_status(decode_dispatch(dispatch_bytes + std::string(1, '\0'))),
              ErrorCode::ProtocolViolation);
  CHECK_ERROR(as_status(decode_completion(completion_bytes + std::string(1, '\0'))),
              ErrorCode::ProtocolViolation);
  CHECK_ERROR(as_status(decode_dispatch_rejected(rejected_bytes + std::string(1, '\0'))),
              ErrorCode::ProtocolViolation);
  CHECK_ERROR(as_status(decode_started(started_bytes + std::string(1, '\0'))),
              ErrorCode::ProtocolViolation);
  CHECK_ERROR(as_status(decode_shutdown(shutdown_bytes + std::string(1, '\0'))),
              ErrorCode::ProtocolViolation);
  CHECK_ERROR(as_status(decode_error(error_bytes + std::string(1, '\0'))),
              ErrorCode::ProtocolViolation);

  // Out-of-range enumerations are rejected, never clamped.
  CompletionEvidence bad_outcome = sample_evidence();
  bad_outcome.outcome = static_cast<CompletionOutcome>(3);
  CHECK_ERROR(as_status(decode_completion(encode(CompletionMessage{bad_outcome}))),
              ErrorCode::ProtocolViolation);

  CompletionEvidence bad_origin = sample_evidence();
  bad_origin.provenance.origin = static_cast<ProvenanceOrigin>(5);
  CHECK_ERROR(as_status(decode_completion(encode(CompletionMessage{bad_origin}))),
              ErrorCode::ProtocolViolation);

  DispatchMessage bad_state;
  bad_state.ticket = sample_ticket();
  bad_state.ticket.state = static_cast<AttemptState>(7);
  CHECK_ERROR(as_status(decode_dispatch(encode(bad_state))), ErrorCode::ProtocolViolation);

  // Unbound identities.
  HelloMessage unbound_worker = sample_hello();
  unbound_worker.worker = WorkerId(0);
  CHECK_ERROR(as_status(decode_hello(encode(unbound_worker))), ErrorCode::HandshakeRejected);

  StartedMessage unbound_attempt;
  unbound_attempt.attempt = DispatchAttemptId(0);
  unbound_attempt.epoch = FabricEpoch(3);
  CHECK_ERROR(as_status(decode_started(encode(unbound_attempt))), ErrorCode::ProtocolViolation);

  CompletionEvidence unbound_evidence = sample_evidence();
  unbound_evidence.worker = WorkerId(0);
  CHECK_ERROR(as_status(decode_completion(encode(CompletionMessage{unbound_evidence}))),
              ErrorCode::ProtocolViolation);

  DispatchMessage unbound_policy;
  unbound_policy.ticket = sample_ticket();
  unbound_policy.ticket.policy = PolicyId(0);
  CHECK_ERROR(as_status(decode_dispatch(encode(unbound_policy))), ErrorCode::ProtocolViolation);

  DispatchMessage unbound_qos;
  unbound_qos.ticket = sample_ticket();
  unbound_qos.ticket.qos = QoSClassId(0);
  CHECK_ERROR(as_status(decode_dispatch(encode(unbound_qos))), ErrorCode::ProtocolViolation);

  DispatchMessage unbound_resource;
  unbound_resource.ticket = sample_ticket();
  unbound_resource.ticket.resource = ResourceId(0);
  CHECK_ERROR(as_status(decode_dispatch(encode(unbound_resource))), ErrorCode::ProtocolViolation);

  // Characterisation: the wire codec accepts a ticket whose reservation
  // generation is set while the reservation identity is zero. That cross-field
  // contradiction is caught later, by the coordinator's ticket revalidation
  // (Scheduler::revalidate / begin_dispatch, ErrorCode::InvalidArgument), not by
  // decode_dispatch. Recorded so the asymmetry is visible rather than assumed.
  DispatchMessage orphan_generation;
  orphan_generation.ticket = sample_ticket();
  orphan_generation.ticket.reservation = ReservationId(0);
  orphan_generation.ticket.reservation_generation = Generation(1);
  CHECK_OK(as_status(decode_dispatch(encode(orphan_generation))));

  // Zero and oversized quantum tickets.
  DispatchMessage zero_quantum;
  zero_quantum.ticket = sample_ticket();
  zero_quantum.ticket.quantum = 0;
  CHECK_ERROR(as_status(decode_dispatch(encode(zero_quantum))), ErrorCode::ProtocolViolation);

  DispatchMessage huge_quantum;
  huge_quantum.ticket = sample_ticket();
  huge_quantum.ticket.quantum = kMaxQuantum + 1;
  CHECK_ERROR(as_status(decode_dispatch(encode(huge_quantum))), ErrorCode::ProtocolViolation);

  // Completion evidence claiming more service than the bound.
  CompletionEvidence huge_served = sample_evidence();
  huge_served.served = kMaxServiceUnits + 1;
  CHECK_ERROR(as_status(decode_completion(encode(CompletionMessage{huge_served}))),
              ErrorCode::ProtocolViolation);

  // Over-long bounded text fields. The encoders truncate them before they reach
  // the wire, so the decoder-side bound is exercised with record-shaped payloads
  // built field by field, which is exactly what a hostile peer would send.
  RecordWriter over_long_hello;
  over_long_hello.u64(21);
  over_long_hello.u64(22);
  over_long_hello.text(std::string(kMaxLabelBytes + 1, 'L'));
  over_long_hello.u64(kProtocolVersion);
  CHECK_ERROR(as_status(decode_hello(over_long_hello.take())), ErrorCode::Oversized);

  RecordWriter over_long_reason;
  over_long_reason.text(std::string(kMaxDetailBytes + 1, 'R'));
  CHECK_ERROR(as_status(decode_shutdown(over_long_reason.take())), ErrorCode::Oversized);

  RecordWriter over_long_detail;
  over_long_detail.u16(static_cast<std::uint16_t>(ErrorCode::Busy));
  over_long_detail.text(std::string(kMaxDetailBytes + 1, 'D'));
  CHECK_ERROR(as_status(decode_error(over_long_detail.take())), ErrorCode::Oversized);

  RecordWriter over_long_rejection;
  over_long_rejection.u64(11);
  over_long_rejection.u64(3);
  over_long_rejection.u16(static_cast<std::uint16_t>(ErrorCode::Busy));
  over_long_rejection.text(std::string(kMaxDetailBytes + 1, 'X'));
  CHECK_ERROR(as_status(decode_dispatch_rejected(over_long_rejection.take())),
              ErrorCode::Oversized);

  // ...while the encoders bound the same fields before they reach the wire.
  HelloMessage oversized_label = sample_hello();
  oversized_label.label = std::string(kMaxLabelBytes + 40, 'T');
  const Result<HelloMessage> trimmed = decode_hello(encode(oversized_label));
  REQUIRE(trimmed.ok());
  CHECK_EQ(trimmed.value().label.size(), kMaxLabelBytes);

  // An unsupported protocol version is a handshake rejection.
  HelloMessage wrong_protocol = sample_hello();
  wrong_protocol.supported_protocol = static_cast<std::uint64_t>(kProtocolVersion + 1);
  CHECK_ERROR(as_status(decode_hello(encode(wrong_protocol))), ErrorCode::HandshakeRejected);

  HelloMessage zero_protocol = sample_hello();
  zero_protocol.supported_protocol = 0;
  CHECK_ERROR(as_status(decode_hello(encode(zero_protocol))), ErrorCode::HandshakeRejected);

  // An accepted handshake must carry a usable authority tuple and frame bound.
  HelloAckMessage no_frame_bound = sample_hello_ack();
  no_frame_bound.max_frame_payload = 0;
  CHECK_ERROR(as_status(decode_hello_ack(encode(no_frame_bound))), ErrorCode::ProtocolViolation);

  HelloAckMessage frame_bound_too_large = sample_hello_ack();
  frame_bound_too_large.max_frame_payload = static_cast<std::uint32_t>(kMaxFramePayload) + 1u;
  CHECK_ERROR(as_status(decode_hello_ack(encode(frame_bound_too_large))),
              ErrorCode::ProtocolViolation);

  HelloAckMessage unbound_ack = sample_hello_ack();
  unbound_ack.worker = WorkerId(0);
  CHECK_ERROR(as_status(decode_hello_ack(encode(unbound_ack))), ErrorCode::ProtocolViolation);

  HelloAckMessage unbound_epoch = sample_hello_ack();
  unbound_epoch.epoch = FabricEpoch(0);
  CHECK_ERROR(as_status(decode_hello_ack(encode(unbound_epoch))), ErrorCode::ProtocolViolation);

  // A rejected handshake carries no authority and is therefore allowed to omit
  // those fields.
  HelloAckMessage refusal;
  refusal.accepted = false;
  refusal.reason = ErrorCode::HandshakeRejected;
  refusal.detail = "worker identity is unbound";
  CHECK_OK(as_status(decode_hello_ack(encode(refusal))));

  // Characterisation: decode_hello validates the worker identity and the
  // protocol version but not the boot identity, so an unbound boot id is
  // accepted today. Recorded here so that tightening it becomes a deliberate,
  // visible change rather than an accident.
  HelloMessage unbound_boot = sample_hello();
  unbound_boot.boot = BootId(0);
  CHECK_OK(as_status(decode_hello(encode(unbound_boot))));
}

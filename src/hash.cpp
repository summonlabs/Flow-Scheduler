// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#include "flow_scheduler/hash.hpp"

#include <array>
#include <cstring>
#include <limits>

namespace flow_scheduler {
namespace {

constexpr std::array<std::uint32_t, 256> make_crc32c_table() {
  std::array<std::uint32_t, 256> table{};
  for (std::uint32_t index = 0; index < 256; ++index) {
    std::uint32_t crc = index;
    for (int bit = 0; bit < 8; ++bit) {
      crc = (crc >> 1) ^ ((crc & 1u) != 0u ? 0x82F63B78u : 0u);
    }
    table[index] = crc;
  }
  return table;
}

constexpr auto kCrc32cTable = make_crc32c_table();

constexpr std::uint64_t kFnvOffset = 0xCBF29CE484222325ULL;
constexpr std::uint64_t kFnvPrime = 0x00000100000001B3ULL;

inline std::uint64_t fnv_byte(std::uint64_t state, std::uint8_t byte) noexcept {
  return (state ^ byte) * kFnvPrime;
}

}  // namespace

std::uint32_t crc32c(const void* data, std::size_t length) noexcept {
  return crc32c_extend(0u, data, length);
}

std::uint32_t crc32c(std::string_view data) noexcept {
  return crc32c_extend(0u, data.data(), data.size());
}

std::uint32_t crc32c_extend(std::uint32_t seed, const void* data, std::size_t length) noexcept {
  const auto* bytes = static_cast<const unsigned char*>(data);
  std::uint32_t crc = seed ^ 0xFFFFFFFFu;
  for (std::size_t index = 0; index < length; ++index) {
    crc = kCrc32cTable[(crc ^ bytes[index]) & 0xFFu] ^ (crc >> 8);
  }
  return crc ^ 0xFFFFFFFFu;
}

std::uint64_t fnv1a64(const void* data, std::size_t length) noexcept {
  const auto* bytes = static_cast<const unsigned char*>(data);
  std::uint64_t state = kFnvOffset;
  for (std::size_t index = 0; index < length; ++index) {
    state = fnv_byte(state, bytes[index]);
  }
  return state;
}

std::uint64_t fnv1a64(std::string_view data) noexcept {
  return fnv1a64(data.data(), data.size());
}

std::uint64_t fnv1a64_extend(std::uint64_t seed, const void* data, std::size_t length) noexcept {
  const auto* bytes = static_cast<const unsigned char*>(data);
  std::uint64_t state = seed;
  for (std::size_t index = 0; index < length; ++index) {
    state = fnv_byte(state, bytes[index]);
  }
  return state;
}

void Digest64::add_u8(std::uint8_t value) noexcept {
  state_ = fnv_byte(state_, value);
}

void Digest64::add_u16(std::uint16_t value) noexcept {
  add_u8(static_cast<std::uint8_t>(value & 0xFFu));
  add_u8(static_cast<std::uint8_t>((value >> 8) & 0xFFu));
}

void Digest64::add_u32(std::uint32_t value) noexcept {
  for (int shift = 0; shift < 32; shift += 8) {
    add_u8(static_cast<std::uint8_t>((value >> shift) & 0xFFu));
  }
}

void Digest64::add_u64(std::uint64_t value) noexcept {
  for (int shift = 0; shift < 64; shift += 8) {
    add_u8(static_cast<std::uint8_t>((value >> shift) & 0xFFu));
  }
}

void Digest64::add_bool(bool value) noexcept { add_u8(value ? 1u : 0u); }

void Digest64::add_bytes(const void* data, std::size_t length) noexcept {
  const auto* bytes = static_cast<const unsigned char*>(data);
  for (std::size_t index = 0; index < length; ++index) {
    state_ = fnv_byte(state_, bytes[index]);
  }
}

void Digest64::add_string(std::string_view text) noexcept {
  add_u64(text.size());
  add_bytes(text.data(), text.size());
}

std::uint64_t Rng::next_below(std::uint64_t bound) noexcept {
  if (bound == 0) {
    return 0;
  }
  // Rejection sampling keeps the distribution exactly uniform without
  // introducing a modulo bias, which matters because scenario parameters are
  // derived from these draws.
  const std::uint64_t limit = std::numeric_limits<std::uint64_t>::max() -
                              (std::numeric_limits<std::uint64_t>::max() % bound);
  for (;;) {
    const std::uint64_t draw = next_u64();
    if (draw < limit) {
      return draw % bound;
    }
  }
}

std::uint64_t Rng::next_range(std::uint64_t low, std::uint64_t high) noexcept {
  if (high <= low) {
    return low;
  }
  const std::uint64_t span = (high - low) + 1u;
  return low + next_below(span);
}

bool Rng::next_bool(double probability_true) noexcept {
  if (probability_true <= 0.0) {
    return false;
  }
  if (probability_true >= 1.0) {
    return true;
  }
  constexpr std::uint64_t kScale = 1ull << 32;
  const auto threshold = static_cast<std::uint64_t>(probability_true * static_cast<double>(kScale));
  return next_below(kScale) < threshold;
}

}  // namespace flow_scheduler

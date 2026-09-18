// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#ifndef FLOW_SCHEDULER_HASH_HPP
#define FLOW_SCHEDULER_HASH_HPP

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace flow_scheduler {

/// CRC-32C (Castagnoli, reflected, polynomial 0x1EDC6F41). Used for frame and
/// durable-record integrity. Implemented in software so that behaviour is
/// identical on every platform and no hardware instruction is required.
[[nodiscard]] std::uint32_t crc32c(const void* data, std::size_t length) noexcept;
[[nodiscard]] std::uint32_t crc32c(std::string_view data) noexcept;
[[nodiscard]] std::uint32_t crc32c_extend(std::uint32_t seed, const void* data,
                                          std::size_t length) noexcept;

/// FNV-1a 64. Used for deterministic, architecture independent digests where
/// cryptographic strength is not required: tie-break digests, explanation
/// digests, provenance digests, synthetic workload derivation.
[[nodiscard]] std::uint64_t fnv1a64(const void* data, std::size_t length) noexcept;
[[nodiscard]] std::uint64_t fnv1a64(std::string_view data) noexcept;
[[nodiscard]] std::uint64_t fnv1a64_extend(std::uint64_t seed, const void* data,
                                           std::size_t length) noexcept;

/// Mix a 64-bit value (SplitMix64 finalizer). Avalanches well and is fully
/// specified, so digests are reproducible across compilers and platforms.
[[nodiscard]] constexpr std::uint64_t mix64(std::uint64_t value) noexcept {
  value += 0x9E3779B97F4A7C15ULL;
  value = (value ^ (value >> 30)) * 0xBF58476D1CE4E5B9ULL;
  value = (value ^ (value >> 27)) * 0x94D049BB133111EBULL;
  return value ^ (value >> 31);
}

/// Incremental digest builder. Deterministic and endianness independent: every
/// integer is folded byte by byte in little-endian order.
class Digest64 {
 public:
  Digest64() noexcept = default;
  explicit Digest64(std::uint64_t seed) noexcept : state_(seed) {}

  void add_u8(std::uint8_t value) noexcept;
  void add_u16(std::uint16_t value) noexcept;
  void add_u32(std::uint32_t value) noexcept;
  void add_u64(std::uint64_t value) noexcept;
  void add_bool(bool value) noexcept;
  void add_bytes(const void* data, std::size_t length) noexcept;
  void add_string(std::string_view text) noexcept;

  [[nodiscard]] std::uint64_t value() const noexcept { return state_; }

 private:
  std::uint64_t state_{0xCBF29CE484222325ULL};
};

/// Deterministic pseudo random generator (SplitMix64). Seeded so that every
/// randomized and property test is exactly reproducible from its seed, which
/// is printed on failure.
class Rng {
 public:
  explicit Rng(std::uint64_t seed) noexcept : state_(seed) {}

  [[nodiscard]] std::uint64_t next_u64() noexcept {
    state_ += 0x9E3779B97F4A7C15ULL;
    return mix64(state_);
  }
  /// Uniform value in [0, bound). Returns 0 when bound == 0.
  [[nodiscard]] std::uint64_t next_below(std::uint64_t bound) noexcept;
  /// Uniform value in [low, high]. Returns low when high <= low.
  [[nodiscard]] std::uint64_t next_range(std::uint64_t low, std::uint64_t high) noexcept;
  [[nodiscard]] bool next_bool(double probability_true) noexcept;
  [[nodiscard]] std::uint64_t seed_state() const noexcept { return state_; }

 private:
  std::uint64_t state_;
};

}  // namespace flow_scheduler

#endif  // FLOW_SCHEDULER_HASH_HPP

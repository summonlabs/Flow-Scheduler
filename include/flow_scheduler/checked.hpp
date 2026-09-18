// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#ifndef FLOW_SCHEDULER_CHECKED_HPP
#define FLOW_SCHEDULER_CHECKED_HPP

#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>

#include "flow_scheduler/error.hpp"

namespace flow_scheduler {

/// Checked arithmetic used for every size, capacity, length, and tick value
/// that can be influenced from outside the library (descriptors, frames,
/// journals, benchmark parameters). Externally influenced values are never
/// allowed to wrap silently.

[[nodiscard]] Result<std::uint64_t> checked_add(std::uint64_t a, std::uint64_t b);
[[nodiscard]] Result<std::uint64_t> checked_sub(std::uint64_t a, std::uint64_t b);
[[nodiscard]] Result<std::uint64_t> checked_mul(std::uint64_t a, std::uint64_t b);
[[nodiscard]] Result<std::uint64_t> checked_add(std::uint64_t a, std::uint64_t b,
                                                std::uint64_t c);
[[nodiscard]] Result<std::uint64_t> checked_mul(std::uint64_t a, std::uint64_t b,
                                                std::uint64_t c);

/// Saturating addition: clamps at the maximum instead of failing. Only used
/// where an unbounded timeline value is genuinely allowed to pin at the top
/// (deadline sentinels), never for capacity accounting.
[[nodiscard]] constexpr std::uint64_t saturating_add(std::uint64_t a,
                                                     std::uint64_t b) noexcept {
  const std::uint64_t max = std::numeric_limits<std::uint64_t>::max();
  return (max - a < b) ? max : a + b;
}

[[nodiscard]] constexpr std::uint64_t saturating_sub(std::uint64_t a,
                                                     std::uint64_t b) noexcept {
  return (a < b) ? 0 : a - b;
}

/// Narrowing conversion that fails instead of truncating.
template <class To, class From>
[[nodiscard]] Result<To> checked_narrow(From value) {
  static_assert(std::is_integral_v<From> && std::is_integral_v<To>,
                "checked_narrow requires integral types");
  if constexpr (std::is_signed_v<From> == std::is_signed_v<To>) {
    if (value < static_cast<From>(std::numeric_limits<To>::min()) ||
        value > static_cast<From>(std::numeric_limits<To>::max())) {
      return Error(ErrorCode::OutOfRange, "value does not fit target type");
    }
    return static_cast<To>(value);
  } else if constexpr (std::is_signed_v<From>) {
    if (value < 0) {
      return Error(ErrorCode::OutOfRange, "negative value cannot narrow to unsigned");
    }
    const auto unsigned_value = static_cast<std::uint64_t>(value);
    if (unsigned_value > static_cast<std::uint64_t>(std::numeric_limits<To>::max())) {
      return Error(ErrorCode::OutOfRange, "value does not fit target type");
    }
    return static_cast<To>(unsigned_value);
  } else {
    const auto unsigned_value = static_cast<std::uint64_t>(value);
    if (unsigned_value > static_cast<std::uint64_t>(std::numeric_limits<To>::max())) {
      return Error(ErrorCode::OutOfRange, "value does not fit target type");
    }
    return static_cast<To>(unsigned_value);
  }
}

/// Increment that fails at the numeric limit rather than wrapping. Generations,
/// epochs, and sequence numbers use this so that a wrap can never resurrect
/// retired authority.
template <class T>
[[nodiscard]] Result<T> checked_next(T value) {
  static_assert(std::is_integral_v<T>, "checked_next requires an integral type");
  if (value == std::numeric_limits<T>::max()) {
    return Error(ErrorCode::ArithmeticOverflow, "counter exhausted; generation cannot advance");
  }
  return static_cast<T>(value + 1);
}

}  // namespace flow_scheduler

#endif  // FLOW_SCHEDULER_CHECKED_HPP

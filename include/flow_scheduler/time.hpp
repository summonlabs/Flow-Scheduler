// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#ifndef FLOW_SCHEDULER_TIME_HPP
#define FLOW_SCHEDULER_TIME_HPP

#include <cstdint>
#include <limits>

namespace flow_scheduler {

/// Scheduler timeline unit: nanoseconds on a monotonic, coordinator-owned
/// timeline. The runtime never interprets wall-clock time; callers supply the
/// current tick. All comparisons are unsigned and total.
using Ticks = std::uint64_t;

/// Sentinel meaning "no deadline bound". Never used as an arithmetic operand
/// without a saturating guard.
inline constexpr Ticks kNoDeadline = std::numeric_limits<Ticks>::max();
/// Sentinel meaning "no release restriction".
inline constexpr Ticks kReleasedImmediately = 0;
/// Sentinel meaning "no reservation window bound".
inline constexpr Ticks kNoWindowBound = std::numeric_limits<Ticks>::max();

[[nodiscard]] constexpr bool has_deadline(Ticks deadline) noexcept {
  return deadline != kNoDeadline;
}

[[nodiscard]] constexpr Ticks ticks_between(Ticks earlier, Ticks later) noexcept {
  return (later < earlier) ? 0 : (later - earlier);
}

/// Time source abstraction. Production uses a monotonic steady clock; tests and
/// benchmarks drive a manual clock so that every temporal decision is exactly
/// reproducible.
class Clock {
 public:
  Clock() = default;
  Clock(const Clock&) = delete;
  Clock& operator=(const Clock&) = delete;
  virtual ~Clock();

  [[nodiscard]] virtual Ticks now() const noexcept = 0;
};

/// Test/benchmark clock. The value only moves when the owner moves it.
class ManualClock final : public Clock {
 public:
  ManualClock() noexcept = default;
  explicit ManualClock(Ticks start) noexcept : now_(start) {}

  [[nodiscard]] Ticks now() const noexcept override { return now_; }
  void set(Ticks value) noexcept { now_ = value; }
  void advance(Ticks delta) noexcept { now_ += delta; }

 private:
  Ticks now_{0};
};

/// Monotonic production clock. Never goes backwards; never maps to wall time.
class SteadyClock final : public Clock {
 public:
  SteadyClock() noexcept;
  [[nodiscard]] Ticks now() const noexcept override;

 private:
  Ticks origin_{0};
};

}  // namespace flow_scheduler

#endif  // FLOW_SCHEDULER_TIME_HPP

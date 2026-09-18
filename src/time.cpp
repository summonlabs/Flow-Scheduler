// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#include "flow_scheduler/time.hpp"

#include <chrono>

namespace flow_scheduler {

Clock::~Clock() = default;

SteadyClock::SteadyClock() noexcept {
  origin_ = static_cast<Ticks>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

Ticks SteadyClock::now() const noexcept {
  const auto current = static_cast<Ticks>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
  // std::chrono::steady_clock is monotonic, so the difference is non-negative;
  // the guard keeps the contract explicit even if a platform misbehaves.
  return current >= origin_ ? current - origin_ : 0;
}

}  // namespace flow_scheduler

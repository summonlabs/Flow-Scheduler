// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#include "flow_scheduler/checked.hpp"

namespace flow_scheduler {
namespace {

constexpr std::uint64_t kMax = std::numeric_limits<std::uint64_t>::max();

}  // namespace

Result<std::uint64_t> checked_add(std::uint64_t a, std::uint64_t b) {
  if (a > kMax - b) {
    return Error(ErrorCode::ArithmeticOverflow, "unsigned addition overflow");
  }
  return a + b;
}

Result<std::uint64_t> checked_sub(std::uint64_t a, std::uint64_t b) {
  if (b > a) {
    return Error(ErrorCode::ArithmeticOverflow, "unsigned subtraction underflow");
  }
  return a - b;
}

Result<std::uint64_t> checked_mul(std::uint64_t a, std::uint64_t b) {
  if (a != 0 && b > kMax / a) {
    return Error(ErrorCode::ArithmeticOverflow, "unsigned multiplication overflow");
  }
  return a * b;
}

Result<std::uint64_t> checked_add(std::uint64_t a, std::uint64_t b, std::uint64_t c) {
  std::uint64_t partial = 0;
  FS_TRY_ASSIGN(partial, checked_add(a, b));
  return checked_add(partial, c);
}

Result<std::uint64_t> checked_mul(std::uint64_t a, std::uint64_t b, std::uint64_t c) {
  std::uint64_t partial = 0;
  FS_TRY_ASSIGN(partial, checked_mul(a, b));
  return checked_mul(partial, c);
}

}  // namespace flow_scheduler

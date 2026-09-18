// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#ifndef FLOW_SCHEDULER_ERROR_HPP
#define FLOW_SCHEDULER_ERROR_HPP

#include <cstdint>
#include <optional>
#include <string>
#include <utility>

namespace flow_scheduler {

/// Every failure mode this runtime can report. Codes are stable and part of
/// the public contract: callers may switch on them.
enum class ErrorCode : std::uint16_t {
  Ok = 0,

  // --- structural / argument validation --------------------------------
  InvalidArgument = 1,
  OutOfRange = 2,
  ArithmeticOverflow = 3,
  Bounded = 4,
  Unsupported = 5,
  Internal = 6,

  // --- lookup ----------------------------------------------------------
  NotFound = 7,
  AlreadyExists = 8,
  UnknownFlow = 9,
  UnknownSchedule = 10,
  UnknownResource = 11,
  UnknownReservation = 12,
  UnknownAttempt = 13,
  UnknownPolicy = 14,
  UnknownWorker = 15,

  // --- authority / staleness -------------------------------------------
  StaleFlowGeneration = 20,
  StalePathGeneration = 21,
  StaleResourceGeneration = 22,
  StaleReservationGeneration = 23,
  StalePolicyGeneration = 24,
  StaleQoSGeneration = 25,
  StalePriorityGeneration = 26,
  StaleScheduleGeneration = 27,
  StaleEpoch = 28,
  StaleAttempt = 29,
  StaleAuthority = 30,
  Fenced = 31,

  // --- lifecycle / legality --------------------------------------------
  InvalidState = 40,
  NotReady = 41,
  NotReleased = 42,
  DependencyUnsatisfied = 43,
  CapacityUnavailable = 44,
  ReservationClosed = 45,
  ReservationExhausted = 46,
  DeadlinePassed = 47,
  Cancelled = 48,
  ZeroQuantum = 49,
  ConcurrencyLimit = 50,
  NotPreemptible = 51,
  PreemptionThrottled = 52,

  // --- completion semantics --------------------------------------------
  DuplicateCompletion = 60,
  ContradictoryCompletion = 61,
  CompletionForClosedAttempt = 62,
  AmbiguousOutcome = 63,

  // --- durable state / transport ---------------------------------------
  CorruptData = 70,
  Truncated = 71,
  Oversized = 72,
  SequenceGap = 73,
  IoFailure = 74,
  TransportFailure = 75,
  PeerClosed = 76,
  HandshakeRejected = 77,
  ProtocolViolation = 78,
  Busy = 79,
};

/// Stable, human readable name for an error code. Never localized.
[[nodiscard]] const char* to_string(ErrorCode code) noexcept;

/// True when the code describes a condition that can become valid again after
/// enough time or state change (readiness, capacity, reservations, deadlines).
[[nodiscard]] bool is_transient(ErrorCode code) noexcept;

/// True when the code describes evidence that was superseded by newer
/// authority and therefore can never be revived for the same tuple.
[[nodiscard]] bool is_staleness(ErrorCode code) noexcept;

inline constexpr std::size_t kMaxErrorMessageBytes = 512;

/// Bounded failure value. Messages are truncated to kMaxErrorMessageBytes so
/// that adversarial input cannot inflate diagnostics without limit.
class Error {
 public:
  Error() noexcept = default;
  Error(ErrorCode code, std::string message);

  [[nodiscard]] ErrorCode code() const noexcept { return code_; }
  [[nodiscard]] const std::string& message() const noexcept { return message_; }
  [[nodiscard]] bool ok() const noexcept { return code_ == ErrorCode::Ok; }
  [[nodiscard]] std::string to_string() const;

 private:
  ErrorCode code_{ErrorCode::Ok};
  std::string message_;
};

/// Success-or-failure without a payload.
class Status {
 public:
  Status() noexcept = default;
  Status(Error error) noexcept : error_(std::move(error)) {}

  [[nodiscard]] static Status success() noexcept { return Status{}; }
  [[nodiscard]] static Status failure(ErrorCode code, std::string message) {
    return Status(Error(code, std::move(message)));
  }
  /// Convenience for the extremely common "named constant message" case.
  [[nodiscard]] static Status failure(ErrorCode code) {
    return Status(Error(code, std::string(flow_scheduler::to_string(code))));
  }

  [[nodiscard]] bool ok() const noexcept { return error_.ok(); }
  [[nodiscard]] explicit operator bool() const noexcept { return ok(); }
  [[nodiscard]] const Error& error() const noexcept { return error_; }
  [[nodiscard]] ErrorCode code() const noexcept { return error_.code(); }
  [[nodiscard]] std::string to_string() const { return error_.to_string(); }

  /// Discards the message and keeps only the code. Used when a lower layer
  /// already produced a more specific diagnostic.
  [[nodiscard]] Status with_code(ErrorCode code) const {
    return ok() ? Status{} : Status::failure(code);
  }

 private:
  Error error_{};
};

/// Success-or-failure carrying a value. Value access is unchecked by design:
/// callers must test ok() first, mirroring std::expected ergonomics without a
/// C++23 requirement.
template <class T>
class Result {
 public:
  Result(T value) noexcept(std::is_nothrow_move_constructible_v<T>)
      : value_(std::in_place, std::move(value)) {}

  Result(Error error) noexcept : error_(std::move(error)) {}

  Result(const Result&) = default;
  Result(Result&&) noexcept = default;
  Result& operator=(const Result&) = default;
  Result& operator=(Result&&) noexcept = default;
  ~Result() = default;

  [[nodiscard]] bool ok() const noexcept { return value_.has_value(); }
  [[nodiscard]] explicit operator bool() const noexcept { return ok(); }
  [[nodiscard]] const Error& error() const noexcept { return error_; }
  [[nodiscard]] ErrorCode code() const noexcept { return error_.code(); }

  [[nodiscard]] T& value() & noexcept { return *value_; }
  [[nodiscard]] const T& value() const& noexcept { return *value_; }
  [[nodiscard]] T&& value() && noexcept { return std::move(*value_); }

  [[nodiscard]] T value_or(T fallback) const {
    return value_.has_value() ? *value_ : std::move(fallback);
  }
  [[nodiscard]] const T* operator->() const noexcept { return &(*value_); }
  [[nodiscard]] T* operator->() noexcept { return &(*value_); }
  [[nodiscard]] const T& operator*() const& noexcept { return *value_; }
  [[nodiscard]] T& operator*() & noexcept { return *value_; }

 private:
  std::optional<T> value_;
  Error error_;
};

/// Propagate a Status failure out of the current function.
#define FS_RETURN_IF_ERROR(expr)                        \
  do {                                                  \
    const ::flow_scheduler::Status fs_status_ = (expr); \
    if (!fs_status_.ok()) {                             \
      return fs_status_.error();                        \
    }                                                   \
  } while (false)

#define FS_CONCAT_DETAIL(a, b) a##b
#define FS_CONCAT(a, b) FS_CONCAT_DETAIL(a, b)
#define FS_TRY_ASSIGN_DETAIL(name, decl, expr) \
  auto name = (expr);                          \
  if (!name.ok()) {                            \
    return name.error();                       \
  }                                            \
  decl = std::move(name).value()

/// Propagate a Result<T> failure. The first argument is either an already
/// declared variable or a declaration such as "const Foo bar", so the macro
/// works in both statement and declaration position. Each expansion gets a
/// unique temporary name from __COUNTER__.
#define FS_TRY_ASSIGN(decl, expr) \
  FS_TRY_ASSIGN_DETAIL(FS_CONCAT(fs_result_, __COUNTER__), decl, expr)

}  // namespace flow_scheduler

#endif  // FLOW_SCHEDULER_ERROR_HPP

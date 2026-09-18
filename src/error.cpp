// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#include "flow_scheduler/error.hpp"

namespace flow_scheduler {

const char* to_string(ErrorCode code) noexcept {
  switch (code) {
    case ErrorCode::Ok: return "ok";
    case ErrorCode::InvalidArgument: return "invalid-argument";
    case ErrorCode::OutOfRange: return "out-of-range";
    case ErrorCode::ArithmeticOverflow: return "arithmetic-overflow";
    case ErrorCode::Bounded: return "bound-exceeded";
    case ErrorCode::Unsupported: return "unsupported";
    case ErrorCode::Internal: return "internal";
    case ErrorCode::NotFound: return "not-found";
    case ErrorCode::AlreadyExists: return "already-exists";
    case ErrorCode::UnknownFlow: return "unknown-flow";
    case ErrorCode::UnknownSchedule: return "unknown-schedule";
    case ErrorCode::UnknownResource: return "unknown-resource";
    case ErrorCode::UnknownReservation: return "unknown-reservation";
    case ErrorCode::UnknownAttempt: return "unknown-attempt";
    case ErrorCode::UnknownPolicy: return "unknown-policy";
    case ErrorCode::UnknownWorker: return "unknown-worker";
    case ErrorCode::StaleFlowGeneration: return "stale-flow-generation";
    case ErrorCode::StalePathGeneration: return "stale-path-generation";
    case ErrorCode::StaleResourceGeneration: return "stale-resource-generation";
    case ErrorCode::StaleReservationGeneration: return "stale-reservation-generation";
    case ErrorCode::StalePolicyGeneration: return "stale-policy-generation";
    case ErrorCode::StaleQoSGeneration: return "stale-qos-generation";
    case ErrorCode::StalePriorityGeneration: return "stale-priority-generation";
    case ErrorCode::StaleScheduleGeneration: return "stale-schedule-generation";
    case ErrorCode::StaleEpoch: return "stale-epoch";
    case ErrorCode::StaleAttempt: return "stale-attempt";
    case ErrorCode::StaleAuthority: return "stale-authority";
    case ErrorCode::Fenced: return "fenced";
    case ErrorCode::InvalidState: return "invalid-state";
    case ErrorCode::NotReady: return "not-ready";
    case ErrorCode::NotReleased: return "not-released";
    case ErrorCode::DependencyUnsatisfied: return "dependency-unsatisfied";
    case ErrorCode::CapacityUnavailable: return "capacity-unavailable";
    case ErrorCode::ReservationClosed: return "reservation-closed";
    case ErrorCode::ReservationExhausted: return "reservation-exhausted";
    case ErrorCode::DeadlinePassed: return "deadline-passed";
    case ErrorCode::Cancelled: return "cancelled";
    case ErrorCode::ZeroQuantum: return "zero-quantum";
    case ErrorCode::ConcurrencyLimit: return "concurrency-limit";
    case ErrorCode::NotPreemptible: return "not-preemptible";
    case ErrorCode::PreemptionThrottled: return "preemption-throttled";
    case ErrorCode::DuplicateCompletion: return "duplicate-completion";
    case ErrorCode::ContradictoryCompletion: return "contradictory-completion";
    case ErrorCode::CompletionForClosedAttempt: return "completion-for-closed-attempt";
    case ErrorCode::AmbiguousOutcome: return "ambiguous-outcome";
    case ErrorCode::CorruptData: return "corrupt-data";
    case ErrorCode::Truncated: return "truncated";
    case ErrorCode::Oversized: return "oversized";
    case ErrorCode::SequenceGap: return "sequence-gap";
    case ErrorCode::IoFailure: return "io-failure";
    case ErrorCode::TransportFailure: return "transport-failure";
    case ErrorCode::PeerClosed: return "peer-closed";
    case ErrorCode::HandshakeRejected: return "handshake-rejected";
    case ErrorCode::ProtocolViolation: return "protocol-violation";
    case ErrorCode::Busy: return "busy";
  }
  return "unknown-error-code";
}

bool is_transient(ErrorCode code) noexcept {
  switch (code) {
    case ErrorCode::NotReady:
    case ErrorCode::NotReleased:
    case ErrorCode::DependencyUnsatisfied:
    case ErrorCode::CapacityUnavailable:
    case ErrorCode::ReservationClosed:
    case ErrorCode::ReservationExhausted:
    case ErrorCode::ConcurrencyLimit:
    case ErrorCode::PreemptionThrottled:
    case ErrorCode::Busy:
      return true;
    default:
      return false;
  }
}

bool is_staleness(ErrorCode code) noexcept {
  switch (code) {
    case ErrorCode::StaleFlowGeneration:
    case ErrorCode::StalePathGeneration:
    case ErrorCode::StaleResourceGeneration:
    case ErrorCode::StaleReservationGeneration:
    case ErrorCode::StalePolicyGeneration:
    case ErrorCode::StaleQoSGeneration:
    case ErrorCode::StalePriorityGeneration:
    case ErrorCode::StaleScheduleGeneration:
    case ErrorCode::StaleEpoch:
    case ErrorCode::StaleAttempt:
    case ErrorCode::StaleAuthority:
    case ErrorCode::Fenced:
    case ErrorCode::CompletionForClosedAttempt:
      return true;
    default:
      return false;
  }
}

Error::Error(ErrorCode code, std::string message) : code_(code) {
  // Bound the diagnostic so adversarial input cannot inflate error payloads.
  if (message.size() > kMaxErrorMessageBytes) {
    message.resize(kMaxErrorMessageBytes);
  }
  message_ = std::move(message);
}

std::string Error::to_string() const {
  if (ok()) {
    return "ok";
  }
  std::string out = flow_scheduler::to_string(code_);
  if (!message_.empty()) {
    out += ": ";
    out += message_;
  }
  return out;
}

}  // namespace flow_scheduler

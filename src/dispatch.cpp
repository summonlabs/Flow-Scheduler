// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#include "flow_scheduler/dispatch.hpp"

#include "flow_scheduler/hash.hpp"

namespace flow_scheduler {

const char* to_string(AttemptState state) noexcept {
  switch (state) {
    case AttemptState::Open: return "open";
    case AttemptState::Started: return "started";
    case AttemptState::Committed: return "committed";
    case AttemptState::Preempted: return "preempted";
    case AttemptState::Cancelled: return "cancelled";
    case AttemptState::Abandoned: return "abandoned";
    case AttemptState::Rejected: return "rejected";
  }
  return "unknown-attempt-state";
}

bool is_attempt_terminal(AttemptState state) noexcept {
  return state != AttemptState::Open && state != AttemptState::Started;
}

const char* to_string(CompletionOutcome outcome) noexcept {
  switch (outcome) {
    case CompletionOutcome::Served: return "served";
    case CompletionOutcome::NoService: return "no-service";
    case CompletionOutcome::Failed: return "failed";
  }
  return "unknown-completion-outcome";
}

const char* to_string(CommitDisposition disposition) noexcept {
  switch (disposition) {
    case CommitDisposition::Applied: return "applied";
    case CommitDisposition::IdempotentDuplicate: return "idempotent-duplicate";
    case CommitDisposition::Rejected: return "rejected";
  }
  return "unknown-disposition";
}

std::uint64_t CompletionEvidence::digest() const {
  // Every field that participates in the authority decision is folded in, so
  // two evidence payloads compare equal exactly when they bind identical
  // authority. The digest is what makes exact duplicate completion detectable
  // without re-deriving the whole decision.
  Digest64 digest;
  digest.add_u64(schedule.value());
  digest.add_u64(schedule_generation.value());
  digest.add_u64(attempt.value());
  digest.add_u64(epoch.value());
  digest.add_u64(flow.value());
  digest.add_u64(flow_generation.value());
  digest.add_u64(worker.value());
  digest.add_u64(boot.value());
  digest.add_u8(static_cast<std::uint8_t>(outcome));
  digest.add_u64(served);
  digest.add_u32(effect_code);
  digest.add_u64(effect_digest);
  digest.add_u64(observed_tick);
  digest.add_u8(static_cast<std::uint8_t>(provenance.origin));
  digest.add_u64(provenance.sequence);
  digest.add_u64(provenance.digest);
  return digest.value();
}

}  // namespace flow_scheduler

// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#include "flow_scheduler/accounting.hpp"

#include <string>

namespace flow_scheduler {

Status AccountingReport::validate() const {
  const std::uint64_t lifecycle_total =
      waiting + ready + scheduled + dispatched + running + completion_reported + cancelling +
      preempting + completed + cancelled + failed + ambiguous;
  if (lifecycle_total != total_flows) {
    return Status::failure(ErrorCode::Internal,
                           "flow lifecycle buckets do not sum to the registered flow count (" +
                               std::to_string(lifecycle_total) + " vs " +
                               std::to_string(total_flows) + ")");
  }
  // Cancelling and Preempting are transient markers inside a single critical
  // section. Observing one at rest would mean a decision escaped its critical
  // section without being normalized.
  if (cancelling != 0 || preempting != 0) {
    return Status::failure(ErrorCode::Internal,
                           "a flow is resting in a transient lifecycle state");
  }
  const std::uint64_t attempt_total =
      attempts_open + attempts_started + attempts_committed + attempts_preempted +
      attempts_cancelled + attempts_abandoned + attempts_rejected;
  if (attempt_total != attempts_total) {
    return Status::failure(ErrorCode::Internal,
                           "dispatch attempt states do not sum to the retained attempt count (" +
                               std::to_string(attempt_total) + " vs " +
                               std::to_string(attempts_total) + ")");
  }
  // completions_applied counts every commit, including commits replayed from
  // durable state, so it is not comparable with the session scoped counters.
  if (completions_duplicate + completions_rejected > completion_reports_received) {
    return Status::failure(ErrorCode::Internal,
                           "session completion dispositions outnumber the reports received");
  }
  if (outstanding_reserved_units > 0 && attempts_open + attempts_started + scheduled == 0) {
    return Status::failure(
        ErrorCode::Internal,
        "service is reserved with no scheduled window or open attempt to hold it");
  }
  if (schedules_issued > 0 && schedule_entries_issued < schedules_issued) {
    return Status::failure(ErrorCode::Internal,
                           "a schedule was issued with no entries; empty rounds must not be "
                           "recorded as schedules");
  }
  if (deadline_misses > total_flows + cancelled + completed + failed) {
    return Status::failure(ErrorCode::Internal, "deadline misses exceed the accountable flows");
  }
  if (epoch == 0) {
    return Status::failure(ErrorCode::Internal, "no fabric epoch is established");
  }
  return Status::success();
}

std::string AccountingReport::to_string() const {
  std::string out = "flows=";
  out += std::to_string(total_flows);
  out += " waiting=";
  out += std::to_string(waiting);
  out += " ready=";
  out += std::to_string(ready);
  out += " scheduled=";
  out += std::to_string(scheduled);
  out += " in-flight=";
  out += std::to_string(dispatched + running + completion_reported);
  out += " completed=";
  out += std::to_string(completed);
  out += " cancelled=";
  out += std::to_string(cancelled);
  out += " failed=";
  out += std::to_string(failed);
  out += " ambiguous=";
  out += std::to_string(ambiguous);
  out += " attempts=";
  out += std::to_string(attempts_total);
  out += " open=";
  out += std::to_string(attempts_open + attempts_started);
  out += " committed=";
  out += std::to_string(attempts_committed);
  out += " preempted=";
  out += std::to_string(attempts_preempted);
  out += " cancelled-attempts=";
  out += std::to_string(attempts_cancelled);
  out += " abandoned=";
  out += std::to_string(attempts_abandoned);
  out += " rejected-attempts=";
  out += std::to_string(attempts_rejected);
  out += " reports=";
  out += std::to_string(completion_reports_received);
  out += " applied=";
  out += std::to_string(completions_applied);
  out += " duplicates=";
  out += std::to_string(completions_duplicate);
  out += " rejected-completions=";
  out += std::to_string(completions_rejected);
  out += " schedules=";
  out += std::to_string(schedules_issued);
  out += " entries=";
  out += std::to_string(schedule_entries_issued);
  out += " rounds=";
  out += std::to_string(arbitration_rounds);
  out += " preemptions=";
  out += std::to_string(preemptions_issued);
  out += " boosts=";
  out += std::to_string(starvation_boosts);
  out += " deadline-misses=";
  out += std::to_string(deadline_misses);
  out += " reserved=";
  out += std::to_string(outstanding_reserved_units);
  out += " credit=";
  out += std::to_string(service_credit_total);
  out += " epoch=";
  out += std::to_string(epoch);
  return out;
}

}  // namespace flow_scheduler

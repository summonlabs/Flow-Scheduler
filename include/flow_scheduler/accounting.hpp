// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#ifndef FLOW_SCHEDULER_ACCOUNTING_HPP
#define FLOW_SCHEDULER_ACCOUNTING_HPP

#include <cstdint>
#include <string>

#include "flow_scheduler/error.hpp"

namespace flow_scheduler {

/// Complete accounting projection of the runtime. validate() proves that the
/// running / waiting / completed books close. Every counter is monotone except
/// the "current" counters, which are point-in-time gauges.
struct AccountingReport {
  // --- flow lifecycle gauges (must sum to total_flows) --------------------
  std::uint64_t total_flows{0};
  std::uint64_t waiting{0};
  std::uint64_t ready{0};
  std::uint64_t scheduled{0};
  std::uint64_t dispatched{0};
  std::uint64_t running{0};
  std::uint64_t completion_reported{0};
  std::uint64_t cancelling{0};
  std::uint64_t preempting{0};
  std::uint64_t completed{0};
  std::uint64_t cancelled{0};
  std::uint64_t failed{0};
  std::uint64_t ambiguous{0};

  // --- dispatch attempt accounting ----------------------------------------
  std::uint64_t attempts_total{0};
  std::uint64_t attempts_open{0};
  std::uint64_t attempts_started{0};
  std::uint64_t attempts_committed{0};
  std::uint64_t attempts_preempted{0};
  std::uint64_t attempts_cancelled{0};
  std::uint64_t attempts_abandoned{0};
  std::uint64_t attempts_rejected{0};

  // --- completion evidence accounting -------------------------------------
  std::uint64_t completion_reports_received{0};
  std::uint64_t completions_applied{0};
  std::uint64_t completions_duplicate{0};
  std::uint64_t completions_rejected{0};

  // --- arbitration accounting ---------------------------------------------
  std::uint64_t schedules_issued{0};
  std::uint64_t schedule_entries_issued{0};
  std::uint64_t preemptions_issued{0};
  std::uint64_t arbitration_rounds{0};
  std::uint64_t starvation_boosts{0};
  std::uint64_t deadline_misses{0};
  std::uint64_t reservations_retired{0};

  // --- service accounting --------------------------------------------------
  /// Service units authorized but not yet resolved. Must equal the sum of
  /// quanta across attempts that are still open or started.
  std::uint64_t outstanding_reserved_units{0};
  /// Service units durably credited to flows.
  std::uint64_t service_credit_total{0};
  /// Service units credited but not yet consumed by authoritative completion.
  std::uint64_t estimated_work_total{0};

  /// Current fabric epoch.
  std::uint64_t epoch{0};
  /// Number of coordinates recovered from durable state.
  std::uint64_t recovered_flows{0};
  std::uint64_t recovered_ambiguous_attempts{0};

  /// Proves the books close. Returns a failure with a precise diagnostic when
  /// any closure identity is violated; this is a defect indicator, not an
  /// expected runtime condition.
  [[nodiscard]] Status validate() const;

  /// Compact one-line rendering for logs and the CLI.
  [[nodiscard]] std::string to_string() const;
};

}  // namespace flow_scheduler

#endif  // FLOW_SCHEDULER_ACCOUNTING_HPP

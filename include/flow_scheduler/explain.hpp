// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#ifndef FLOW_SCHEDULER_EXPLAIN_HPP
#define FLOW_SCHEDULER_EXPLAIN_HPP

#include <cstdint>
#include <string>
#include <vector>

#include "flow_scheduler/accounting.hpp"
#include "flow_scheduler/identity.hpp"
#include "flow_scheduler/model.hpp"
#include "flow_scheduler/schedule.hpp"
#include "flow_scheduler/time.hpp"

namespace flow_scheduler {

/// One bounded step in a flow's decision history.
struct ExplanationStep {
  Ticks at_tick{0};
  FlowLifecycle lifecycle{FlowLifecycle::Waiting};
  DecisionReason reason{DecisionReason::Selected};
  ScheduleId schedule{};
  DispatchAttemptId attempt{};
  /// Bounded, single line, ASCII only.
  std::string detail{};
};

/// Bound a free-form diagnostic to kMaxExplanationBytes and strip control
/// characters so that explanations are always single-line, printable, and
/// bounded regardless of what a flow label or error path supplied.
[[nodiscard]] std::string bounded_detail(std::string_view text);

/// Explanation of a single flow: why it is in its current lifecycle state, and
/// the bounded history of decisions that produced it.
struct FlowExplanation {
  FlowId flow{};
  Generation generation{};
  FlowLifecycle lifecycle{FlowLifecycle::Waiting};
  DecisionReason last_reason{DecisionReason::Selected};
  /// Digest over the ordering key that most recently ranked this flow.
  std::uint64_t ordering_digest{0};
  std::uint32_t tier{0};
  FlowAccounting accounting{};
  std::vector<ExplanationStep> history{};
  /// True when the history was truncated to respect the explanation bound.
  bool history_truncated{false};
};

/// Explanation of one arbitration round: every entry and every deferral with
/// its reason, plus the digest that makes the round reproducible.
struct ScheduleExplanation {
  ScheduleId schedule{};
  Generation generation{};
  FabricEpoch epoch{};
  Ticks at_tick{0};
  std::uint64_t digest{0};
  std::vector<std::string> entries{};
  std::vector<std::string> deferred{};
  bool truncated{false};
};

}  // namespace flow_scheduler

#endif  // FLOW_SCHEDULER_EXPLAIN_HPP

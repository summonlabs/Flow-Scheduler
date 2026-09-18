// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#ifndef FLOW_SCHEDULER_MODEL_HPP
#define FLOW_SCHEDULER_MODEL_HPP

#include <cstdint>
#include <string>
#include <vector>

#include "flow_scheduler/error.hpp"
#include "flow_scheduler/identity.hpp"
#include "flow_scheduler/time.hpp"

namespace flow_scheduler {

// ---------------------------------------------------------------------------
// Hard structural bounds. Every externally supplied collection or scalar is
// validated against these before it can influence arbitration.
// ---------------------------------------------------------------------------
inline constexpr std::uint32_t kMaxFlows = 1u << 20;          // 1,048,576
inline constexpr std::uint32_t kMaxResources = 1u << 16;      // 65,536
inline constexpr std::uint32_t kMaxReservations = 1u << 18;   // 262,144
inline constexpr std::uint32_t kMaxPriorityClasses = 1u << 12;
inline constexpr std::uint32_t kMaxQoSClasses = 1u << 12;
inline constexpr std::uint32_t kMaxDependenciesPerFlow = 4096;
inline constexpr std::uint32_t kMaxEntriesPerSchedule = 1u << 16;
inline constexpr std::uint64_t kMaxServiceUnits = 1ull << 48;
inline constexpr std::uint64_t kMaxQuantum = 1ull << 40;
inline constexpr std::uint64_t kMaxTickHorizon = 1ull << 60;
inline constexpr std::size_t kMaxExplanationBytes = 2048;

// ---------------------------------------------------------------------------
// Externally supplied bindings. Each carries its own generation so that a
// decision can be bound to the exact revision of the thing it depends on.
// ---------------------------------------------------------------------------

/// Binding to the network resource whose service capacity is being arbitrated.
/// The runtime never computes, places, or reserves this resource; it only
/// consumes a prior, external binding and revalidates its generation.
struct ResourceBinding {
  ResourceId resource{};
  Generation generation{};

  [[nodiscard]] bool bound() const noexcept {
    return resource.valid() && generation.valid();
  }
  friend bool operator==(const ResourceBinding& a, const ResourceBinding& b) noexcept {
    return a.resource == b.resource && a.generation == b.generation;
  }
};

/// Binding to the externally computed path. Never modified here.
struct PathBinding {
  PathId path{};
  Generation generation{};

  [[nodiscard]] bool bound() const noexcept { return path.valid() && generation.valid(); }
  friend bool operator==(const PathBinding& a, const PathBinding& b) noexcept {
    return a.path == b.path && a.generation == b.generation;
  }
};

/// Binding to an externally granted reservation. Absent means "unreserved".
struct ReservationBinding {
  ReservationId reservation{};
  Generation generation{};

  [[nodiscard]] bool bound() const noexcept {
    return reservation.valid() && generation.valid();
  }
  friend bool operator==(const ReservationBinding& a, const ReservationBinding& b) noexcept {
    return a.reservation == b.reservation && a.generation == b.generation;
  }
};

struct PriorityBinding {
  PriorityClassId priority{};
  Generation generation{};

  [[nodiscard]] bool bound() const noexcept {
    return priority.valid() && generation.valid();
  }
  friend bool operator==(const PriorityBinding& a, const PriorityBinding& b) noexcept {
    return a.priority == b.priority && a.generation == b.generation;
  }
};

struct QoSBinding {
  QoSClassId qos{};
  Generation generation{};

  [[nodiscard]] bool bound() const noexcept { return qos.valid() && generation.valid(); }
  friend bool operator==(const QoSBinding& a, const QoSBinding& b) noexcept {
    return a.qos == b.qos && a.generation == b.generation;
  }
};

// ---------------------------------------------------------------------------
// Resource / reservation / class descriptors
// ---------------------------------------------------------------------------

/// A resource is a unit of temporal capacity under arbitration: for example a
/// transmit opportunity set on an already-selected path. Capacity and overlap
/// limits are supplied externally; the runtime only enforces them.
struct ResourceDescriptor {
  ResourceId resource{};
  Generation generation{};
  /// Service units available per interval.
  std::uint64_t capacity{0};
  /// Length of the capacity interval in ticks. Capacity refills each interval.
  Ticks interval{0};
  /// Maximum number of concurrent open dispatches on this resource.
  std::uint64_t max_overlap{1};
  Provenance provenance{};
};

struct ReservationDescriptor {
  ReservationId reservation{};
  Generation generation{};
  ResourceId resource{};
  Generation resource_generation{};
  /// Half-open window [begin, end) during which the reservation is usable.
  Ticks window_begin{0};
  Ticks window_end{0};
  /// Service units granted inside the window.
  std::uint64_t capacity{0};
  /// When true, in-window service on the resource is restricted to flows bound
  /// to this reservation.
  bool exclusive{false};
  Provenance provenance{};
};

/// External priority definition. Lower rank is more urgent. The runtime never
/// assigns ranks; it only orders by the supplied ones.
struct PriorityClassDescriptor {
  PriorityClassId priority{};
  Generation generation{};
  std::uint32_t rank{0};
  /// Relative fairness weight used by the weighted-fair stage.
  std::uint64_t weight{1};
  Provenance provenance{};
};

/// External QoS class definition. A preemption-protected class makes every
/// bound flow non-preemptible regardless of its own flag.
struct QoSClassDescriptor {
  QoSClassId qos{};
  Generation generation{};
  bool preemption_protected{false};
  /// Upper bound the class places on per-dispatch service, if any.
  std::uint64_t max_service_per_dispatch{0};  // 0 == unconstrained
  Provenance provenance{};
};

// ---------------------------------------------------------------------------
// Flow descriptor
// ---------------------------------------------------------------------------

struct FlowDescriptor {
  FlowId flow{};
  Generation generation{};

  PathBinding path{};
  ResourceBinding resource{};
  /// Optional. Unbound means the flow may run outside any reservation window.
  ReservationBinding reservation{};
  PriorityBinding priority{};
  QoSBinding qos{};
  FairnessGroupId fairness_group{};

  /// Total service units the flow needs to be considered authoritatively done.
  std::uint64_t estimated_work{0};
  /// Maximum service units authorized per dispatch.
  std::uint64_t service_quantum{1};
  /// Minimum service that must be delivered within min_service_window.
  std::uint64_t min_service{0};
  Ticks min_service_window{0};

  /// Earliest tick at which the flow may be considered ready.
  Ticks release_tick{0};
  /// Latest tick at which dispatch is legal. kNoDeadline means unbounded.
  Ticks deadline_tick{kNoDeadline};

  /// Relative weight inside the fairness group. 0 is normalized to 1.
  std::uint64_t weight{1};

  /// Whether the flow may be preempted while running. The bound QoS class can
  /// force this to false.
  bool preemptible{true};

  /// When true the flow becomes Ready as soon as it is released and its
  /// dependencies are authoritatively complete. When false, an explicit
  /// notify_readiness(Ready) is also required. Either way "no flow before
  /// ready" holds: readiness is the only gate into arbitration.
  bool auto_ready{true};

  /// Caller supplied readiness signal, carried through descriptor replacement.
  std::uint8_t readiness_signal{0};  // 0 == Unset, 1 == Ready, 2 == NotReady

  /// Flows that must be authoritatively complete before this one is ready.
  std::vector<FlowId> dependencies{};

  TraceId trace{};
  Provenance provenance{};
};

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

/// The separation the runtime must preserve:
/// Ready != Scheduled != Dispatched != Running != CompletionReported !=
/// Completed. Only Completed (committed under the current epoch) is
/// authoritative completion.
enum class FlowLifecycle : std::uint8_t {
  /// Admitted externally but not eligible: not released, dependencies unmet,
  /// or its reservation window is not open.
  Waiting = 0,
  /// Eligible for arbitration right now.
  Ready = 1,
  /// Placed into an issued schedule; not yet handed to a worker.
  Scheduled = 2,
  /// A dispatch frame was issued and durably journaled.
  Dispatched = 3,
  /// The worker acknowledged that execution started.
  Running = 4,
  /// The worker reported completion; not yet committed.
  CompletionReported = 5,
  /// Authoritative completion committed under the current epoch.
  Completed = 6,
  /// Cancellation requested while work was open.
  Cancelling = 7,
  /// Cancelled. Terminal; can never mutate authoritative state again.
  Cancelled = 8,
  /// Preemption in progress.
  Preempting = 9,
  /// Terminal failure: the flow cannot make progress.
  Failed = 10,
  /// Recovered with an unknown outcome. Requires explicit resolution before it
  /// can become Ready again. Liveness is never restored implicitly.
  Ambiguous = 11,
};

[[nodiscard]] const char* to_string(FlowLifecycle lifecycle) noexcept;
[[nodiscard]] bool is_terminal(FlowLifecycle lifecycle) noexcept;
[[nodiscard]] bool holds_open_work(FlowLifecycle lifecycle) noexcept;

/// Accounting buckets. Every flow is in exactly one bucket, which is what
/// makes the running/waiting/completed accounting close.
enum class AccountingBucket : std::uint8_t {
  Waiting = 0,
  Ready = 1,
  Scheduled = 2,
  /// Dispatched, Running, CompletionReported, Cancelling, Preempting.
  InFlight = 3,
  Completed = 4,
  Cancelled = 5,
  Failed = 6,
  Ambiguous = 7,
};

[[nodiscard]] AccountingBucket bucket_of(FlowLifecycle lifecycle) noexcept;
[[nodiscard]] const char* to_string(AccountingBucket bucket) noexcept;

struct FlowAccounting {
  std::uint64_t service_credit{0};      ///< total service units credited
  std::uint64_t dispatches_issued{0};
  std::uint64_t dispatches_started{0};
  std::uint64_t completions_committed{0};
  std::uint64_t duplicate_completions{0};
  std::uint64_t rejected_completions{0};
  std::uint64_t preemptions{0};
  std::uint64_t deadline_misses{0};
  std::uint64_t wait_ticks_total{0};
  std::uint64_t starvation_boosts{0};
};

/// Point-in-time projection of a flow. Returned by value so callers can never
/// observe internally mutated state without the coordinator lock.
struct FlowSnapshot {
  FlowId flow{};
  Generation generation{};
  FlowLifecycle lifecycle{FlowLifecycle::Waiting};
  std::uint64_t estimated_work{0};
  std::uint64_t served_work{0};
  std::uint64_t outstanding_reserved{0};
  Ticks release_tick{0};
  Ticks deadline_tick{kNoDeadline};
  Ticks last_change_tick{0};
  Ticks eligible_since_tick{0};
  DispatchAttemptId open_attempt{};
  FlowAccounting accounting{};
  Provenance provenance{};
};

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

[[nodiscard]] Status validate(const ResourceDescriptor& descriptor);
[[nodiscard]] Status validate(const ReservationDescriptor& descriptor);
[[nodiscard]] Status validate(const PriorityClassDescriptor& descriptor);
[[nodiscard]] Status validate(const QoSClassDescriptor& descriptor);
[[nodiscard]] Status validate(const FlowDescriptor& descriptor);

/// Deterministic digest over a descriptor's arbitrated content. Two descriptors
/// with the same digest arbitrate identically.
[[nodiscard]] std::uint64_t digest_of(const ResourceDescriptor& descriptor);
[[nodiscard]] std::uint64_t digest_of(const ReservationDescriptor& descriptor);
[[nodiscard]] std::uint64_t digest_of(const PriorityClassDescriptor& descriptor);
[[nodiscard]] std::uint64_t digest_of(const QoSClassDescriptor& descriptor);
[[nodiscard]] std::uint64_t digest_of(const FlowDescriptor& descriptor);

/// True when the reservation window [begin, end) contains the tick.
[[nodiscard]] bool window_contains(const ReservationDescriptor& reservation, Ticks tick) noexcept;

/// Nominal service units the reservation still grants at the given tick. The
/// grant is a total over the window, consumed by dispatched quanta; the caller
/// subtracts what it has already reserved. Returns 0 outside the window.
[[nodiscard]] std::uint64_t reservation_grant_at(const ReservationDescriptor& reservation,
                                                 Ticks tick) noexcept;

}  // namespace flow_scheduler

#endif  // FLOW_SCHEDULER_MODEL_HPP

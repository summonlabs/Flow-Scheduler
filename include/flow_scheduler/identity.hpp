// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#ifndef FLOW_SCHEDULER_IDENTITY_HPP
#define FLOW_SCHEDULER_IDENTITY_HPP

#include <cstdint>
#include <functional>
#include <string>
#include <type_traits>

#include "flow_scheduler/checked.hpp"

namespace flow_scheduler {

/// Strongly typed identity handle. Distinct tag types make every identity
/// family a separate C++ type so that a PathId can never be passed where a
/// ReservationId is expected. The default constructed value is the reserved
/// "unbound" sentinel and is never a legal identity.
template <class Tag, class Rep = std::uint64_t>
class Id {
 public:
  using rep_type = Rep;
  using tag_type = Tag;

  constexpr Id() noexcept = default;
  constexpr explicit Id(Rep value) noexcept : value_(value) {}

  [[nodiscard]] constexpr Rep value() const noexcept { return value_; }
  [[nodiscard]] constexpr bool valid() const noexcept { return value_ != Rep{0}; }
  [[nodiscard]] constexpr explicit operator bool() const noexcept { return valid(); }
  [[nodiscard]] static constexpr Id unbound() noexcept { return Id{}; }

  friend constexpr bool operator==(Id a, Id b) noexcept { return a.value_ == b.value_; }
  friend constexpr bool operator!=(Id a, Id b) noexcept { return a.value_ != b.value_; }
  friend constexpr bool operator<(Id a, Id b) noexcept { return a.value_ < b.value_; }
  friend constexpr bool operator>(Id a, Id b) noexcept { return a.value_ > b.value_; }
  friend constexpr bool operator<=(Id a, Id b) noexcept { return a.value_ <= b.value_; }
  friend constexpr bool operator>=(Id a, Id b) noexcept { return a.value_ >= b.value_; }
  friend constexpr auto operator<=>(Id a, Id b) noexcept { return a.value_ <=> b.value_; }

 private:
  Rep value_{0};
};

// --- identity families ------------------------------------------------------
struct FlowTag;
struct ScheduleTag;
struct DispatchAttemptTag;
struct ResourceTag;
struct PathTag;
struct ReservationTag;
struct PriorityClassTag;
struct QoSClassTag;
struct PolicyTag;
struct FabricEpochTag;
struct WorkerTag;
struct BootTag;
struct GenerationTag;
struct FairnessGroupTag;
struct TraceTag;

using FlowId = Id<FlowTag>;
using ScheduleId = Id<ScheduleTag>;
using DispatchAttemptId = Id<DispatchAttemptTag>;
using ResourceId = Id<ResourceTag>;
using PathId = Id<PathTag>;
using ReservationId = Id<ReservationTag>;
using PriorityClassId = Id<PriorityClassTag>;
using QoSClassId = Id<QoSClassTag>;
using PolicyId = Id<PolicyTag>;
using FabricEpoch = Id<FabricEpochTag>;
using WorkerId = Id<WorkerTag>;
using BootId = Id<BootTag>;
using Generation = Id<GenerationTag>;
using FairnessGroupId = Id<FairnessGroupTag>;
using TraceId = Id<TraceTag>;

/// Stable family name for diagnostics. Specialized per tag below.
template <class Tag>
[[nodiscard]] const char* id_family_name() noexcept;

#define FS_DECLARE_ID_FAMILY(TagType, Text)                     \
  template <>                                                   \
  [[nodiscard]] inline const char* id_family_name<TagType>()    \
      noexcept {                                                \
    return Text;                                                \
  }

FS_DECLARE_ID_FAMILY(FlowTag, "flow")
FS_DECLARE_ID_FAMILY(ScheduleTag, "schedule")
FS_DECLARE_ID_FAMILY(DispatchAttemptTag, "attempt")
FS_DECLARE_ID_FAMILY(ResourceTag, "resource")
FS_DECLARE_ID_FAMILY(PathTag, "path")
FS_DECLARE_ID_FAMILY(ReservationTag, "reservation")
FS_DECLARE_ID_FAMILY(PriorityClassTag, "priority")
FS_DECLARE_ID_FAMILY(QoSClassTag, "qos")
FS_DECLARE_ID_FAMILY(PolicyTag, "policy")
FS_DECLARE_ID_FAMILY(FabricEpochTag, "epoch")
FS_DECLARE_ID_FAMILY(WorkerTag, "worker")
FS_DECLARE_ID_FAMILY(BootTag, "boot")
FS_DECLARE_ID_FAMILY(GenerationTag, "generation")
FS_DECLARE_ID_FAMILY(FairnessGroupTag, "fairness-group")
FS_DECLARE_ID_FAMILY(TraceTag, "trace")

#undef FS_DECLARE_ID_FAMILY

template <class Tag, class Rep>
[[nodiscard]] std::string to_string(Id<Tag, Rep> id) {
  std::string out = id_family_name<Tag>();
  out.push_back('#');
  out.append(std::to_string(id.value()));
  return out;
}

/// Advance a generation/epoch counter by exactly one, failing at the numeric
/// limit. Compile-time tagged so a FlowId can never be advanced as a counter.
template <class Tag, class Rep>
[[nodiscard]] Result<Id<Tag, Rep>> advance(Id<Tag, Rep> current) {
  const Result<Rep> next_value = checked_next(current.value());
  if (!next_value.ok()) {
    return next_value.error();
  }
  return Id<Tag, Rep>(next_value.value());
}

/// Where a piece of evidence or a piece of durable state came from. Recovery
/// and replay produce different provenance than live operation, and the
/// distinction is preserved so that recovered state can never masquerade as
/// freshly observed authority.
enum class ProvenanceOrigin : std::uint8_t {
  Unknown = 0,   ///< Not established. Never treated as positive authority.
  External = 1,  ///< Supplied by a caller across the public API.
  Internal = 2,  ///< Produced by this runtime's own arbitration.
  Recovered = 3, ///< Reconstructed from durable state during restart.
  Synthetic = 4, ///< Produced by the synthetic benchmark workload generator.
};

[[nodiscard]] const char* to_string(ProvenanceOrigin origin) noexcept;

/// Binds a value to its origin, a monotonic sequence within that origin, and a
/// digest over the exact bytes that justified it. A Provenance with origin
/// Unknown or an unset sequence is advisory only and is rejected wherever
/// positive authority is required.
struct Provenance {
  ProvenanceOrigin origin{ProvenanceOrigin::Unknown};
  std::uint64_t sequence{0};
  std::uint64_t digest{0};

  [[nodiscard]] bool established() const noexcept {
    return origin != ProvenanceOrigin::Unknown;
  }
  [[nodiscard]] bool authoritative() const noexcept {
    return origin == ProvenanceOrigin::External || origin == ProvenanceOrigin::Internal;
  }

  friend bool operator==(const Provenance& a, const Provenance& b) noexcept {
    return a.origin == b.origin && a.sequence == b.sequence && a.digest == b.digest;
  }
  friend bool operator!=(const Provenance& a, const Provenance& b) noexcept {
    return !(a == b);
  }
};

[[nodiscard]] std::string to_string(const Provenance& provenance);

}  // namespace flow_scheduler

namespace std {
template <class Tag, class Rep>
struct hash<flow_scheduler::Id<Tag, Rep>> {
  [[nodiscard]] size_t operator()(const flow_scheduler::Id<Tag, Rep>& id) const noexcept {
    return std::hash<Rep>{}(id.value());
  }
};
}  // namespace std

#endif  // FLOW_SCHEDULER_IDENTITY_HPP

// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#ifndef FLOW_SCHEDULER_VERSION_HPP
#define FLOW_SCHEDULER_VERSION_HPP

#include <cstdint>
#include <string>

namespace flow_scheduler {

inline constexpr int kVersionMajor = 1;
inline constexpr int kVersionMinor = 0;
inline constexpr int kVersionPatch = 0;
inline constexpr const char* kVersionString = "1.0.0";

/// Durable-record compatibility generation. Bumped whenever the on-disk
/// journal or snapshot encoding changes in a way that older readers cannot
/// interpret. Readers reject unknown generations rather than guessing.
inline constexpr std::uint16_t kDurableFormatGeneration = 1;

[[nodiscard]] const char* version_string() noexcept;

/// Human readable build identification: compiler and architecture. Used only
/// for diagnostics output; never used as scheduling authority.
[[nodiscard]] const std::string& build_identification();

}  // namespace flow_scheduler

#endif  // FLOW_SCHEDULER_VERSION_HPP

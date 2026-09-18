// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#include "flow_scheduler/version.hpp"

#include <array>

#if defined(_MSC_VER)
#define FS_COMPILER_NAME "MSVC " FS_STRINGIZE(_MSC_VER)
#define FS_STRINGIZE_IMPL(x) #x
#define FS_STRINGIZE(x) FS_STRINGIZE_IMPL(x)
#elif defined(__clang__)
#define FS_COMPILER_NAME "Clang " __clang_version__
#elif defined(__GNUC__)
#define FS_COMPILER_NAME "GCC " __VERSION__
#else
#define FS_COMPILER_NAME "unknown compiler"
#endif

namespace flow_scheduler {
namespace {

std::string make_identification() {
  std::string out = "flow_scheduler/";
  out += kVersionString;
  out += " [";
  out += FS_COMPILER_NAME;
  out += ", cxx20, durable-format ";
  out += std::to_string(static_cast<unsigned>(kDurableFormatGeneration));
  out += "]";
  return out;
}

}  // namespace

const char* version_string() noexcept { return kVersionString; }

const std::string& build_identification() {
  static const std::string identification = make_identification();
  return identification;
}

}  // namespace flow_scheduler

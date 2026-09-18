// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#include "flow_scheduler/explain.hpp"

namespace flow_scheduler {

std::string bounded_detail(std::string_view text) {
  std::string out;
  out.reserve(text.size() < kMaxExplanationBytes ? text.size() : kMaxExplanationBytes);
  for (const char raw : text) {
    if (out.size() + 3 >= kMaxExplanationBytes) {
      out += "...";
      break;
    }
    const auto byte = static_cast<unsigned char>(raw);
    if (byte < 0x20u || byte == 0x7Fu) {
      out.push_back(' ');
    } else {
      out.push_back(static_cast<char>(byte));
    }
  }
  if (out.size() > kMaxExplanationBytes) {
    out.resize(kMaxExplanationBytes);
  }
  return out;
}

}  // namespace flow_scheduler

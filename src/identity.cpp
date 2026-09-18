// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#include "flow_scheduler/identity.hpp"

namespace flow_scheduler {

const char* to_string(ProvenanceOrigin origin) noexcept {
  switch (origin) {
    case ProvenanceOrigin::Unknown: return "unknown";
    case ProvenanceOrigin::External: return "external";
    case ProvenanceOrigin::Internal: return "internal";
    case ProvenanceOrigin::Recovered: return "recovered";
    case ProvenanceOrigin::Synthetic: return "synthetic";
  }
  return "unknown";
}

std::string to_string(const Provenance& provenance) {
  std::string out = to_string(provenance.origin);
  out += "/seq=";
  out.append(std::to_string(provenance.sequence));
  out += "/digest=";
  out.append(std::to_string(provenance.digest));
  return out;
}

}  // namespace flow_scheduler

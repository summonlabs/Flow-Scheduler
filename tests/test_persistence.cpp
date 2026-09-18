// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.
//
// Durable-store and restart behaviour. The store is exercised directly (append,
// replay, torn tail, corruption, snapshot) and through Scheduler with
// SchedulerOptions::state_directory set, which is the only way to prove that a
// restart neither loses committed work nor silently invents authority.

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

#include "flow_scheduler/accounting.hpp"
#include "flow_scheduler/hash.hpp"
#include "flow_scheduler/journal.hpp"
#include "flow_scheduler/scheduler.hpp"
#include "flow_scheduler/version.hpp"
#include "test_framework.hpp"

namespace {

using namespace flow_scheduler;

/// CHECK_OK / CHECK_ERROR / REQUIRE_OK require an expression that evaluates to
/// Status; Result<T> carries a code, so it is adapted without losing it.
Status as_status(const Status& status) { return status; }

template <class T>
Status as_status(const Result<T>& result) {
  return result.ok() ? Status::success() : Status(result.error());
}

// ---------------------------------------------------------------------------
// Byte helpers
// ---------------------------------------------------------------------------

void patch_u16(std::string& bytes, std::size_t offset, std::uint16_t value) {
  bytes[offset] = static_cast<char>(value & 0xFFu);
  bytes[offset + 1] = static_cast<char>((value >> 8) & 0xFFu);
}

void patch_u32(std::string& bytes, std::size_t offset, std::uint32_t value) {
  for (std::size_t index = 0; index < 4; ++index) {
    bytes[offset + index] = static_cast<char>((value >> (8u * index)) & 0xFFu);
  }
}

void patch_u64(std::string& bytes, std::size_t offset, std::uint64_t value) {
  for (std::size_t index = 0; index < 8; ++index) {
    bytes[offset + index] = static_cast<char>((value >> (8u * index)) & 0xFFu);
  }
}

std::uint32_t read_u32(const std::string& bytes, std::size_t offset) {
  std::uint32_t value = 0;
  for (std::size_t index = 0; index < 4; ++index) {
    value |= static_cast<std::uint32_t>(static_cast<unsigned char>(bytes[offset + index]))
             << (8u * index);
  }
  return value;
}

/// Recompute a journal record's header checksum after a field of that header has
/// been deliberately altered, so that the checksum is not what rejects it.
void reseal_record_header(std::string& bytes, std::size_t record_offset) {
  patch_u32(bytes, record_offset + 20, crc32c(bytes.data() + record_offset, 20));
}

std::string patterned_payload(std::size_t length, std::uint8_t seed) {
  std::string payload(length, '\0');
  for (std::size_t index = 0; index < length; ++index) {
    const std::uint8_t step = static_cast<std::uint8_t>(index % 251u);
    payload[index] = static_cast<char>((seed + step) & 0xFFu);
  }
  return payload;
}

/// A hand-built journal record following the layout documented in journal.hpp.
/// Used to inject records that the writer itself refuses to produce.
std::string raw_journal_record(std::uint16_t type, std::uint64_t sequence,
                               const std::string& payload) {
  std::string record(kJournalHeaderBytes, '\0');
  patch_u32(record, 0, kJournalRecordMagic);
  patch_u16(record, 4, kDurableFormatGeneration);
  patch_u16(record, 6, type);
  patch_u64(record, 8, sequence);
  patch_u32(record, 16, static_cast<std::uint32_t>(payload.size()));
  patch_u32(record, 20, crc32c(record.data(), 20));
  record.append(payload);
  std::string trailer(4, '\0');
  patch_u32(trailer, 0, crc32c(payload.data(), payload.size()));
  record.append(trailer);
  return record;
}

// ---------------------------------------------------------------------------
// Files
// ---------------------------------------------------------------------------

class TempDirectory {
 public:
  explicit TempDirectory(const char* tag) {
    std::error_code error;
    const std::filesystem::path base = std::filesystem::temp_directory_path(error);
    path_ = base / ("flow_scheduler_test_" + std::string(tag) + "_" + std::to_string(next_nonce()));
    std::filesystem::remove_all(path_, error);
    std::filesystem::create_directories(path_, error);
    creation_error_ = error;
  }
  TempDirectory(const TempDirectory&) = delete;
  TempDirectory& operator=(const TempDirectory&) = delete;
  ~TempDirectory() {
    std::error_code error;
    std::filesystem::remove_all(path_, error);
  }

  [[nodiscard]] bool created() const noexcept { return !creation_error_; }
  [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }
  [[nodiscard]] std::string path_string() const { return path_.string(); }
  [[nodiscard]] std::filesystem::path file(const char* name) const { return path_ / name; }

 private:
  static std::uint64_t next_nonce() {
    static std::atomic<std::uint64_t> counter{0};
    const auto stamp = static_cast<std::uint64_t>(
        std::chrono::steady_clock::now().time_since_epoch().count());
    return mix64(stamp) ^
           (counter.fetch_add(1, std::memory_order_relaxed) * 0x9E3779B97F4A7C15ull);
  }

  std::filesystem::path path_;
  std::error_code creation_error_;
};

std::string read_file(const std::filesystem::path& path) {
  std::ifstream stream(path, std::ios::binary);
  std::ostringstream buffer;
  buffer << stream.rdbuf();
  return buffer.str();
}

void write_file(const std::filesystem::path& path, const std::string& bytes) {
  std::ofstream stream(path, std::ios::binary | std::ios::trunc);
  stream.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
  stream.close();
  CHECK(stream.good());
}

void truncate_file_to(const std::filesystem::path& path, std::size_t size) {
  std::error_code error;
  std::filesystem::resize_file(path, size, error);
  CHECK(!error);
}

// ---------------------------------------------------------------------------
// Journal fixtures
// ---------------------------------------------------------------------------

const std::vector<JournalRecordType>& fixture_types() {
  static const std::vector<JournalRecordType> types = {
      JournalRecordType::PolicyInstalled,       JournalRecordType::ResourceRegistered,
      JournalRecordType::ReservationRegistered, JournalRecordType::EpochAdvanced,
      JournalRecordType::FlowAdmitted};
  return types;
}

const std::vector<std::string>& fixture_payloads() {
  static const std::vector<std::string> payloads = {std::string(), patterned_payload(10, 0x01u),
                                                    patterned_payload(100, 0x02u),
                                                    patterned_payload(3, 0x03u),
                                                    patterned_payload(64, 0x04u)};
  return payloads;
}

/// Byte offset of every fixture record, plus one past the last record.
std::vector<std::size_t> fixture_boundaries() {
  std::vector<std::size_t> boundaries;
  std::size_t offset = 0;
  for (const std::string& payload : fixture_payloads()) {
    boundaries.push_back(offset);
    offset += journal_record_bytes(payload.size());
  }
  boundaries.push_back(offset);
  return boundaries;
}

void write_fixture_journal(const std::string& directory) {
  const Result<std::unique_ptr<DurableStore>> store = DurableStore::open(directory, true);
  REQUIRE(store.ok());
  for (std::size_t index = 0; index < fixture_payloads().size(); ++index) {
    REQUIRE_OK(store.value()->append(fixture_types()[index], fixture_payloads()[index]));
  }
}

/// How many fixture records are complete inside the first N bytes.
std::size_t complete_records_at(std::size_t bytes) {
  const std::vector<std::size_t> boundaries = fixture_boundaries();
  std::size_t records = 0;
  while (records < fixture_payloads().size() && boundaries[records + 1] <= bytes) {
    ++records;
  }
  return records;
}

// ---------------------------------------------------------------------------
// Scheduler fixtures
// ---------------------------------------------------------------------------

Provenance external_provenance(std::uint64_t sequence) {
  Provenance provenance;
  provenance.origin = ProvenanceOrigin::External;
  provenance.sequence = sequence;
  provenance.digest = mix64(sequence);
  return provenance;
}

SchedulerOptions durable_options(const std::string& directory) {
  SchedulerOptions options;
  options.policy.policy = PolicyId(1);
  options.policy.generation = Generation(1);
  options.policy.preemption = PreemptionMode::Tiered;
  options.policy.deadline_ordering = true;
  options.policy.weighted_fairness = true;
  options.policy.min_service_guarantee = true;
  options.policy.strict_deadlines = true;
  options.policy.starvation_bound_ticks = 1000;
  options.policy.deadline_urgency_ticks = 100;
  options.policy.min_preempt_service = 0;
  options.policy.max_preemptions_per_interval = 64;
  options.policy.preemption_interval_ticks = 1000;
  options.policy.dispatch_delivery_horizon = 100000;
  options.policy.default_max_overlap = 4;
  options.policy.retained_schedule_history = 4096;
  options.policy.provenance = external_provenance(1);
  options.policy.provenance.digest = digest_of(options.policy);
  options.state_directory = directory;
  options.flush_to_storage = true;
  options.require_compatible_format = true;
  return options;
}

ResourceDescriptor fixture_resource() {
  ResourceDescriptor descriptor;
  descriptor.resource = ResourceId(1);
  descriptor.generation = Generation(1);
  descriptor.capacity = 1'000'000;
  descriptor.interval = 1000;
  descriptor.max_overlap = 4;
  descriptor.provenance = external_provenance(1);
  return descriptor;
}

FlowDescriptor fixture_flow(FlowId flow, std::uint64_t estimated_work, Ticks release_tick) {
  FlowDescriptor descriptor;
  descriptor.flow = flow;
  descriptor.generation = Generation(1);
  descriptor.path.path = PathId(flow.value());
  descriptor.path.generation = Generation(1);
  descriptor.resource = {ResourceId(1), Generation(1)};
  descriptor.priority = {PriorityClassId(1), Generation(1)};
  descriptor.qos = {QoSClassId(1), Generation(1)};
  descriptor.fairness_group = FairnessGroupId(1);
  descriptor.estimated_work = estimated_work;
  descriptor.service_quantum = 4;
  descriptor.release_tick = release_tick;
  descriptor.trace = TraceId(flow.value());
  descriptor.provenance = external_provenance(flow.value());
  return descriptor;
}

Status register_fixture_topology(Scheduler& scheduler) {
  FS_RETURN_IF_ERROR(scheduler.register_resource(fixture_resource()));
  for (std::uint32_t rank = 0; rank < 4; ++rank) {
    PriorityClassDescriptor priority;
    priority.priority = PriorityClassId(rank + 1);
    priority.generation = Generation(1);
    priority.rank = rank;
    priority.weight = 1;
    priority.provenance = external_provenance(rank + 1);
    FS_RETURN_IF_ERROR(scheduler.register_priority_class(priority));
  }
  QoSClassDescriptor qos;
  qos.qos = QoSClassId(1);
  qos.generation = Generation(1);
  qos.provenance = external_provenance(1);
  return scheduler.register_qos_class(qos);
}

struct DispatchCycle {
  DispatchTicket ticket{};
  CommitOutcome outcome{};
};

/// Drives one Run entry through dispatch, start and completion.
Result<DispatchCycle> run_entry(Scheduler& scheduler, const Schedule& schedule,
                                const ScheduleEntry& entry, Ticks now, WorkerId worker,
                                BootId boot, std::uint64_t served) {
  DispatchCycle cycle;
  FS_TRY_ASSIGN(cycle.ticket, scheduler.begin_dispatch(schedule.schedule, schedule.generation,
                                                       entry.ordinal, worker, boot, now));
  // The worker acknowledges the start before it reports completion, so the flow
  // passes through Running and the AttemptStarted record becomes part of the
  // durable history that a restart has to replay.
  FS_RETURN_IF_ERROR(scheduler.mark_started(cycle.ticket, now));
  CompletionEvidence evidence;
  evidence.schedule = cycle.ticket.schedule;
  evidence.schedule_generation = cycle.ticket.schedule_generation;
  evidence.attempt = cycle.ticket.attempt;
  evidence.epoch = cycle.ticket.epoch;
  evidence.flow = cycle.ticket.flow;
  evidence.flow_generation = cycle.ticket.flow_generation;
  evidence.worker = cycle.ticket.worker;
  evidence.boot = cycle.ticket.boot;
  evidence.outcome = CompletionOutcome::Served;
  evidence.served = served;
  evidence.effect_code = 1;
  evidence.effect_digest = mix64(cycle.ticket.attempt.value());
  evidence.observed_tick = now;
  evidence.provenance = external_provenance(cycle.ticket.attempt.value());
  FS_TRY_ASSIGN(cycle.outcome, scheduler.complete(evidence, now));
  return cycle;
}

bool same_flow_identity(const FlowSnapshot& left, const FlowSnapshot& right) {
  return left.flow == right.flow && left.generation == right.generation &&
         left.lifecycle == right.lifecycle && left.estimated_work == right.estimated_work &&
         left.served_work == right.served_work &&
         left.outstanding_reserved == right.outstanding_reserved &&
         left.release_tick == right.release_tick && left.deadline_tick == right.deadline_tick;
}

bool same_flow_accounting(const FlowAccounting& left, const FlowAccounting& right) {
  return left.service_credit == right.service_credit &&
         left.dispatches_issued == right.dispatches_issued &&
         left.dispatches_started == right.dispatches_started &&
         left.completions_committed == right.completions_committed &&
         left.duplicate_completions == right.duplicate_completions &&
         left.rejected_completions == right.rejected_completions &&
         left.preemptions == right.preemptions && left.deadline_misses == right.deadline_misses &&
         left.wait_ticks_total == right.wait_ticks_total &&
         left.starvation_boosts == right.starvation_boosts;
}

/// Every authoritative field of an accounting report. The two completion
/// rejection counters are deliberately excluded: the implementation counts a
/// rejected report before it examines the epoch, so those two diagnostics move
/// even when nothing authoritative changes (see the defect report). No other
/// counter may move for a rejected completion.
bool same_authoritative_accounting(const AccountingReport& before, const AccountingReport& after) {
  return before.total_flows == after.total_flows && before.waiting == after.waiting &&
         before.ready == after.ready && before.scheduled == after.scheduled &&
         before.dispatched == after.dispatched && before.running == after.running &&
         before.completion_reported == after.completion_reported &&
         before.cancelling == after.cancelling && before.preempting == after.preempting &&
         before.completed == after.completed && before.cancelled == after.cancelled &&
         before.failed == after.failed && before.ambiguous == after.ambiguous &&
         before.attempts_total == after.attempts_total &&
         before.attempts_open == after.attempts_open &&
         before.attempts_started == after.attempts_started &&
         before.attempts_committed == after.attempts_committed &&
         before.attempts_preempted == after.attempts_preempted &&
         before.attempts_cancelled == after.attempts_cancelled &&
         before.attempts_abandoned == after.attempts_abandoned &&
         before.attempts_rejected == after.attempts_rejected &&
         before.completions_applied == after.completions_applied &&
         before.completions_duplicate == after.completions_duplicate &&
         before.schedules_issued == after.schedules_issued &&
         before.schedule_entries_issued == after.schedule_entries_issued &&
         before.preemptions_issued == after.preemptions_issued &&
         before.arbitration_rounds == after.arbitration_rounds &&
         before.starvation_boosts == after.starvation_boosts &&
         before.deadline_misses == after.deadline_misses &&
         before.reservations_retired == after.reservations_retired &&
         before.outstanding_reserved_units == after.outstanding_reserved_units &&
         before.service_credit_total == after.service_credit_total &&
         before.estimated_work_total == after.estimated_work_total && before.epoch == after.epoch;
}

}  // namespace

// ---------------------------------------------------------------------------
// Journal append / replay
// ---------------------------------------------------------------------------

FLOW_TEST(Persistence, journal_append_replay_round_trip) {
  TempDirectory directory("journal_round_trip");
  REQUIRE(directory.created());

  std::vector<JournalRecordType> expected_types;
  std::vector<std::string> expected_payloads;
  std::uint64_t expected_bytes = 0;
  constexpr std::uint64_t kRecordsPerType = 3;
  for (const JournalRecordType type : fixture_types()) {
    for (std::uint64_t index = 0; index < kRecordsPerType; ++index) {
      const std::size_t length =
          11u * static_cast<std::size_t>(static_cast<std::uint16_t>(type)) +
          static_cast<std::size_t>(index) + 1u;
      const std::string payload =
          patterned_payload(length, static_cast<std::uint8_t>(7u * index + 1u));
      expected_types.push_back(type);
      expected_payloads.push_back(payload);
      expected_bytes += static_cast<std::uint64_t>(journal_record_bytes(payload.size()));
    }
  }
  const std::uint64_t record_count = static_cast<std::uint64_t>(expected_payloads.size());

  {
    const Result<std::unique_ptr<DurableStore>> store =
        DurableStore::open(directory.path_string(), true);
    REQUIRE(store.ok());
    CHECK_EQ(store.value()->directory(), directory.path_string());
    CHECK_EQ(store.value()->last_sequence(), 0u);
    for (std::size_t index = 0; index < expected_payloads.size(); ++index) {
      REQUIRE_OK(store.value()->append(expected_types[index], expected_payloads[index]));
      CHECK_EQ(store.value()->last_sequence(), static_cast<std::uint64_t>(index) + 1u);
    }
    // The writer refuses what the reader would have to reject.
    CHECK_ERROR(store.value()->append(JournalRecordType::Unknown, "x"), ErrorCode::InvalidArgument);
    const std::string oversized(kMaxJournalRecordPayload + 1u, 'o');
    CHECK_ERROR(store.value()->append(JournalRecordType::Checkpoint, oversized),
                ErrorCode::Oversized);
    CHECK_EQ(store.value()->last_sequence(), record_count);
  }

  // Reopen: sequence numbers, byte counts and payloads are reproduced exactly.
  {
    const Result<std::unique_ptr<DurableStore>> store =
        DurableStore::open(directory.path_string(), true);
    REQUIRE(store.ok());
    CHECK_EQ(store.value()->last_sequence(), record_count);

    std::vector<JournalRecordType> seen_types;
    std::vector<std::uint64_t> seen_sequences;
    std::vector<std::string> seen_payloads;
    JournalScanReport report;
    const Status replayed = store.value()->replay(
        false, report, [&](const JournalRecordView& view) {
          seen_types.push_back(view.type);
          seen_sequences.push_back(view.sequence);
          seen_payloads.emplace_back(view.payload);
          return Status::success();
        });
    REQUIRE_OK(replayed);
    CHECK_EQ(report.records, record_count);
    CHECK_EQ(report.bytes, expected_bytes);
    CHECK_EQ(report.last_sequence, record_count);
    CHECK_EQ(report.skipped_records, 0u);
    CHECK(!report.tail_truncated);
    CHECK_EQ(report.truncate_to_bytes, expected_bytes);
    CHECK_EQ(seen_types.size(), expected_types.size());
    for (std::size_t index = 0; index < expected_payloads.size(); ++index) {
      CHECK(seen_types[index] == expected_types[index]);
      CHECK_EQ(seen_sequences[index], static_cast<std::uint64_t>(index) + 1u);
      CHECK_EQ(seen_payloads[index], expected_payloads[index]);
    }

    // Appending continues the sequence exactly after a reopen.
    REQUIRE_OK(store.value()->append(JournalRecordType::Checkpoint, "after-reopen"));
    CHECK_EQ(store.value()->last_sequence(), record_count + 1u);

    std::uint64_t visited = 0;
    JournalScanReport second;
    REQUIRE_OK(store.value()->replay(false, second, [&](const JournalRecordView& view) {
      ++visited;
      if (view.sequence == record_count + 1u) {
        CHECK(view.type == JournalRecordType::Checkpoint);
        CHECK_EQ(std::string(view.payload), std::string("after-reopen"));
      }
      return Status::success();
    }));
    CHECK_EQ(visited, record_count + 1u);
    CHECK_EQ(second.last_sequence, record_count + 1u);
    CHECK(!second.tail_truncated);
  }

  // A record type this build does not know is refused, not silently skipped.
  {
    const std::string unknown_type_record =
        raw_journal_record(static_cast<std::uint16_t>(21), 1, "future-record");
    write_file(directory.file("journal.log"), unknown_type_record);
    const Result<std::unique_ptr<DurableStore>> store =
        DurableStore::open(directory.path_string(), true);
    REQUIRE(store.ok());
    JournalScanReport report;
    CHECK_ERROR(store.value()->replay(false, report, nullptr), ErrorCode::Unsupported);
  }
}

// ---------------------------------------------------------------------------
// Torn tail
// ---------------------------------------------------------------------------

FLOW_TEST(Persistence, torn_tail_is_reported_on_open_and_repaired_lazily) {
  // A crash during an append leaves a partial record at the tail. Opening the
  // store reports that artifact instead of hiding it, leaves the bytes on disk,
  // and repairs lazily: the next append discards the partial record before it
  // writes, so the sequence never gains a hole.
  const std::vector<std::size_t> boundaries = fixture_boundaries();
  const std::size_t total = boundaries.back();
  const std::size_t last_start = boundaries[boundaries.size() - 2];
  const std::vector<std::size_t> cuts = {
      total - 1,                         // inside the last record's trailer
      total - kJournalTrailerBytes - 5,  // inside the last record's payload
      last_start + 10,                   // inside the last record's header
      last_start,                        // exactly on a record boundary
      boundaries[2] + 30,                // inside a middle record
  };

  for (const std::size_t cut : cuts) {
    TempDirectory directory("torn_lazy");
    write_fixture_journal(directory.path_string());
    truncate_file_to(directory.file("journal.log"), cut);

    const std::size_t expected_records = complete_records_at(cut);
    const std::uint64_t expected_bytes = static_cast<std::uint64_t>(boundaries[expected_records]);
    const bool torn = cut != expected_bytes;
    const std::string appended = "after-lazy-repair";

    {
      const Result<std::unique_ptr<DurableStore>> store =
          DurableStore::open(directory.path_string(), true);
      REQUIRE(store.ok());
      CHECK_EQ(store.value()->open_tail_truncated(), torn);
      // The sequence position is the last complete record either way.
      CHECK_EQ(store.value()->last_sequence(), static_cast<std::uint64_t>(expected_records));
      // The partial bytes are still there: opening does not rewrite the file.
      std::error_code error;
      const std::uintmax_t on_disk = std::filesystem::file_size(directory.file("journal.log"), error);
      CHECK(!error);
      CHECK_EQ(on_disk, static_cast<std::uintmax_t>(cut));

      // The next append repairs the tail first, then continues the sequence.
      REQUIRE_OK(store.value()->append(JournalRecordType::Checkpoint, appended));
      CHECK_EQ(store.value()->last_sequence(), static_cast<std::uint64_t>(expected_records) + 1u);
    }

    {
      const Result<std::unique_ptr<DurableStore>> store =
          DurableStore::open(directory.path_string(), true);
      REQUIRE(store.ok());
      CHECK(!store.value()->open_tail_truncated());
      std::vector<std::string> seen;
      std::uint64_t visited = 0;
      JournalScanReport report;
      REQUIRE_OK(store.value()->replay(false, report, [&](const JournalRecordView& view) {
        ++visited;
        CHECK_EQ(view.sequence, visited);
        seen.emplace_back(view.payload);
        return Status::success();
      }));
      CHECK_EQ(visited, static_cast<std::uint64_t>(expected_records) + 1u);
      CHECK_EQ(report.records, static_cast<std::uint64_t>(expected_records) + 1u);
      CHECK(!report.tail_truncated);
      CHECK_EQ(seen.size(), expected_records + 1u);
      for (std::size_t index = 0; index < expected_records; ++index) {
        CHECK_EQ(seen[index], fixture_payloads()[index]);
      }
      CHECK_EQ(seen.back(), appended);
      std::error_code error;
      const std::uintmax_t repaired_size =
          std::filesystem::file_size(directory.file("journal.log"), error);
      CHECK(!error);
      CHECK_EQ(
          repaired_size,
          static_cast<std::uintmax_t>(expected_bytes + journal_record_bytes(appended.size())));
    }
  }
}

FLOW_TEST(Persistence, replay_reports_and_repairs_a_torn_tail) {
  const std::vector<std::size_t> boundaries = fixture_boundaries();
  const std::size_t total = boundaries.back();
  const std::size_t last_start = boundaries[boundaries.size() - 2];
  const std::vector<std::size_t> cuts = {
      total - 1,                         // inside the last record's trailer
      total - kJournalTrailerBytes - 5,  // inside the last record's payload
      last_start + 10,                   // inside the last record's header
      last_start,                        // exactly on a record boundary
      boundaries[3] + 1,                 // one byte into a middle record
  };

  for (const std::size_t cut : cuts) {
    TempDirectory directory("torn_replay");
    write_fixture_journal(directory.path_string());
    truncate_file_to(directory.file("journal.log"), cut);

    const std::size_t expected_records = complete_records_at(cut);
    const std::uint64_t expected_bytes = static_cast<std::uint64_t>(boundaries[expected_records]);
    const bool torn = cut != expected_bytes;

    {
      const Result<std::unique_ptr<DurableStore>> store =
          DurableStore::open(directory.path_string(), true);
      REQUIRE(store.ok());

      // Without repair the partial tail is reported, exactly and only once.
      std::vector<std::string> seen;
      JournalScanReport report;
      REQUIRE_OK(store.value()->replay(false, report, [&](const JournalRecordView& view) {
        seen.emplace_back(view.payload);
        return Status::success();
      }));
      CHECK_EQ(report.records, static_cast<std::uint64_t>(expected_records));
      CHECK_EQ(report.bytes, expected_bytes);
      CHECK_EQ(report.last_sequence, static_cast<std::uint64_t>(expected_records));
      CHECK_EQ(report.truncate_to_bytes, expected_bytes);
      CHECK_EQ(report.skipped_records, 0u);
      CHECK_EQ(report.tail_truncated, torn);
      CHECK_EQ(seen.size(), expected_records);
      for (std::size_t index = 0; index < seen.size(); ++index) {
        CHECK_EQ(seen[index], fixture_payloads()[index]);
      }

      // repair=true truncates the partial record in place and clears the pending
      // repair, but the open-time observation is still reported.
      JournalScanReport repaired;
      REQUIRE_OK(store.value()->replay(true, repaired, nullptr));
      CHECK_EQ(repaired.records, static_cast<std::uint64_t>(expected_records));
      CHECK_EQ(repaired.bytes, expected_bytes);
      CHECK_EQ(repaired.tail_truncated, torn);
      CHECK_EQ(store.value()->open_tail_truncated(), torn);
      std::error_code error;
      const std::uintmax_t repaired_size =
          std::filesystem::file_size(directory.file("journal.log"), error);
      CHECK(!error);
      CHECK_EQ(repaired_size, static_cast<std::uintmax_t>(expected_bytes));
    }

    {
      const Result<std::unique_ptr<DurableStore>> store =
          DurableStore::open(directory.path_string(), true);
      REQUIRE(store.ok());
      CHECK(!store.value()->open_tail_truncated());
      CHECK_EQ(store.value()->last_sequence(), static_cast<std::uint64_t>(expected_records));

      // The repaired journal is appendable and contiguously replayable.
      REQUIRE_OK(store.value()->append(JournalRecordType::Checkpoint, "after-repair"));
      CHECK_EQ(store.value()->last_sequence(), static_cast<std::uint64_t>(expected_records) + 1u);

      std::uint64_t visited = 0;
      JournalScanReport final_report;
      REQUIRE_OK(store.value()->replay(false, final_report, [&](const JournalRecordView& view) {
        ++visited;
        CHECK_EQ(view.sequence, visited);
        return Status::success();
      }));
      CHECK_EQ(visited, static_cast<std::uint64_t>(expected_records) + 1u);
      CHECK(!final_report.tail_truncated);
      CHECK_EQ(final_report.truncate_to_bytes,
               expected_bytes + static_cast<std::uint64_t>(journal_record_bytes(12u)));
    }
  }
}

// ---------------------------------------------------------------------------
// Corruption
// ---------------------------------------------------------------------------

FLOW_TEST(Persistence, corruption_in_a_completed_record_is_detected) {
  const std::vector<std::size_t> boundaries = fixture_boundaries();

  // 1. A flipped payload byte in the middle of the file. The record is complete
  //    (header, payload and trailer are all present), so this is corruption and
  //    not a torn tail: replay must fail rather than skip the record.
  {
    TempDirectory directory("corrupt_payload");
    write_fixture_journal(directory.path_string());
    std::string bytes = read_file(directory.file("journal.log"));
    const std::size_t offset = boundaries[2] + kJournalHeaderBytes + 10;
    CHECK(offset < boundaries[3]);
    bytes[offset] = static_cast<char>(bytes[offset] ^ 0x20);
    write_file(directory.file("journal.log"), bytes);

    const Result<std::unique_ptr<DurableStore>> store =
        DurableStore::open(directory.path_string(), true);
    CHECK_ERROR(as_status(store), ErrorCode::CorruptData);
  }

  // 2. A flipped byte in a record header is caught by the header checksum.
  {
    TempDirectory directory("corrupt_header");
    write_fixture_journal(directory.path_string());
    std::string bytes = read_file(directory.file("journal.log"));
    bytes[boundaries[2] + 17] = static_cast<char>(bytes[boundaries[2] + 17] ^ 0x01);
    write_file(directory.file("journal.log"), bytes);

    const Result<std::unique_ptr<DurableStore>> store =
        DurableStore::open(directory.path_string(), true);
    CHECK_ERROR(as_status(store), ErrorCode::CorruptData);
  }

  // 3. Garbage in the first four bytes destroys the record magic.
  {
    TempDirectory directory("corrupt_magic");
    write_fixture_journal(directory.path_string());
    std::string bytes = read_file(directory.file("journal.log"));
    patch_u32(bytes, 0, 0x00000000u);
    write_file(directory.file("journal.log"), bytes);

    const Result<std::unique_ptr<DurableStore>> store =
        DurableStore::open(directory.path_string(), true);
    CHECK_ERROR(as_status(store), ErrorCode::CorruptData);
  }

  // 4. A record whose declared payload length exceeds the bound is rejected as
  //    Oversized even though its header checksum matches.
  {
    TempDirectory directory("corrupt_oversized");
    write_fixture_journal(directory.path_string());
    std::string bytes = read_file(directory.file("journal.log"));
    patch_u32(bytes, boundaries[1] + 16, kMaxJournalRecordPayload + 1u);
    reseal_record_header(bytes, boundaries[1]);
    write_file(directory.file("journal.log"), bytes);

    const Result<std::unique_ptr<DurableStore>> store =
        DurableStore::open(directory.path_string(), true);
    CHECK_ERROR(as_status(store), ErrorCode::Oversized);
  }

  // 5. A sequence that is not exactly previous plus one is a gap, never a silent
  //    renumbering.
  {
    TempDirectory directory("corrupt_sequence");
    write_fixture_journal(directory.path_string());
    std::string bytes = read_file(directory.file("journal.log"));
    patch_u64(bytes, boundaries[2] + 8, 9u);
    reseal_record_header(bytes, boundaries[2]);
    write_file(directory.file("journal.log"), bytes);

    const Result<std::unique_ptr<DurableStore>> store =
        DurableStore::open(directory.path_string(), true);
    CHECK_ERROR(as_status(store), ErrorCode::SequenceGap);
  }

  // 6. A format generation this build cannot interpret is refused explicitly,
  //    so a newer writer is never read as if it were an older one.
  {
    TempDirectory directory("corrupt_generation");
    write_fixture_journal(directory.path_string());
    std::string bytes = read_file(directory.file("journal.log"));
    patch_u16(bytes, 4, static_cast<std::uint16_t>(kDurableFormatGeneration + 1));
    reseal_record_header(bytes, 0);
    write_file(directory.file("journal.log"), bytes);

    const Result<std::unique_ptr<DurableStore>> store =
        DurableStore::open(directory.path_string(), true);
    CHECK_ERROR(as_status(store), ErrorCode::Unsupported);
  }

  // 7. open() and replay() disagree about exactly one thing: a record whose type
  //    this build does not know. open() cannot know whether the type is
  //    meaningful, so it tolerates the record; replay() is the layer that
  //    interprets records and refuses it. This is also the one durable failure
  //    replay() can report that open() did not already report, because open()
  //    runs the same integrity scan and refuses to hand out a corrupt store.
  {
    TempDirectory directory("unknown_type_replay");
    write_fixture_journal(directory.path_string());
    std::string bytes = read_file(directory.file("journal.log"));
    bytes += raw_journal_record(static_cast<std::uint16_t>(21), 6, "future-record");
    write_file(directory.file("journal.log"), bytes);

    const Result<std::unique_ptr<DurableStore>> store =
        DurableStore::open(directory.path_string(), true);
    REQUIRE(store.ok());
    CHECK_EQ(store.value()->last_sequence(), 6u);

    JournalScanReport report;
    CHECK_ERROR(store.value()->replay(false, report, nullptr), ErrorCode::Unsupported);
    CHECK_ERROR(store.value()->replay(true, report, nullptr), ErrorCode::Unsupported);
    CHECK_EQ(store.value()->last_sequence(), 6u);
  }
}

// ---------------------------------------------------------------------------
// Snapshot
// ---------------------------------------------------------------------------

namespace {

/// Snapshot round trip plus every corruption mode, with the store owned and
/// released inside this function so the caller can reopen the same directory.
void snapshot_round_trip_and_corruption_rounds(const std::filesystem::path& directory,
                                               const std::string& payload) {
  const Result<std::unique_ptr<DurableStore>> store =
      DurableStore::open(directory.string(), true);
  REQUIRE(store.ok());

  // A missing snapshot is NotFound, not an empty success.
  CHECK_ERROR(as_status(store.value()->read_snapshot()), ErrorCode::NotFound);

  // compact() both writes the snapshot and rotates the journal away.
  REQUIRE_OK(store.value()->append(JournalRecordType::FlowAdmitted, "before-compaction"));
  CHECK_EQ(store.value()->last_sequence(), 1u);
  REQUIRE_OK(store.value()->compact(payload));
  CHECK_EQ(store.value()->last_sequence(), 0u);
  const Result<std::string> restored = store.value()->read_snapshot();
  REQUIRE(restored.ok());
  CHECK_EQ(restored.value(), payload);

  const std::filesystem::path snapshot_path = directory / "snapshot.bin";
  const std::string valid = read_file(snapshot_path);
  CHECK(valid.size() > 20u);

  // A flipped payload byte leaves the header checksum valid, so the payload
  // checksum is what rejects it. The snapshot header is 20 bytes wide.
  {
    std::string corrupt = valid;
    corrupt[25] = static_cast<char>(corrupt[25] ^ 0x04);
    write_file(snapshot_path, corrupt);
    CHECK_ERROR(as_status(store.value()->read_snapshot()), ErrorCode::CorruptData);
  }

  // A flipped payload checksum field is caught by the header checksum.
  {
    std::string corrupt = valid;
    patch_u32(corrupt, 12, read_u32(valid, 12) ^ 0x1u);
    write_file(snapshot_path, corrupt);
    CHECK_ERROR(as_status(store.value()->read_snapshot()), ErrorCode::CorruptData);
  }

  // A truncated snapshot is rejected: the declared payload length and the file
  // size must agree.
  {
    write_file(snapshot_path, valid.substr(0, valid.size() - 3));
    CHECK_ERROR(as_status(store.value()->read_snapshot()), ErrorCode::CorruptData);
  }

  // A snapshot file too short to hold a header is rejected without reading it.
  {
    write_file(snapshot_path, std::string(7, 'x'));
    CHECK_ERROR(as_status(store.value()->read_snapshot()), ErrorCode::CorruptData);
    write_file(snapshot_path, std::string());
    CHECK_ERROR(as_status(store.value()->read_snapshot()), ErrorCode::CorruptData);
  }

  // A snapshot claiming a different format generation is Unsupported.
  {
    std::string corrupt = valid;
    patch_u16(corrupt, 4, static_cast<std::uint16_t>(kDurableFormatGeneration + 1));
    patch_u32(corrupt, 16, crc32c(corrupt.data(), 16));
    write_file(snapshot_path, corrupt);
    CHECK_ERROR(as_status(store.value()->read_snapshot()), ErrorCode::Unsupported);
  }

  // Garbage of a plausible size is rejected on the magic.
  {
    write_file(snapshot_path, patterned_payload(64, 0x99u));
    CHECK_ERROR(as_status(store.value()->read_snapshot()), ErrorCode::CorruptData);
  }

  // The store recovers as soon as a valid snapshot is written again.
  REQUIRE_OK(store.value()->write_snapshot(payload));
  const Result<std::string> rewritten = store.value()->read_snapshot();
  REQUIRE(rewritten.ok());
  CHECK_EQ(rewritten.value(), payload);
}

}  // namespace

FLOW_TEST(Persistence, snapshot_round_trip_and_corruption) {
  TempDirectory directory("snapshot");
  const std::string payload = patterned_payload(2000, 0x77u);
  snapshot_round_trip_and_corruption_rounds(directory.path(), payload);

  // Reopening the directory after the store was released finds the snapshot.
  const Result<std::unique_ptr<DurableStore>> reopened =
      DurableStore::open(directory.path_string(), true);
  REQUIRE(reopened.ok());
  const Result<std::string> from_disk = reopened.value()->read_snapshot();
  REQUIRE(from_disk.ok());
  CHECK_EQ(from_disk.value(), payload);
}

// ---------------------------------------------------------------------------
// Scheduler restart
// ---------------------------------------------------------------------------

FLOW_TEST(Persistence, scheduler_restart_retires_open_work_as_ambiguous) {
  TempDirectory directory("restart");
  REQUIRE(directory.created());
  const SchedulerOptions options = durable_options(directory.path_string());

  constexpr Ticks kRoundTick = 100;
  constexpr Ticks kResolveTick = 1000;

  FabricEpoch first_epoch;
  DispatchTicket stranded_ticket;
  CompletionEvidence stranded_evidence;
  FlowId started_flow{};
  FlowId dispatched_flow{};
  const FlowId waiting_flow(3);
  std::vector<FlowSnapshot> before_restart_flows;

  {
    const Result<std::unique_ptr<Scheduler>> created = Scheduler::create(options);
    REQUIRE(created.ok());
    Scheduler& scheduler = *created.value();
    REQUIRE_OK(register_fixture_topology(scheduler));
    REQUIRE_OK(scheduler.admit_flow(fixture_flow(FlowId(1), 4, 0)));
    REQUIRE_OK(scheduler.admit_flow(fixture_flow(FlowId(2), 4, 0)));
    REQUIRE_OK(scheduler.admit_flow(fixture_flow(FlowId(3), 4, 5'000'000)));
    CHECK_OK(as_status(scheduler.accounting().validate()));

    const Result<FabricEpoch> epoch = scheduler.epoch();
    REQUIRE(epoch.ok());
    first_epoch = epoch.value();
    CHECK(first_epoch.valid());

    const Result<ArbitrationOutcome> outcome = scheduler.arbitrate(kRoundTick);
    REQUIRE(outcome.ok());
    CHECK_EQ(outcome.value().schedule.entries.size(), 2u);
    CHECK_EQ(outcome.value().schedule.epoch.value(), first_epoch.value());

    const Result<DispatchTicket> started =
        scheduler.begin_dispatch(outcome.value().schedule.schedule,
                                 outcome.value().schedule.generation, 0, WorkerId(1), BootId(1),
                                 kRoundTick);
    REQUIRE(started.ok());
    REQUIRE_OK(scheduler.mark_started(started.value(), kRoundTick));
    started_flow = started.value().flow;

    const Result<DispatchTicket> dispatched =
        scheduler.begin_dispatch(outcome.value().schedule.schedule,
                                 outcome.value().schedule.generation, 1, WorkerId(1), BootId(1),
                                 kRoundTick);
    REQUIRE(dispatched.ok());
    dispatched_flow = dispatched.value().flow;
    stranded_ticket = dispatched.value();

    // Evidence produced under the pre-restart epoch, never committed.
    stranded_evidence.schedule = stranded_ticket.schedule;
    stranded_evidence.schedule_generation = stranded_ticket.schedule_generation;
    stranded_evidence.attempt = stranded_ticket.attempt;
    stranded_evidence.epoch = stranded_ticket.epoch;
    stranded_evidence.flow = stranded_ticket.flow;
    stranded_evidence.flow_generation = stranded_ticket.flow_generation;
    stranded_evidence.worker = stranded_ticket.worker;
    stranded_evidence.boot = stranded_ticket.boot;
    stranded_evidence.outcome = CompletionOutcome::Served;
    stranded_evidence.served = stranded_ticket.quantum;
    stranded_evidence.effect_code = 1;
    stranded_evidence.effect_digest = mix64(stranded_ticket.attempt.value());
    stranded_evidence.observed_tick = kRoundTick;
    stranded_evidence.provenance = external_provenance(stranded_ticket.attempt.value());

    const Result<FlowSnapshot> started_snapshot = scheduler.flow(started_flow);
    REQUIRE(started_snapshot.ok());
    CHECK(started_snapshot.value().lifecycle == FlowLifecycle::Running);
    CHECK(started_snapshot.value().open_attempt.valid());
    const Result<FlowSnapshot> dispatched_snapshot = scheduler.flow(dispatched_flow);
    REQUIRE(dispatched_snapshot.ok());
    CHECK(dispatched_snapshot.value().lifecycle == FlowLifecycle::Dispatched);

    const AccountingReport before_restart = scheduler.accounting();
    CHECK_EQ(before_restart.running, 1u);
    CHECK_EQ(before_restart.dispatched, 1u);
    CHECK_EQ(before_restart.waiting, 1u);
    CHECK_OK(as_status(before_restart.validate()));
    for (const FlowId id : {FlowId(1), FlowId(2), FlowId(3)}) {
      const Result<FlowSnapshot> snapshot = scheduler.flow(id);
      REQUIRE(snapshot.ok());
      before_restart_flows.push_back(snapshot.value());
    }
  }  // The scheduler is destroyed with two dispatch attempts still open.

  {
    const Result<std::unique_ptr<Scheduler>> created = Scheduler::create(options);
    REQUIRE(created.ok());
    Scheduler& scheduler = *created.value();
    const RecoveryReport& recovery = scheduler.recovery_report();

    CHECK(recovery.recovered);
    CHECK(recovery.epoch_before < recovery.epoch_after);
    const Result<FabricEpoch> epoch = scheduler.epoch();
    REQUIRE(epoch.ok());
    CHECK(epoch.value().value() > first_epoch.value());
    CHECK_EQ(recovery.attempts_abandoned, 2u);
    CHECK_EQ(recovery.flows_made_ambiguous, 2u);
    CHECK_EQ(recovery.flows_restored, 3u);
    // The topology tables are counted whether they came from a snapshot or from
    // the journal alone.
    CHECK_EQ(recovery.resources_restored, 1u);
    CHECK_EQ(recovery.priority_classes_restored, 4u);
    CHECK_EQ(recovery.qos_classes_restored, 1u);
    CHECK_EQ(recovery.reservations_restored, 0u);
    CHECK_EQ(recovery.snapshot_records, 0u);
    CHECK(recovery.journal_records_replayed > 0u);
    CHECK(recovery.journal_bytes_replayed > 0u);
    // The previous incarnation was destroyed cleanly, so no partial record was
    // discarded. A torn tail is repaired by DurableStore::open before recover()
    // ever replays, which is why this flag can only be false on this path.
    CHECK(!recovery.tail_truncated);

    // The flows that held open work are ambiguous, never Running.
    for (const FlowId id : {started_flow, dispatched_flow}) {
      const Result<FlowSnapshot> snapshot = scheduler.flow(id);
      REQUIRE(snapshot.ok());
      CHECK(snapshot.value().lifecycle == FlowLifecycle::Ambiguous);
      CHECK(snapshot.value().lifecycle != FlowLifecycle::Running);
      CHECK(!holds_open_work(snapshot.value().lifecycle));
      CHECK(!snapshot.value().open_attempt.valid());
    }
    const Result<FlowSnapshot> untouched = scheduler.flow(waiting_flow);
    REQUIRE(untouched.ok());
    CHECK(untouched.value().lifecycle == FlowLifecycle::Waiting);



    const AccountingReport recovered = scheduler.accounting();
    CHECK_EQ(recovered.running, 0u);
    CHECK_EQ(recovered.dispatched, 0u);
    CHECK_EQ(recovered.ambiguous, 2u);
    CHECK_EQ(recovered.waiting, 1u);
    CHECK_EQ(recovered.attempts_total, 0u);
    CHECK_OK(as_status(recovered.validate()));

    // Authority issued by the previous incarnation is retired with its epoch.
    CHECK_ERROR(as_status(scheduler.attempt(stranded_ticket.attempt)), ErrorCode::UnknownAttempt);
    CHECK_ERROR(scheduler.revalidate(stranded_ticket, kResolveTick), ErrorCode::StaleEpoch);

    // The resource, priority and qos tables survived the restart with their
    // original generations: the same binding is accepted and a binding to a
    // generation that never existed is still refused.
    REQUIRE_OK(scheduler.admit_flow(fixture_flow(FlowId(4), 2, 0)));
    FlowDescriptor stale_binding = fixture_flow(FlowId(5), 2, 0);
    stale_binding.resource.generation = Generation(7);
    CHECK_ERROR(scheduler.admit_flow(stale_binding), ErrorCode::StaleResourceGeneration);

    // A completion carrying the pre-restart epoch is rejected and mutates no
    // authoritative accounting.
    const AccountingReport before_rejection = scheduler.accounting();
    const Result<CommitOutcome> rejection = scheduler.complete(stranded_evidence, kResolveTick);
    REQUIRE(rejection.ok());
    CHECK(rejection.value().disposition == CommitDisposition::Rejected);
    CHECK(rejection.value().code == ErrorCode::StaleEpoch);
    const AccountingReport after_rejection = scheduler.accounting();
    CHECK(same_authoritative_accounting(before_rejection, after_rejection));
    // The rejection is a diagnostic, not authority: the two rejection counters
    // advance by exactly one and no authoritative field moves.
    CHECK_EQ(after_rejection.completion_reports_received,
             before_rejection.completion_reports_received + 1u);
    CHECK_EQ(after_rejection.completions_rejected, before_rejection.completions_rejected + 1u);
    CHECK_OK(as_status(after_rejection.validate()));

    const Result<FlowSnapshot> still_ambiguous = scheduler.flow(stranded_ticket.flow);
    REQUIRE(still_ambiguous.ok());
    CHECK(still_ambiguous.value().lifecycle == FlowLifecycle::Ambiguous);
    CHECK_EQ(still_ambiguous.value().served_work, 0u);
    CHECK_EQ(still_ambiguous.value().outstanding_reserved, 0u);

    // Retry resolution returns the flow to Waiting and it is arbitrated again.
    CHECK_OK(scheduler.resolve_ambiguous(stranded_ticket.flow, stranded_ticket.flow_generation,
                                         AmbiguityResolution::Retry, kResolveTick));
    const Result<FlowSnapshot> retried = scheduler.flow(stranded_ticket.flow);
    REQUIRE(retried.ok());
    CHECK(retried.value().lifecycle == FlowLifecycle::Waiting);
    CHECK_OK(as_status(scheduler.accounting().validate()));

    const Result<ArbitrationOutcome> next = scheduler.arbitrate(kResolveTick);
    REQUIRE(next.ok());
    bool rearbitrated = false;
    for (const ScheduleEntry& entry : next.value().schedule.entries) {
      if (entry.flow == stranded_ticket.flow) {
        rearbitrated = true;
        CHECK(entry.kind == DecisionKind::Run);
        CHECK(entry.quantum >= 1u);
      }
      CHECK(entry.flow != started_flow);  // still ambiguous, so still ineligible
    }
    CHECK(rearbitrated);

    // Abandon makes the remaining flow terminal and it is never arbitrated again.
    CHECK_OK(scheduler.resolve_ambiguous(started_flow, before_restart_flows[0].generation,
                                         AmbiguityResolution::Abandon, kResolveTick));
    const Result<FlowSnapshot> abandoned = scheduler.flow(started_flow);
    REQUIRE(abandoned.ok());
    CHECK(abandoned.value().lifecycle == FlowLifecycle::Failed);
    CHECK(is_terminal(abandoned.value().lifecycle));
    CHECK_OK(as_status(scheduler.accounting().validate()));
  }
}

// A crash that leaves a partial record at the tail of a Scheduler-owned journal
// must be reported by recovery, must not lose the committed records before it,
// and must leave the store appendable without a sequence hole.
FLOW_TEST(Persistence, scheduler_recovery_reports_a_torn_tail) {
  TempDirectory directory("scheduler_torn_tail");
  const SchedulerOptions options = durable_options(directory.path_string());
  std::uint64_t good_records = 0;

  {
    const Result<std::unique_ptr<Scheduler>> created = Scheduler::create(options);
    REQUIRE(created.ok());
    Scheduler& scheduler = *created.value();
    REQUIRE_OK(register_fixture_topology(scheduler));
    REQUIRE_OK(scheduler.admit_flow(fixture_flow(FlowId(1), 4, 0)));
    REQUIRE_OK(scheduler.admit_flow(fixture_flow(FlowId(2), 4, 0)));
    good_records = scheduler.journal_position();
    CHECK(good_records > 0u);
    CHECK_OK(as_status(scheduler.accounting().validate()));
  }  // The store is closed here, so the journal can be edited from outside.

  {
    // A crash in the middle of an append leaves a partial record behind: here
    // the first ten bytes of a valid record header.
    std::error_code error;
    const std::uintmax_t before_size =
        std::filesystem::file_size(directory.file("journal.log"), error);
    CHECK(!error);
    const std::string partial =
        raw_journal_record(static_cast<std::uint16_t>(JournalRecordType::FlowAdmitted),
                           good_records + 1u, "torn-append")
            .substr(0, 10);
    std::ofstream stream(directory.file("journal.log"), std::ios::binary | std::ios::app);
    stream.write(partial.data(), static_cast<std::streamsize>(partial.size()));
    stream.close();
    CHECK(stream.good());
    const std::uintmax_t after_size =
        std::filesystem::file_size(directory.file("journal.log"), error);
    CHECK(!error);
    CHECK_EQ(after_size, before_size + partial.size());
  }

  {
    const Result<std::unique_ptr<Scheduler>> created = Scheduler::create(options);
    REQUIRE(created.ok());
    Scheduler& scheduler = *created.value();
    const RecoveryReport& recovery = scheduler.recovery_report();
    CHECK(recovery.recovered);
    CHECK(recovery.tail_truncated);  // the crash artifact is reported, not hidden
    CHECK_EQ(recovery.journal_records_replayed, good_records);
    CHECK_EQ(recovery.flows_restored, 2u);
    CHECK_EQ(recovery.resources_restored, 1u);
    CHECK_EQ(recovery.priority_classes_restored, 4u);
    CHECK_EQ(recovery.qos_classes_restored, 1u);
    CHECK_EQ(recovery.attempts_abandoned, 0u);

    const Result<FlowSnapshot> first = scheduler.flow(FlowId(1));
    REQUIRE(first.ok());
    CHECK(first.value().lifecycle == FlowLifecycle::Waiting);
    CHECK_OK(as_status(scheduler.accounting().validate()));

    // Recovery repaired the tail, so the store is appendable again and the new
    // record continues the sequence without a hole.
    REQUIRE_OK(scheduler.admit_flow(fixture_flow(FlowId(3), 4, 0)));
    CHECK(scheduler.journal_position() > recovery.journal_records_replayed);
  }

  {
    // The third incarnation replays the repaired journal cleanly.
    const Result<std::unique_ptr<Scheduler>> created = Scheduler::create(options);
    REQUIRE(created.ok());
    Scheduler& scheduler = *created.value();
    CHECK(!scheduler.recovery_report().tail_truncated);
    CHECK_EQ(scheduler.flow_count(), 3u);
    CHECK_OK(as_status(scheduler.accounting().validate()));
  }
}

// ---------------------------------------------------------------------------
// Checkpoint round trip
// ---------------------------------------------------------------------------

FLOW_TEST(Persistence, checkpoint_round_trip_restores_the_flow_table) {
  TempDirectory directory("checkpoint");
  REQUIRE(directory.created());
  const SchedulerOptions options = durable_options(directory.path_string());

  std::vector<FlowSnapshot> expected_flows;
  std::uint64_t post_checkpoint_records = 0;

  {
    const Result<std::unique_ptr<Scheduler>> created = Scheduler::create(options);
    REQUIRE(created.ok());
    Scheduler& scheduler = *created.value();
    REQUIRE_OK(register_fixture_topology(scheduler));
    REQUIRE_OK(scheduler.admit_flow(fixture_flow(FlowId(1), 4, 0)));
    REQUIRE_OK(scheduler.admit_flow(fixture_flow(FlowId(2), 4, 0)));
    REQUIRE_OK(scheduler.admit_flow(fixture_flow(FlowId(3), 4, 0)));

    const Result<ArbitrationOutcome> first = scheduler.arbitrate(10);
    REQUIRE(first.ok());
    CHECK_EQ(first.value().schedule.entries.size(), 3u);
    for (const ScheduleEntry& entry : first.value().schedule.entries) {
      const Result<DispatchCycle> cycle =
          run_entry(scheduler, first.value().schedule, entry, 10, WorkerId(1), BootId(1), 4);
      REQUIRE(cycle.ok());
      CHECK(cycle.value().outcome.disposition == CommitDisposition::Applied);
      CHECK_EQ(cycle.value().outcome.credited, 4u);
      CHECK(cycle.value().outcome.flow_completed);
    }

    const AccountingReport before_checkpoint = scheduler.accounting();
    CHECK_EQ(before_checkpoint.service_credit_total, 12u);
    CHECK_EQ(before_checkpoint.completed, 3u);
    CHECK_EQ(before_checkpoint.waiting, 0u);
    CHECK_EQ(before_checkpoint.attempts_committed, 3u);
    CHECK_OK(as_status(before_checkpoint.validate()));

    // The checkpoint folds the whole state image into the snapshot and rotates
    // the journal away.
    REQUIRE_OK(scheduler.checkpoint());
    CHECK_EQ(scheduler.journal_position(), 0u);

    // Continue mutating after the checkpoint: the snapshot alone is no longer
    // the whole state, and the journal alone does not carry the older half.
    REQUIRE_OK(scheduler.admit_flow(fixture_flow(FlowId(4), 4, 0)));
    const Result<ArbitrationOutcome> second = scheduler.arbitrate(20);
    REQUIRE(second.ok());
    CHECK_EQ(second.value().schedule.entries.size(), 1u);
    const Result<DispatchCycle> cycle =
        run_entry(scheduler, second.value().schedule, second.value().schedule.entries[0], 20,
                  WorkerId(1), BootId(1), 4);
    REQUIRE(cycle.ok());
    CHECK(cycle.value().outcome.disposition == CommitDisposition::Applied);
    CHECK(cycle.value().outcome.flow_completed);
    CHECK(scheduler.journal_position() > 0u);
    post_checkpoint_records = scheduler.journal_position();

    const AccountingReport after_mutation = scheduler.accounting();
    CHECK_EQ(after_mutation.service_credit_total, 16u);
    CHECK_EQ(after_mutation.completed, 4u);
    CHECK_EQ(after_mutation.waiting, 0u);
    CHECK_EQ(after_mutation.attempts_committed, 4u);
    CHECK_OK(as_status(after_mutation.validate()));

    for (const FlowId id : {FlowId(1), FlowId(2), FlowId(3), FlowId(4)}) {
      const Result<FlowSnapshot> snapshot = scheduler.flow(id);
      REQUIRE(snapshot.ok());
      expected_flows.push_back(snapshot.value());
    }
  }

  {
    const Result<std::unique_ptr<Scheduler>> created = Scheduler::create(options);
    REQUIRE(created.ok());
    Scheduler& scheduler = *created.value();
    const RecoveryReport& recovery = scheduler.recovery_report();
    CHECK(recovery.recovered);
    CHECK_EQ(recovery.snapshot_records, 1u);
    CHECK_EQ(recovery.journal_records_replayed, post_checkpoint_records);
    CHECK_EQ(recovery.attempts_abandoned, 0u);
    CHECK_EQ(recovery.flows_restored, 4u);
    CHECK_EQ(scheduler.flow_count(), 4u);
    // The epoch advance retires every retained attempt, including the ones that
    // were already closed, so the retained attempt table is empty after a
    // restart even though nothing was abandoned.
    CHECK_EQ(scheduler.attempt_count(), 0u);

    for (const FlowSnapshot& expected : expected_flows) {
      const Result<FlowSnapshot> restored = scheduler.flow(expected.flow);
      REQUIRE(restored.ok());
      CHECK(same_flow_identity(restored.value(), expected));
      CHECK(same_flow_accounting(restored.value().accounting, expected.accounting));
      CHECK_EQ(restored.value().generation.value(), expected.generation.value());
    }

    // The flow table is restored exactly from the snapshot plus the journal
    // written since the checkpoint.
    const AccountingReport restored = scheduler.accounting();
    CHECK_EQ(restored.total_flows, 4u);
    CHECK_EQ(restored.completed, 4u);
    CHECK_EQ(restored.waiting, 0u);
    CHECK_EQ(restored.service_credit_total, 16u);
    CHECK_EQ(restored.estimated_work_total, 16u);
    CHECK_EQ(restored.schedules_issued, 2u);
    CHECK_EQ(restored.schedule_entries_issued, 4u);
    CHECK_EQ(restored.outstanding_reserved_units, 0u);
    CHECK_EQ(restored.attempts_total, 0u);
    // completions_applied is advanced on the shared apply path, so replaying the
    // post-checkpoint CompletionCommitted record reproduces the live figure: the
    // three from the snapshot plus the fourth committed after the checkpoint.
    CHECK_EQ(restored.completions_applied, 4u);
    // completion_reports_received and arbitration_rounds are session gauges: they
    // are restored from the snapshot but are not reproduced by replaying a
    // durable record, so they stay at the checkpoint values.
    CHECK_EQ(restored.completion_reports_received, 3u);
    CHECK_EQ(restored.arbitration_rounds, 1u);
    CHECK_OK(as_status(restored.validate()));

    // The restored table is live: a new flow can be admitted, arbitrated and
    // completed, and the restored state is not treated as authoritative history
    // that blocks new work.
    REQUIRE_OK(scheduler.admit_flow(fixture_flow(FlowId(5), 2, 0)));
    const Result<ArbitrationOutcome> outcome = scheduler.arbitrate(30);
    REQUIRE(outcome.ok());
    CHECK_EQ(outcome.value().schedule.entries.size(), 1u);
    const Result<DispatchCycle> cycle =
        run_entry(scheduler, outcome.value().schedule, outcome.value().schedule.entries[0], 30,
                  WorkerId(1), BootId(1), 2);
    REQUIRE(cycle.ok());
    CHECK(cycle.value().outcome.disposition == CommitDisposition::Applied);
    CHECK(cycle.value().outcome.flow_completed);
    CHECK_OK(as_status(scheduler.accounting().validate()));
  }

  // A third incarnation proves the snapshot written earlier plus the journal
  // written since are compacted and replayed together without loss.
  {
    const Result<std::unique_ptr<Scheduler>> created = Scheduler::create(options);
    REQUIRE(created.ok());
    Scheduler& scheduler = *created.value();
    CHECK_EQ(scheduler.flow_count(), 5u);
    const Result<FlowSnapshot> finished = scheduler.flow(FlowId(5));
    REQUIRE(finished.ok());
    CHECK(finished.value().lifecycle == FlowLifecycle::Completed);
    CHECK_EQ(finished.value().served_work, 2u);
    const Result<FlowSnapshot> earlier = scheduler.flow(FlowId(4));
    REQUIRE(earlier.ok());
    CHECK(earlier.value().lifecycle == FlowLifecycle::Completed);
    CHECK_EQ(earlier.value().served_work, 4u);
    // The third incarnation replays two durable completions (flows 4 and 5) on
    // top of the snapshot's three, so the cumulative figure keeps climbing.
    CHECK_EQ(scheduler.accounting().completions_applied, 5u);
    CHECK_OK(as_status(scheduler.accounting().validate()));
  }
}

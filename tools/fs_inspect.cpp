// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.
//
// flow_scheduler_inspect: inspector for a durable state directory.
//
// It reports the journal scan (records, bytes, last sequence, torn tail), the
// per-record-type census, the snapshot presence and size, and, with --dump, one
// line per record plus the durable fabric epoch.
//
// Read-only contract. DurableStore::open() reports a partial record at the tail
// through open_tail_truncated() and deliberately leaves the bytes in place, so
// opening the directory to inspect it modifies nothing. --verify (the default)
// therefore inspects the directory in place and never repairs: a torn tail is
// reported as the crash artifact it is, and the bytes stay exactly as they were
// on disk. --no-verify asks replay() to truncate the partial record back to the
// last good record boundary, which is the only mode that writes.
//
// Exit codes: 0 inspected (or repaired with --no-verify), 1 corrupt or missing
// store, 2 usage error.

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

#include "flow_scheduler/hash.hpp"
#include "flow_scheduler/journal.hpp"

namespace {

using flow_scheduler::DurableStore;
using flow_scheduler::ErrorCode;
using flow_scheduler::JournalRecordType;
using flow_scheduler::JournalRecordView;
using flow_scheduler::JournalScanReport;
using flow_scheduler::RecordReader;
using flow_scheduler::Result;
using flow_scheduler::Status;
using flow_scheduler::kJournalRecordMagic;
using flow_scheduler::kSnapshotMagic;
using flow_scheduler::to_string;

/// Prefix of every diagnostic this tool writes.
constexpr const char* kPrefix = "flow_scheduler_inspect: ";

struct Options {
  std::string state{};
  bool verify{true};
  bool dump{false};
};

struct JournalCensus {
  std::map<JournalRecordType, std::uint64_t> by_type{};
  std::uint64_t epoch_current{0};
  bool epoch_seen{false};
};

void print_usage(std::FILE* stream) {
  std::fputs(
      "usage: flow_scheduler_inspect --state <dir> [--verify|--no-verify] [--dump]\n"
      "\n"
      "  --state <dir>   durable state directory to inspect (required)\n"
      "  --verify        inspect without modifying the directory (default); a torn\n"
      "                  tail is reported and left in place\n"
      "  --no-verify     repair a torn journal tail in the inspected directory\n"
      "  --dump          one line per journal record plus the durable epoch\n"
      "\n"
      "The final stdout line is INSPECT records=<n> bytes=<n> last=<n>\n"
      "snapshot=<yes|no> torn_tail=<yes|no>.\n",
      stream);
}

[[noreturn]] void fail_usage(const std::string& message) {
  std::fprintf(stderr, "%s%s\n", kPrefix, message.c_str());
  print_usage(stderr);
  std::fflush(stderr);
  std::exit(2);
}

[[noreturn]] void fail(const std::string& message) {
  std::fprintf(stderr, "%s%s\n", kPrefix, message.c_str());
  std::fflush(stderr);
  std::exit(1);
}

Options parse_options(int argc, char** argv) {
  Options options;
  bool state_set = false;
  for (int index = 1; index < argc; ++index) {
    const std::string_view flag = argv[index];
    if (flag == "--state") {
      if (index + 1 >= argc) {
        fail_usage("--state requires a directory");
      }
      options.state = argv[++index];
      if (options.state.empty()) {
        fail_usage("--state requires a non-empty directory");
      }
      state_set = true;
    } else if (flag == "--verify") {
      options.verify = true;
    } else if (flag == "--no-verify") {
      options.verify = false;
    } else if (flag == "--dump") {
      options.dump = true;
    } else {
      fail_usage("unknown flag: " + std::string(flag));
    }
  }
  if (!state_set) {
    fail_usage("--state is required");
  }
  return options;
}

/// Leading magic of a durable file, so the journal and the snapshot can be told
/// apart without depending on their file names.
std::uint32_t magic_of(const std::filesystem::path& file, std::uintmax_t& size, bool& readable) {
  std::error_code error;
  size = std::filesystem::file_size(file, error);
  if (error) {
    readable = false;
    return 0;
  }
  readable = true;
  if (size < 4) {
    return 0;
  }
  std::ifstream stream(file, std::ios::binary);
  if (!stream) {
    readable = false;
    return 0;
  }
  char header[4] = {0, 0, 0, 0};
  stream.read(header, 4);
  if (stream.gcount() != 4) {
    readable = false;
    return 0;
  }
  return static_cast<std::uint32_t>(static_cast<unsigned char>(header[0])) |
         (static_cast<std::uint32_t>(static_cast<unsigned char>(header[1])) << 8) |
         (static_cast<std::uint32_t>(static_cast<unsigned char>(header[2])) << 16) |
         (static_cast<std::uint32_t>(static_cast<unsigned char>(header[3])) << 24);
}

void decode_epoch_record(std::string_view payload, std::uint64_t& previous, std::uint64_t& next,
                         std::uint8_t& reason, std::uint64_t& tick) {
  previous = 0;
  next = 0;
  reason = 0;
  tick = 0;
  RecordReader reader(payload);
  Result<std::uint64_t> value = reader.u64();
  if (!value.ok()) {
    return;
  }
  previous = value.value();
  value = reader.u64();
  if (!value.ok()) {
    return;
  }
  next = value.value();
  const Result<std::uint8_t> raw_reason = reader.u8();
  if (!raw_reason.ok()) {
    return;
  }
  reason = raw_reason.value();
  value = reader.u64();
  if (!value.ok()) {
    return;
  }
  tick = value.value();
}

int run(const Options& options) {
  std::error_code error;
  const std::filesystem::path source(options.state);
  if (!std::filesystem::is_directory(source, error) || error) {
    fail("state directory does not exist: " + options.state);
  }

  // Classify the durable files. A journal is identified by its record magic; a
  // journal that has been compacted away is empty, which is a legal state and
  // only used as a fallback so that an unrelated empty file cannot shadow a
  // real journal.
  struct DurableFile {
    std::filesystem::path path{};
    std::uintmax_t size{0};
    std::uint32_t magic{0};
  };
  std::vector<DurableFile> files;
  std::error_code enumerate_error;
  std::filesystem::directory_iterator iterator(source, enumerate_error);
  if (enumerate_error) {
    fail("cannot enumerate the state directory: " + enumerate_error.message());
  }
  for (const std::filesystem::directory_entry& entry : iterator) {
    std::error_code type_error;
    if (!entry.is_regular_file(type_error) || type_error) {
      continue;
    }
    std::uintmax_t size = 0;
    bool readable = false;
    const std::uint32_t magic = magic_of(entry.path(), size, readable);
    if (!readable) {
      fail("cannot read " + entry.path().string());
    }
    DurableFile file;
    file.path = entry.path();
    file.size = size;
    file.magic = magic;
    files.push_back(std::move(file));
  }
  std::filesystem::path journal;
  std::uintmax_t journal_bytes = 0;
  bool have_journal = false;
  for (const DurableFile& file : files) {
    if (file.magic == kJournalRecordMagic) {
      journal = file.path;
      journal_bytes = file.size;
      have_journal = true;
      break;
    }
  }
  if (!have_journal) {
    for (const DurableFile& file : files) {
      if (file.size == 0) {
        journal = file.path;
        journal_bytes = 0;
        have_journal = true;
        break;
      }
    }
  }
  std::filesystem::path snapshot;
  bool have_snapshot = false;
  for (const DurableFile& file : files) {
    if (file.magic == kSnapshotMagic) {
      snapshot = file.path;
      have_snapshot = true;
      break;
    }
  }
  if (!have_journal) {
    fail("state directory does not hold a durable journal: " + options.state);
  }

  // Opening reports a torn tail and leaves it in place, so this is safe in
  // --verify mode. Corruption refuses the open instead of guessing.
  Result<std::unique_ptr<DurableStore>> store = DurableStore::open(options.state, true);
  if (!store.ok()) {
    fail("durable state is corrupt: " + store.error().to_string());
  }
  const bool open_torn = store.value()->open_tail_truncated();

  std::printf("STORE directory=%s journal=%s bytes=%llu tail_at_open=%s\n", options.state.c_str(),
              journal.filename().string().c_str(),
              static_cast<unsigned long long>(journal_bytes), open_torn ? "torn" : "clean");

  bool snapshot_present = false;
  std::uintmax_t snapshot_payload = 0;
  const Result<std::string> loaded_snapshot = store.value()->read_snapshot();
  if (loaded_snapshot.ok()) {
    snapshot_present = true;
    snapshot_payload = static_cast<std::uintmax_t>(loaded_snapshot.value().size());
  } else if (loaded_snapshot.code() != ErrorCode::NotFound) {
    fail("snapshot cannot be read: " + loaded_snapshot.error().to_string());
  }
  if (snapshot_present) {
    std::printf("SNAPSHOT present bytes=%llu file=%s\n",
                static_cast<unsigned long long>(snapshot_payload),
                snapshot.filename().string().c_str());
  } else {
    std::printf("SNAPSHOT absent\n");
  }
  if (have_snapshot && !snapshot_present) {
    std::printf("SNAPSHOT-FILE unreferenced\n");
  }

  JournalCensus census;
  JournalScanReport report;
  const Status replayed = store.value()->replay(
      !options.verify, report, [&census, &options](const JournalRecordView& view) -> Status {
        census.by_type[view.type] += 1;
        if (view.type == JournalRecordType::EpochAdvanced && view.payload.size() >= 25) {
          std::uint64_t previous = 0;
          std::uint64_t next = 0;
          std::uint8_t reason = 0;
          std::uint64_t tick = 0;
          decode_epoch_record(view.payload, previous, next, reason, tick);
          census.epoch_current = next;
          census.epoch_seen = true;
          if (options.dump) {
            std::printf("EPOCH seq=%llu previous=%llu next=%llu reason=%u tick=%llu\n",
                        static_cast<unsigned long long>(view.sequence),
                        static_cast<unsigned long long>(previous),
                        static_cast<unsigned long long>(next), static_cast<unsigned>(reason),
                        static_cast<unsigned long long>(tick));
          }
        }
        if (options.dump) {
          std::printf("RECORD seq=%llu type=%s payload=%llu\n",
                      static_cast<unsigned long long>(view.sequence), to_string(view.type),
                      static_cast<unsigned long long>(view.payload.size()));
        }
        return Status::success();
      });
  if (!replayed.ok()) {
    fail("journal cannot be replayed: " + replayed.to_string());
  }

  // A torn tail is the partial record the scan refused, whether it was already
  // visible when the store opened or is only seen by this scan. The scan report
  // is filled before any repair is applied, so the value is the state of the
  // bytes, not the state after --no-verify rewrote them.
  const bool torn_tail = open_torn || report.tail_truncated;

  for (const auto& entry : census.by_type) {
    std::printf("RECORD-TYPE %s count=%llu\n", to_string(entry.first),
                static_cast<unsigned long long>(entry.second));
  }
  if (census.by_type.empty()) {
    std::printf("RECORD-TYPE none count=0\n");
  }
  std::printf("SCAN records=%llu bytes=%llu last_sequence=%llu skipped=%llu torn_tail=%s "
              "truncate_to_bytes=%llu\n",
              static_cast<unsigned long long>(report.records),
              static_cast<unsigned long long>(report.bytes),
              static_cast<unsigned long long>(report.last_sequence),
              static_cast<unsigned long long>(report.skipped_records),
              torn_tail ? "yes" : "no",
              static_cast<unsigned long long>(report.truncate_to_bytes));
  if (options.dump) {
    std::printf("EPOCH-CURRENT %llu\n",
                static_cast<unsigned long long>(census.epoch_seen ? census.epoch_current : 0));
  }
  if (torn_tail && !options.verify) {
    std::printf("REPAIR torn_tail=yes truncated_to=%llu\n",
                static_cast<unsigned long long>(report.truncate_to_bytes));
  }

  std::printf("INSPECT records=%llu bytes=%llu last=%llu snapshot=%s torn_tail=%s\n",
              static_cast<unsigned long long>(report.records),
              static_cast<unsigned long long>(report.bytes),
              static_cast<unsigned long long>(report.last_sequence),
              snapshot_present ? "yes" : "no", torn_tail ? "yes" : "no");
  std::fflush(stdout);
  if (torn_tail && options.verify) {
    std::fprintf(stderr,
                 "%sthe journal tail is torn and --verify leaves it untouched; rerun with "
                 "--no-verify to truncate it back to the last good record boundary (%llu bytes)\n",
                 kPrefix, static_cast<unsigned long long>(report.truncate_to_bytes));
    std::fflush(stderr);
  }
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  const Options options = parse_options(argc, argv);
  return run(options);
}

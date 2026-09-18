// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#ifndef FLOW_SCHEDULER_JOURNAL_HPP
#define FLOW_SCHEDULER_JOURNAL_HPP

#include <cstdint>
#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "flow_scheduler/error.hpp"
#include "flow_scheduler/hash.hpp"

namespace flow_scheduler {

/// Record families written to the durable journal. Values are part of the
/// on-disk format and must never be renumbered.
enum class JournalRecordType : std::uint16_t {
  Unknown = 0,
  PolicyInstalled = 1,
  ResourceRegistered = 2,
  ReservationRegistered = 3,
  ReservationRetired = 4,
  PriorityClassRegistered = 5,
  QoSClassRegistered = 6,
  EpochAdvanced = 7,
  FlowAdmitted = 8,
  FlowUpdated = 9,
  FlowReadinessChanged = 10,
  FlowCancelled = 11,
  FlowRetired = 12,
  ScheduleIssued = 13,
  ScheduleReleased = 14,
  AttemptOpened = 15,
  AttemptStarted = 16,
  AttemptClosed = 17,
  CompletionCommitted = 18,
  AmbiguityResolved = 19,
  Checkpoint = 20,
};

[[nodiscard]] const char* to_string(JournalRecordType type) noexcept;

/// Layout of one journal record:
///
///   offset  size  field
///   ------  ----  -----------------------------------------------------------
///        0     4  magic 0x314A5346 ('FSJ1')
///        4     2  format generation (kDurableFormatGeneration)
///        6     2  record type
///        8     8  monotone sequence number, first record is 1
///       16     4  payload length
///       20     4  CRC-32C over header bytes [0,20)
///       24     N  payload
///     24+N     4  CRC-32C over payload bytes
///
/// A record whose header CRC fails, whose payload CRC fails while the full
/// payload is present, whose declared length exceeds the bound, or whose
/// sequence is not exactly previous+1 is corrupt: the reader fails rather than
/// guessing. A record that is cut short by end-of-file is a torn tail write,
/// which is the expected artifact of a crash during append; the reader reports
/// it and discards it.
inline constexpr std::uint32_t kJournalRecordMagic = 0x314A5346u;
inline constexpr std::size_t kJournalHeaderBytes = 24;
inline constexpr std::size_t kJournalTrailerBytes = 4;
inline constexpr std::size_t kJournalRecordOverhead =
    kJournalHeaderBytes + kJournalTrailerBytes;
inline constexpr std::uint32_t kMaxJournalRecordPayload = 1u << 20;  // 1 MiB
/// Upper bound on a whole-state snapshot blob. Larger than a journal record
/// because a snapshot is written as one atomic file rather than appended.
inline constexpr std::uint64_t kMaxSnapshotPayload = 64ull * 1024ull * 1024ull;  // 64 MiB

/// Exact on-disk size of a record carrying the given payload length.
[[nodiscard]] constexpr std::size_t journal_record_bytes(std::size_t payload_length) noexcept {
  return kJournalHeaderBytes + payload_length + kJournalTrailerBytes;
}

struct JournalRecordView {
  JournalRecordType type{JournalRecordType::Unknown};
  std::uint64_t sequence{0};
  std::string_view payload{};
};

/// Immutable snapshot blob: magic 'FSS1', format generation, payload length,
/// payload CRC, payload.
inline constexpr std::uint32_t kSnapshotMagic = 0x31535346u;

/// Result of reading a durable store.
struct JournalScanReport {
  std::uint64_t records{0};
  std::uint64_t bytes{0};
  std::uint64_t last_sequence{0};
  std::uint64_t skipped_records{0};
  bool tail_truncated{false};
  std::uint64_t truncate_to_bytes{0};
};

/// Append-only, integrity checked, crash-safe durable store.
///
/// Ordering contract: append() returns only after the record is on stable
/// storage (when flush_to_storage is enabled). A caller may therefore treat a
/// successful append as durable, and must not acknowledge externally visible
/// progress before it.
class DurableStore {
 public:
  DurableStore(const DurableStore&) = delete;
  DurableStore& operator=(const DurableStore&) = delete;
  ~DurableStore();

  /// Open (creating if needed) a store rooted at the directory. Journals and
  /// snapshots live in that directory.
  [[nodiscard]] static Result<std::unique_ptr<DurableStore>> open(
      const std::string& directory, bool flush_to_storage);

  /// Append a record durably. Sequence numbers are assigned internally and are
  /// strictly monotone.
  [[nodiscard]] Status append(JournalRecordType type, std::string_view payload);

  /// Walk the journal in order. Records that fail integrity checks produce an
  /// error; a partial record at the tail is reported via tail_truncated.
  /// When repair is requested, a torn tail is truncated back to the last good
  /// record boundary so subsequent appends continue the sequence.
  [[nodiscard]] Status replay(bool repair, JournalScanReport& report,
                              const std::function<Status(const JournalRecordView&)>& visitor);

  /// Read a stored snapshot payload. Returns NotFound when absent.
  [[nodiscard]] Result<std::string> read_snapshot();
  /// Write a snapshot atomically: temp file, flush, rename over the target.
  [[nodiscard]] Status write_snapshot(std::string_view payload);

  /// Compact the journal: the caller supplies the snapshot payload and the
  /// journal is rotated away atomically after the snapshot is durable.
  [[nodiscard]] Status compact(std::string_view snapshot_payload);

  [[nodiscard]] std::uint64_t last_sequence() const noexcept;
  [[nodiscard]] const std::string& directory() const noexcept;
  /// True when a partial record was found at the tail of the journal when it
  /// was opened. The store does not repair it at open time: the crash artifact
  /// is reported to the caller, and the next append (or an explicit
  /// replay(repair=true)) truncates it back to the last good record boundary.
  [[nodiscard]] bool open_tail_truncated() const noexcept;

 private:
  DurableStore();
  struct State;
  std::unique_ptr<State> state_;
};

/// Build the canonical byte encoding of a record payload. Kept here so that
/// tests and tooling can construct and inspect records without linking against
/// the scheduler internals.
class RecordWriter {
 public:
  /// Default ceiling on a single encoder's buffer. Encoders that legitimately
  /// produce larger payloads (the state snapshot) pass an explicit limit.
  static constexpr std::size_t kDefaultLimit = 4u * 1024u * 1024u;

  RecordWriter() = default;
  explicit RecordWriter(std::size_t limit) noexcept : limit_(limit) {}

  void u8(std::uint8_t value);
  void u16(std::uint16_t value);
  void u32(std::uint32_t value);
  void u64(std::uint64_t value);
  void i64(std::int64_t value);
  void boolean(bool value);
  void bytes(std::string_view value);
  void text(std::string_view value);
  [[nodiscard]] const std::string& data() const noexcept { return buffer_; }
  [[nodiscard]] std::string take() { return std::move(buffer_); }
  /// True when a write was refused because the buffer limit was reached. The
  /// caller must treat an overflowed writer as a failure, never as a
  /// truncated-but-valid payload.
  [[nodiscard]] bool overflowed() const noexcept { return overflowed_; }

 private:
  std::string buffer_;
  std::size_t limit_{kDefaultLimit};
  bool overflowed_{false};
};

/// Bounds-checked reader. Every read validates remaining length and returns a
/// structured failure instead of reading past the end.
class RecordReader {
 public:
  explicit RecordReader(std::string_view data) noexcept : data_(data) {}

  [[nodiscard]] Result<std::uint8_t> u8();
  [[nodiscard]] Result<std::uint16_t> u16();
  [[nodiscard]] Result<std::uint32_t> u32();
  [[nodiscard]] Result<std::uint64_t> u64();
  [[nodiscard]] Result<std::int64_t> i64();
  [[nodiscard]] Result<bool> boolean();
  [[nodiscard]] Result<std::string_view> bytes();
  [[nodiscard]] Result<std::string_view> text();
  [[nodiscard]] std::size_t remaining() const noexcept { return data_.size() - offset_; }
  [[nodiscard]] bool exhausted() const noexcept { return offset_ == data_.size(); }

 private:
  [[nodiscard]] Result<std::string_view> raw(std::size_t count);
  std::string_view data_;
  std::size_t offset_{0};
};

}  // namespace flow_scheduler

#endif  // FLOW_SCHEDULER_JOURNAL_HPP

// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.

#include "flow_scheduler/journal.hpp"

#include <cstdio>
#include <cstring>
#include <filesystem>
#include <limits>
#include <memory>
#include <mutex>
#include <system_error>
#include <utility>

#if defined(_WIN32)
#include <io.h>
#else
#include <unistd.h>
#endif

#include "flow_scheduler/version.hpp"

namespace flow_scheduler {
namespace {

/// Hard ceiling on journal size before a compaction is required. Bounds
/// durable growth so a long running coordinator cannot fill a disk silently.
constexpr std::uint64_t kMaxJournalBytes = 512ull * 1024ull * 1024ull;

constexpr const char* kJournalFileName = "journal.log";
constexpr const char* kSnapshotFileName = "snapshot.bin";
constexpr const char* kSnapshotTempName = "snapshot.bin.tmp";

std::string join_path(const std::string& directory, const char* leaf) {
  std::filesystem::path path(directory);
  path /= leaf;
  return path.string();
}

Status io_failure(const char* operation, const std::string& path) {
  std::string detail(operation);
  detail += " failed for ";
  detail += path;
  return Status::failure(ErrorCode::IoFailure, std::move(detail));
}

Status flush_to_stable_storage(std::FILE* file, const std::string& path) {
  if (std::fflush(file) != 0) {
    return io_failure("fflush", path);
  }
#if defined(_WIN32)
  const int descriptor = _fileno(file);
  if (descriptor < 0) {
    return io_failure("_fileno", path);
  }
  if (_commit(descriptor) != 0) {
    return io_failure("_commit", path);
  }
#else
  const int descriptor = fileno(file);
  if (descriptor < 0) {
    return io_failure("fileno", path);
  }
  if (fsync(descriptor) != 0) {
    return io_failure("fsync", path);
  }
#endif
  return Status::success();
}

Status truncate_file(std::FILE* file, std::uint64_t size, const std::string& path) {
  if (std::fflush(file) != 0) {
    return io_failure("fflush", path);
  }
#if defined(_WIN32)
  const int descriptor = _fileno(file);
  if (descriptor < 0) {
    return io_failure("_fileno", path);
  }
  if (_chsize_s(descriptor, static_cast<long long>(size)) != 0) {
    return io_failure("_chsize_s", path);
  }
#else
  const int descriptor = fileno(file);
  if (descriptor < 0) {
    return io_failure("fileno", path);
  }
  if (ftruncate(descriptor, static_cast<off_t>(size)) != 0) {
    return io_failure("ftruncate", path);
  }
#endif
  return Status::success();
}

void encode_u32_le(char* out, std::uint32_t value) noexcept {
  out[0] = static_cast<char>(value & 0xFFu);
  out[1] = static_cast<char>((value >> 8) & 0xFFu);
  out[2] = static_cast<char>((value >> 16) & 0xFFu);
  out[3] = static_cast<char>((value >> 24) & 0xFFu);
}

std::uint32_t decode_u32_le(const char* in) noexcept {
  return static_cast<std::uint32_t>(static_cast<unsigned char>(in[0])) |
         (static_cast<std::uint32_t>(static_cast<unsigned char>(in[1])) << 8) |
         (static_cast<std::uint32_t>(static_cast<unsigned char>(in[2])) << 16) |
         (static_cast<std::uint32_t>(static_cast<unsigned char>(in[3])) << 24);
}

void encode_u64_le(char* out, std::uint64_t value) noexcept {
  for (int index = 0; index < 8; ++index) {
    out[index] = static_cast<char>((value >> (8 * index)) & 0xFFu);
  }
}

std::uint64_t decode_u64_le(const char* in) noexcept {
  std::uint64_t value = 0;
  for (int index = 0; index < 8; ++index) {
    value |= static_cast<std::uint64_t>(static_cast<unsigned char>(in[index])) << (8 * index);
  }
  return value;
}

void encode_u16_le(char* out, std::uint16_t value) noexcept {
  out[0] = static_cast<char>(value & 0xFFu);
  out[1] = static_cast<char>((value >> 8) & 0xFFu);
}

std::uint16_t decode_u16_le(const char* in) noexcept {
  return static_cast<std::uint16_t>(static_cast<unsigned char>(in[0])) |
         static_cast<std::uint16_t>(static_cast<unsigned char>(in[1]) << 8);
}

enum class ScanStop { Complete, TornTail, VisitorRejected };

struct ScanOutcome {
  Status status = Status::success();
  ScanStop stop = ScanStop::Complete;
  std::uint64_t records = 0;
  std::uint64_t valid_bytes = 0;
  std::uint64_t last_sequence = 0;
  std::uint64_t skipped = 0;
};

using RecordVisitor = std::function<Status(const JournalRecordView&)>;

ScanOutcome scan_journal(std::FILE* file, const std::string& path, bool stop_on_unknown,
                         const RecordVisitor& visitor) {
  ScanOutcome outcome;
  if (std::fseek(file, 0, SEEK_SET) != 0) {
    outcome.status = io_failure("fseek", path);
    return outcome;
  }
  std::string buffer;
  std::uint64_t expected_sequence = 1;
  for (;;) {
    char header[kJournalHeaderBytes];
    const std::size_t header_read = std::fread(header, 1, kJournalHeaderBytes, file);
    if (header_read == 0) {
      break;  // Clean end of journal.
    }
    if (header_read != kJournalHeaderBytes) {
      outcome.stop = ScanStop::TornTail;
      break;
    }
    const std::uint32_t magic = decode_u32_le(header);
    if (magic != kJournalRecordMagic) {
      outcome.status = Status::failure(ErrorCode::CorruptData,
                                       "journal record magic mismatch (corruption)");
      return outcome;
    }
    const std::uint16_t format = decode_u16_le(header + 4);
    if (format != kDurableFormatGeneration) {
      outcome.status = Status::failure(
          ErrorCode::Unsupported,
          std::string("journal format generation ") + std::to_string(format) +
              " is not supported by this build");
      return outcome;
    }
    const std::uint16_t type_value = decode_u16_le(header + 6);
    const std::uint64_t sequence = decode_u64_le(header + 8);
    const std::uint32_t payload_length = decode_u32_le(header + 16);
    const std::uint32_t header_crc = decode_u32_le(header + 20);
    if (header_crc != crc32c(header, 20)) {
      outcome.status = Status::failure(ErrorCode::CorruptData,
                                       "journal record header checksum mismatch");
      return outcome;
    }
    if (payload_length > kMaxJournalRecordPayload) {
      outcome.status = Status::failure(ErrorCode::Oversized,
                                       "journal record payload exceeds the configured bound");
      return outcome;
    }
    if (sequence != expected_sequence) {
      outcome.status = Status::failure(ErrorCode::SequenceGap,
                                       "journal sequence is not contiguous");
      return outcome;
    }
    buffer.resize(payload_length);
    if (payload_length > 0) {
      const std::size_t payload_read = std::fread(buffer.data(), 1, payload_length, file);
      if (payload_read != payload_length) {
        outcome.stop = ScanStop::TornTail;
        break;
      }
    }
    char trailer[kJournalTrailerBytes];
    const std::size_t trailer_read = std::fread(trailer, 1, kJournalTrailerBytes, file);
    if (trailer_read != kJournalTrailerBytes) {
      outcome.stop = ScanStop::TornTail;
      break;
    }
    const std::uint32_t payload_crc = decode_u32_le(trailer);
    if (payload_crc != crc32c(buffer.data(), buffer.size())) {
      outcome.status = Status::failure(ErrorCode::CorruptData,
                                       "journal record payload checksum mismatch");
      return outcome;
    }
    if (stop_on_unknown && type_value != 0 &&
        type_value > static_cast<std::uint16_t>(JournalRecordType::Checkpoint)) {
      outcome.status = Status::failure(ErrorCode::Unsupported,
                                       "journal record type is not supported by this build");
      return outcome;
    }
    JournalRecordView view;
    view.type = static_cast<JournalRecordType>(type_value);
    view.sequence = sequence;
    view.payload = buffer;
    if (visitor) {
      const Status visited = visitor(view);
      if (!visited.ok()) {
        outcome.status = visited;
        outcome.stop = ScanStop::VisitorRejected;
        return outcome;
      }
    }
    ++outcome.records;
    outcome.valid_bytes += static_cast<std::uint64_t>(journal_record_bytes(payload_length));
    outcome.last_sequence = sequence;
    expected_sequence = sequence + 1;
  }
  return outcome;
}

}  // namespace

const char* to_string(JournalRecordType type) noexcept {
  switch (type) {
    case JournalRecordType::Unknown: return "unknown";
    case JournalRecordType::PolicyInstalled: return "policy-installed";
    case JournalRecordType::ResourceRegistered: return "resource-registered";
    case JournalRecordType::ReservationRegistered: return "reservation-registered";
    case JournalRecordType::ReservationRetired: return "reservation-retired";
    case JournalRecordType::PriorityClassRegistered: return "priority-class-registered";
    case JournalRecordType::QoSClassRegistered: return "qos-class-registered";
    case JournalRecordType::EpochAdvanced: return "epoch-advanced";
    case JournalRecordType::FlowAdmitted: return "flow-admitted";
    case JournalRecordType::FlowUpdated: return "flow-updated";
    case JournalRecordType::FlowReadinessChanged: return "flow-readiness-changed";
    case JournalRecordType::FlowCancelled: return "flow-cancelled";
    case JournalRecordType::FlowRetired: return "flow-retired";
    case JournalRecordType::ScheduleIssued: return "schedule-issued";
    case JournalRecordType::ScheduleReleased: return "schedule-released";
    case JournalRecordType::AttemptOpened: return "attempt-opened";
    case JournalRecordType::AttemptStarted: return "attempt-started";
    case JournalRecordType::AttemptClosed: return "attempt-closed";
    case JournalRecordType::CompletionCommitted: return "completion-committed";
    case JournalRecordType::AmbiguityResolved: return "ambiguity-resolved";
    case JournalRecordType::Checkpoint: return "checkpoint";
  }
  return "unknown-record-type";
}

// ---------------------------------------------------------------------------
// RecordWriter
// ---------------------------------------------------------------------------

void RecordWriter::u8(std::uint8_t value) {
  if (overflowed_ || buffer_.size() + 1 > limit_) {
    overflowed_ = true;
    return;
  }
  buffer_.push_back(static_cast<char>(value));
}

void RecordWriter::u16(std::uint16_t value) {
  if (overflowed_ || buffer_.size() + 2 > limit_) {
    overflowed_ = true;
    return;
  }
  char raw[2];
  encode_u16_le(raw, value);
  buffer_.append(raw, 2);
}

void RecordWriter::u32(std::uint32_t value) {
  if (overflowed_ || buffer_.size() + 4 > limit_) {
    overflowed_ = true;
    return;
  }
  char raw[4];
  encode_u32_le(raw, value);
  buffer_.append(raw, 4);
}

void RecordWriter::u64(std::uint64_t value) {
  if (overflowed_ || buffer_.size() + 8 > limit_) {
    overflowed_ = true;
    return;
  }
  char raw[8];
  encode_u64_le(raw, value);
  buffer_.append(raw, 8);
}

void RecordWriter::i64(std::int64_t value) {
  u64(static_cast<std::uint64_t>(value));
}

void RecordWriter::boolean(bool value) { u8(value ? 1u : 0u); }

void RecordWriter::bytes(std::string_view value) {
  if (overflowed_ || value.size() > limit_ || buffer_.size() + 8 + value.size() > limit_) {
    overflowed_ = true;
    return;
  }
  u64(value.size());
  buffer_.append(value.data(), value.size());
}

void RecordWriter::text(std::string_view value) { bytes(value); }

// ---------------------------------------------------------------------------
// RecordReader
// ---------------------------------------------------------------------------

Result<std::string_view> RecordReader::raw(std::size_t count) {
  if (count > remaining()) {
    return Error(ErrorCode::Truncated, "record payload ended before the declared field");
  }
  const std::string_view view = data_.substr(offset_, count);
  offset_ += count;
  return view;
}

Result<std::uint8_t> RecordReader::u8() {
  std::string_view view;
  FS_TRY_ASSIGN(view, raw(1));
  return static_cast<std::uint8_t>(static_cast<unsigned char>(view[0]));
}

Result<std::uint16_t> RecordReader::u16() {
  std::string_view view;
  FS_TRY_ASSIGN(view, raw(2));
  return decode_u16_le(view.data());
}

Result<std::uint32_t> RecordReader::u32() {
  std::string_view view;
  FS_TRY_ASSIGN(view, raw(4));
  return decode_u32_le(view.data());
}

Result<std::uint64_t> RecordReader::u64() {
  std::string_view view;
  FS_TRY_ASSIGN(view, raw(8));
  return decode_u64_le(view.data());
}

Result<std::int64_t> RecordReader::i64() {
  std::uint64_t value = 0;
  FS_TRY_ASSIGN(value, u64());
  return static_cast<std::int64_t>(value);
}

Result<bool> RecordReader::boolean() {
  std::uint8_t value = 0;
  FS_TRY_ASSIGN(value, u8());
  if (value > 1u) {
    return Error(ErrorCode::InvalidArgument, "boolean field is not 0 or 1");
  }
  return value == 1u;
}

Result<std::string_view> RecordReader::bytes() {
  std::uint64_t length = 0;
  FS_TRY_ASSIGN(length, u64());
  if (length > static_cast<std::uint64_t>(remaining())) {
    return Error(ErrorCode::Truncated, "declared byte field exceeds the remaining payload");
  }
  return raw(static_cast<std::size_t>(length));
}

Result<std::string_view> RecordReader::text() { return bytes(); }

// ---------------------------------------------------------------------------
// DurableStore
// ---------------------------------------------------------------------------

struct DurableStore::State {
  std::string directory;
  std::string journal_path;
  std::string snapshot_path;
  std::string snapshot_temp_path;
  std::FILE* journal{nullptr};
  bool flush_to_storage{true};
  std::uint64_t last_sequence{0};
  std::uint64_t valid_bytes{0};
  bool open_tail_truncated{false};
  bool torn_tail_pending{false};

  ~State() {
    if (journal != nullptr) {
      std::fclose(journal);
      journal = nullptr;
    }
  }
};

namespace {

constexpr std::size_t kSnapshotHeaderBytes = 20;

std::string build_journal_record(JournalRecordType type, std::uint64_t sequence,
                                 std::string_view payload) {
  std::string record;
  record.resize(kJournalHeaderBytes);
  encode_u32_le(&record[0], kJournalRecordMagic);
  encode_u16_le(&record[4], kDurableFormatGeneration);
  encode_u16_le(&record[6], static_cast<std::uint16_t>(type));
  encode_u64_le(&record[8], sequence);
  encode_u32_le(&record[16], static_cast<std::uint32_t>(payload.size()));
  encode_u32_le(&record[20], crc32c(record.data(), 20));
  record.append(payload.data(), payload.size());
  char trailer[kJournalTrailerBytes];
  encode_u32_le(trailer, crc32c(payload.data(), payload.size()));
  record.append(trailer, kJournalTrailerBytes);
  return record;
}

std::string build_snapshot_blob(std::string_view payload) {
  std::string blob;
  blob.resize(kSnapshotHeaderBytes);
  encode_u32_le(&blob[0], kSnapshotMagic);
  encode_u16_le(&blob[4], kDurableFormatGeneration);
  encode_u16_le(&blob[6], 0);
  encode_u32_le(&blob[8], static_cast<std::uint32_t>(payload.size()));
  encode_u32_le(&blob[12], crc32c(payload.data(), payload.size()));
  encode_u32_le(&blob[16], crc32c(blob.data(), 16));
  blob.append(payload.data(), payload.size());
  return blob;
}

}  // namespace

DurableStore::DurableStore() : state_(std::make_unique<State>()) {}

DurableStore::~DurableStore() = default;

Result<std::unique_ptr<DurableStore>> DurableStore::open(const std::string& directory,
                                                         bool flush_to_storage) {
  if (directory.empty()) {
    return Error(ErrorCode::InvalidArgument, "durable store requires a non-empty directory");
  }
  std::error_code error;
  std::filesystem::create_directories(std::filesystem::path(directory), error);
  if (error) {
    return Error(ErrorCode::IoFailure, "cannot create state directory: " + error.message());
  }

  auto store = std::unique_ptr<DurableStore>(new DurableStore());
  store->state_->directory = directory;
  store->state_->journal_path = join_path(directory, kJournalFileName);
  store->state_->snapshot_path = join_path(directory, kSnapshotFileName);
  store->state_->snapshot_temp_path = join_path(directory, kSnapshotTempName);
  store->state_->flush_to_storage = flush_to_storage;

#if defined(_WIN32)
  if (fopen_s(&store->state_->journal, store->state_->journal_path.c_str(), "ab+") != 0) {
    store->state_->journal = nullptr;
  }
#else
  store->state_->journal = std::fopen(store->state_->journal_path.c_str(), "ab+");
#endif
  if (store->state_->journal == nullptr) {
    return Error(ErrorCode::IoFailure, "cannot open journal file: " + store->state_->journal_path);
  }

  // Integrity scan. A torn tail is repaired in place so that subsequent
  // appends continue the sequence; anything else is corruption and refuses to
  // open rather than silently discarding durable history.
  const ScanOutcome outcome =
      scan_journal(store->state_->journal, store->state_->journal_path, false, RecordVisitor{});
  if (!outcome.status.ok()) {
    return outcome.status.error();
  }
  // The torn tail is deliberately left in place: it is the evidence of the
  // crash, it is reported through open_tail_truncated()/replay(), and it is
  // truncated lazily before the next append so the sequence stays contiguous.
  store->state_->open_tail_truncated = outcome.stop == ScanStop::TornTail;
  store->state_->torn_tail_pending = store->state_->open_tail_truncated;
  store->state_->valid_bytes = outcome.valid_bytes;
  store->state_->last_sequence = outcome.last_sequence;
  if (std::fseek(store->state_->journal, 0, SEEK_END) != 0) {
    return Error(ErrorCode::IoFailure, "cannot seek journal to end");
  }
  return store;
}

Status DurableStore::append(JournalRecordType type, std::string_view payload) {
  if (type == JournalRecordType::Unknown) {
    return Status::failure(ErrorCode::InvalidArgument, "cannot append an unknown record type");
  }
  if (payload.size() > kMaxJournalRecordPayload) {
    return Status::failure(ErrorCode::Oversized, "record payload exceeds the configured bound");
  }
  if (state_->last_sequence == std::numeric_limits<std::uint64_t>::max()) {
    return Status::failure(ErrorCode::ArithmeticOverflow, "journal sequence exhausted");
  }
  const std::uint64_t record_bytes =
      static_cast<std::uint64_t>(journal_record_bytes(payload.size()));
  if (state_->valid_bytes > kMaxJournalBytes ||
      record_bytes > kMaxJournalBytes - state_->valid_bytes) {
    return Status::failure(ErrorCode::Bounded,
                           "journal size bound reached; a checkpoint is required");
  }
  if (state_->torn_tail_pending) {
    // Appending after a torn tail would leave a hole in the sequence, so the
    // partial record is discarded first.
    FS_RETURN_IF_ERROR(truncate_file(state_->journal, state_->valid_bytes,
                                     state_->journal_path));
    FS_RETURN_IF_ERROR(flush_to_stable_storage(state_->journal, state_->journal_path));
    state_->torn_tail_pending = false;
  }
  const std::uint64_t sequence = state_->last_sequence + 1;
  const std::string record = build_journal_record(type, sequence, payload);
  if (std::fseek(state_->journal, 0, SEEK_END) != 0) {
    return io_failure("fseek", state_->journal_path);
  }
  if (std::fwrite(record.data(), 1, record.size(), state_->journal) != record.size()) {
    return io_failure("fwrite", state_->journal_path);
  }
  // Durable before the caller is told the mutation happened.
  if (state_->flush_to_storage) {
    FS_RETURN_IF_ERROR(flush_to_stable_storage(state_->journal, state_->journal_path));
  } else {
    if (std::fflush(state_->journal) != 0) {
      return io_failure("fflush", state_->journal_path);
    }
  }
  state_->last_sequence = sequence;
  state_->valid_bytes += record_bytes;
  return Status::success();
}

Status DurableStore::replay(bool repair, JournalScanReport& report,
                            const std::function<Status(const JournalRecordView&)>& visitor) {
  const ScanOutcome outcome = scan_journal(state_->journal, state_->journal_path, true, visitor);
  if (!outcome.status.ok()) {
    return outcome.status;
  }
  report.records = outcome.records;
  report.bytes = outcome.valid_bytes;
  report.last_sequence = outcome.last_sequence;
  report.skipped_records = outcome.skipped;
  report.tail_truncated = outcome.stop == ScanStop::TornTail;
  report.truncate_to_bytes = outcome.valid_bytes;
  if (outcome.stop == ScanStop::TornTail && repair) {
    FS_RETURN_IF_ERROR(
        truncate_file(state_->journal, outcome.valid_bytes, state_->journal_path));
    FS_RETURN_IF_ERROR(
        flush_to_stable_storage(state_->journal, state_->journal_path));
    state_->valid_bytes = outcome.valid_bytes;
    state_->last_sequence = outcome.last_sequence;
    state_->torn_tail_pending = false;
  }
  if (std::fseek(state_->journal, 0, SEEK_END) != 0) {
    return io_failure("fseek", state_->journal_path);
  }
  return Status::success();
}

Result<std::string> DurableStore::read_snapshot() {
  std::error_code error;
  if (!std::filesystem::exists(std::filesystem::path(state_->snapshot_path), error) || error) {
    return Error(ErrorCode::NotFound, "no snapshot present");
  }
  const auto size = std::filesystem::file_size(std::filesystem::path(state_->snapshot_path), error);
  if (error) {
    return Error(ErrorCode::IoFailure, "cannot size snapshot: " + error.message());
  }
  const std::uint64_t max_blob = kMaxSnapshotPayload + kSnapshotHeaderBytes;
  if (size < kSnapshotHeaderBytes || size > max_blob) {
    return Error(ErrorCode::CorruptData, "snapshot size is outside the permitted range");
  }
  std::string blob;
  blob.resize(static_cast<std::size_t>(size));
  std::FILE* file = nullptr;
#if defined(_WIN32)
  if (fopen_s(&file, state_->snapshot_path.c_str(), "rb") != 0) {
    file = nullptr;
  }
#else
  file = std::fopen(state_->snapshot_path.c_str(), "rb");
#endif
  if (file == nullptr) {
    return Error(ErrorCode::IoFailure, "cannot open snapshot file");
  }
  const std::size_t read = std::fread(blob.data(), 1, blob.size(), file);
  std::fclose(file);
  if (read != blob.size()) {
    return Error(ErrorCode::Truncated, "snapshot file is shorter than its declared size");
  }
  if (decode_u32_le(blob.data()) != kSnapshotMagic) {
    return Error(ErrorCode::CorruptData, "snapshot magic mismatch");
  }
  if (decode_u16_le(blob.data() + 4) != kDurableFormatGeneration) {
    return Error(ErrorCode::Unsupported, "snapshot format generation is not supported");
  }
  if (decode_u32_le(blob.data() + 16) != crc32c(blob.data(), 16)) {
    return Error(ErrorCode::CorruptData, "snapshot header checksum mismatch");
  }
  const std::uint32_t payload_length = decode_u32_le(blob.data() + 8);
  if (payload_length != blob.size() - kSnapshotHeaderBytes) {
    return Error(ErrorCode::CorruptData, "snapshot payload length disagrees with the file size");
  }
  const std::string_view payload(blob.data() + kSnapshotHeaderBytes, payload_length);
  if (decode_u32_le(blob.data() + 12) != crc32c(payload.data(), payload.size())) {
    return Error(ErrorCode::CorruptData, "snapshot payload checksum mismatch");
  }
  return std::string(payload);
}

Status DurableStore::write_snapshot(std::string_view payload) {
  if (payload.size() > kMaxSnapshotPayload) {
    return Status::failure(ErrorCode::Oversized, "snapshot payload exceeds the configured bound");
  }
  const std::string blob = build_snapshot_blob(payload);
  std::FILE* file = nullptr;
#if defined(_WIN32)
  if (fopen_s(&file, state_->snapshot_temp_path.c_str(), "wb") != 0) {
    file = nullptr;
  }
#else
  file = std::fopen(state_->snapshot_temp_path.c_str(), "wb");
#endif
  if (file == nullptr) {
    return io_failure("fopen", state_->snapshot_temp_path);
  }
  const bool wrote_all = std::fwrite(blob.data(), 1, blob.size(), file) == blob.size();
  Status flush_status = wrote_all ? flush_to_stable_storage(file, state_->snapshot_temp_path)
                                  : io_failure("fwrite", state_->snapshot_temp_path);
  std::fclose(file);
  if (!flush_status.ok()) {
    std::error_code ignored;
    std::filesystem::remove(std::filesystem::path(state_->snapshot_temp_path), ignored);
    return flush_status;
  }
  // Atomic replacement: readers either observe the previous snapshot or the
  // new one, never a partially written file.
  std::error_code error;
  std::filesystem::rename(std::filesystem::path(state_->snapshot_temp_path),
                          std::filesystem::path(state_->snapshot_path), error);
  if (error) {
    std::error_code ignored;
    std::filesystem::remove(std::filesystem::path(state_->snapshot_temp_path), ignored);
    return Status::failure(ErrorCode::IoFailure,
                           "cannot replace snapshot: " + error.message());
  }
  return Status::success();
}

Status DurableStore::compact(std::string_view snapshot_payload) {
  FS_RETURN_IF_ERROR(write_snapshot(snapshot_payload));
  // The snapshot is durable before the journal is discarded. A crash between
  // the two leaves the journal intact, which merely replays the same compacted
  // history on top of the snapshot.
  FS_RETURN_IF_ERROR(truncate_file(state_->journal, 0, state_->journal_path));
  FS_RETURN_IF_ERROR(flush_to_stable_storage(state_->journal, state_->journal_path));
  if (std::fseek(state_->journal, 0, SEEK_END) != 0) {
    return io_failure("fseek", state_->journal_path);
  }
  state_->last_sequence = 0;
  state_->valid_bytes = 0;
  state_->torn_tail_pending = false;
  return Status::success();
}

std::uint64_t DurableStore::last_sequence() const noexcept { return state_->last_sequence; }

const std::string& DurableStore::directory() const noexcept { return state_->directory; }

bool DurableStore::open_tail_truncated() const noexcept { return state_->open_tail_truncated; }

}  // namespace flow_scheduler


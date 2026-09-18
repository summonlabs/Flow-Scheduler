// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.
//
// Real process, real socket, real durable state tests.
//
// Nothing here is simulated: the coordinator and the workers are OS processes
// started from the tool paths the test target defines, they talk over TCP
// loopback, and the durability assertions are made against files that a crashed
// process left behind. There are deliberately no timeouts on any child: a hung
// child hangs this test, which is the intended way to notice it.

#include <windows.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

#include "flow_scheduler/journal.hpp"
#include "flow_scheduler/transport.hpp"
#include "test_framework.hpp"

namespace {

namespace fs = std::filesystem;

/// Command lines are assembled by hand, so every path must be quoted: the
/// scratch directory under the user's temporary directory routinely contains a
/// space, and an unquoted path would silently become two arguments.
std::string quote_argument(std::string_view value) {
  std::string quoted;
  quoted.reserve(value.size() + 2);
  quoted.push_back('"');
  for (const char character : value) {
    if (character == '"') {
      quoted.push_back('\\');
    }
    quoted.push_back(character);
  }
  // A trailing backslash would escape the closing quote.
  if (!value.empty() && value.back() == '\\') {
    quoted.push_back('\\');
  }
  quoted.push_back('"');
  return quoted;
}

std::string join_command(const std::vector<std::string>& arguments) {
  std::string command;
  for (const std::string& argument : arguments) {
    if (!command.empty()) {
      command.push_back(' ');
    }
    command += quote_argument(argument);
  }
  return command;
}

struct Child {
  HANDLE process{nullptr};
  fs::path stdout_path{};
  fs::path stderr_path{};
  std::string command{};
  DWORD exit_code{0};
  bool reaped{false};
};

std::uint64_t& tag_counter() {
  static std::uint64_t counter = 0;
  return counter;
}

/// Start an OS process with stdout and stderr captured into files.
Child spawn_child(const std::vector<std::string>& arguments, const fs::path& log_directory,
                  const std::string& tag) {
  Child child;
  child.command = join_command(arguments);
  child.stdout_path = log_directory / (tag + ".out");
  child.stderr_path = log_directory / (tag + ".err");

  SECURITY_ATTRIBUTES attributes{};
  attributes.nLength = static_cast<DWORD>(sizeof(SECURITY_ATTRIBUTES));
  attributes.bInheritHandle = TRUE;
  const HANDLE out = CreateFileA(child.stdout_path.string().c_str(), GENERIC_WRITE,
                                 FILE_SHARE_READ | FILE_SHARE_WRITE, &attributes, CREATE_ALWAYS,
                                 FILE_ATTRIBUTE_NORMAL, nullptr);
  REQUIRE(out != INVALID_HANDLE_VALUE);
  const HANDLE err = CreateFileA(child.stderr_path.string().c_str(), GENERIC_WRITE,
                                 FILE_SHARE_READ | FILE_SHARE_WRITE, &attributes, CREATE_ALWAYS,
                                 FILE_ATTRIBUTE_NORMAL, nullptr);
  REQUIRE(err != INVALID_HANDLE_VALUE);

  STARTUPINFOA startup{};
  startup.cb = static_cast<DWORD>(sizeof(STARTUPINFOA));
  startup.dwFlags = STARTF_USESTDHANDLES;
  startup.hStdOutput = out;
  startup.hStdError = err;
  startup.hStdInput = GetStdHandle(STD_INPUT_HANDLE);

  std::vector<char> command_line(child.command.begin(), child.command.end());
  command_line.push_back('\0');
  PROCESS_INFORMATION information{};
  const BOOL started = CreateProcessA(nullptr, command_line.data(), nullptr, nullptr, TRUE,
                                      CREATE_NO_WINDOW, nullptr, nullptr, &startup, &information);
  CloseHandle(out);
  CloseHandle(err);
  REQUIRE(started != FALSE);
  CloseHandle(information.hThread);
  child.process = information.hProcess;
  return child;
}

/// Wait forever for the child to exit; there is no timeout by design.
DWORD wait_for_child(Child& child) {
  if (child.reaped) {
    return child.exit_code;
  }
  REQUIRE(child.process != nullptr);
  const DWORD waited = WaitForSingleObject(child.process, INFINITE);
  REQUIRE(waited == WAIT_OBJECT_0);
  DWORD code = 0;
  REQUIRE(GetExitCodeProcess(child.process, &code) != FALSE);
  CloseHandle(child.process);
  child.process = nullptr;
  child.exit_code = code;
  child.reaped = true;
  return code;
}

std::string read_text(const fs::path& file) {
  std::ifstream stream(file, std::ios::binary);
  if (!stream) {
    return std::string();
  }
  stream.seekg(0, std::ios::end);
  const std::streamoff size = stream.tellg();
  stream.seekg(0, std::ios::beg);
  std::string text;
  if (size > 0) {
    text.resize(static_cast<std::size_t>(size));
    stream.read(text.data(), size);
    text.resize(static_cast<std::size_t>(stream.gcount()));
  }
  return text;
}

fs::path make_workspace(const std::string& tag) {
  std::error_code error;
  const fs::path base = fs::temp_directory_path(error);
  REQUIRE(!error);
  const fs::path root = base / ("flow_scheduler_mp_" + tag + "_" +
                                std::to_string(static_cast<unsigned long>(GetCurrentProcessId())) +
                                "_" + std::to_string(tag_counter()++));
  fs::remove_all(root, error);
  error.clear();
  const bool created = fs::create_directories(root, error);
  REQUIRE(created || !error);
  return root;
}

void remove_workspace(const fs::path& root) {
  std::error_code error;
  fs::remove_all(root, error);
}

/// Reserve a loopback port by binding it and letting go again. A fixed port
/// cannot be read from the coordinator's stdout while the test is waiting for
/// it to exit, and the coordinator prints an ephemeral port only after it has
/// already bound one.
std::uint16_t pick_free_port() {
  flow_scheduler::Result<flow_scheduler::Listener> listener =
      flow_scheduler::Listener::bind("127.0.0.1", 0);
  REQUIRE(listener.ok());
  const std::uint16_t port = listener.value().port();
  listener.value().close();
  REQUIRE(port != 0);
  return port;
}

const char* coordinator_path() { return FLOW_SCHEDULER_COORDINATOR_PATH; }
const char* worker_path() { return FLOW_SCHEDULER_WORKER_PATH; }
const char* inspect_path() { return FLOW_SCHEDULER_INSPECT_PATH; }

/// The run summary the coordinator prints as its last line.
struct Summary {
  std::uint64_t completed{0};
  std::uint64_t rounds{0};
  std::uint64_t epoch{0};
  std::uint64_t dispatches{0};
};

std::optional<std::string> find_line(const std::string& text, std::string_view prefix) {
  std::size_t start = 0;
  while (start <= text.size()) {
    const std::size_t end = text.find('\n', start);
    const std::string_view line(text.data() + start,
                                (end == std::string::npos ? text.size() : end) - start);
    if (line.rfind(prefix, 0) == 0) {
      return std::string(line);
    }
    if (end == std::string::npos) {
      break;
    }
    start = end + 1;
  }
  return std::nullopt;
}

std::optional<std::uint64_t> number_field(std::string_view line, std::string_view key) {
  const std::size_t at = line.find(key);
  if (at == std::string_view::npos) {
    return std::nullopt;
  }
  std::size_t index = at + key.size();
  std::uint64_t value = 0;
  bool any = false;
  while (index < line.size() && line[index] >= '0' && line[index] <= '9') {
    value = value * 10u + static_cast<std::uint64_t>(line[index] - '0');
    ++index;
    any = true;
  }
  if (!any) {
    return std::nullopt;
  }
  return value;
}

Summary require_summary(const std::string& output) {
  const std::optional<std::string> line = find_line(output, "SUMMARY ");
  REQUIRE(line.has_value());
  const std::optional<std::uint64_t> completed = number_field(*line, "completed=");
  const std::optional<std::uint64_t> rounds = number_field(*line, "rounds=");
  const std::optional<std::uint64_t> epoch = number_field(*line, "epoch=");
  const std::optional<std::uint64_t> dispatches = number_field(*line, "dispatches=");
  REQUIRE(completed.has_value());
  REQUIRE(rounds.has_value());
  REQUIRE(epoch.has_value());
  REQUIRE(dispatches.has_value());
  Summary summary;
  summary.completed = *completed;
  summary.rounds = *rounds;
  summary.epoch = *epoch;
  summary.dispatches = *dispatches;
  return summary;
}

struct Inspection {
  std::uint64_t records{0};
  std::uint64_t bytes{0};
  std::uint64_t last{0};
  bool snapshot{false};
  bool torn_tail{false};
};

Inspection require_inspection(const std::string& output) {
  const std::optional<std::string> line = find_line(output, "INSPECT ");
  REQUIRE(line.has_value());
  const std::optional<std::uint64_t> records = number_field(*line, "records=");
  const std::optional<std::uint64_t> bytes = number_field(*line, "bytes=");
  const std::optional<std::uint64_t> last = number_field(*line, "last=");
  REQUIRE(records.has_value());
  REQUIRE(bytes.has_value());
  REQUIRE(last.has_value());
  Inspection inspection;
  inspection.records = *records;
  inspection.bytes = *bytes;
  inspection.last = *last;
  inspection.snapshot = line->find("snapshot=yes") != std::string::npos;
  inspection.torn_tail = line->find("torn_tail=yes") != std::string::npos;
  return inspection;
}

std::optional<fs::path> find_journal(const fs::path& directory) {
  std::error_code error;
  for (const fs::directory_entry& entry : fs::directory_iterator(directory, error)) {
    if (error) {
      return std::nullopt;
    }
    if (!entry.is_regular_file(error) || error) {
      continue;
    }
    std::ifstream stream(entry.path(), std::ios::binary);
    if (!stream) {
      continue;
    }
    char header[4] = {0, 0, 0, 0};
    stream.read(header, 4);
    if (stream.gcount() != 4) {
      continue;
    }
    const std::uint32_t magic =
        static_cast<std::uint32_t>(static_cast<unsigned char>(header[0])) |
        (static_cast<std::uint32_t>(static_cast<unsigned char>(header[1])) << 8) |
        (static_cast<std::uint32_t>(static_cast<unsigned char>(header[2])) << 16) |
        (static_cast<std::uint32_t>(static_cast<unsigned char>(header[3])) << 24);
    if (magic == flow_scheduler::kJournalRecordMagic) {
      return entry.path();
    }
  }
  return std::nullopt;
}

/// Append bytes without going through any library writer: this is a torn write,
/// exactly what a crash in the middle of an append leaves behind.
void append_bytes(const fs::path& file, std::string_view bytes) {
  std::ofstream stream(file, std::ios::binary | std::ios::app);
  REQUIRE(stream.good());
  stream.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
  REQUIRE(stream.good());
}

void overwrite_byte(const fs::path& file, std::streamoff offset, char value) {
  std::fstream stream(file, std::ios::binary | std::ios::in | std::ios::out);
  REQUIRE(stream.good());
  stream.seekp(offset);
  stream.put(value);
  REQUIRE(stream.good());
}

std::uintmax_t size_of(const fs::path& file) {
  std::error_code error;
  const std::uintmax_t size = fs::file_size(file, error);
  REQUIRE(!error);
  return size;
}

/// Run a command to completion and return the finished child.
Child run_to_completion(const std::vector<std::string>& arguments, const fs::path& log_directory,
                        const std::string& tag) {
  Child child = spawn_child(arguments, log_directory, tag);
  static_cast<void>(wait_for_child(child));
  return child;
}

}  // namespace

// A coordinator and two real worker processes complete a synthetic workload
// over TCP loopback. The SUMMARY line must report committed flows and
// dispatches, the coordinator must exit 0, and both workers must observe the
// shutdown frame.
FLOW_TEST(multiprocess, coordinator_with_two_real_workers_completes_workload) {
  const fs::path root = make_workspace("cluster");
  const fs::path logs = root / "logs";
  std::error_code error;
  REQUIRE(fs::create_directories(logs, error) || !error);
  const std::uint16_t port = pick_free_port();
  const std::string port_text = std::to_string(port);

  Child coordinator = spawn_child({coordinator_path(), "--port", port_text, "--workers", "2",
                                   "--flows", "24", "--rounds", "2048", "--seed", "7"},
                                  logs, "coordinator");
  Child first = spawn_child({worker_path(), "--port", port_text, "--worker", "1", "--boot", "1",
                             "--label", "worker-1"},
                            logs, "worker-1");
  Child second = spawn_child({worker_path(), "--port", port_text, "--worker", "2", "--boot", "1",
                              "--label", "worker-2"},
                             logs, "worker-2");

  const DWORD coordinator_code = wait_for_child(coordinator);
  const std::string output = read_text(coordinator.stdout_path);
  CHECK_EQ(coordinator_code, static_cast<DWORD>(0));
  const Summary summary = require_summary(output);
  CHECK(summary.completed > 0);
  CHECK(summary.dispatches > 0);
  CHECK(summary.rounds > 0);
  CHECK(summary.epoch >= 1);

  CHECK_EQ(wait_for_child(first), static_cast<DWORD>(0));
  CHECK_EQ(wait_for_child(second), static_cast<DWORD>(0));
  CHECK(read_text(first.stdout_path).find("BYE") != std::string::npos);
  CHECK(read_text(second.stdout_path).find("BYE") != std::string::npos);

  remove_workspace(root);
}

// A coordinator killed in the middle of a run leaves durable state behind; the
// next incarnation recovers it and the fabric epoch strictly advances. The
// epoch of the killed run is read back from the durable journal with the
// inspector, so the comparison is made against what the crashed process really
// wrote, not against a value this test remembered.
FLOW_TEST(multiprocess, killed_coordinator_restart_advances_epoch) {
  const fs::path root = make_workspace("restart");
  const fs::path logs = root / "logs";
  const fs::path state = root / "state";
  std::error_code error;
  REQUIRE(fs::create_directories(logs, error) || !error);

  // --- run A: crash abruptly after three committed completions.
  const std::uint16_t first_port = pick_free_port();
  const std::string first_port_text = std::to_string(first_port);
  Child crashing = spawn_child({coordinator_path(), "--port", first_port_text, "--state",
                                state.string(), "--workers", "2", "--flows", "24", "--rounds",
                                "2048", "--seed", "3", "--exit-after", "3"},
                               logs, "crash-coordinator");
  Child first_worker = spawn_child({worker_path(), "--port", first_port_text, "--worker", "1",
                                    "--boot", "1", "--label", "worker-1"},
                                   logs, "crash-worker-1");
  Child second_worker = spawn_child({worker_path(), "--port", first_port_text, "--worker", "2",
                                     "--boot", "1", "--label", "worker-2"},
                                    logs, "crash-worker-2");

  CHECK_EQ(wait_for_child(crashing), static_cast<DWORD>(0));
  const std::string crashed_output = read_text(crashing.stdout_path);
  CHECK(crashed_output.find("CRASH 3") != std::string::npos);
  // The workers notice the death (end of stream, or a send into a closed
  // socket) and never report a clean shutdown, because none was sent.
  CHECK(wait_for_child(first_worker) != static_cast<DWORD>(0));
  CHECK(wait_for_child(second_worker) != static_cast<DWORD>(0));

  // --- run A's durable epoch, read back from the journal it wrote.
  Child inspection = run_to_completion({inspect_path(), "--state", state.string(), "--dump"}, logs,
                                       "inspect-after-crash");
  CHECK_EQ(inspection.exit_code, static_cast<DWORD>(0));
  const std::string dump = read_text(inspection.stdout_path);
  const std::optional<std::string> epoch_line = find_line(dump, "EPOCH-CURRENT ");
  REQUIRE(epoch_line.has_value());
  const std::optional<std::uint64_t> crashed_epoch = number_field(*epoch_line, "EPOCH-CURRENT ");
  REQUIRE(crashed_epoch.has_value());

  // --- run B: restart against the same state directory.
  const std::uint16_t second_port = pick_free_port();
  const std::string second_port_text = std::to_string(second_port);
  Child restarted = spawn_child({coordinator_path(), "--port", second_port_text, "--state",
                                 state.string(), "--workers", "2", "--flows", "24", "--rounds",
                                 "4096", "--seed", "3"},
                                logs, "restart-coordinator");
  Child third_worker = spawn_child({worker_path(), "--port", second_port_text, "--worker", "1",
                                    "--boot", "1", "--label", "worker-1"},
                                   logs, "restart-worker-1");
  Child fourth_worker = spawn_child({worker_path(), "--port", second_port_text, "--worker", "2",
                                     "--boot", "1", "--label", "worker-2"},
                                    logs, "restart-worker-2");

  CHECK_EQ(wait_for_child(restarted), static_cast<DWORD>(0));
  const Summary restarted_summary = require_summary(read_text(restarted.stdout_path));
  // The recovered incarnation opens the fabric with a new epoch: strictly
  // greater than the epoch the killed run was arbitrating under.
  CHECK(restarted_summary.epoch > *crashed_epoch);
  // Recovery advances the epoch exactly once per incarnation, so a restart that
  // silently ignored the durable state (or double-advanced) would show here.
  CHECK_EQ(restarted_summary.epoch, *crashed_epoch + 1u);
  // The recovered population is still accounted for, and the restarted
  // coordinator issues new dispatch frames instead of merely reporting restored
  // completions.
  CHECK(restarted_summary.completed > 0);
  CHECK(restarted_summary.dispatches > 0);
  CHECK_EQ(wait_for_child(third_worker), static_cast<DWORD>(0));
  CHECK_EQ(wait_for_child(fourth_worker), static_cast<DWORD>(0));

  remove_workspace(root);
}

// Worker death is survivable. --kill-after closes worker 0's socket in the
// middle of a durable run to simulate the process disappearing: the coordinator
// must not crash, it must retire the peer, abandon the attempts that peer owned
// instead of silently crediting them, and keep committing work on the survivor.
FLOW_TEST(multiprocess, killed_worker_is_retired_and_the_run_continues) {
  const fs::path root = make_workspace("kill");
  const fs::path logs = root / "logs";
  const fs::path state = root / "state";
  std::error_code error;
  REQUIRE(fs::create_directories(logs, error) || !error);

  const std::uint16_t port = pick_free_port();
  const std::string port_text = std::to_string(port);
  Child coordinator = spawn_child({coordinator_path(), "--port", port_text, "--state",
                                   state.string(), "--workers", "2", "--flows", "16", "--rounds",
                                   "2048", "--seed", "11", "--kill-after", "2"},
                                  logs, "coordinator");
  Child first = spawn_child({worker_path(), "--port", port_text, "--worker", "1", "--boot", "1"},
                            logs, "worker-1");
  Child second = spawn_child({worker_path(), "--port", port_text, "--worker", "2", "--boot", "1"},
                             logs, "worker-2");

  CHECK_EQ(wait_for_child(coordinator), static_cast<DWORD>(0));
  const Summary summary = require_summary(read_text(coordinator.stdout_path));
  CHECK(summary.completed > 0);
  CHECK(summary.dispatches > 0);
  // The retired peer never receives a shutdown frame, so exactly one worker
  // reports a clean shutdown and the other notices the loss.
  const DWORD first_code = wait_for_child(first);
  const DWORD second_code = wait_for_child(second);
  CHECK((first_code == static_cast<DWORD>(0)) != (second_code == static_cast<DWORD>(0)));
  const std::string diagnostics = read_text(coordinator.stderr_path);
  CHECK(diagnostics.find("simulated worker death") != std::string::npos);
  CHECK(diagnostics.find("retired") != std::string::npos);

  remove_workspace(root);
}

// The inspector reports exactly what the durable bytes say. DurableStore::open()
// reports a partial record at the tail through open_tail_truncated() and leaves
// the bytes in place, so inspecting a directory in the default mode modifies
// nothing; --no-verify is the only mode that writes, and it truncates the
// partial record back to the last good record boundary.
//
// Implemented behaviour, asserted precisely below:
//   * a clean store         -> exit 0, records > 0, torn_tail=no
//   * 5 appended bytes      -> --verify reports torn_tail=yes, exits 0 and
//                              leaves the journal byte-for-byte untouched
//   * 5 appended bytes      -> --no-verify reports torn_tail=yes (the crash
//                              artifact it found), exits 0 and truncates the
//                              tail back to the last good record boundary
//   * a corrupt record body -> refuses with a stderr diagnostic, non-zero exit,
//                              and never modifies the directory
FLOW_TEST(multiprocess, inspect_reports_torn_tail_and_repairs_only_when_asked) {
  const fs::path root = make_workspace("inspect");
  const fs::path logs = root / "logs";
  const fs::path state = root / "state";
  std::error_code error;
  REQUIRE(fs::create_directories(logs, error) || !error);

  // Produce durable state with a real coordinator and a real worker.
  const std::uint16_t port = pick_free_port();
  const std::string port_text = std::to_string(port);
  Child coordinator = spawn_child({coordinator_path(), "--port", port_text, "--state",
                                   state.string(), "--workers", "1", "--flows", "8", "--rounds",
                                   "1024", "--seed", "5"},
                                  logs, "coordinator");
  Child worker = spawn_child({worker_path(), "--port", port_text, "--worker", "1", "--boot", "1"},
                             logs, "worker");
  CHECK_EQ(wait_for_child(coordinator), static_cast<DWORD>(0));
  CHECK_EQ(wait_for_child(worker), static_cast<DWORD>(0));

  const std::optional<fs::path> journal = find_journal(state);
  REQUIRE(journal.has_value());
  const std::uintmax_t clean_bytes = size_of(*journal);
  CHECK(clean_bytes > 0);

  // --- a clean store: the default mode inspects and reports.
  Child clean =
      run_to_completion({inspect_path(), "--state", state.string()}, logs, "inspect-clean");
  CHECK_EQ(clean.exit_code, static_cast<DWORD>(0));
  const Inspection clean_report = require_inspection(read_text(clean.stdout_path));
  CHECK(clean_report.records > 0);
  CHECK(clean_report.bytes > 0);
  CHECK(clean_report.last > 0);
  CHECK_EQ(clean_report.torn_tail, false);
  // The coordinator never checkpoints, so no snapshot exists.
  CHECK_EQ(clean_report.snapshot, false);

  // --- a torn tail, repaired on request.
  append_bytes(*journal, "abcde");
  CHECK_EQ(size_of(*journal), clean_bytes + 5u);
  Child repairing = run_to_completion({inspect_path(), "--state", state.string(), "--no-verify"},
                                      logs, "inspect-repair");
  CHECK_EQ(repairing.exit_code, static_cast<DWORD>(0));
  const Inspection repaired = require_inspection(read_text(repairing.stdout_path));
  CHECK_EQ(repaired.torn_tail, true);
  CHECK_EQ(repaired.records, clean_report.records);
  CHECK_EQ(repaired.last, clean_report.last);
  CHECK_EQ(size_of(*journal), clean_bytes);

  // --- a torn tail with the default mode: report it, and touch nothing.
  append_bytes(*journal, "abcde");
  CHECK_EQ(size_of(*journal), clean_bytes + 5u);
  Child observing =
      run_to_completion({inspect_path(), "--state", state.string()}, logs, "inspect-verify-torn");
  CHECK_EQ(observing.exit_code, static_cast<DWORD>(0));
  const Inspection observed = require_inspection(read_text(observing.stdout_path));
  CHECK_EQ(observed.torn_tail, true);
  // The torn bytes are the crash artifact, so --verify names them on stderr and
  // does not repair them.
  CHECK(!read_text(observing.stderr_path).empty());
  CHECK_EQ(size_of(*journal), clean_bytes + 5u);

  // --- repair it again, then damage a record body: corruption is never repaired.
  Child final_repair = run_to_completion({inspect_path(), "--state", state.string(), "--no-verify"},
                                         logs, "inspect-final-repair");
  CHECK_EQ(final_repair.exit_code, static_cast<DWORD>(0));
  CHECK_EQ(size_of(*journal), clean_bytes);
  // Byte 0 of the first record's payload: the header checksum still verifies, so
  // this damages the record body rather than its framing.
  overwrite_byte(*journal, 24, static_cast<char>(0x5A));
  Child corrupt =
      run_to_completion({inspect_path(), "--state", state.string()}, logs, "inspect-corrupt");
  CHECK(corrupt.exit_code != static_cast<DWORD>(0));
  CHECK(!read_text(corrupt.stderr_path).empty());
  CHECK_EQ(size_of(*journal), clean_bytes);

  remove_workspace(root);
}

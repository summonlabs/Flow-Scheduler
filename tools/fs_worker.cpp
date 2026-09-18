// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.
//
// flow_scheduler_worker: a real worker process for the coordinator/worker
// transport contract.
//
// The worker performs exactly three things and nothing else: it completes the
// Hello/HelloAck handshake, it answers every Dispatch frame with a Started
// frame followed by a Completion frame whose evidence is bound to the exact
// ticket it was handed, and it exits when the coordinator says Shutdown.
//
// The worker deliberately does not revalidate authority: it cannot know the
// coordinator's current epoch, policy, generations, or reservations, so
// pretending to check them locally would be theatre. It echoes the ticket's own
// authority tuple into the evidence and lets the coordinator, which owns that
// authority, decide whether the evidence is still current. That is why the
// --stale-evidence switch exists: it fabricates an epoch the coordinator has
// already retired so the rejection path can be exercised from the outside.
//
// Connect is retried for a bounded window so that a supervisor may start
// workers before the coordinator reaches its accept loop, and so that the
// process does not have to guess how long the coordinator needs to bind.

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "flow_scheduler/hash.hpp"
#include "flow_scheduler/protocol.hpp"
#include "flow_scheduler/transport.hpp"

namespace {

using flow_scheduler::BootId;
using flow_scheduler::CompletionEvidence;
using flow_scheduler::CompletionMessage;
using flow_scheduler::CompletionOutcome;
using flow_scheduler::DispatchMessage;
using flow_scheduler::DispatchTicket;
using flow_scheduler::ErrorCode;
using flow_scheduler::ErrorMessage;
using flow_scheduler::FrameChannel;
using flow_scheduler::HelloAckMessage;
using flow_scheduler::HelloMessage;
using flow_scheduler::MessageType;
using flow_scheduler::ProvenanceOrigin;
using flow_scheduler::Result;
using flow_scheduler::ShutdownMessage;
using flow_scheduler::Socket;
using flow_scheduler::StartedMessage;
using flow_scheduler::Status;
using flow_scheduler::WorkerId;
using flow_scheduler::decode_completion;
using flow_scheduler::decode_dispatch;
using flow_scheduler::decode_error;
using flow_scheduler::decode_hello_ack;
using flow_scheduler::decode_shutdown;
using flow_scheduler::encode;
using flow_scheduler::to_string;
using flow_scheduler::wait_readable;

/// How long a worker keeps retrying the initial connect before it gives up.
constexpr std::uint32_t kConnectWindowMs = 10000;
/// Per-attempt connect timeout.
constexpr std::uint32_t kConnectAttemptMs = 1000;
/// Delay between connect attempts.
constexpr std::uint32_t kConnectRetryDelayMs = 20;
/// The handshake reply must arrive inside this window.
constexpr std::uint32_t kHandshakeBudgetMs = 10000;
/// Slice used while waiting for the handshake reply.
constexpr std::uint32_t kPollSliceMs = 50;

struct Options {
  std::string host{"127.0.0.1"};
  std::uint64_t port{0};
  bool port_set{false};
  std::uint64_t worker{0};
  bool worker_set{false};
  std::uint64_t boot{1};
  std::string label{};
  std::int64_t served{-1};
  std::uint64_t exit_after{0};
  bool stale_evidence{false};
  bool quiet{false};
};

void print_usage(std::FILE* stream) {
  std::fputs(
      "usage: flow_scheduler_worker --port <n> --worker <id> [options]\n"
      "\n"
      "  --host <address>     coordinator address (default 127.0.0.1)\n"
      "  --port <n>           coordinator TCP port (required)\n"
      "  --worker <id>        worker identity offered at handshake (required)\n"
      "  --boot <id>          worker boot incarnation (default 1)\n"
      "  --label <text>       diagnostic label echoed by the coordinator\n"
      "  --served <units>     service reported per dispatch; -1 (default) reports\n"
      "                       exactly the authorized ticket quantum\n"
      "  --exit-after <n>     exit abruptly after n completions (0 disables)\n"
      "  --stale-evidence     report evidence with the epoch incremented by one\n"
      "  --quiet              suppress the startup banner\n"
      "\n"
      "exit codes: 0 shutdown, 2 usage, 3 handshake rejected, 4 protocol or\n"
      "            transport failure, 5 peer closed before shutdown\n",
      stream);
}

bool parse_u64(std::string_view text, std::uint64_t& out) {
  if (text.empty()) {
    return false;
  }
  std::uint64_t value = 0;
  for (const char character : text) {
    if (character < '0' || character > '9') {
      return false;
    }
    const std::uint64_t digit = static_cast<std::uint64_t>(character - '0');
    if (value > (UINT64_MAX - digit) / 10u) {
      return false;
    }
    value = value * 10u + digit;
  }
  out = value;
  return true;
}

bool parse_i64(std::string_view text, std::int64_t& out) {
  bool negative = false;
  if (!text.empty() && (text.front() == '-' || text.front() == '+')) {
    negative = text.front() == '-';
    text.remove_prefix(1);
  }
  std::uint64_t magnitude = 0;
  if (!parse_u64(text, magnitude)) {
    return false;
  }
  if (magnitude > static_cast<std::uint64_t>(INT64_MAX)) {
    return false;
  }
  const std::int64_t value = static_cast<std::int64_t>(magnitude);
  out = negative ? -value : value;
  return true;
}

[[noreturn]] void fail_usage(const std::string& message) {
  std::fprintf(stderr, "flow_scheduler_worker: %s\n", message.c_str());
  print_usage(stderr);
  std::fflush(stderr);
  std::exit(2);
}

[[noreturn]] void fail(std::string_view message, int code) {
  std::fprintf(stderr, "flow_scheduler_worker: %.*s\n", static_cast<int>(message.size()),
               message.data());
  std::fflush(stderr);
  std::exit(code);
}

Options parse_options(int argc, char** argv) {
  Options options;
  for (int index = 1; index < argc; ++index) {
    const std::string_view flag = argv[index];
    const auto value_of = [&](const char* name) -> std::string_view {
      if (index + 1 >= argc) {
        fail_usage(std::string(name) + " requires a value");
      }
      ++index;
      return std::string_view(argv[index]);
    };
    if (flag == "--host") {
      options.host = std::string(value_of("--host"));
    } else if (flag == "--port") {
      if (!parse_u64(value_of("--port"), options.port) || options.port == 0 ||
          options.port > 65535u) {
        fail_usage("--port requires a value in [1, 65535]");
      }
      options.port_set = true;
    } else if (flag == "--worker") {
      if (!parse_u64(value_of("--worker"), options.worker) || options.worker == 0) {
        fail_usage("--worker requires a non-zero identity");
      }
      options.worker_set = true;
    } else if (flag == "--boot") {
      if (!parse_u64(value_of("--boot"), options.boot) || options.boot == 0) {
        fail_usage("--boot requires a non-zero incarnation");
      }
    } else if (flag == "--label") {
      options.label = std::string(value_of("--label"));
      if (options.label.size() > flow_scheduler::kMaxLabelBytes) {
        fail_usage("--label exceeds kMaxLabelBytes");
      }
    } else if (flag == "--served") {
      if (!parse_i64(value_of("--served"), options.served)) {
        fail_usage("--served requires an integer (-1 selects the ticket quantum)");
      }
    } else if (flag == "--exit-after") {
      if (!parse_u64(value_of("--exit-after"), options.exit_after)) {
        fail_usage("--exit-after requires a non-negative count");
      }
    } else if (flag == "--stale-evidence") {
      options.stale_evidence = true;
    } else if (flag == "--quiet") {
      options.quiet = true;
    } else {
      fail_usage("unknown flag: " + std::string(flag));
    }
  }
  if (!options.port_set) {
    fail_usage("--port is required");
  }
  if (!options.worker_set) {
    fail_usage("--worker is required");
  }
  return options;
}

/// Read one frame, but only after the socket reports readable inside the
/// budget. Keeps a silent peer from parking the process in a blocking receive.
Result<bool> read_frame_bounded(FrameChannel& channel, std::uint32_t budget_ms,
                                std::uint16_t& type, std::string& payload) {
  const auto started = std::chrono::steady_clock::now();
  for (;;) {
    const Socket* socket = &channel.socket();
    const Result<std::vector<std::size_t>> ready = wait_readable({socket}, kPollSliceMs);
    if (!ready.ok()) {
      return ready.error();
    }
    if (!ready.value().empty()) {
      return channel.read_frame(type, payload);
    }
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::steady_clock::now() - started)
                             .count();
    if (elapsed >= static_cast<long long>(budget_ms)) {
      return flow_scheduler::Error(ErrorCode::TransportFailure,
                                   "handshake reply did not arrive inside the budget");
    }
  }
}

Result<Socket> connect_with_retry(const Options& options) {
  const auto started = std::chrono::steady_clock::now();
  Status last_error = Status::failure(ErrorCode::TransportFailure, "connect was never attempted");
  for (;;) {
    Result<Socket> socket = Socket::connect(
        options.host, static_cast<std::uint16_t>(options.port), kConnectAttemptMs);
    if (socket.ok()) {
      return socket;
    }
    last_error = socket.error();
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::steady_clock::now() - started)
                             .count();
    if (elapsed >= static_cast<long long>(kConnectWindowMs)) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(kConnectRetryDelayMs));
  }
  return flow_scheduler::Error(
      ErrorCode::TransportFailure,
      "cannot connect to " + options.host + ":" + std::to_string(options.port) + " within " +
          std::to_string(kConnectWindowMs) + " ms (" + last_error.to_string() + ")");
}

CompletionEvidence make_evidence(const DispatchTicket& ticket, const Options& options,
                                 std::uint64_t observed_tick) {
  CompletionEvidence evidence;
  evidence.schedule = ticket.schedule;
  evidence.schedule_generation = ticket.schedule_generation;
  evidence.attempt = ticket.attempt;
  evidence.epoch = options.stale_evidence ? flow_scheduler::FabricEpoch(ticket.epoch.value() + 1u)
                                          : ticket.epoch;
  evidence.flow = ticket.flow;
  evidence.flow_generation = ticket.flow_generation;
  evidence.worker = ticket.worker;
  evidence.boot = ticket.boot;
  evidence.outcome = CompletionOutcome::Served;
  if (options.served < 0) {
    evidence.served = ticket.quantum;
  } else {
    const std::uint64_t requested = static_cast<std::uint64_t>(options.served);
    evidence.served = requested < ticket.quantum ? requested : ticket.quantum;
  }
  evidence.effect_code = 1;
  evidence.effect_digest = flow_scheduler::mix64(ticket.digest ^ ticket.attempt.value());
  evidence.observed_tick = observed_tick;
  evidence.provenance.origin = ProvenanceOrigin::External;
  evidence.provenance.sequence = ticket.attempt.value();
  evidence.provenance.digest = flow_scheduler::mix64(evidence.effect_digest);
  return evidence;
}

int run(const Options& options) {
  const Status initialized = flow_scheduler::transport_init();
  if (!initialized.ok()) {
    fail("transport initialization failed: " + initialized.to_string(), 4);
  }

  Result<Socket> connected = connect_with_retry(options);
  if (!connected.ok()) {
    fail(connected.error().to_string(), 2);
  }
  FrameChannel channel(std::move(connected).value());

  HelloMessage hello;
  hello.worker = WorkerId(options.worker);
  hello.boot = BootId(options.boot);
  hello.label = options.label;
  hello.supported_protocol = flow_scheduler::kProtocolVersion;
  Status sent = channel.send_frame(static_cast<std::uint16_t>(MessageType::Hello), encode(hello));
  if (!sent.ok()) {
    fail("cannot offer the handshake: " + sent.to_string(), 4);
  }

  std::uint16_t type = 0;
  std::string payload;
  Result<bool> received = read_frame_bounded(channel, kHandshakeBudgetMs, type, payload);
  if (!received.ok()) {
    fail("handshake failed: " + received.error().to_string(), 4);
  }
  if (!received.value()) {
    fail("coordinator closed the connection during the handshake", 4);
  }
  if (static_cast<MessageType>(type) == MessageType::Error) {
    const Result<ErrorMessage> error = decode_error(payload);
    if (!error.ok()) {
      fail("coordinator sent an undecodable error frame", 4);
    }
    fail("coordinator rejected the connection: " + error.value().detail, 4);
  }
  if (static_cast<MessageType>(type) != MessageType::HelloAck) {
    fail(std::string("expected a handshake acknowledgement, received ") + to_string(
             static_cast<MessageType>(type)),
         4);
  }
  const Result<HelloAckMessage> ack = decode_hello_ack(payload);
  if (!ack.ok()) {
    fail("handshake acknowledgement is malformed: " + ack.error().to_string(), 4);
  }
  if (!ack.value().accepted) {
    std::fprintf(stderr, "flow_scheduler_worker: handshake rejected (%s): %s\n",
                 to_string(ack.value().reason), ack.value().detail.c_str());
    std::fflush(stderr);
    return 3;
  }
  if (!options.quiet) {
    std::printf("WORKER worker=%llu boot=%llu epoch=%llu port=%llu\n",
                static_cast<unsigned long long>(ack.value().worker.value()),
                static_cast<unsigned long long>(ack.value().boot.value()),
                static_cast<unsigned long long>(ack.value().epoch.value()),
                static_cast<unsigned long long>(options.port));
    std::fflush(stdout);
  }

  std::uint64_t observed_tick = 0;
  std::uint64_t completions = 0;
  for (;;) {
    type = 0;
    payload.clear();
    Result<bool> next = channel.read_frame(type, payload);
    if (!next.ok()) {
      fail("transport failure while reading a frame: " + next.error().to_string(), 4);
    }
    if (!next.value()) {
      fail("coordinator closed the connection before shutdown", 5);
    }
    switch (static_cast<MessageType>(type)) {
      case MessageType::Dispatch: {
        const Result<DispatchMessage> dispatch = decode_dispatch(payload);
        if (!dispatch.ok()) {
          fail("dispatch frame is malformed: " + dispatch.error().to_string(), 4);
        }
        ++observed_tick;
        const DispatchTicket& ticket = dispatch.value().ticket;
        StartedMessage started;
        started.attempt = ticket.attempt;
        started.epoch = ticket.epoch;
        started.at_tick = observed_tick;
        Status acknowledged =
            channel.send_frame(static_cast<std::uint16_t>(MessageType::Started), encode(started));
        if (!acknowledged.ok()) {
          fail("cannot report the start of the attempt: " + acknowledged.to_string(), 4);
        }
        CompletionMessage completion;
        completion.evidence = make_evidence(ticket, options, observed_tick);
        Status reported = channel.send_frame(static_cast<std::uint16_t>(MessageType::Completion),
                                             encode(completion));
        if (!reported.ok()) {
          fail("cannot report the completion: " + reported.to_string(), 4);
        }
        ++completions;
        if (options.exit_after != 0 && completions >= options.exit_after) {
          std::fprintf(stderr,
                       "flow_scheduler_worker: abrupt exit after %llu completions\n",
                       static_cast<unsigned long long>(completions));
          std::fflush(stderr);
          std::_Exit(0);
        }
        break;
      }
      case MessageType::Shutdown: {
        const Result<ShutdownMessage> shutdown = decode_shutdown(payload);
        if (!shutdown.ok()) {
          fail("shutdown frame is malformed: " + shutdown.error().to_string(), 4);
        }
        std::printf("BYE\n");
        std::fflush(stdout);
        return 0;
      }
      case MessageType::Error: {
        const Result<ErrorMessage> error = decode_error(payload);
        if (!error.ok()) {
          fail("coordinator sent an undecodable error frame", 4);
        }
        fail("coordinator reported " + std::string(to_string(error.value().code)) + ": " +
                 error.value().detail,
             4);
      }
      default:
        fail(std::string("unexpected frame type: ") + to_string(static_cast<MessageType>(type)), 4);
    }
  }
}

}  // namespace

int main(int argc, char** argv) {
  const Options options = parse_options(argc, argv);
  return run(options);
}

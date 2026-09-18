// Flow Scheduler 1.0.0 — deterministic temporal arbitration runtime.
// Copyright 2026 Summon Software Labs.
// Licensed under the Apache License, Version 2.0.
//
// flow_scheduler_coordinator: a real coordinator process.
//
// The coordinator owns every piece of authority: it admits the synthetic
// population, arbitrates, opens dispatch attempts durably, hands the resulting
// tickets to worker processes as Dispatch frames, and commits the completion
// evidence they return. Workers never decide anything; they echo the ticket's
// own authority tuple back and the coordinator decides whether that evidence is
// still current.
//
// Delivery model. One loop iteration is one arbitration round. The round's Run
// entries are handed to workers round robin, Preempt entries are delivered by
// silence because arbitrate() already closed the attempt it displaced, and then
// the loop takes whatever the workers have sent. wait_readable() bounds the
// wait, so the loop keeps making temporal progress when no worker has data.
//
// Why the sockets are read through Socket plus FrameAccumulator rather than
// through FrameChannel: a FrameChannel drains up to 64 KiB from the socket per
// receive and returns one frame, keeping the rest of that burst in a private
// accumulator. A multiplexing coordinator that only reads when select() reports
// the *socket* readable therefore strands every coalesced frame behind a socket
// that has nothing left to report, and the run stalls with completions already
// in memory. Driving the accumulator explicitly fixes that: buffered frames are
// always delivered before any socket is polled, and every frame that arrives in
// one receive is delivered immediately.
//
// Failure model. A worker that dies, sends an unreadable frame, or closes its
// socket is retired: every attempt it owned is abandoned (never silently
// converted into success), the ambiguity that creates is resolved explicitly
// with AmbiguityResolution::Retry so the remaining workers can continue, and
// the run goes on. Nothing calls abort(); every loop is bounded and every
// failure path prints a diagnostic to stderr and returns non-zero.
//
// Tick model. The timeline is coordinator owned and purely logical: the run
// starts at tick 0 and advances by a fixed step derived from --interval. Ticks
// are never wall-clock and never restart-dependent, which is what lets a
// restarted coordinator pick the same durable state straight back up: the
// runtime only restores last-tick state from a compacted snapshot, and this
// tool never checkpoints, so a recovered incarnation always resumes from tick 0
// with the recovered population and the advanced epoch.

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "flow_scheduler/accounting.hpp"
#include "flow_scheduler/framing.hpp"
#include "flow_scheduler/hash.hpp"
#include "flow_scheduler/model.hpp"
#include "flow_scheduler/policy.hpp"
#include "flow_scheduler/protocol.hpp"
#include "flow_scheduler/scheduler.hpp"
#include "flow_scheduler/transport.hpp"

namespace {

using flow_scheduler::AccountingReport;
using flow_scheduler::AmbiguityResolution;
using flow_scheduler::ArbitrationOutcome;
using flow_scheduler::BootId;
using flow_scheduler::CommitDisposition;
using flow_scheduler::CommitOutcome;
using flow_scheduler::CompletionEvidence;
using flow_scheduler::CompletionMessage;
using flow_scheduler::DecisionKind;
using flow_scheduler::DispatchAttemptId;
using flow_scheduler::DispatchMessage;
using flow_scheduler::DispatchRejectedMessage;
using flow_scheduler::DispatchTicket;
using flow_scheduler::ErrorCode;
using flow_scheduler::ErrorMessage;
using flow_scheduler::FabricEpoch;
using flow_scheduler::FairnessGroupId;
using flow_scheduler::FlowDescriptor;
using flow_scheduler::FlowId;
using flow_scheduler::FlowLifecycle;
using flow_scheduler::FrameAccumulator;
using flow_scheduler::FrameHeader;
using flow_scheduler::Generation;
using flow_scheduler::HelloAckMessage;
using flow_scheduler::HelloMessage;
using flow_scheduler::Listener;
using flow_scheduler::MessageType;
using flow_scheduler::PathId;
using flow_scheduler::PolicyBinding;
using flow_scheduler::PolicyId;
using flow_scheduler::PreemptionMode;
using flow_scheduler::PriorityClassDescriptor;
using flow_scheduler::PriorityClassId;
using flow_scheduler::Provenance;
using flow_scheduler::ProvenanceOrigin;
using flow_scheduler::QoSClassDescriptor;
using flow_scheduler::QoSClassId;
using flow_scheduler::RecoveryReport;
using flow_scheduler::ResourceDescriptor;
using flow_scheduler::ResourceId;
using flow_scheduler::Result;
using flow_scheduler::Rng;
using flow_scheduler::Schedule;
using flow_scheduler::ScheduleEntry;
using flow_scheduler::Scheduler;
using flow_scheduler::SchedulerOptions;
using flow_scheduler::SchedulingPolicy;
using flow_scheduler::ShutdownMessage;
using flow_scheduler::Socket;
using flow_scheduler::StartedMessage;
using flow_scheduler::Status;
using flow_scheduler::Ticks;
using flow_scheduler::TraceId;
using flow_scheduler::WorkerId;
using flow_scheduler::decode_completion;
using flow_scheduler::decode_dispatch_rejected;
using flow_scheduler::decode_error;
using flow_scheduler::decode_hello;
using flow_scheduler::decode_started;
using flow_scheduler::digest_of;
using flow_scheduler::encode;
using flow_scheduler::encode_frame;
using flow_scheduler::kMaxFramePayload;
using flow_scheduler::kMaxLabelBytes;
using flow_scheduler::kNoDeadline;
using flow_scheduler::mix64;
using flow_scheduler::to_string;
using flow_scheduler::transport_init;
using flow_scheduler::wait_readable;

/// A peer may not burn more than this many frames proving it is not a worker.
constexpr std::size_t kMaxHandshakeFrames = 8;
/// The whole handshake for one connection must finish inside this window.
constexpr std::uint32_t kHandshakeBudgetMs = 30000;
/// Slice used while waiting inside a bounded receive.
constexpr std::uint32_t kPollSliceMs = 50;
/// How long a round waits for outstanding worker output before advancing.
constexpr std::uint32_t kDrainWaitMs = 50;
/// Upper bound on frames consumed inside one round, so a chatty peer cannot
/// monopolise the loop.
constexpr std::size_t kMaxFramesPerDrain = 16384;
/// stderr diagnostics are bounded: a broken workload must not flood a terminal.
constexpr std::uint64_t kMaxDiagnostics = 16;
/// wait_readable() cannot watch more sockets than FD_SETSIZE, so the worker
/// count is bounded well below it.
constexpr std::uint64_t kMaxWorkers = 60;
/// Prefix of every diagnostic this tool writes.
constexpr const char* kPrefix = "flow_scheduler_coordinator: ";

struct Options {
  std::uint64_t port{0};
  std::string state{};
  std::uint64_t workers{1};
  std::uint64_t flows{64};
  std::uint64_t rounds{4096};
  std::uint64_t interval{1000};
  std::uint64_t capacity{64};
  std::uint64_t concurrency{2};
  std::uint64_t quantum{4};
  std::uint64_t work{16};
  std::uint64_t seed{1};
  std::uint64_t kill_after{0};
  std::uint64_t exit_after{0};
};

/// One accepted worker connection: the socket, the frames already received but
/// not yet delivered, and the identity it announced.
struct Peer {
  Socket socket{};
  FrameAccumulator accumulator{};
  WorkerId worker{};
  BootId boot{};
  bool alive{false};
};

struct OpenAttempt {
  std::size_t peer{0};
  DispatchTicket ticket{};
  bool started{false};
};

enum class ReceiveStatus {
  /// A complete frame was delivered.
  Frame,
  /// Nothing was buffered and nothing arrived inside the budget.
  Idle,
  /// The peer closed its send direction at a frame boundary.
  Closed,
};

struct ReceiveOutcome {
  ReceiveStatus status{ReceiveStatus::Idle};
  std::uint16_t type{0};
  std::string payload{};
};

void print_usage(std::FILE* stream) {
  std::fputs(
      "usage: flow_scheduler_coordinator [options]\n"
      "\n"
      "  --port <n>          TCP port on 127.0.0.1 (default 0 = ephemeral; the\n"
      "                      chosen port is printed as PORT <n> and flushed)\n"
      "  --state <dir>       durable state directory (omit for volatile mode)\n"
      "  --workers <n>       worker handshakes accepted before the run (default 1)\n"
      "  --flows <n>         synthetic flows admitted (default 64)\n"
      "  --rounds <n>        maximum arbitration rounds (default 4096)\n"
      "  --interval <ticks>  resource capacity interval (default 1000)\n"
      "  --capacity <units>  resource capacity per interval (default 64)\n"
      "  --concurrency <n>   resource max_overlap (default 2)\n"
      "  --quantum <units>   per-dispatch service quantum (default 4)\n"
      "  --work <units>      estimated work per flow (default 16)\n"
      "  --seed <n>          deterministic workload seed (default 1)\n"
      "  --kill-after <n>    close worker 0's socket after n committed completions\n"
      "                      and continue on the remaining workers (0 disables)\n"
      "  --exit-after <n>    print CRASH <n> and exit abruptly after n committed\n"
      "                      completions (0 disables)\n"
      "\n"
      "The final stdout lines are the SUMMARY line and the accounting one-liner.\n"
      "exit codes: 0 completed run, 1 runtime failure, 2 usage error\n",
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

[[noreturn]] void fail_usage(const std::string& message) {
  std::fprintf(stderr, "%s%s\n", kPrefix, message.c_str());
  print_usage(stderr);
  std::fflush(stderr);
  std::exit(2);
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
    const auto number_of = [&](const char* name, std::uint64_t& target) {
      if (!parse_u64(value_of(name), target)) {
        fail_usage(std::string(name) + " requires a non-negative integer");
      }
    };
    if (flag == "--port") {
      number_of("--port", options.port);
      if (options.port > 65535u) {
        fail_usage("--port must be in [0, 65535]");
      }
    } else if (flag == "--state") {
      options.state = std::string(value_of("--state"));
      if (options.state.empty()) {
        fail_usage("--state requires a non-empty directory");
      }
    } else if (flag == "--workers") {
      number_of("--workers", options.workers);
      if (options.workers > kMaxWorkers) {
        fail_usage("--workers must be in [0, 60]");
      }
    } else if (flag == "--flows") {
      number_of("--flows", options.flows);
      if (options.flows > static_cast<std::uint64_t>(flow_scheduler::kMaxFlows)) {
        fail_usage("--flows exceeds kMaxFlows");
      }
    } else if (flag == "--rounds") {
      number_of("--rounds", options.rounds);
      if (options.rounds == 0 || options.rounds > 100000000ull) {
        fail_usage("--rounds must be in [1, 100000000]");
      }
    } else if (flag == "--interval") {
      number_of("--interval", options.interval);
      if (options.interval == 0 || options.interval > flow_scheduler::kMaxTickHorizon) {
        fail_usage("--interval is outside the tick horizon");
      }
    } else if (flag == "--capacity") {
      number_of("--capacity", options.capacity);
      if (options.capacity == 0 || options.capacity > flow_scheduler::kMaxServiceUnits) {
        fail_usage("--capacity is outside [1, kMaxServiceUnits]");
      }
    } else if (flag == "--concurrency") {
      number_of("--concurrency", options.concurrency);
      if (options.concurrency == 0 || options.concurrency > flow_scheduler::kMaxServiceUnits) {
        fail_usage("--concurrency is outside [1, kMaxServiceUnits]");
      }
    } else if (flag == "--quantum") {
      number_of("--quantum", options.quantum);
      if (options.quantum == 0 || options.quantum > flow_scheduler::kMaxQuantum) {
        fail_usage("--quantum is outside [1, kMaxQuantum]");
      }
    } else if (flag == "--work") {
      number_of("--work", options.work);
      if (options.work == 0 || options.work > flow_scheduler::kMaxServiceUnits) {
        fail_usage("--work is outside [1, kMaxServiceUnits]");
      }
    } else if (flag == "--seed") {
      number_of("--seed", options.seed);
    } else if (flag == "--kill-after") {
      number_of("--kill-after", options.kill_after);
    } else if (flag == "--exit-after") {
      number_of("--exit-after", options.exit_after);
    } else {
      fail_usage("unknown flag: " + std::string(flag));
    }
  }
  return options;
}

void diagnose(const std::string& message) {
  std::fprintf(stderr, "%s%s\n", kPrefix, message.c_str());
  std::fflush(stderr);
}

Provenance external_provenance(std::uint64_t sequence) {
  Provenance provenance;
  provenance.origin = ProvenanceOrigin::External;
  provenance.sequence = sequence;
  provenance.digest = mix64(sequence);
  return provenance;
}

SchedulingPolicy make_policy(const Options& options) {
  SchedulingPolicy policy;
  policy.policy = PolicyId(1);
  policy.generation = Generation(1);
  policy.preemption = PreemptionMode::Tiered;
  policy.deadline_ordering = true;
  policy.weighted_fairness = true;
  policy.min_service_guarantee = true;
  policy.strict_deadlines = true;
  policy.starvation_bound_ticks = 1000;
  policy.deadline_urgency_ticks = 100;
  policy.min_preempt_service = 0;
  policy.max_preemptions_per_interval = 64;
  policy.preemption_interval_ticks = 1000;
  policy.dispatch_delivery_horizon = 100000;
  policy.default_max_overlap = options.concurrency;
  policy.retained_schedule_history = 4096;
  policy.provenance = external_provenance(1);
  policy.provenance.digest = digest_of(policy);
  return policy;
}

/// The whole coordinator. One instance owns one run.
class Coordinator {
 public:
  explicit Coordinator(Options options) : options_(std::move(options)) {}

  int run();

 private:
  bool open_scheduler();
  bool register_topology();
  bool admit_flows();
  void resolve_ambiguous_flows();
  bool handshake(Listener& listener);

  /// Deliver frames already buffered in the peer's accumulator before touching
  /// the socket, then poll. The budget bounds the whole call and the slice
  /// bounds a single wait, so the round loop stays in control of its latency.
  Result<ReceiveOutcome> receive_within(Peer& peer, std::uint32_t budget_ms,
                                        std::uint32_t slice_ms);
  /// Non-blocking receive: everything already buffered, plus one immediate poll.
  Result<ReceiveOutcome> receive_now(Peer& peer);
  Status send_to(Peer& peer, MessageType type, std::string_view payload);

  bool main_loop();
  void dispatch_schedule(const Schedule& schedule);
  void drain(bool wait);
  std::size_t drain_pass();
  void handle_frame(std::size_t peer, std::uint16_t type, std::string_view payload);
  void retire_peer(std::size_t peer, std::string_view reason);
  void shutdown_peers();
  std::size_t pick_worker();
  std::uint64_t nominal_work(FlowId flow) const;
  bool all_flows_terminal();
  /// Honours --exit-after (never returns) and --kill-after (retires worker 0).
  void maybe_finish_after_completion();
  void report(std::string_view message);

  Options options_{};
  std::vector<Peer> peers_{};
  std::unordered_map<DispatchAttemptId, OpenAttempt> attempts_{};
  std::unique_ptr<Scheduler> scheduler_{};
  Ticks now_{0};
  Ticks round_step_{1};
  std::uint64_t flows_admitted_{0};
  std::uint64_t rounds_run_{0};
  std::uint64_t dispatches_issued_{0};
  std::uint64_t completions_applied_{0};
  std::uint64_t completions_rejected_{0};
  std::uint64_t dispatch_failures_{0};
  std::uint64_t ambiguities_resolved_{0};
  std::uint64_t abandoned_on_peer_loss_{0};
  std::uint64_t diagnostics_{0};
  std::size_t round_robin_{0};
  bool worker_killed_{false};
  bool converged_{false};
  bool fatal_{false};
};

void Coordinator::report(std::string_view message) {
  if (diagnostics_ >= kMaxDiagnostics) {
    if (diagnostics_ == kMaxDiagnostics) {
      ++diagnostics_;
      diagnose("further diagnostics suppressed");
    }
    return;
  }
  ++diagnostics_;
  diagnose(std::string(message));
}

Result<ReceiveOutcome> Coordinator::receive_now(Peer& peer) {
  return receive_within(peer, 0, 0);
}

Result<ReceiveOutcome> Coordinator::receive_within(Peer& peer, std::uint32_t budget_ms,
                                                   std::uint32_t slice_ms) {
  ReceiveOutcome outcome;
  if (!peer.socket.valid()) {
    outcome.status = ReceiveStatus::Closed;
    return outcome;
  }
  const auto started = std::chrono::steady_clock::now();
  for (;;) {
    FrameHeader header;
    std::string body;
    const Result<bool> next = peer.accumulator.next(header, body);
    if (!next.ok()) {
      return next.error();
    }
    if (next.value()) {
      outcome.status = ReceiveStatus::Frame;
      outcome.type = header.type;
      outcome.payload = std::move(body);
      return outcome;
    }
    const Socket* socket = &peer.socket;
    const Result<std::vector<std::size_t>> ready = wait_readable({socket}, slice_ms);
    if (!ready.ok()) {
      return ready.error();
    }
    if (!ready.value().empty()) {
      char buffer[65536];
      const Result<std::size_t> received = peer.socket.recv_some(buffer, sizeof(buffer));
      if (!received.ok()) {
        return received.error();
      }
      if (received.value() == 0) {
        outcome.status = ReceiveStatus::Closed;
        return outcome;
      }
      const Status pushed =
          peer.accumulator.push(std::string_view(buffer, received.value()));
      if (!pushed.ok()) {
        return pushed.error();
      }
      continue;
    }
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::steady_clock::now() - started)
                             .count();
    if (elapsed >= static_cast<long long>(budget_ms)) {
      outcome.status = ReceiveStatus::Idle;
      return outcome;
    }
  }
}

Status Coordinator::send_to(Peer& peer, MessageType type, std::string_view payload) {
  const Result<std::string> frame = encode_frame(static_cast<std::uint16_t>(type), payload);
  if (!frame.ok()) {
    return frame.error();
  }
  return peer.socket.send_all(frame.value());
}

bool Coordinator::open_scheduler() {
  SchedulerOptions options;
  options.policy = make_policy(options_);
  options.state_directory = options_.state;
  options.flush_to_storage = true;
  options.require_compatible_format = true;
  Result<std::unique_ptr<Scheduler>> created = Scheduler::create(options);
  if (!created.ok()) {
    diagnose("cannot open the scheduler: " + created.error().to_string());
    return false;
  }
  scheduler_ = std::move(created).value();
  const RecoveryReport& recovery = scheduler_->recovery_report();
  if (recovery.recovered) {
    report("recovered durable state: epoch_before=" + std::to_string(recovery.epoch_before.value()) +
           " epoch_after=" + std::to_string(recovery.epoch_after.value()) +
           " flows_restored=" + std::to_string(recovery.flows_restored) +
           " attempts_abandoned=" + std::to_string(recovery.attempts_abandoned) +
           " flows_made_ambiguous=" + std::to_string(recovery.flows_made_ambiguous) +
           " journal_records=" + std::to_string(recovery.journal_records_replayed) +
           " tail_truncated=" + (recovery.tail_truncated ? "yes" : "no"));
  }
  // A recovered store already carries the topology and the population. Only a
  // store that was recovered without any flow (a crash between the topology
  // records and the first admission) falls through to a fresh setup, which the
  // runtime refuses because the topology generations are already spent: that is
  // reported rather than papered over.
  if (recovery.recovered && scheduler_->flow_count() > 0) {
    flows_admitted_ = static_cast<std::uint64_t>(scheduler_->flow_count());
    resolve_ambiguous_flows();
    return true;
  }
  if (!register_topology()) {
    return false;
  }
  return admit_flows();
}

bool Coordinator::register_topology() {
  ResourceDescriptor resource;
  resource.resource = ResourceId(1);
  resource.generation = Generation(1);
  resource.capacity = options_.capacity;
  resource.interval = options_.interval;
  resource.max_overlap = options_.concurrency;
  resource.provenance = external_provenance(1);
  Status status = scheduler_->register_resource(resource);
  if (!status.ok()) {
    diagnose("cannot register the resource: " + status.to_string());
    return false;
  }
  for (std::uint64_t index = 0; index < 4; ++index) {
    PriorityClassDescriptor descriptor;
    descriptor.priority = PriorityClassId(index + 1);
    descriptor.generation = Generation(1);
    descriptor.rank = static_cast<std::uint32_t>(index);
    descriptor.weight = 1;
    descriptor.provenance = external_provenance(index + 2);
    status = scheduler_->register_priority_class(descriptor);
    if (!status.ok()) {
      diagnose("cannot register priority class " + std::to_string(index + 1) + ": " +
               status.to_string());
      return false;
    }
  }
  QoSClassDescriptor qos;
  qos.qos = QoSClassId(1);
  qos.generation = Generation(1);
  qos.preemption_protected = false;
  qos.max_service_per_dispatch = 0;
  qos.provenance = external_provenance(6);
  status = scheduler_->register_qos_class(qos);
  if (!status.ok()) {
    diagnose("cannot register the qos class: " + status.to_string());
    return false;
  }
  return true;
}

bool Coordinator::admit_flows() {
  if (options_.flows == 0) {
    flows_admitted_ = 0;
    return true;
  }
  Rng rng(options_.seed);
  // Releases are spread across one capacity interval so the population arrives
  // over time; deadlines sit far outside the round budget so a run that stays
  // inside --rounds never fails a flow for a reason the operator did not ask
  // for. Odd flow identities carry a deadline, even ones do not.
  const std::uint64_t release_span = options_.interval;
  const std::uint64_t deadline_span = options_.interval * 64u;
  for (std::uint64_t index = 0; index < options_.flows; ++index) {
    const FlowId flow(index + 1);
    const std::uint64_t release = rng.next_below(release_span);
    const std::uint64_t weight = 1u + rng.next_below(4u);
    const std::uint64_t priority_slot = rng.next_below(4u);
    FlowDescriptor descriptor;
    descriptor.flow = flow;
    descriptor.generation = Generation(1);
    descriptor.path = {PathId(1), Generation(1)};
    descriptor.resource = {ResourceId(1), Generation(1)};
    descriptor.priority = {PriorityClassId(priority_slot + 1), Generation(1)};
    descriptor.qos = {QoSClassId(1), Generation(1)};
    descriptor.fairness_group = FairnessGroupId(index % 4u + 1u);
    descriptor.estimated_work = options_.work;
    descriptor.service_quantum = options_.quantum;
    descriptor.release_tick = release;
    descriptor.deadline_tick = (flow.value() % 2u == 1u) ? release + deadline_span : kNoDeadline;
    descriptor.weight = weight;
    descriptor.preemptible = true;
    descriptor.auto_ready = true;
    descriptor.trace = TraceId(flow.value());
    descriptor.provenance = external_provenance(flow.value());
    const Status admitted = scheduler_->admit_flow(descriptor);
    if (!admitted.ok()) {
      diagnose("cannot admit flow " + std::to_string(flow.value()) + ": " + admitted.to_string());
      return false;
    }
  }
  flows_admitted_ = options_.flows;
  return true;
}

void Coordinator::resolve_ambiguous_flows() {
  // The synthetic population is contiguous from 1 and a recovered store holds
  // exactly that population, so a direct identity walk enumerates it without
  // waiting for a query API that does not exist. The tool applies the explicit
  // operator policy Retry: an abandoned attempt means the outcome is unknown,
  // and the runtime must never restore liveness implicitly, so the decision is
  // taken here, counted, and reported.
  const std::size_t count = scheduler_->flow_count();
  for (std::size_t index = 0; index < count; ++index) {
    const FlowId flow(static_cast<std::uint64_t>(index) + 1u);
    const Result<flow_scheduler::FlowSnapshot> snapshot = scheduler_->flow(flow);
    if (!snapshot.ok() || snapshot.value().lifecycle != FlowLifecycle::Ambiguous) {
      continue;
    }
    const Status resolved = scheduler_->resolve_ambiguous(
        flow, snapshot.value().generation, AmbiguityResolution::Retry, now_);
    if (!resolved.ok()) {
      report("cannot resolve the ambiguous outcome of " + to_string(flow) + ": " +
             resolved.to_string());
      continue;
    }
    ++ambiguities_resolved_;
  }
}

bool Coordinator::handshake(Listener& listener) {
  peers_.reserve(static_cast<std::size_t>(options_.workers));
  for (std::uint64_t index = 0; index < options_.workers; ++index) {
    Result<Socket> accepted = listener.accept();
    if (!accepted.ok()) {
      diagnose("cannot accept worker connection " + std::to_string(index + 1) + ": " +
               accepted.error().to_string());
      return false;
    }
    Peer candidate;
    candidate.socket = std::move(accepted).value();
    HelloMessage hello;
    bool greeted = false;
    for (std::size_t frame = 0; frame < kMaxHandshakeFrames && !greeted; ++frame) {
      const Result<ReceiveOutcome> received =
          receive_within(candidate, kHandshakeBudgetMs, kPollSliceMs);
      if (!received.ok()) {
        diagnose("worker connection " + std::to_string(index + 1) +
                 " failed the handshake: " + received.error().to_string());
        candidate.socket.close();
        return false;
      }
      if (received.value().status != ReceiveStatus::Frame) {
        diagnose("worker connection " + std::to_string(index + 1) +
                 (received.value().status == ReceiveStatus::Closed
                      ? " closed before offering a hello"
                      : " did not offer a hello inside the handshake budget"));
        candidate.socket.close();
        return false;
      }
      if (static_cast<MessageType>(received.value().type) != MessageType::Hello) {
        ErrorMessage error;
        error.code = ErrorCode::ProtocolViolation;
        error.detail = "the first frame on a worker connection must be a hello";
        static_cast<void>(send_to(candidate, MessageType::Error, encode(error)));
        diagnose("worker connection " + std::to_string(index + 1) + " offered " +
                 to_string(static_cast<MessageType>(received.value().type)) +
                 " instead of a hello");
        candidate.socket.close();
        return false;
      }
      const Result<HelloMessage> decoded = decode_hello(received.value().payload);
      if (!decoded.ok()) {
        HelloAckMessage rejection;
        rejection.accepted = false;
        rejection.reason = decoded.error().code();
        rejection.detail = decoded.error().message();
        static_cast<void>(send_to(candidate, MessageType::HelloAck, encode(rejection)));
        diagnose("worker connection " + std::to_string(index + 1) +
                 " offered an invalid hello: " + decoded.error().to_string());
        candidate.socket.close();
        return false;
      }
      hello = decoded.value();
      greeted = true;
    }
    if (!greeted) {
      diagnose("worker connection " + std::to_string(index + 1) +
               " exhausted the handshake frame budget without a hello");
      candidate.socket.close();
      return false;
    }
    for (const Peer& peer : peers_) {
      if (peer.worker == hello.worker && peer.boot == hello.boot) {
        HelloAckMessage rejection;
        rejection.accepted = false;
        rejection.reason = ErrorCode::Fenced;
        rejection.detail = "this worker incarnation is already connected";
        static_cast<void>(send_to(candidate, MessageType::HelloAck, encode(rejection)));
        diagnose("worker " + to_string(hello.worker) + " boot " + to_string(hello.boot) +
                 " is already connected");
        candidate.socket.close();
        return false;
      }
    }

    const Result<FabricEpoch> epoch = scheduler_->epoch();
    const Result<PolicyBinding> binding = scheduler_->policy_binding();
    if (!epoch.ok() || !binding.ok()) {
      diagnose(std::string("cannot advertise the coordinator authority: ") +
               (epoch.ok() ? binding.error().to_string() : epoch.error().to_string()));
      candidate.socket.close();
      return false;
    }
    HelloAckMessage acknowledgement;
    acknowledgement.accepted = true;
    acknowledgement.reason = ErrorCode::Ok;
    acknowledgement.detail = "accepted";
    acknowledgement.worker = hello.worker;
    acknowledgement.boot = hello.boot;
    acknowledgement.epoch = epoch.value();
    acknowledgement.policy = binding.value().policy;
    acknowledgement.policy_generation = binding.value().generation;
    acknowledgement.policy_digest = binding.value().digest;
    acknowledgement.max_frame_payload = kMaxFramePayload;
    acknowledgement.heartbeat_ticks = 0;
    const Status sent = send_to(candidate, MessageType::HelloAck, encode(acknowledgement));
    if (!sent.ok()) {
      diagnose("cannot acknowledge worker " + to_string(hello.worker) + ": " + sent.to_string());
      candidate.socket.close();
      return false;
    }
    candidate.worker = hello.worker;
    candidate.boot = hello.boot;
    candidate.alive = true;
    peers_.push_back(std::move(candidate));
    if (!options_.state.empty()) {
      report("worker " + to_string(hello.worker) + " boot " + to_string(hello.boot) + " (label '" +
             hello.label.substr(0, kMaxLabelBytes) + "') accepted at epoch " +
             std::to_string(epoch.value().value()));
    }
  }
  return true;
}

std::size_t Coordinator::pick_worker() {
  const std::size_t total = peers_.size();
  for (std::size_t probe = 0; probe < total; ++probe) {
    const std::size_t index = (round_robin_ + probe) % total;
    if (peers_[index].alive) {
      round_robin_ = (index + 1) % total;
      return index;
    }
  }
  return total;
}

std::uint64_t Coordinator::nominal_work(FlowId flow) const {
  const Result<flow_scheduler::FlowSnapshot> snapshot = scheduler_->flow(flow);
  if (!snapshot.ok()) {
    return 0;
  }
  const std::uint64_t estimated = snapshot.value().estimated_work;
  const std::uint64_t served = snapshot.value().served_work;
  return estimated > served ? estimated - served : 0;
}

void Coordinator::dispatch_schedule(const Schedule& schedule) {
  for (const ScheduleEntry& entry : schedule.entries) {
    if (entry.kind != DecisionKind::Run) {
      // Preempt entries need no message: arbitrate() already closed the
      // displaced attempt, so silence is the complete delivery.
      continue;
    }
    const std::size_t peer_index = pick_worker();
    if (peer_index >= peers_.size()) {
      return;  // No live worker; undelivered entries are reclaimed by the horizon.
    }
    Peer& peer = peers_[peer_index];
    const Result<DispatchTicket> ticket = scheduler_->begin_dispatch(
        schedule.schedule, schedule.generation, entry.ordinal, peer.worker, peer.boot, now_);
    if (!ticket.ok()) {
      ++dispatch_failures_;
      report("dispatch refused for " + to_string(entry.flow) + " at tick " + std::to_string(now_) +
             ": " + ticket.error().to_string());
      continue;
    }
    OpenAttempt attempt;
    attempt.peer = peer_index;
    attempt.ticket = ticket.value();
    attempts_[ticket.value().attempt] = attempt;
    DispatchMessage message;
    message.ticket = ticket.value();
    message.nominal_work = nominal_work(ticket.value().flow);
    const Status sent = send_to(peer, MessageType::Dispatch, encode(message));
    if (!sent.ok()) {
      report("cannot deliver " + to_string(ticket.value().attempt) + " to worker " +
             to_string(peer.worker) + ": " + sent.to_string());
      retire_peer(peer_index, "the dispatch frame could not be delivered");
      continue;
    }
    ++dispatches_issued_;
  }
}

void Coordinator::retire_peer(std::size_t peer, std::string_view reason) {
  if (peer >= peers_.size()) {
    return;
  }
  Peer& entry = peers_[peer];
  if (entry.alive) {
    entry.alive = false;
    entry.socket.close();
  }
  std::uint64_t abandoned = 0;
  for (auto attempt = attempts_.begin(); attempt != attempts_.end();) {
    if (attempt->second.peer != peer) {
      ++attempt;
      continue;
    }
    const Status status = scheduler_->abandon_attempt(attempt->first, reason, now_);
    if (!status.ok() && status.code() != ErrorCode::UnknownAttempt) {
      report("cannot abandon " + to_string(attempt->first) + ": " + status.to_string());
    }
    attempt = attempts_.erase(attempt);
    ++abandoned;
  }
  abandoned_on_peer_loss_ += abandoned;
  diagnose("worker " + to_string(entry.worker) + " retired (" + std::string(reason) + "); " +
           std::to_string(abandoned) + " open attempt(s) abandoned");
  resolve_ambiguous_flows();
}

void Coordinator::handle_frame(std::size_t peer, std::uint16_t type, std::string_view payload) {
  switch (static_cast<MessageType>(type)) {
    case MessageType::Started: {
      const Result<StartedMessage> started = decode_started(payload);
      if (!started.ok()) {
        report("worker " + to_string(peers_[peer].worker) +
               " sent an undecodable started frame: " + started.error().to_string());
        return;
      }
      const auto found = attempts_.find(started.value().attempt);
      if (found == attempts_.end()) {
        report("started frame names " + to_string(started.value().attempt) + ", which is not open");
        return;
      }
      if (found->second.started) {
        return;
      }
      const Status marked = scheduler_->mark_started(found->second.ticket, now_);
      if (!marked.ok()) {
        // A refusal here is a stale, preempted or already-closed attempt, not a
        // transport failure: the completion path still decides the outcome.
        report("cannot mark the start of " + to_string(started.value().attempt) + ": " +
               marked.to_string());
        return;
      }
      found->second.started = true;
      return;
    }
    case MessageType::Completion: {
      const Result<CompletionMessage> completion = decode_completion(payload);
      if (!completion.ok()) {
        report("worker " + to_string(peers_[peer].worker) +
               " sent undecodable completion evidence: " + completion.error().to_string());
        return;
      }
      const CompletionEvidence& evidence = completion.value().evidence;
      const Result<CommitOutcome> committed = scheduler_->complete(evidence, now_);
      if (!committed.ok()) {
        diagnose("cannot commit the completion of " + to_string(evidence.attempt) + ": " +
                 committed.error().to_string());
        fatal_ = true;
        return;
      }
      if (committed.value().disposition == CommitDisposition::Applied) {
        attempts_.erase(evidence.attempt);
        ++completions_applied_;
        maybe_finish_after_completion();
        return;
      }
      ++completions_rejected_;
      report("completion of " + to_string(evidence.attempt) + " was not applied (" +
             to_string(committed.value().code) + "): " + committed.value().detail);
      return;
    }
    case MessageType::DispatchRejected: {
      const Result<DispatchRejectedMessage> rejected = decode_dispatch_rejected(payload);
      if (!rejected.ok()) {
        report("worker " + to_string(peers_[peer].worker) +
               " sent an undecodable dispatch rejection: " + rejected.error().to_string());
        return;
      }
      const auto found = attempts_.find(rejected.value().attempt);
      if (found != attempts_.end()) {
        const Status status = scheduler_->abandon_attempt(
            rejected.value().attempt, "the worker rejected the dispatch", now_);
        if (!status.ok() && status.code() != ErrorCode::UnknownAttempt) {
          report("cannot abandon " + to_string(rejected.value().attempt) + ": " +
                 status.to_string());
        }
        attempts_.erase(found);
      }
      report("worker " + to_string(peers_[peer].worker) + " rejected " +
             to_string(rejected.value().attempt) + " (" + to_string(rejected.value().code) +
             "): " + rejected.value().detail);
      return;
    }
    case MessageType::Error: {
      const Result<ErrorMessage> error = decode_error(payload);
      const std::string detail =
          error.ok() ? std::string(to_string(error.value().code)) + ": " + error.value().detail
                     : std::string("undecodable error frame");
      retire_peer(peer, "the worker reported " + detail);
      return;
    }
    default: {
      ErrorMessage error;
      error.code = ErrorCode::ProtocolViolation;
      error.detail = "unexpected frame type outside the dispatch protocol";
      static_cast<void>(send_to(peers_[peer], MessageType::Error, encode(error)));
      retire_peer(peer, std::string("unexpected frame type ") +
                            to_string(static_cast<MessageType>(type)));
      return;
    }
  }
}

/// Take everything the workers have already sent, without waiting. Returns the
/// number of frames delivered; sets fatal_ when the run must stop.
std::size_t Coordinator::drain_pass() {
  std::size_t frames = 0;
  for (std::size_t index = 0; index < peers_.size(); ++index) {
    if (!peers_[index].alive) {
      continue;
    }
    for (;;) {
      const Result<ReceiveOutcome> received = receive_now(peers_[index]);
      if (!received.ok()) {
        retire_peer(index, "transport failure: " + received.error().to_string());
        break;
      }
      if (received.value().status == ReceiveStatus::Closed) {
        retire_peer(index, "peer closed the connection");
        break;
      }
      if (received.value().status != ReceiveStatus::Frame) {
        break;
      }
      ++frames;
      handle_frame(index, received.value().type, received.value().payload);
      if (fatal_) {
        return frames;
      }
      if (frames >= kMaxFramesPerDrain) {
        report("frame budget exhausted inside one round; deferring worker output");
        return frames;
      }
    }
  }
  return frames;
}

void Coordinator::drain(bool wait) {
  if (drain_pass() > 0 || fatal_ || !wait) {
    return;
  }
  std::vector<const Socket*> sockets;
  sockets.reserve(peers_.size());
  for (const Peer& peer : peers_) {
    if (peer.alive) {
      sockets.push_back(&peer.socket);
    }
  }
  if (sockets.empty()) {
    return;
  }
  const Result<std::vector<std::size_t>> ready = wait_readable(sockets, kDrainWaitMs);
  if (!ready.ok()) {
    report("cannot poll worker sockets: " + ready.error().to_string());
    return;
  }
  if (ready.value().empty()) {
    return;
  }
  static_cast<void>(drain_pass());
}

void Coordinator::maybe_finish_after_completion() {
  if (options_.exit_after != 0 && completions_applied_ >= options_.exit_after) {
    std::printf("CRASH %llu\n", static_cast<unsigned long long>(completions_applied_));
    std::fflush(stdout);
    // Deliberately abrupt: no destructor, no flush of anything else, no
    // shutdown frame. This is the coordinator crash the durability tests need.
    std::_Exit(0);
  }
  if (options_.kill_after != 0 && completions_applied_ >= options_.kill_after && !worker_killed_) {
    worker_killed_ = true;
    if (!peers_.empty()) {
      retire_peer(0, "simulated worker death (--kill-after)");
    }
  }
}

bool Coordinator::all_flows_terminal() {
  if (flows_admitted_ == 0) {
    return true;
  }
  const AccountingReport report_now = scheduler_->accounting();
  const std::uint64_t terminal = report_now.completed + report_now.cancelled + report_now.failed;
  return terminal >= flows_admitted_;
}

bool Coordinator::main_loop() {
  round_step_ = options_.interval / 100u;
  if (round_step_ == 0) {
    round_step_ = 1;
  }
  while (rounds_run_ < options_.rounds) {
    if (all_flows_terminal()) {
      converged_ = true;
      break;
    }
    const Result<ArbitrationOutcome> outcome = scheduler_->arbitrate(now_);
    if (!outcome.ok()) {
      diagnose("arbitration failed at tick " + std::to_string(now_) + ": " +
               outcome.error().to_string());
      return false;
    }
    ++rounds_run_;
    dispatch_schedule(outcome.value().schedule);
    drain(!attempts_.empty());
    if (fatal_) {
      return false;
    }
    now_ += round_step_;
  }
  return true;
}

void Coordinator::shutdown_peers() {
  for (Peer& peer : peers_) {
    if (!peer.alive) {
      continue;
    }
    ShutdownMessage shutdown;
    shutdown.reason = "run complete";
    const Status sent = send_to(peer, MessageType::Shutdown, encode(shutdown));
    if (!sent.ok()) {
      report("cannot tell worker " + to_string(peer.worker) + " to stop: " + sent.to_string());
    }
    // Half-close rather than close: the shutdown frame must reach the worker
    // instead of being discarded by an abortive close, so the send direction
    // ends without resetting the connection.
    peer.socket.shutdown_send();
    peer.alive = false;
  }
}

int Coordinator::run() {
  const Status initialized = transport_init();
  if (!initialized.ok()) {
    diagnose("transport initialization failed: " + initialized.to_string());
    return 1;
  }
  Result<Listener> bound = Listener::bind("127.0.0.1", static_cast<std::uint16_t>(options_.port));
  if (!bound.ok()) {
    diagnose("cannot bind 127.0.0.1:" + std::to_string(options_.port) + ": " +
             bound.error().to_string());
    return 1;
  }
  Listener listener = std::move(bound).value();
  if (options_.port == 0) {
    std::printf("PORT %u\n", static_cast<unsigned>(listener.port()));
    std::fflush(stdout);
  }
  if (!open_scheduler()) {
    return 1;
  }
  if (!handshake(listener)) {
    return 1;
  }
  if (!main_loop()) {
    return 1;
  }
  shutdown_peers();

  const AccountingReport report_now = scheduler_->accounting();
  const Status books = report_now.validate();
  if (!books.ok()) {
    diagnose("accounting does not close: " + books.to_string());
    return 1;
  }
  const Result<FabricEpoch> epoch = scheduler_->epoch();
  if (!epoch.ok()) {
    diagnose("cannot read the fabric epoch: " + epoch.error().to_string());
    return 1;
  }
  std::printf("SUMMARY completed=%llu rounds=%llu epoch=%llu dispatches=%llu\n",
              static_cast<unsigned long long>(report_now.completed),
              static_cast<unsigned long long>(rounds_run_),
              static_cast<unsigned long long>(epoch.value().value()),
              static_cast<unsigned long long>(dispatches_issued_));
  std::fflush(stdout);
  std::printf("%s\n", report_now.to_string().c_str());
  std::fflush(stdout);
  // Dispatch refusals and rejected completions are counted apart from
  // transport failures: a run that reports completed=0 because every completion
  // was refused is a different defect from one that never dispatched at all.
  diagnose("run counters: dispatch_frames=" + std::to_string(dispatches_issued_) +
           " dispatch_refused=" + std::to_string(dispatch_failures_) +
           " completions_applied=" + std::to_string(completions_applied_) +
           " completions_rejected=" + std::to_string(completions_rejected_) +
           " ambiguities_resolved=" + std::to_string(ambiguities_resolved_) +
           " abandoned_on_peer_loss=" + std::to_string(abandoned_on_peer_loss_));
  if (!converged_) {
    diagnose("round budget exhausted with " +
             std::to_string(flows_admitted_ -
                            (report_now.completed + report_now.cancelled + report_now.failed)) +
             " flow(s) still non-terminal");
  }
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  const Options options = parse_options(argc, argv);
  Coordinator coordinator(options);
  return coordinator.run();
}

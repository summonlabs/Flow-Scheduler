# Flow Scheduler

A vendor-neutral C++20 runtime for **deterministic temporal arbitration of competing
network flows** under readiness, priority, deadlines, fairness, reservations, capacity,
and generation-bound authority.

Flow Scheduler answers exactly one question:

> Among flows that are already admitted and otherwise legal, which flow should run now,
> which must wait, which may overlap, and which temporal schedule remains authoritative
> as readiness, capacity, reservations, deadlines, policy, or generations change?

## Boundary

**Owned by this runtime**

* temporal ordering and arbitration among already-admitted flows;
* readiness gating, strict priority, weighted fairness, deadlines, minimum service
  guarantees, and starvation prevention;
* reservation windows, capacity intervals, and concurrency/overlap ceilings as
  *externally supplied constraints* that the arbiter enforces;
* legal preemption and yield decisions;
* binding a dispatch decision to the exact generations that justified it, and
  revalidating before dispatch;
* committing completion evidence exactly once and rejecting stale evidence.

**Deliberately not owned**

* admission control - flows arrive already admitted;
* path legality, path computation, flow placement, or route state;
* bandwidth reservation, rate enforcement, queue implementation, congestion control,
  or forwarding;
* the definition of priority classes or QoS classes - these are registered externally as
  opaque identities with externally supplied ranks and weights, and the runtime only
  consumes them.

The runtime never computes a path, never reserves bandwidth, never enforces a rate, and
never forwards anything. It decides *when* a flow that is already legal may use a
resource window that someone else owns.

## Separate states

The runtime keeps these strictly distinct, because collapsing any two of them is how
schedulers lose correctness:

```
Ready != Scheduled != Dispatched != Running != CompletionReported != Completed
                                                                    (authoritative)
```

* **Ready** - eligible for arbitration right now.
* **Scheduled** - placed in an issued schedule entry; the service window is *reserved*
  but nothing has been handed to a worker.
* **Dispatched** - a dispatch frame was durably journaled with a full authority tuple.
* **Running** - the worker acknowledged the start.
* **CompletionReported** - the worker reported a result; nothing is committed yet.
* **Completed** - service was committed under the current epoch. Only this is
  authoritative completion.

## Requirements

* A C++20 compiler. Validation for this release was performed with MSVC 19.44
  (Visual Studio 2022) under `/W4 /WX /permissive-`. The library also builds with GCC
  and Clang under the equivalent strict warning set.
* CMake 3.20 or newer.
* No third-party dependencies. The only platform surface used is the socket API
  (Winsock on Windows, POSIX sockets elsewhere).

## Build

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
ctest --test-dir build --output-on-failure
```

Options:

| Option | Default | Meaning |
| --- | --- | --- |
| `FLOW_SCHEDULER_BUILD_TESTS` | ON | Build the test suite |
| `FLOW_SCHEDULER_BUILD_TOOLS` | ON | Build the coordinator/worker/inspector tools |
| `FLOW_SCHEDULER_BUILD_BENCH` | ON | Build the synthetic benchmark |
| `FLOW_SCHEDULER_WARNINGS_AS_ERRORS` | ON | Treat first-party warnings as errors |
| `FLOW_SCHEDULER_ENABLE_ASAN` | OFF | Build with AddressSanitizer where supported |
| `FLOW_SCHEDULER_BUILD_SHARED` | OFF | Build `flow_scheduler` as a shared library |

## Install and consume

```sh
cmake --install build --prefix /some/prefix
```

```cmake
find_package(FlowScheduler 1.0.0 REQUIRED)
target_link_libraries(my_target PRIVATE flow_scheduler::flow_scheduler)
```

## Quick start

```cpp
#include "flow_scheduler/scheduler.hpp"

using namespace flow_scheduler;

SchedulerOptions options;
options.policy.policy = PolicyId(1);
options.policy.generation = Generation(1);
options.policy.provenance.origin = ProvenanceOrigin::External;
options.policy.provenance.sequence = 1;
options.policy.provenance.digest = digest_of(options.policy);

auto scheduler = Scheduler::create(options).value();   // handle the error in real code

ResourceDescriptor resource;
resource.resource = ResourceId(1);
resource.generation = Generation(1);
resource.capacity = 64;      // service units per interval
resource.interval = 1'000;   // ticks
resource.max_overlap = 8;    // concurrent dispatch windows
resource.provenance = {/* External, sequence, digest */};
scheduler->register_resource(resource);

PriorityClassDescriptor priority;
priority.priority = PriorityClassId(1);
priority.generation = Generation(1);
priority.rank = 0;           // lower rank is more urgent
priority.weight = 1;
priority.provenance = {/* ... */};
scheduler->register_priority_class(priority);

QoSClassDescriptor qos;
qos.qos = QoSClassId(1);
qos.generation = Generation(1);
qos.provenance = {/* ... */};
scheduler->register_qos_class(qos);

FlowDescriptor flow;
flow.flow = FlowId(1);
flow.generation = Generation(1);
flow.path = {PathId(1), Generation(1)};               // supplied by the caller
flow.resource = {ResourceId(1), Generation(1)};
flow.priority = {PriorityClassId(1), Generation(1)};
flow.qos = {QoSClassId(1), Generation(1)};
flow.fairness_group = FairnessGroupId(1);
flow.estimated_work = 16;
flow.service_quantum = 4;
flow.release_tick = 0;
flow.deadline_tick = 8'000;
flow.provenance = {/* ... */};
scheduler->admit_flow(flow);

const ArbitrationOutcome outcome = scheduler->arbitrate(0).value();
for (const ScheduleEntry& entry : outcome.schedule.entries) {
  if (entry.kind != DecisionKind::Run) continue;
  const DispatchTicket ticket =
      scheduler->begin_dispatch(outcome.schedule.schedule, outcome.schedule.generation,
                                entry.ordinal, WorkerId(1), BootId(1), 0).value();
  // hand the ticket to a worker, which must revalidate before executing
  scheduler->mark_started(ticket, 0);
  CompletionEvidence evidence;
  evidence.schedule = ticket.schedule;
  evidence.schedule_generation = ticket.schedule_generation;
  evidence.attempt = ticket.attempt;
  evidence.epoch = ticket.epoch;
  evidence.flow = ticket.flow;
  evidence.flow_generation = ticket.flow_generation;
  evidence.worker = ticket.worker;
  evidence.boot = ticket.boot;
  evidence.outcome = CompletionOutcome::Served;
  evidence.served = ticket.quantum;
  evidence.provenance = {/* External, sequence, digest */};
  scheduler->complete(evidence, 0);
}
```

## Model

Every identity is a distinct C++ type, so a `PathId` can never be passed where a
`ReservationId` is expected: `FlowId`, `Generation`, `ScheduleId`,
`DispatchAttemptId`, `ResourceId`, `PathId`, `ReservationId`, `PriorityClassId`,
`QoSClassId`, `PolicyId`, `FabricEpoch`, `WorkerId`, `BootId`, `FairnessGroupId`, plus
`Provenance` (origin, monotone sequence, digest) attached to every externally supplied
definition.

A flow descriptor binds readiness (`release_tick`, an explicit readiness signal, and
dependencies), temporal obligation (`deadline_tick`), work (`estimated_work`,
`service_quantum`), fairness (`fairness_group`, `weight`, `min_service` and its
window), preemptibility, and the externally computed path/resource/reservation/
priority/QoS bindings - each with its own generation.

## Arbitration

One round is a total deterministic ordering followed by a greedy assignment.

**Tiers** (primary key, most urgent first):

| Tier | Condition |
| --- | --- |
| 0 | deadline-urgent: `deadline_tick - now <= policy.deadline_urgency_ticks` |
| 1 | starving: continuously eligible for at least `policy.starvation_bound_ticks` |
| 2 | minimum-service deficient inside its window |
| 3 | normal |

**Refinements inside a tier**, in order: priority rank (lower is more urgent), earliest
deadline, weighted-fair virtual finish stamp, larger weight, `FlowId`, generation. The
last two keys make the order strict and total, so ordering never depends on container
iteration order, allocation addresses, thread scheduling, or wall-clock time.

**Weighted fairness** uses self-clocked fair queueing with integer fixed-point stamps:
on entry to the ready set a flow is stamped `F = max(F, V) + quantum / weight`, where
`V` is the flow's fairness group clock and is monotone. A flow that was idle therefore
cannot convert a stale stamp into a burst, and service is shared in proportion to
weight. All arithmetic on the decision path is integer, so there is no floating-point
rounding anywhere.

**Minimum service** promotes a flow that has not yet received `min_service` inside its
rolling window.

**Starvation bound**: the age tier guarantees that a continuously eligible flow
outranks every normal-tier flow once it has waited `starvation_bound_ticks`. Priority is
therefore never permission for unbounded starvation.

**Service window**: the granted quantum is the smallest of the remaining work, the
flow's `service_quantum`, the QoS class bound, the policy ceiling, the resource capacity
still free *in this round*, the reservation grant still free *in this round*, and (under
a strict deadline policy) the remaining slack. A window that would be zero is never
issued; the flow is deferred with the precise reason instead.

**Preemption** happens only when the concurrency ceiling actually blocks a strictly
better candidate, and only when the incumbent is preemptible, not protected by its QoS
class, has already received `min_preempt_service`, the preemption throttle has not
fired, and the configured `PreemptionMode` authorizes it. One case is not policy at all:
a running flow that has crossed its deadline is always stopped, because executing past
the window is illegal.

Each round is emitted as a `Schedule` carrying the epoch, the policy generation, every
entry's full generation tuple, the arbitration tier, the reason, and a digest over the
canonical encoding.

## Authority and generations

A dispatch is legal only while *all* of the following still match: fabric epoch, policy
identity and generation, schedule identity and generation, flow generation, path
binding, resource generation, reservation identity and generation, QoS class generation,
priority class generation, the deadline, the reservation window end, and the flow's
lifecycle. `begin_dispatch` and `revalidate` check the whole tuple. Any mismatch returns
the specific staleness code and mutates nothing.

Advancing the fabric epoch retires every outstanding dispatch authority: open attempts
become `Abandoned`, the flows that held them become `Ambiguous`, reservations are
released, and the retained schedule and attempt history is cleared. Recovery from
durable state always advances the epoch, so a restarted coordinator can never be
confused with the one whose authority is still in flight, and worker liveness is never
restored.

## Durability

`DurableStore` keeps an append-only journal plus an atomically replaced snapshot in a
directory. Every record is length-prefixed, integrity-checked with CRC-32C, and carries
a strictly contiguous sequence number. `append` returns only after the bytes are on
stable storage, so the ordering contract is *durable before acknowledged*:

```
validate -> bind authority -> plan -> reserve -> journal -> perform -> verify
        -> commit -> retire/cleanup
```

Failure classification is deliberate:

* a **short read at the tail** is a torn write, the expected artifact of a crash during
  append. It is reported (`open_tail_truncated()`, `JournalScanReport::tail_truncated`)
  and repaired lazily before the next append;
* a **complete record with a bad checksum**, a bad magic, an unsupported format
  generation, an oversized payload, or a **sequence gap** is corruption. The store
  refuses to open rather than guessing or silently skipping records;
* a **snapshot** is written to a temporary file, flushed, and atomically renamed, so a
  reader sees either the previous snapshot or the new one.

Recovery separates durable configuration and history, committed authoritative state,
unfinished attempts, ambiguous outcomes, stale live authority, and evidence requiring
revalidation. It never restores liveness: a flow that was mid-flight becomes `Ambiguous`
and requires an explicit `resolve_ambiguous(Retry | Abandon)` before it can be
arbitrated again.

## Completion

`CompletionEvidence` binds the schedule generation, dispatch attempt, worker identity,
worker boot, epoch, flow generation, the observed outcome, the credited service, and an
externally supplied effect code and digest.

* **exact duplicate** (byte-identical evidence for an already committed attempt)
  returns `IdempotentDuplicate`: acknowledged, nothing mutates;
* **contradictory evidence** for a committed attempt is rejected;
* **closed attempt** (preempted, cancelled, abandoned) is rejected, so preempted and
  cancelled work can never complete under stale authority;
* **stale epoch, flow generation, or schedule generation** is rejected;
* **evidence from a fenced worker incarnation** (wrong worker or boot) is rejected;
* **service beyond the authorized quantum**, or service observed after the deadline, is
  rejected.

Every rejection leaves authoritative state untouched. The diagnostic counters
`completion_reports_received` and `completions_rejected` do advance, because a report
was in fact received and refused.

## Accounting

`AccountingReport::validate()` proves the books close:

* the twelve lifecycle gauges sum to `total_flows`;
* `cancelling` and `preempting` are always zero at rest - they are transient inside a
  single critical section and are normalized before any call returns;
* the seven attempt states sum to `attempts_total`;
* session completion dispositions never outnumber the reports received;
* reserved-but-unresolved service implies a scheduled window or an open attempt, and an
  open attempt always holds reserved service;
* a schedule is never issued with zero entries;
* the fabric epoch is established.

## Distributed coordinator and workers

`flow_scheduler_coordinator` and `flow_scheduler_worker` are separate OS processes
speaking a length-prefixed, CRC-checked frame protocol over TCP loopback. The handshake
binds `WorkerId` and `BootId`. A worker that dies is detected by the coordinator, its
open attempts are abandoned rather than silently completed, and its replacement takes a
new boot identity so the dead incarnation's frames are fenced.
`flow_scheduler_inspect` reads a state directory read-only, reports the journal shape,
and repairs a torn tail only when explicitly asked.

## Tools

```sh
flow_scheduler_coordinator --port 0 --workers 2 --flows 64 --state ./state
flow_scheduler_worker      --port <n> --worker 1 --boot 1
flow_scheduler_inspect     --state ./state
flow_scheduler_bench       --sweep
```

## Synthetic benchmark

`flow_scheduler_bench` measures **completed** scheduling work - flows that reached
authoritative completion - over the required dimensions: flow count, priority classes,
fairness groups, deadline density, dependency density, and concurrency.

Everything it reports is **SYNTHETIC**: the population is generated in-process by a
seeded generator, and no physical network, NIC, switch, or link is involved. The numbers
below come from one Windows x64 machine, a Release build, and are illustrative rather
than a performance contract.

```
flow_scheduler_bench 1.0.0 - SYNTHETIC workload, no physical network involved

flows  prio  groups  deadline  depends  concur  admitted  completed  dispatches  p50-wait  p99-wait  seconds
  250     4       4      0.50     0.25       8       250        206        2159     16000     74000    0.022
 2000     4       4      0.50     0.25       8      2000       1729       17429     16000   1602000    1.053
 4000     4       4      0.50     0.25       8      4000       3476       35134     16000   3280000    4.560
 2000     1       4      0.50     0.25       8      2000       1873       18468     16000    947000    1.023
 2000    16       4      0.50     0.25       8      2000       1672       16977     16000   1606000    0.920
 2000     4       1      0.50     0.25       8      2000       1718       17365     16000   1606000    0.945
 2000     4      32      0.50     0.25       8      2000       1726       17427     16000   1609000    0.903
 2000     4       4      0.00     0.25       8      2000       2000       20132     22000   1539000    1.049
 2000     4       4      1.00     0.25       8      2000       1414       15607     16000   1532000    0.804
 2000     4       4      0.50     0.00       8      2000       1699       18181     16000   1806000    0.989
 2000     4       4      0.50     1.00       8      2000        116        1180      1000      1000    0.374
 2000     4       4      0.50     0.25       1      2000       1103       11811     39000   8214000    4.195
 2000     4       4      0.50     0.25      64      2000       1992       19588     16000    870000    0.594

Column notes: deadline and depends are densities in [0,1]; concur is the resource
max_overlap; waits are ticks. Every row converged (all admitted flows reached a
terminal state). completed < admitted wherever deadline-carrying flows crossed
their window before being served, and the depends=1.00 row is small because a
dense dependency graph fails a whole chain when its root misses its deadline - the
runtime fails dependents deterministically rather than leaving them waiting forever.
Each row is one run of the Release binary on the same machine; a round ranks the
whole ready population, so per-round cost is O(ready log ready) and throughput falls
as the population grows.
```

## Validation status

**REAL** - executed against the actual artifact:

* the full unit, integration, property, adversarial, concurrency, and persistence test
  suite, built and run under MSVC `/W4 /WX /permissive-` in Debug and Release;
* real multiprocess tests: separate coordinator and worker executables, real TCP
  loopback sockets, a real durable state directory, a coordinator killed mid-run and
  restarted, and epoch advancement plus stale-epoch rejection verified across the
  restart;
* a CMake install plus an independent downstream `find_package(FlowScheduler 1.0.0)`
  consumer project configured, built, and run against the installed tree;
* a fresh clone of the tagged commit, configured and built from committed sources only.

**SYNTHETIC**:

* every benchmark figure. The workload is a seeded generator, not captured traffic and
  not a network measurement.

**UNSUPPORTED - not claimed**:

* no multi-node, multi-switch, RDMA, NVLink, optical, NIC, or DPU validation of any kind.
  This runtime never touches a network interface; it arbitrates among flows whose paths
  and reservations were computed elsewhere;
* no GPU or accelerator validation;
* no claim that the arbitration policy reproduces any specific hardware scheduler or any
  standardised QoS scheme; priority ranks and weights are supplied by the caller.

## Documented properties and limits

* **Determinism** - identical durable state and identical inputs produce schedules with
  byte-identical ordering and digests. This is asserted by comparing independent
  schedulers built in different admission orders, and over seeded randomized scenarios.
* **No flow before ready** - readiness is re-derived at the round boundary *and* again
  for every candidate during selection, so a reservation closing or a deadline crossing
  mid-round cannot leak a dispatch.
* **No illegal post-window execution** - a Run entry past its deadline is never emitted,
  a dispatch past the deadline is refused, a running flow past its deadline is stopped,
  and service observed after the deadline is not credited.
* **Starvation bound** - an eligible flow cannot wait longer than
  `starvation_bound_ticks` while lower-tier work runs, provided its resource has any
  capacity at all.
* **No double logical commit** - an attempt commits at most once, and duplicates are
  detected by evidence digest.
* **Capacity charges persist for the interval.** A preempted or abandoned attempt does
  not refund its capacity charge, because the runtime cannot prove that no service
  occurred. This can only under-utilise a resource, never over-commit it.
* **Duplicate-completion idempotency is scoped to retained authority.** Advancing the
  epoch, including a restart, retires the attempt table, so a duplicate from before the
  advance is refused as stale rather than acknowledged as a duplicate. Either way
  nothing mutates.
* **Cumulative counters.** `completions_applied` is reproduced by journal replay and is
  therefore meaningful after a restart. `completion_reports_received`,
  `completions_duplicate`, `completions_rejected`, and `arbitration_rounds` are
  session-scoped gauges. `flow_scheduler_inspect` is the authority on durable history.
* **Retention.** Retained schedule history is bounded by
  `policy.retained_schedule_history`, and a schedule with outstanding authority is never
  retired, so the bound is a floor on what is dropped rather than a promise that
  everything is kept. Completions naming a retired schedule or attempt are refused.
* **Bounded everything.** Flow, resource, reservation, class, dependency, entry,
  payload, frame, journal, snapshot, explanation, and diagnostic sizes are all bounded,
  and every externally influenced size uses checked arithmetic.
* **Single lock, no callbacks.** The runtime never invokes caller code, never emits
  events, and never acquires a second lock while holding the first, so there is no
  re-entrancy path. A lock audit found exactly one acquisition per public entry point
  and no nesting.
* **Decision history is live-only.** Per-flow explanation history is bounded and is not
  restored across a restart; the durable journal is the audit record.

## Repository layout

```
include/flow_scheduler/   public headers
src/                      library implementation
tests/                    unit, integration, property, adversarial, concurrency,
                          persistence and real multiprocess tests
tools/                    coordinator, worker, and durable-state inspector
bench/                    synthetic benchmark
cmake/                    CMake package configuration template
```

## License

Apache License 2.0. Copyright 2026 Summon Software Labs. No telemetry transmission.

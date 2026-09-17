# Node Inventory Convergence Design

**Issue:** ROCm/spur#800
**Supersedes:** PR #834 (reworked per the "Proposed direction" review, comment 5673428784)
**Date:** 2026-09-16

## Problem

`spurd` discovers node device inventory exactly once, at agent startup
(`crates/spurd/src/main.rs` → `reporter::discover_resources`). The 30s heartbeat
carries no resource payload, so a device set that changes out of band never
reaches the controller. The controller keeps scheduling against hardware that no
longer describes the node, producing dispatch failures (`gpu/resource allocation
mismatch`, `DispatchError::ResourcesUnavailable`) that count against
`max_batch_requeue` and eventually park the job for an operator.

The out-of-band change this targets is an AMD MI300X **compute-partition switch**
(SPX↔CPX via `amd-smi`): the driver tears down the current KFD topology nodes and
builds a new set — one physical GPU presents as 1 logical device in SPX and 8 in
CPX. A partition switch is only performed on a **drained, idle** node (a hardware
constraint of the GPU); no workloads run across the switch. A GPU fault or reset
that drops a device mid-run is a secondary, defensive case.

## Non-goals

- Regenerating on-disk CDI specs. On nodes provisioned with static CDI specs,
  `auto_detect` is suppressed (`CdiCache::load`), so periodic re-discovery re-reads
  unchanged files. Convergence works only on the KFD auto-detect path. Documented
  as a known limitation.
- Auth-mode re-registration hardening (label revert under `auth.mode=required`,
  expired join token, unbounded 60s warning loop). Orthogonal to convergence;
  deferred to a follow-up issue.
- Changing the wire type of `GpuResource.device_id` or any existing proto/WAL
  field. All additions are new tags / `#[serde(default)]` fields.

## Design

Adopts the four-point "Proposed direction" from the PR review: (1) stable device
identity + generation, (2) diff-and-classify rather than wholesale swap, (3) an
explicit policy for a job whose device vanishes, (4) drain gate for
operator-initiated repartitions. The resolved tradeoff is the asymmetric middle:
**adopt free-capacity changes live; require explicit-fail-or-drain for
allocated-capacity changes.**

### 1. Stable device identity + generation

Today `device_id` is a positional dense `0..N-1` counter
(`assign_device_ids`, `crates/spur-devices/src/registry/device_registry.rs:177`).
It wears two hats: the scheduler's allocation-accounting key *and* the positional
visible index handed to the GPU driver (`AllocationResult::gpu_list()` →
`ROCR_VISIBLE_DEVICES`, `crates/spur-sched/src/cons_tres.rs:276`; `SPUR_JOB_GPUS`,
`crates/spur-devices/src/inject.rs:133`). Because it is positional, removing a
non-last GPU renumbers every higher one, and re-indexing under a live allocation
mis-maps surviving jobs (an in-use GPU shown free → double-schedulable). This is
the review blocker.

**Fix — decouple the two hats.** Discovery already reads a durable KFD identity
into `KfdGpuNode` (`node_id`, `render_minor`, `unique_id`, `location_id`;
`crates/spur-devices/src/cdi/discovery.rs:50-63`) and then discards it when
collapsing to the positional id. Stop discarding it:

- Carry a **stable identity** on `GpuResource`. Use the KFD `render_minor` — the
  per-logical-device key that maps to `/dev/dri/renderD*`. Within a partition mode
  it is unique per schedulable unit; in CPX each of the N partitions is its own KFD
  topology node with its own `render_minor`. BDF is unsuitable: in CPX all N
  logical devices under one physical GPU share the same PCI BDF (confirmed in the
  AMD k8s-device-plugin, which derives `devID` from `location_id` and relies on
  that alias to copy partition metadata from the physical GPU to its XCP
  partitions — `internal/pkg/amdgpu/amdgpu.go`). `render_minor` is already read
  into `KfdGpuNode` (`crates/spur-devices/src/cdi/discovery.rs:53`) and discarded;
  stop discarding it.
- Additive proto field `GpuResource.stable_id` (new tag) and matching
  `#[serde(default)]` Rust field. `device_id` is unchanged — it stays the
  positional visible index for injection only.
- **The scheduler's allocation accounting keys on `stable_id`,** not the
  positional index. A removal becomes a set-diff by stable id; a survivor never
  renumbers, so re-indexing can never corrupt another job's accounting.

- Add `ResourceSet.generation` (additive, new tag; `#[serde(default)]`). `spurd`
  bumps it on every topology rebuild. An allocation records the generation it was
  made under; the controller rejects a dispatch whose generation is stale. This is
  needed because `render_minor` values themselves **reuse** across a mode switch
  (`renderD128` exists in both SPX and CPX but names different logical devices), so
  a repartition can present a `stable_id` that still "exists" yet means something
  else. The generation makes `stable_id` unambiguous across a rebuild: identity +
  generation together are airtight where either alone is not.

Upgrade compatibility: no field renamed/removed/retyped; no WAL variant changed.
A new controller replaying old Raft entries sees `generation`/`stable_id` default
and behaves as today.

### 2. Diff and classify — never swap

`spurd` runs a periodic re-discovery task: rebuild the device registry (re-running
KFD/CDI discovery), recompute the `ResourceSet`, and compare against the last
reported set **by stable id**. Debounce with **seen-twice**: a change must appear
on two consecutive reads before it is acted on, so a mid-transition partial read
(driver tearing down 1 KFD node and building 8) never triggers a spurious
converge. A discovery error on a tick skips the tick.

Detection is separated from policy: `refresh_once()` returns a **classified
outcome**; a small policy fn acts on it. Both are unit-testable without a live
agent.

| Delta (by stable id) | Job impact | Action |
|---|---|---|
| Device added (SPX→CPX growth) | none | adopt immediately (§2a) |
| Free device removed | none | adopt immediately (§2a) |
| Allocated device removed / repartitioned | job loses hardware | §3 |

#### 2a. Adopt (free-capacity change)

Rebuild local `NodeAllocation` capacity from the fresh `ResourceSet` (there are no
allocations on the affected devices to preserve), swap the shared device registry
so injection uses the new device set, bump `generation`, and **re-register** so
the controller converges via the existing `register_node` →
`RegistrationAction::Update` → `WalOperation::NodeUpdate` path
(`crates/spurctld/src/cluster.rs:2481`). No new controller plumbing: `NodeUpdate`
already carries a new `total_resources` and preserves `Drain`/`Down`,
`admin_locked`, and `agent_start_time`.

### 3. Policy for allocated-device changes

When a device that a job holds vanishes:

1. Emit an event **naming the stable id** (log + node/job event).
2. Fail the owning job explicitly with a device-lost reason and a **requeue
   signal**, building on the existing `NodeAllocation::reconcile()` / `owners`
   tracking (`crates/spur-sched/src/cons_tres.rs:225`), mirroring Slurm's
   `NODE_FAIL → requeue`.
3. Drop the device from capacity.

**Never free-and-reallocate a surviving device under another running job.**
Stable-id accounting (§1) makes the mis-map structurally impossible; this section
adds the explicit, observable failure so the requeue is not left to chance and the
operator gets a signal naming the device.

This is the defensive path (fault / operator skipped the required drain), not the
partition path.

### 4. Drain gate for operator-initiated repartitions

A partition switch is performed on a drained, idle node. The system enforces and
honors that: when a confirmed change affects allocated devices on a busy node,
set a **system drain** (`cluster.drain_node()` → `Draining` while jobs run,
`admin_locked = false`; reason-prefix at `crates/spurctld/src/cluster.rs:464`) so
no new work lands. Jobs finish or requeue; on the next idle tick the change
classifies as §2a and applies. `check_node_health`
(`crates/spurctld/src/cluster.rs:3294`) lifts the system drain automatically once
the cause clears, while an operator's own drain (`admin_locked = true`) stays put.
Idle nodes never drain; a node that shrank under load resumes itself.

Auto-detect reacts to change; it never yanks GPUs from running jobs.

## Data flow

```
spurd refresh task (interval)
  └─ rebuild registry → discover_resources() → fresh ResourceSet (+ stable_id, +bump gen candidate)
       └─ classify(last_reported, fresh) by stable_id, debounced seen-twice
            ├─ Added / FreeRemoved      → adopt: swap registry, bump generation, re-register (NodeUpdate)
            └─ AllocatedRemoved
                 ├─ node idle           → adopt (same as above)
                 └─ node busy           → §3 fail+requeue owning job, drop device
                                          + §4 system-drain if operator repartition under load
controller register_node (existing path)
  └─ RegistrationAction::Update → WalOperation::NodeUpdate → total_resources converges
  └─ dispatch: reject allocation whose recorded generation != node's current generation
```

## Components and boundaries

- **`crates/spur-core/src/resource.rs`** — `GpuResource.stable_id`,
  `ResourceSet.generation` (additive, defaulted). Pure data.
- **`crates/spur-devices`** — stop discarding KFD identity: thread `render_minor`
  from `KfdGpuNode` through `DeviceEntry` into `GpuResource.stable_id`. Discovery
  only.
- **`crates/spur-sched/src/cons_tres.rs`** — allocation accounting keys on
  `stable_id`; `NodeAllocation` capacity rebuild-on-adopt; `reconcile()`-based
  device-lost fail+requeue. Scheduler logic, unit-tested in isolation.
- **`crates/spurd/src/reporter.rs`** — `resources` behind a lock; `refresh_once()`
  returning a classified outcome; `discover_resources` populates `stable_id` +
  `generation`.
- **`crates/spurd/src/main.rs`** — periodic refresh task wiring detection → policy.
- **`crates/spurctld`** — generation check at dispatch; system-drain-on-busy +
  auto-recover reuse `drain_node` / `check_node_health`. `proto/slurm.proto` —
  two additive fields.

## Testing

Unit:
- Stable-id diff/classify: added, free-removed, allocated-removed each classify
  correctly; classification is independent of positional order.
- Seen-twice debounce: a one-tick change is not acted on; a two-tick change is.
- Generation mismatch: a dispatch recorded under an old generation is rejected.
- Capacity rebuild on adopt: grow and shrink rebuild cleanly; live allocations on
  unaffected devices are preserved by stable id.
- Device-lost policy: an allocated device removal fails the owning job with a
  requeue signal and drops only that device; survivors untouched.

Controller integration:
- `register_node` Update on an idle node converges `total_resources`.
- Busy node with an allocated-device change is system-drained (`admin_locked =
  false`) and auto-recovered by `check_node_health` once idle; an operator drain
  is not lifted.

Manual (documented, run on GPU testbed — not claimed as automated):
- On a drained node, flip SPX→CPX (`amd-smi`), confirm `spur scontrol show node`
  reflects the new GRES count within the refresh interval without a `spurd`
  restart, and a GPU job lands and dispatches successfully.

## Upgrade / breaking-change review

- Proto: only two new tags appended; no renumber/remove/retype. FFI/REST/wire
  compatible.
- WAL/snapshot: `stable_id` and `generation` are `#[serde(default)]`; no variant
  renamed/removed. Old entries replay cleanly.
- Config: unchanged.
- CLI/REST user surface: unchanged (GRES count is more accurate, not
  differently-shaped).

## Open follow-ups (separate issues)

- Auth-mode re-registration hardening: bounded backoff on repeated re-register
  failure; suppress startup-label resend on an inventory-only re-register.
- Static CDI spec regeneration so convergence also covers non-auto-detect nodes.
- A per-job evidence channel (agent re-collects what each running job still holds;
  controller reconciles per job) so allocated-device changes could eventually be
  adopted live rather than via drain.

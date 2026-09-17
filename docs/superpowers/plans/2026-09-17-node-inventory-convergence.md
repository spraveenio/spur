# Node Inventory Convergence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `spurd` re-discover node device inventory on an interval and converge the controller on out-of-band changes (AMD MI300X SPX↔CPX partition switches primarily), keying GPU allocation accounting on a stable identity instead of a positional index, so a device set change never corrupts surviving allocations.

**Architecture:** Thread the KFD `render_minor` (already discovered, currently discarded) through the device metadata pipeline into a new additive `GpuResource.stable_id`; add an additive `ResourceSet.generation` counter bumped on every topology rebuild. The scheduler's `NodeAllocation` keys GPU accounting on `stable_id`. `spurd` runs a periodic refresh that diffs old-vs-new by `stable_id`, classifies the delta, and either adopts a free-capacity change live (re-register → controller `NodeUpdate`) or, for an allocated-device change on a busy node, system-drains the node (auto-recovered by existing `check_node_health`). Detection (`refresh_once`) is separated from policy so both are unit-testable.

**Tech Stack:** Rust workspace (`spur-core`, `spur-devices`, `spur-sched`, `spurd`, `spurctld`), Prost/tonic gRPC (`proto/slurm.proto`), openraft WAL (serde-JSON), tokio.

**Spec:** `docs/superpowers/specs/2026-09-16-node-inventory-convergence-design.md`

## Global Constraints

- **No breaking proto/WAL/config/CLI changes.** Only append new proto tags; never renumber/remove/retype an existing field. Every new Rust field on a serde type reachable from `WalOperation` or `ResourceSet`/`GpuResource` must be `#[serde(default)]` or `Option`. (AGENTS.md "Breaking Changes".)
- **Do not change the proto package name** (`package slurm;`).
- **No `unwrap()` in library code** — use `?` or explicit handling. Test code may use `unwrap`.
- **Comments are for *why*, not *what*.** 1–3 lines max. No issue/PR numbers in comments.
- **Conventional-commit PR/commit titles:** `<type>(<scope>): <message>`, imperative, lowercase, no trailing period. Scope = crate name.
- **Validation gates** (run before declaring any task done where it compiles a crate):
  `cargo clippy --workspace --exclude spur-ffi --all-targets --locked` (no warnings) and
  `cargo test --locked` (all pass, no external services).
- **Build env:** `CC=gcc-7` is required for this workspace (gcc-9 breaks aws-lc-sys). Prefix cargo invocations with `CC=gcc-7` if the default toolchain is gcc-9. Cargo is at `~/.cargo/bin/cargo`.
- **`stable_id` concrete key:** the KFD `render_minor` (a `u32`). It is per-logical-device and repopulates on a partition switch. Values *reuse* across a mode switch — this is exactly why the generation counter exists.
- **`device_id` is unchanged** — it remains the positional visible index fed to the GPU runtime (`ROCR_VISIBLE_DEVICES` via `gpu_list()` / `SPUR_JOB_GPUS`). Only *accounting identity* moves to `stable_id`.

---

## File Structure

- `proto/slurm.proto` — add `GpuResource.stable_id` (tag 6), `ResourceSet.generation` (tag 5). Additive only.
- `crates/spur-core/src/resource.rs` — add `stable_id: u32` to `GpuResource`, `generation: u64` to `ResourceSet` (both `#[serde(default)]`).
- `crates/spur-devices/src/cdi/annotations.rs` — add `RENDER_MINOR` annotation key + `render_minor` field on `DeviceMetadata`, threaded through `from_annotations`/`to_annotations`.
- `crates/spur-devices/src/cdi/discovery.rs` — carry `render_minor` from `DiscoveredGpu` into `DeviceMetadata` (it already reads it).
- `crates/spur-devices/src/registry/entry.rs` — add `stable_id: u32` to `DeviceEntry`, populated from metadata.
- `crates/spurd/src/reporter.rs` — populate `GpuResource.stable_id`/`ResourceSet.generation` in `gpus_from_registry`/`discover_resources`; extend `resource_to_proto`; add `resources` lock + `update_resources` + `classify`/`refresh_once`.
- `crates/spurctld/src/server.rs` — extend `proto_to_resource_set` and controller-side `resource_to_proto` with the new fields.
- `crates/spur-sched/src/cons_tres.rs` — key GPU accounting on `stable_id`; add `update_capacity`; add device-lost classification helper.
- `crates/spurd/src/main.rs` — spawn the periodic refresh task wiring detection → policy.

Task order is dependency-first: data fields → discovery threading → proto conversions → scheduler accounting → agent refresh/policy → controller generation guard → docs.

---

### Task 1: Add `stable_id` and `generation` to core resource types

**Files:**
- Modify: `crates/spur-core/src/resource.rs:16-32`
- Test: `crates/spur-core/src/resource.rs` (inline `#[cfg(test)]`)

**Interfaces:**
- Produces: `GpuResource { device_id, gpu_type, memory_mb, peer_gpus, link_type, stable_id: u32 }`; `ResourceSet { cpus, memory_mb, gpus, generic, generation: u64 }`. Both new fields `#[serde(default)]`.

- [ ] **Step 1: Write the failing test** (append to the `#[cfg(test)]` module in `resource.rs`; if none exists, create one)

```rust
#[cfg(test)]
mod convergence_fields_tests {
    use super::*;

    #[test]
    fn gpu_resource_deserializes_without_stable_id() {
        // Old Raft/JSON entries have no stable_id; must default to 0.
        let json = r#"{"device_id":3,"gpu_type":"mi300x","memory_mb":196608,"peer_gpus":[],"link_type":"XGMI"}"#;
        let g: GpuResource = serde_json::from_str(json).unwrap();
        assert_eq!(g.stable_id, 0);
        assert_eq!(g.device_id, 3);
    }

    #[test]
    fn resource_set_deserializes_without_generation() {
        let json = r#"{"cpus":8,"memory_mb":1024,"gpus":[],"generic":{}}"#;
        let r: ResourceSet = serde_json::from_str(json).unwrap();
        assert_eq!(r.generation, 0);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `CC=gcc-7 cargo test -p spur-core convergence_fields_tests --locked`
Expected: FAIL to compile — `no field stable_id on GpuResource` / `no field generation on ResourceSet`.

- [ ] **Step 3: Add the fields**

In `GpuResource` (after `link_type`):
```rust
    /// Stable per-logical-device identity (KFD render_minor). Unlike `device_id`
    /// (a positional visible index for injection), this survives a mid-set
    /// removal without renumbering, so allocation accounting keys on it.
    #[serde(default)]
    pub stable_id: u32,
```
In `ResourceSet` (after `generic`):
```rust
    /// Bumped by the agent on every topology rebuild. An allocation records the
    /// generation it was made under; a repartition that reuses a stable_id value
    /// for a different logical device is caught by a generation mismatch.
    #[serde(default)]
    pub generation: u64,
```

- [ ] **Step 4: Fix any construction sites that now fail to compile**

Run: `CC=gcc-7 cargo build -p spur-core --locked` — struct literals of `GpuResource`/`ResourceSet` outside this crate are addressed in later tasks; within `spur-core` add `stable_id: 0` / `generation: 0` to any local literal the compiler flags.

- [ ] **Step 5: Run test to verify it passes**

Run: `CC=gcc-7 cargo test -p spur-core convergence_fields_tests --locked`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/spur-core/src/resource.rs
git commit -m "feat(spur-core): add stable_id and generation to resource types"
```

---

### Task 2: Add the two additive proto fields

**Files:**
- Modify: `proto/slurm.proto:16-21` (`ResourceSet`), `proto/slurm.proto:38-44` (`GpuResource`)

**Interfaces:**
- Produces: proto `GpuResource.stable_id` (tag 6, `uint32`); proto `ResourceSet.generation` (tag 5, `uint64`). Consumed by Tasks 4 & 8's conversion fns.

- [ ] **Step 1: Add the fields (no test — proto is codegen; the build is the check)**

In `message ResourceSet` (after `generic = 4`):
```proto
  uint64 generation = 5;  // bumped by the agent on each topology rebuild
```
In `message GpuResource` (after `link_type = 5`):
```proto
  uint32 stable_id = 6;   // KFD render_minor; stable per-logical-device identity
```

- [ ] **Step 2: Verify codegen compiles**

Run: `CC=gcc-7 cargo build -p spur-proto --locked`
Expected: builds; generated `GpuResource`/`ResourceSet` now expose `stable_id`/`generation`.

- [ ] **Step 3: Commit**

```bash
git add proto/slurm.proto
git commit -m "feat(proto): add GpuResource.stable_id and ResourceSet.generation"
```

---

### Task 3: Thread `render_minor` through device metadata into `DeviceEntry`

**Files:**
- Modify: `crates/spur-devices/src/cdi/annotations.rs:26-32,76-160`
- Modify: `crates/spur-devices/src/cdi/discovery.rs:285-335` (`to_cdi_device` / `DeviceMetadata` build)
- Modify: `crates/spur-devices/src/registry/entry.rs:16-37,53-` (`DeviceEntry` + `from_cdi`)
- Test: `crates/spur-devices/src/cdi/annotations.rs` (inline)

**Interfaces:**
- Consumes: `DiscoveredGpu.render_minor` (already present, `discovery.rs:272`).
- Produces: `DeviceMetadata.render_minor: Option<u32>`; `DeviceEntry.stable_id: u32`; annotation key `spur.amd.com/render-minor`.

- [ ] **Step 1: Write the failing test** (append to the annotations `#[cfg(test)]` module)

```rust
#[test]
fn render_minor_round_trips_through_annotations() {
    let meta = DeviceMetadata {
        render_minor: Some(129),
        ..DeviceMetadata::default()
    };
    let ann = meta.to_annotations();
    let back = DeviceMetadata::from_annotations(&ann);
    assert_eq!(back.render_minor, Some(129));
}
```
(If `DeviceMetadata` has no `Default`, construct it with all existing fields set explicitly instead of `..default()`.)

- [ ] **Step 2: Run test to verify it fails**

Run: `CC=gcc-7 cargo test -p spur-devices render_minor_round_trips --locked`
Expected: FAIL — `no field render_minor on DeviceMetadata`.

- [ ] **Step 3: Add the annotation key and metadata field**

In `annotations.rs` constants (near `UNIQUE_ID`):
```rust
pub const RENDER_MINOR: &str = "spur.amd.com/render-minor";
```
Add to `struct DeviceMetadata`:
```rust
    pub render_minor: Option<u32>,
```
In `from_annotations`, add:
```rust
            render_minor: annotations.get(RENDER_MINOR).and_then(|s| s.parse().ok()),
```
In `to_annotations`, add (mirroring the `unique_id` block):
```rust
        if let Some(rm) = self.render_minor {
            map.insert(RENDER_MINOR.into(), rm.to_string());
        }
```

- [ ] **Step 4: Populate it at discovery**

In `discovery.rs` `to_cdi_device`, where `DeviceMetadata { ... }` is built (around line 290), add:
```rust
            render_minor: Some(self.render_minor),
```

- [ ] **Step 5: Carry it onto `DeviceEntry`**

In `entry.rs`, add to `struct DeviceEntry`:
```rust
    pub stable_id: u32,
```
In `from_cdi`, set it from the parsed metadata (the fn already builds `meta`):
```rust
        stable_id: meta.render_minor.unwrap_or(0),
```
Fix any other `DeviceEntry { .. }` literal the compiler flags (e.g. the countable-pool builder in `device_registry.rs`) by adding `stable_id: 0`.

- [ ] **Step 6: Run tests to verify they pass**

Run: `CC=gcc-7 cargo test -p spur-devices --locked`
Expected: PASS (new test + existing).

- [ ] **Step 7: Commit**

```bash
git add crates/spur-devices/src/cdi/annotations.rs crates/spur-devices/src/cdi/discovery.rs crates/spur-devices/src/registry/entry.rs crates/spur-devices/src/registry/device_registry.rs
git commit -m "feat(spur-devices): carry KFD render_minor as device stable_id"
```

---

### Task 4: Populate `stable_id`/`generation` in agent discovery + proto conversion

**Files:**
- Modify: `crates/spurd/src/reporter.rs:254-273` (`gpus_from_registry`), `:223-234` (`discover_resources`), `:421-442` (`resource_to_proto`)
- Test: `crates/spurd/src/reporter.rs` (inline)

**Interfaces:**
- Consumes: `DeviceEntry.stable_id` (Task 3); proto `stable_id` field (Task 2).
- Produces: `GpuResource.stable_id` set from the registry entry; `resource_to_proto` copies both new fields. `discover_resources` leaves `generation: 0` (the refresh task in Task 7 owns bumping it).

- [ ] **Step 1: Write the failing test** (append to reporter `#[cfg(test)]`; reuse its `CdiSpec` fixture pattern from `test_gpus_from_registry_link_type`)

```rust
#[test]
fn gpus_from_registry_sets_stable_id_from_render_minor() {
    let spec = CdiSpec {
        cdi_version: "0.6.0".into(),
        kind: "amd.com/gpu".into(),
        annotations: Default::default(),
        devices: vec![CdiDevice {
            name: "0".into(),
            annotations: [
                (annotations::GPU_TYPE.into(), "mi300x".into()),
                (annotations::RENDER_MINOR.into(), "129".into()),
            ]
            .into(),
            container_edits: Some(ContainerEdits {
                device_nodes: vec![DeviceNode {
                    path: "/dev/dri/renderD129".into(),
                    host_path: None, r#type: None, major: None, minor: None,
                    file_mode: None, permissions: None, uid: None, gid: None,
                }],
                ..Default::default()
            }),
        }],
    };
    let mut registry = DeviceRegistry::new();
    registry.load_cdi_specs(vec![spec]);
    let gpus = gpus_from_registry(&registry);
    assert_eq!(gpus.len(), 1);
    assert_eq!(gpus[0].stable_id, 129);
}
```
(Match the exact `CdiCache`/registry-loading call the neighboring test uses — if it constructs the registry differently, copy that construction verbatim.)

- [ ] **Step 2: Run test to verify it fails**

Run: `CC=gcc-7 cargo test -p spurd gpus_from_registry_sets_stable_id --locked`
Expected: FAIL — `stable_id` is `0` (default) or `no field stable_id`.

- [ ] **Step 3: Populate in `gpus_from_registry`**

In the `GpuResource { .. }` literal (`reporter.rs:265-271`) add:
```rust
            stable_id: entry.stable_id,
```

- [ ] **Step 4: Extend `resource_to_proto`**

In the proto `GpuResource { .. }` literal (`reporter.rs:428-437`) add `stable_id: g.stable_id,`; after `generic: r.generic.clone(),` add `generation: r.generation,`.

- [ ] **Step 5: Run tests to verify they pass**

Run: `CC=gcc-7 cargo test -p spurd reporter --locked`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/spurd/src/reporter.rs
git commit -m "feat(spurd): populate GpuResource.stable_id from the device registry"
```

---

### Task 5: Key GPU allocation accounting on `stable_id`

**Files:**
- Modify: `crates/spur-sched/src/cons_tres.rs:80-99` (`allocated_gpu_ids`, `conflicting_owners`), `:122-200` (`release`, `allocate_for_job`)
- Test: `crates/spur-sched/src/cons_tres.rs` (inline)

**Interfaces:**
- Consumes: `GpuResource.stable_id` (Task 1).
- Produces: `NodeAllocation` GPU allocate/release/lookup match on `g.stable_id`; the `gpu_device_ids: &[u32]` argument to `allocate_for_job` is interpreted as **stable ids**. `AllocationResult.gpu_ids` holds stable ids. (Injection continues to use `device_id`; see note in Task 6.)

> **Design note for the implementer:** today `allocate_for_job` matches the controller-supplied ids against `g.device_id` (`cons_tres.rs:171`) and `release` against `g.device_id` (`:134`). Because the controller learns GPUs from the same discovery, the value it allocates is whatever the agent advertised. We switch the *matched field* to `stable_id` on the node side so a mid-set removal (positional `device_id` renumbers, `stable_id` does not) never mis-maps a survivor. The wire value the controller sends in `gpu_ids` is the id it selected from the advertised inventory; after Task 8 the controller selects and sends `stable_id`. Keep the change internally consistent: whichever field the node matches on for `allocate` it must also match on for `release` and `conflicting_owners`.

- [ ] **Step 1: Write the failing test** (append to the `cons_tres` test module)

```rust
#[test]
fn allocation_survives_positional_renumber_by_stable_id() {
    // Node has 3 GPUs; device_id is positional (0,1,2), stable_id is durable
    // (128,129,130). A job holds the middle GPU (stable 129). After the first
    // GPU vanishes and capacity is rebuilt with device_id renumbered (0,1) but
    // stable_id preserved (129,130), the job's GPU must still resolve.
    let gpus = vec![
        GpuResource { device_id: 0, gpu_type: "mi300x".into(), memory_mb: 0, peer_gpus: vec![], link_type: GpuLinkType::XGMI, stable_id: 128 },
        GpuResource { device_id: 1, gpu_type: "mi300x".into(), memory_mb: 0, peer_gpus: vec![], link_type: GpuLinkType::XGMI, stable_id: 129 },
        GpuResource { device_id: 2, gpu_type: "mi300x".into(), memory_mb: 0, peer_gpus: vec![], link_type: GpuLinkType::XGMI, stable_id: 130 },
    ];
    let rs = ResourceSet { cpus: 8, memory_mb: 1024, gpus, generic: Default::default(), generation: 1 };
    let mut node = NodeAllocation::new("n".into(), &rs);
    // allocate by stable id 129
    node.allocate_for_job(7, 0, 0, &[129]).unwrap();
    assert_eq!(node.free_gpus(None), 2);
    // release by the same stable id frees it
    assert!(node.release_job(7));
    assert_eq!(node.free_gpus(None), 3);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `CC=gcc-7 cargo test -p spur-sched allocation_survives_positional_renumber --locked`
Expected: FAIL — allocate matches on `device_id`, so `&[129]` is "unknown" (`GpusUnavailable`).

- [ ] **Step 3: Switch the matched field to `stable_id`**

- `allocate_for_job` (`:171`): `.position(|g| g.stable_id == id)`.
- `release` (`:134`): `.position(|g| g.stable_id == device_id)`.
- `allocated_gpu_ids` (`:86`): `.map(|g| g.stable_id)`.
- `conflicting_owners` doc/semantics unchanged (it compares against whatever ids `gpu_ids` holds — now stable ids).

- [ ] **Step 4: Update existing tests that assumed positional == stable**

The existing `test_allocate_gpus_by_device_id`, `test_record_then_release_noncontiguous_device_ids`, and `test_allocate_rejects_unknown_device_id` construct nodes via `make_node`/`make_node_with_ids`, which set only `device_id`. Update `make_node_with_ids` (`:294-307`) to set `stable_id: device_id` on each `GpuResource` so those tests keep their meaning (positional == stable in the simple case). Do not weaken their assertions.

- [ ] **Step 5: Run tests to verify all pass**

Run: `CC=gcc-7 cargo test -p spur-sched --locked`
Expected: PASS (new + existing).

- [ ] **Step 6: Commit**

```bash
git add crates/spur-sched/src/cons_tres.rs
git commit -m "fix(spur-sched): key GPU allocation accounting on stable_id"
```

---

### Task 6: Add `NodeAllocation::update_capacity` (rebuild preserving live allocations by stable_id)

**Files:**
- Modify: `crates/spur-sched/src/cons_tres.rs` (add method on `NodeAllocation`, after `new`)
- Test: `crates/spur-sched/src/cons_tres.rs` (inline)

**Interfaces:**
- Consumes: `ResourceSet` (fresh inventory), `NodeAllocation` (existing owners).
- Produces:
  ```rust
  /// Outcome of applying a fresh inventory to a node's live allocation.
  pub enum CapacityChange {
      /// Applied cleanly: cpu/mem/gpu capacity rebuilt from `fresh`; any live
      /// allocations remain valid (their stable_ids still present).
      Applied,
      /// One or more stable_ids that a job currently holds are gone in `fresh`.
      /// Capacity is NOT modified. Carries the affected (job_id, lost_stable_ids).
      AllocatedDevicesLost(Vec<(u32, Vec<u32>)>),
  }
  pub fn update_capacity(&mut self, fresh: &ResourceSet) -> CapacityChange
  ```
  Later tasks call this from the agent refresh: `Applied` → adopt; `AllocatedDevicesLost` → policy (drain/fail).

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn update_capacity_grows_and_preserves_live_allocation() {
    let mk = |ids: &[(u32,u32)]| ResourceSet {
        cpus: 8, memory_mb: 1024,
        gpus: ids.iter().map(|&(d,s)| GpuResource {
            device_id: d, gpu_type: "mi300x".into(), memory_mb: 0,
            peer_gpus: vec![], link_type: GpuLinkType::XGMI, stable_id: s,
        }).collect(),
        generic: Default::default(), generation: 1,
    };
    // start with 2 gpus (stable 128,129); job holds stable 129
    let mut node = NodeAllocation::new("n".into(), &mk(&[(0,128),(1,129)]));
    node.allocate_for_job(7, 0, 0, &[129]).unwrap();
    // grow to 4 gpus (SPX->CPX-ish); 129 still present
    let change = node.update_capacity(&mk(&[(0,128),(1,129),(2,130),(3,131)]));
    assert!(matches!(change, CapacityChange::Applied));
    // free count reflects new total minus the still-held one
    assert_eq!(node.gpus.len(), 4);
    assert_eq!(node.free_gpus(None), 3);
    // the held allocation is intact: releasing it frees exactly one
    assert!(node.release_job(7));
    assert_eq!(node.free_gpus(None), 4);
}

#[test]
fn update_capacity_reports_lost_allocated_device_without_applying() {
    let mk = |ids: &[(u32,u32)]| ResourceSet {
        cpus: 8, memory_mb: 1024,
        gpus: ids.iter().map(|&(d,s)| GpuResource {
            device_id: d, gpu_type: "mi300x".into(), memory_mb: 0,
            peer_gpus: vec![], link_type: GpuLinkType::XGMI, stable_id: s,
        }).collect(),
        generic: Default::default(), generation: 1,
    };
    let mut node = NodeAllocation::new("n".into(), &mk(&[(0,128),(1,129)]));
    node.allocate_for_job(7, 0, 0, &[129]).unwrap();
    // 129 vanishes (the device the job holds)
    let change = node.update_capacity(&mk(&[(0,128)]));
    match change {
        CapacityChange::AllocatedDevicesLost(v) => assert_eq!(v, vec![(7, vec![129])]),
        other => panic!("expected AllocatedDevicesLost, got {other:?}"),
    }
    // capacity untouched: still 2 gpus, job still holds its device
    assert_eq!(node.gpus.len(), 2);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `CC=gcc-7 cargo test -p spur-sched update_capacity --locked`
Expected: FAIL — `no method update_capacity` / `no enum CapacityChange`.

- [ ] **Step 3: Implement**

Add the enum (derive `Debug, Clone, PartialEq, Eq`) and method:
```rust
pub fn update_capacity(&mut self, fresh: &ResourceSet) -> CapacityChange {
    let fresh_ids: std::collections::HashSet<u32> =
        fresh.gpus.iter().map(|g| g.stable_id).collect();

    // Which live-owned stable ids are gone in the fresh set?
    let mut lost: Vec<(u32, Vec<u32>)> = self
        .owners
        .iter()
        .filter_map(|(job, alloc)| {
            let gone: Vec<u32> = alloc
                .gpu_ids
                .iter()
                .copied()
                .filter(|id| !fresh_ids.contains(id))
                .collect();
            (!gone.is_empty()).then_some((*job, gone))
        })
        .collect();
    if !lost.is_empty() {
        lost.sort_by_key(|(job, _)| *job);
        return CapacityChange::AllocatedDevicesLost(lost);
    }

    // Safe to rebuild: no held device disappeared. Preserve allocation bitmaps
    // by re-deriving them from owners against the new gpu table.
    self.total_cpus = fresh.cpus;
    self.allocated_cpus.resize(fresh.cpus as usize, false);
    self.total_memory_mb = fresh.memory_mb;
    self.gpus = fresh.gpus.clone();
    self.gpu_allocated = vec![false; self.gpus.len()];
    for alloc in self.owners.values() {
        for &sid in &alloc.gpu_ids {
            if let Some(idx) = self.gpus.iter().position(|g| g.stable_id == sid) {
                self.gpu_allocated[idx] = true;
            }
        }
    }
    CapacityChange::Applied
}
```
(If `owners` is private to the module, this method is in the same `impl` so it has access. `allocated_memory_mb` is intentionally preserved as-is — a shrink below live usage is impossible here because `AllocatedDevicesLost` short-circuits GPU loss, and CPU/mem shrink under load is out of scope for the partition case.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `CC=gcc-7 cargo test -p spur-sched update_capacity --locked`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/spur-sched/src/cons_tres.rs
git commit -m "feat(spur-sched): add NodeAllocation::update_capacity with stable-id diff"
```

---

### Task 7: Reporter refresh — lock resources, add `update_resources`, `classify`, `refresh_once`

**Files:**
- Modify: `crates/spurd/src/reporter.rs:30-51` (struct), `:54-78` (`new`), `:97-` (register snapshot), `:223-234` (`discover_resources`)
- Modify: `crates/spurd/src/agent_server.rs:1186`, `:2824` (read the now-locked resources)
- Test: `crates/spurd/src/reporter.rs` (inline)

**Interfaces:**
- Produces:
  - `NodeReporter.resources: std::sync::RwLock<ResourceSet>` (was a plain field).
  - `pub fn update_resources(&self, fresh: ResourceSet) -> bool` — swaps and returns whether it changed (compares ignoring `generation`; see below).
  - `pub fn snapshot_resources(&self) -> ResourceSet` — clone under the read lock (used by `register`, agent_server, and the refresh task).
  - `#[derive(Debug, Clone, PartialEq, Eq)] pub enum InventoryDelta { Unchanged, FreeCapacityChanged, AllocatedDevicesLost }` and
    `pub fn classify(old: &ResourceSet, new: &ResourceSet, allocated_stable_ids: &HashSet<u32>) -> InventoryDelta`.
- Consumes: `AllocationResult`/owners via the caller (Task 9 passes `allocated_stable_ids`).

- [ ] **Step 1: Write failing tests**

```rust
#[test]
fn classify_detects_free_growth_and_allocated_loss() {
    use std::collections::HashSet;
    let g = |d: u32, s: u32| GpuResource {
        device_id: d, gpu_type: "mi300x".into(), memory_mb: 0,
        peer_gpus: vec![], link_type: GpuLinkType::XGMI, stable_id: s,
    };
    let rs = |gpus: Vec<GpuResource>| ResourceSet {
        cpus: 8, memory_mb: 1024, gpus, generic: Default::default(), generation: 0,
    };
    let two = rs(vec![g(0,128), g(1,129)]);
    let three = rs(vec![g(0,128), g(1,129), g(2,130)]);
    let one = rs(vec![g(0,128)]);

    // unchanged
    assert_eq!(classify(&two, &two, &HashSet::new()), InventoryDelta::Unchanged);
    // grew, nothing allocated -> free capacity change
    assert_eq!(classify(&two, &three, &HashSet::new()), InventoryDelta::FreeCapacityChanged);
    // 129 removed but not allocated -> free capacity change
    assert_eq!(classify(&two, &one, &HashSet::new()), InventoryDelta::FreeCapacityChanged);
    // 129 removed AND held -> allocated loss
    let held: HashSet<u32> = [129].into_iter().collect();
    assert_eq!(classify(&two, &one, &held), InventoryDelta::AllocatedDevicesLost);
}

#[test]
fn update_resources_reports_change_ignoring_generation() {
    let base = ResourceSet { cpus: 4, memory_mb: 512, gpus: vec![], generic: Default::default(), generation: 1 };
    let reporter = test_reporter(base.clone()); // helper builds a NodeReporter with given resources
    // same content, different generation -> not a change
    let mut same = base.clone(); same.generation = 2;
    assert!(!reporter.update_resources(same));
    // different cpu count -> change
    let mut diff = base.clone(); diff.cpus = 8;
    assert!(reporter.update_resources(diff));
    assert_eq!(reporter.snapshot_resources().cpus, 8);
}
```
(Add a small `fn test_reporter(resources: ResourceSet) -> NodeReporter` in the test module constructing `NodeReporter::new(...)` with dummy args — reuse whatever the existing reporter tests use for `held_jobs`/addresses; if none exists, build a `NodeAddress` literal and an empty `Arc<dyn HeldJobs>` the same way production code does.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `CC=gcc-7 cargo test -p spurd classify_detects --locked`
Expected: FAIL — `no fn classify` / `resources` is not a lock.

- [ ] **Step 3: Put `resources` behind a lock**

Change the field to `pub resources: std::sync::RwLock<ResourceSet>`; in `new` wrap with `std::sync::RwLock::new(resources)`. Update `register` to snapshot before the await:
```rust
        let resources = resource_to_proto(&self.snapshot_resources());
```
and use `resources` in the `RegisterAgentRequest`.

- [ ] **Step 4: Add the methods**

```rust
    pub fn snapshot_resources(&self) -> ResourceSet {
        self.resources.read().unwrap().clone()
    }

    /// Swap the reported inventory if its schedulable content changed. Ignores
    /// `generation` so a pure generation bump does not itself count as a change.
    pub fn update_resources(&self, fresh: ResourceSet) -> bool {
        let mut cur = self.resources.write().unwrap();
        let changed = cur.cpus != fresh.cpus
            || cur.memory_mb != fresh.memory_mb
            || cur.gpus != fresh.gpus
            || cur.generic != fresh.generic;
        *cur = fresh;
        changed
    }
```
Add the free-function `classify` (module level, not a method):
```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InventoryDelta {
    Unchanged,
    FreeCapacityChanged,
    AllocatedDevicesLost,
}

pub fn classify(
    old: &ResourceSet,
    new: &ResourceSet,
    allocated_stable_ids: &std::collections::HashSet<u32>,
) -> InventoryDelta {
    if old.cpus == new.cpus
        && old.memory_mb == new.memory_mb
        && old.gpus == new.gpus
        && old.generic == new.generic
    {
        return InventoryDelta::Unchanged;
    }
    let new_ids: std::collections::HashSet<u32> =
        new.gpus.iter().map(|g| g.stable_id).collect();
    let lost_held = allocated_stable_ids.iter().any(|id| !new_ids.contains(id));
    if lost_held {
        InventoryDelta::AllocatedDevicesLost
    } else {
        InventoryDelta::FreeCapacityChanged
    }
}
```

- [ ] **Step 5: Fix the two agent_server read sites**

`agent_server.rs:1186` (`&reporter.resources`) → `&reporter.snapshot_resources()`. `agent_server.rs:2824` (`let resources = &self.reporter.resources;`) → `let resources = self.reporter.snapshot_resources();` (adjust the following borrow to the owned value).

- [ ] **Step 6: Run the crate tests**

Run: `CC=gcc-7 cargo test -p spurd --locked`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/spurd/src/reporter.rs crates/spurd/src/agent_server.rs
git commit -m "feat(spurd): lock reporter inventory and add classify/update_resources"
```

---

### Task 8: Controller — carry new fields through proto conversions + generation guard at dispatch

**Files:**
- Modify: `crates/spurctld/src/server.rs:4106-4126` (`proto_to_resource_set`), `:4634-` (controller `resource_to_proto`)
- Modify: `crates/spurctld/src/scheduler_loop.rs` near the dispatch device-selection (the site that builds the GPU id list sent to the agent) — send `stable_id`
- Test: `crates/spurctld/src/server.rs` (inline) for the conversion round-trip

**Interfaces:**
- Consumes: proto `stable_id`/`generation` (Task 2).
- Produces: `proto_to_resource_set` and controller `resource_to_proto` copy both fields; the id the controller allocates and sends to the agent for a GPU is the GPU's `stable_id`.

> **Implementer note:** confirm the exact dispatch site by grepping for where a node's `total_resources.gpus` are selected into an allocation id list handed to `LaunchJob` (search `scheduler_loop.rs` for `gpus` / `device_id` / `gpu_ids` near allocation building). The change is: select and transmit `g.stable_id` instead of `g.device_id`. If the controller currently stores/سends `device_id`, switching to `stable_id` keeps the node's Task 5 matching consistent. Keep `device_id` flowing for injection metadata if a separate field carries it; do not drop `device_id` from the proto.

- [ ] **Step 1: Write the failing test** (conversion round-trip in `server.rs` tests)

```rust
#[test]
fn resource_set_proto_round_trip_preserves_stable_id_and_generation() {
    use spur_core::resource::{GpuResource, GpuLinkType, ResourceSet};
    let rs = ResourceSet {
        cpus: 8, memory_mb: 1024,
        gpus: vec![GpuResource {
            device_id: 1, gpu_type: "mi300x".into(), memory_mb: 196608,
            peer_gpus: vec![], link_type: GpuLinkType::XGMI, stable_id: 129,
        }],
        generic: Default::default(), generation: 42,
    };
    let proto = super::resource_to_proto(&rs); // controller-side converter
    let back = proto_to_resource_set(proto);
    assert_eq!(back.generation, 42);
    assert_eq!(back.gpus[0].stable_id, 129);
}
```
(Match the actual name/visibility of the controller `resource_to_proto` at `server.rs:4634` — adjust the call path in the test accordingly.)

- [ ] **Step 2: Run test to verify it fails**

Run: `CC=gcc-7 cargo test -p spurctld resource_set_proto_round_trip --locked`
Expected: FAIL — fields dropped (default 0) or missing in the literal.

- [ ] **Step 3: Extend both converters**

In `proto_to_resource_set` `GpuResource { .. }` add `stable_id: g.stable_id,`; after `generic: r.generic,` add `generation: r.generation,`. Do the mirror in the controller `resource_to_proto` (add `stable_id` to the proto gpu literal and `generation` to the proto set literal).

- [ ] **Step 4: Send `stable_id` at dispatch**

At the confirmed dispatch selection site, use the GPU's `stable_id` as the allocated id transmitted to the agent. Add a focused unit test if the selection logic is a pure function; otherwise rely on the existing scheduler_loop tests plus the conversion test above and note the manual repro (Task 10) as end-to-end coverage.

- [ ] **Step 5: Run tests**

Run: `CC=gcc-7 cargo test -p spurctld --locked`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/spurctld/src/server.rs crates/spurctld/src/scheduler_loop.rs
git commit -m "feat(spurctld): carry stable_id/generation and dispatch by stable_id"
```

---

### Task 9: Agent refresh task — wire detection → policy in `main.rs`

**Files:**
- Modify: `crates/spurd/src/main.rs:347-393` (after the reporter/agent are built), add the periodic task
- Modify: `crates/spurd/src/agent_server.rs` — expose a small accessor to read the live allocation's held stable ids and to call `update_capacity` (e.g. `pub async fn allocated_stable_ids(&self) -> HashSet<u32>` and `pub async fn apply_capacity(&self, fresh: &ResourceSet) -> CapacityChange`)
- Test: `crates/spurd/src/agent_server.rs` (inline) for the two accessors

**Interfaces:**
- Consumes: `classify` (Task 7), `snapshot_resources`/`update_resources` (Task 7), `NodeAllocation::update_capacity` (Task 6), the shared `Arc<Mutex<DeviceRegistry>>` (`main.rs:349`) and `AgentService`.
- Produces: a spawned 60s task implementing:
  1. rebuild registry (re-run discovery), `discover_resources`, compute a candidate fresh set with `generation = current + 1`.
  2. `classify(old, fresh, allocated_stable_ids)`; **seen-twice debounce**: only act when the same non-`Unchanged` delta is observed on two consecutive ticks.
  3. `FreeCapacityChanged` → `apply_capacity` (expects `Applied`), swap the shared registry, `update_resources`, `reporter.register()` (converge controller).
  4. `AllocatedDevicesLost` → log naming the lost stable ids; the controller-side drain/fail is driven by the re-register carrying the change (defensive path). Do **not** apply capacity.

- [ ] **Step 1: Write the failing tests** for the accessors

```rust
#[tokio::test]
async fn allocated_stable_ids_reflects_live_allocation() {
    let svc = test_agent_service_with_gpus(&[(0,128),(1,129)]); // helper: builds AgentService with a 2-gpu node
    svc.allocation.lock().await.allocate_for_job(7, 0, 0, &[129]).unwrap();
    let ids = svc.allocated_stable_ids().await;
    assert!(ids.contains(&129) && ids.len() == 1);
}

#[tokio::test]
async fn apply_capacity_grows_when_idle() {
    let svc = test_agent_service_with_gpus(&[(0,128),(1,129)]);
    let fresh = resource_set_with_gpus(&[(0,128),(1,129),(2,130),(3,131)]);
    assert!(matches!(svc.apply_capacity(&fresh).await, CapacityChange::Applied));
    assert_eq!(svc.allocation.lock().await.gpus.len(), 4);
}
```
(Use the crate's existing agent-service test constructor if present; otherwise add `test_agent_service_with_gpus` building the minimal `AgentService` the same way other `agent_server` tests do. If no such harness exists, keep these two accessors trivially thin and test `update_capacity` behavior at the `spur-sched` layer only, and mark these as integration-covered by Task 10 — do not fabricate a harness that simulates the unit.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `CC=gcc-7 cargo test -p spurd allocated_stable_ids --locked`
Expected: FAIL — accessors don't exist.

- [ ] **Step 3: Add the accessors** on `AgentService`

```rust
    pub async fn allocated_stable_ids(&self) -> std::collections::HashSet<u32> {
        self.allocation.lock().await.allocated_gpu_ids().into_iter().collect()
    }

    pub async fn apply_capacity(&self, fresh: &spur_core::resource::ResourceSet)
        -> spur_sched::cons_tres::CapacityChange {
        self.allocation.lock().await.update_capacity(fresh)
    }
```

- [ ] **Step 4: Spawn the refresh task** in `main.rs` after `agent_service` is constructed (it needs the registry `Arc` and the service). Reuse the existing `registry` clone and a `reporter` clone:

```rust
    // Periodic inventory re-discovery: converge the controller on out-of-band
    // device changes (e.g. an MI300X SPX/CPX partition switch on a drained node)
    // without a spurd restart. Detection is debounced (seen-twice) so a
    // mid-transition partial read never triggers a spurious converge.
    {
        let registry = registry.clone();
        let reporter = reporter.clone();
        let service = agent_service.handle_for_refresh(); // Arc-like handle; see note
        tokio::spawn(async move {
            let mut last_delta = reporter::InventoryDelta::Unchanged;
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            interval.tick().await; // consume immediate first tick
            loop {
                interval.tick().await;
                let fresh = {
                    let mut reg = registry.lock().await;
                    reg.rediscover(); // re-run CDI/KFD discovery in place; see note
                    let mut rs = reporter::discover_resources(&reg);
                    rs.generation = reporter.snapshot_resources().generation + 1;
                    rs
                };
                let held = service.allocated_stable_ids().await;
                let old = reporter.snapshot_resources();
                let delta = reporter::classify(&old, &fresh, &held);
                if delta == reporter::InventoryDelta::Unchanged {
                    last_delta = delta;
                    continue;
                }
                // seen-twice debounce
                if delta != last_delta {
                    last_delta = delta;
                    continue;
                }
                match delta {
                    reporter::InventoryDelta::FreeCapacityChanged => {
                        match service.apply_capacity(&fresh).await {
                            spur_sched::cons_tres::CapacityChange::Applied => {
                                reporter.update_resources(fresh);
                                if let Err(e) = reporter.register().await {
                                    warn!(error = %e, "inventory re-register failed; will retry next tick");
                                    last_delta = reporter::InventoryDelta::Unchanged;
                                }
                            }
                            spur_sched::cons_tres::CapacityChange::AllocatedDevicesLost(v) => {
                                warn!(?v, "inventory change raced an allocation; deferring");
                            }
                        }
                    }
                    reporter::InventoryDelta::AllocatedDevicesLost => {
                        warn!("allocated device vanished; controller will drain via re-register");
                        reporter.update_resources(fresh);
                        let _ = reporter.register().await;
                    }
                    reporter::InventoryDelta::Unchanged => {}
                }
                last_delta = reporter::InventoryDelta::Unchanged;
            }
        });
    }
```

> **Implementer notes (resolve against real APIs, do not invent):**
> - `handle_for_refresh()` is a stand-in: if `agent_service` is already wrapped in an `Arc` (check `main.rs:441-` construction and how it's passed to the gRPC server), clone that `Arc` and drop this helper. If it is owned/moved into `serve`, add a `Clone`-able handle holding `allocation: Arc<Mutex<NodeAllocation>>` + `reporter` before it is moved. Prefer the smallest change that gives the task access to `allocated_stable_ids`/`apply_capacity`.
> - `rediscover()` is a stand-in for "rebuild the registry from a fresh discovery." Check `init_device_registry` (`main.rs:531`) and `DeviceRegistry` for an existing rebuild/reload entry point; if none exists, build a fresh `DeviceRegistry` via the same path `init_device_registry` uses and swap `*reg = fresh_registry`. Do not partially mutate.
> - Keep the interval a named const; wire it to config only if a config field already exists (do not add one in this plan).

- [ ] **Step 5: Run the crate tests + clippy**

Run: `CC=gcc-7 cargo test -p spurd --locked && CC=gcc-7 cargo clippy -p spurd --all-targets --locked`
Expected: PASS, no warnings.

- [ ] **Step 6: Commit**

```bash
git add crates/spurd/src/main.rs crates/spurd/src/agent_server.rs
git commit -m "feat(spurd): periodic inventory refresh with debounced classify-and-converge"
```

---

### Task 10: Docs + workspace validation + follow-up issue

**Files:**
- Modify: the docs page describing node registration / GPU inventory (find it: `rg -l "register|inventory|gres|scontrol show node" docs/`). Add a short subsection: "Node inventory convergence — spurd re-discovers device inventory periodically and re-registers on change; partition switches must be done on a drained node; static CDI-spec nodes only converge if the spec is regenerated."
- Create: nothing new unless the docs structure needs a page.

**Interfaces:** none (docs + validation).

- [ ] **Step 1: Update the docs page** with the subsection above, matching the surrounding page's tone and depth. Note the known limitation (static CDI specs) and the drain requirement explicitly.

- [ ] **Step 2: Full workspace validation**

Run: `CC=gcc-7 cargo clippy --workspace --exclude spur-ffi --all-targets --locked`
Expected: no warnings.
Run: `CC=gcc-7 cargo test --locked`
Expected: all pass.

- [ ] **Step 3: File the deferred follow-up issue** (auth-mode re-register hardening)

```bash
gh issue create --repo ROCm/spur \
  --title "spurd re-registration under non-default auth: label revert, token expiry, unbounded retry" \
  --body "$(cat <<'EOF'
When spurd re-registers to converge a changed device inventory (see #800 convergence work), re-registration replays the startup RegisterAgentRequest. On non-default auth clusters this has two rough edges:

- Under auth.mode=required, the request resends startup labels; an admin relabel is either silently reverted (permissive) or fails PermissionDenied, and the failure repeats every refresh interval with no backoff.
- On a token-mode cluster an expired join token makes re-registration permanently fail.

Expected: an inventory-only re-register should not resend startup labels, and repeated re-register failure should back off rather than warn on every tick.

Reproduce: set auth.mode=required (or token mode with a short-lived token), change a node's GPU partition mode out of band on a drained node, and observe the re-register attempts in spurd logs.
EOF
)"
```

- [ ] **Step 4: Commit docs**

```bash
git add docs/
git commit -m "docs(spurd): document node inventory convergence and its limits"
```

---

## Self-Review

**Spec coverage:**
- Stable identity (spec §1) → Tasks 1–5, 8.
- Generation counter (spec §1) → Tasks 1, 2, 4, 8; bumped in Task 9.
- Diff-and-classify, never swap (spec §2) → Tasks 6 (`update_capacity` diff), 7 (`classify`), 9 (wiring + seen-twice debounce).
- Adopt free-capacity live (spec §2a) → Task 9 `FreeCapacityChanged` branch → `register`.
- Policy for allocated-device loss (spec §3) → Tasks 6 (`AllocatedDevicesLost`), 9 (defensive branch + re-register so controller drains).
- Drain gate + auto-recover (spec §4) → reuses existing `drain_node`/`check_node_health`; driven by the re-register carrying the change (Task 9). No new controller plumbing, per spec.
- Static-CDI limitation, auth follow-up (spec non-goals) → Task 10 docs + follow-up issue.
- Upgrade-clean (spec §"Upgrade / breaking-change review") → Task 1 default tests, additive proto (Task 2).

**Known coverage gap made explicit:** the controller-side *generation guard at dispatch* (reject a dispatch whose recorded generation is stale) is scoped in spec §1 but only partially realized here — Task 8 transmits `generation`/`stable_id` and Task 9 bumps it, but a hard reject at dispatch on generation mismatch depends on where the controller records an allocation's generation, which is not yet pinned to a single site in this plan. Executor: during Task 8, grep the dispatch/allocation-record path; if a natural site exists to stamp+check generation, add a Task 8b with a unit test; if it requires a WAL/proto allocation-record change, STOP and surface it for a scope decision rather than forcing it. The stable-id accounting (Task 5) already removes the corruption blocker independently of the generation guard, so shipping without 8b is safe but less airtight against the same-tick reuse edge.

**Placeholder scan:** no TBD/TODO; every code step has concrete code. Two `handle_for_refresh()`/`rediscover()` stand-ins are explicitly flagged with resolution instructions against real APIs (not silent placeholders) because the exact handle/rebuild entry point must be read from code at execution time.

**Type consistency:** `stable_id: u32`, `generation: u64`, `CapacityChange`, `InventoryDelta`, `classify(old,new,allocated_stable_ids)`, `update_capacity(&mut self,&ResourceSet)->CapacityChange`, `snapshot_resources`, `update_resources`, `allocated_stable_ids`, `apply_capacity` are used consistently across Tasks 1–9.

# Spur Federation Controller Architecture and Delivery Plan

Status: proposed; implementation requires separate approval.

## A. Existing architecture and reusable extension points

### Current control plane

Spur is already divided along the boundary that federation needs:

- `spurctld` owns scheduling and the authoritative cluster state. Every controller,
  including a single controller, participates in a site-local OpenRaft group.
- `spurd` is a node agent. It is not an appropriate federation integration point
  because it does not own the cluster-wide view or controller leadership.
- `spurstepd` is deliberately isolated to supervising one job or step and must not
  acquire federation responsibilities.
- `spur-cli`, the REST compatibility layer, and the C FFI are user-facing Slurm
  compatibility surfaces. Federation must not change their existing wire contracts.
- Accounting runs inside `spurctld` but persists to PostgreSQL. Its connection,
  retry, reconciliation, bounded-query, and audit patterns are useful references
  for the federation store.

`ClusterManager` in `crates/spurctld/src/cluster.rs` is the correct site-side
boundary for federated operations. Its public methods propose `WalOperation`
entries and therefore preserve the site's normal validation, authorization, and
Raft commit path. Federation must never mutate in-memory state directly.

`crates/spurctld/src/raft.rs` exposes leadership and committed state. A federation
client should run only on the site leader, stop or fence itself when leadership is
lost, and publish data derived from committed state. The federation controller
must never join this Raft group, replicate its log, or write its snapshots.

### Existing federation placeholder

The current `[federation]` configuration contains direct peer names and controller
addresses. `scheduler_loop.rs` attempts to forward unschedulable jobs directly to
those peers. This is a prototype, not a foundation for the central controller:

- it requires site-to-site inbound reachability;
- it has no stable site identity, tenant boundary, credential lifecycle, or
  protocol negotiation;
- forwarding is not durably idempotent and may be retried after partial failure;
- it couples federation policy to the local scheduling loop;
- it cannot provide a coherent global audit trail.

Keep the existing fields readable during a deprecation window, but do not extend
this protocol. The new design uses an additive client configuration and a separate
federation protocol. Removing or changing the old fields would be a configuration
compatibility break and requires a separately announced migration.

### Authentication and authorization

`spur-core/src/auth.rs`, `spurctld/src/auth_middleware.rs`, and
`spurctld/src/rpc_middleware.rs` provide bearer authentication and native Spur
credential verification. `spur-core/src/rbac.rs` provides site-local roles and
authorization helpers. These are reusable for the final local authorization check,
but they do not supply enterprise OIDC, federation tenants, per-site mTLS identity,
or centrally administered scope bindings.

Federation therefore needs a distinct identity plane. It must not reuse one shared
JWT signing key or one static secret across sites, and it must not treat a
federation administrator as an implicit site root user.

### Networking

`spur-net` already handles IPv4/IPv6-safe address normalization and formatting in
`comm_addr.rs`, and WireGuard key/config/peer lifecycle in `wireguard.rs` and
`mesh.rs`. Reuse those primitives when the federation tooling manages a WireGuard
peer. Do not reuse the compute-node mesh membership as the federation identity or
assume its CIDR is available at every site.

WireGuard is one way to reach the federation endpoint. It does not replace
application authentication, authorization, or TLS.

### API and persistence

The existing Slurm protobuf package in `proto/slurm.proto` is public and feeds the
REST and FFI compatibility layers. Federation messages belong in a new
`proto/federation.proto` package so federation evolution cannot renumber, rename,
or reinterpret Slurm fields.

The current Axum layer is intentionally a small Slurm-compatible REST API. Its
state/layer/test organization can be reused, but the federation management API
must have its own versioned namespace and response models.

The accounting database is not the federation database. Federation should reuse
its `sqlx` operational patterns while owning migrations, tables, retention, and
availability independently.

## B. Target architecture

```
 administrators / automation / web UI
                  |
             OIDC + HTTPS
                  v
       +---------------------------+
       | federation API replicas   |
       | API, RBAC, audit, policy  |
       | session and event service |
       +-------------+-------------+
                     |
                 PostgreSQL
                     |
       +-------------+-------------+
       |             |             |
       | established outbound sessions
       v             v             v
   site A leader  site B leader  site C leader
   site Raft      site Raft      site Raft
```

The product consists of:

1. A new `spur-federation-controller` binary. Replicas are stateless apart from
   bounded in-memory connection state; durable state is in PostgreSQL.
2. A lightweight federation client task inside `spurctld`.
3. A separate bidirectional gRPC protocol initiated by the site.
4. A versioned management API and web UI using OIDC and federation RBAC.
5. Deployment and lifecycle automation in `ROCm/spur-toolkit`.

The federation controller is not a scheduler replica and not a member of a site's
Raft quorum. It observes sites and submits bounded operations through their normal
leader path. A site remains schedulable and administrable when federation,
PostgreSQL, DNS, or the wide-area network is unavailable.

### Why the client belongs in `spurctld`

Embedding a small client task in `spurctld` is preferable to a separate agent:

- `spurctld` already knows which replica is leader and has a committed cluster
  view.
- Operations can call `ClusterManager` without exposing another privileged local
  API or duplicating local authentication.
- Site identity, event offsets, and command results can follow controller
  leadership with less machinery.
- The task is idle or disconnected most of the time and does not justify another
  service, package, health model, or upgrade sequence.

A tiny external agent would improve process isolation, but it would require a
new authenticated local API, leader discovery, local credential distribution,
and another failure domain. That cost outweighs the isolation benefit for this
control-plane-only task. The client should be a focused module with bounded
queues and no scheduling logic so it can be split later without changing the
wire protocol.

## C. Isolation, identity, and authorization

### Site and tenant identity

A site has a generated immutable `site_id`; IP addresses, DNS names, WireGuard
addresses, display name, and organization membership are mutable attributes.
Every site belongs to exactly one tenant. Cross-tenant reads and writes are denied
in both API authorization and database query construction.

Each site receives a unique credential. No credential is valid for another site.
The authenticated certificate identity is matched against the registered
`tenant_id` and `site_id` before a session becomes active.

### Enrollment

Enrollment is a two-step flow:

1. A tenant administrator creates a pending site registration and receives a
   short-lived, single-use bootstrap token or an approved certificate signing
   request workflow.
2. The site authenticates to the enrollment endpoint, proves possession of its
   private key, and receives a site certificate plus the trust bundle.

Bootstrap material is stored hashed, has an expiry and use count, and is never a
long-lived session credential. Enrollment is separately rate-limited and audited.
Production deployments should integrate with an external CA. Development
self-signed credentials must be explicit and visibly marked.

### mTLS lifecycle

- Certificates bind tenant, site, key identifier, permitted use, and expiry.
- Private keys are generated and retained at the site; normal enrollment submits
  a CSR rather than exporting the key.
- Rotation starts before expiry, proves possession using the current credential,
  and supports a bounded overlap of old and new certificates.
- Revocation immediately prevents new sessions and fences an existing session.
- Credential metadata and revocation status are durable and auditable.
- Trust-bundle rotation supports overlapping roots and an explicit retirement
  deadline.

WireGuard keys and mTLS keys are different credentials with independent rotation.

### Human and service identity

The management API uses OIDC Authorization Code flow with PKCE for browser users.
The controller validates issuer, audience, signature, expiry, and nonce and maps
stable subject/group claims into federation identities. Automation uses narrowly
scoped service credentials; it does not borrow browser sessions or site
certificates.

The federation controller owns role bindings scoped to tenant, site, and resource.
Initial roles should be:

- federation administrator;
- tenant administrator;
- site operator;
- auditor;
- read-only viewer;
- workload submitter.

Permissions are action based rather than inferred only from role names. Sensitive
operations require explicit site scope. Default is deny.

### Defense in depth for site operations

The federation controller performs the first RBAC and policy decision. The site
performs a second authorization decision against an allowlist of federated
operations and local policy before proposing a change. A central role never
bypasses local safety checks, ownership checks, admission hooks, resource limits,
or Raft.

Every command carries the initiating principal, tenant, authorization decision,
policy version, request ID, deadline, and idempotency key. The site records this
attribution with the result. Secrets and bearer credentials are excluded from
logs, events, diagnostics, and audit payloads.

## D. Pull session, protocol, and transport

### Session shape

Add a dedicated bidirectional streaming gRPC service, conceptually:

```
rpc Connect(stream SiteToFederation) returns (stream FederationToSite)
```

Only the site dials. Once authenticated, the controller can send commands on the
existing stream. No site API port needs to be exposed to the federation network,
and NAT mappings remain alive through heartbeats.

The first site message is `Hello`, containing:

- site and tenant identity;
- Spur and federation protocol versions;
- controller identity, term, leadership epoch, and committed revision;
- supported capabilities and command versions;
- transport metadata;
- certificate key identifier;
- the last durable event and command acknowledgements.

The controller replies with `Welcome`, containing:

- negotiated protocol version and capabilities;
- session ID and monotonic fencing token;
- heartbeat and stale thresholds;
- controller time;
- requested snapshot/event resume point;
- credential rotation or policy metadata when applicable.

Only one unfenced active session may issue commands for a site. A PostgreSQL
compare-and-set lease chooses the owning controller replica. A newer leadership
epoch or session fencing token invalidates an older stream.

### Envelope and compatibility

Every message envelope includes protocol version, site ID, session ID, fencing
token, sequence number, correlation ID, timestamp, and payload type. Commands also
include operation ID, idempotency key, deadline, actor, and policy context.

Initial payloads:

- hello, welcome, heartbeat, goodbye;
- inventory snapshot and snapshot request;
- ordered event batch and event acknowledgement;
- command, command acknowledgement, progress, and terminal result;
- policy/config metadata;
- credential rotation notice and proof;
- protocol error and resynchronization request.

Protocol negotiation selects the highest mutually supported version from declared
ranges. Additive protobuf fields and new optional capabilities are preferred.
Unknown optional messages are ignored; unknown required capabilities reject the
session with a clear compatibility error. Protobuf tags are never reused.

Federation release metadata declares the supported site-version, protocol, config,
and database-schema ranges. Mixed compatible site versions are a normal state.

### Delivery semantics

Site events are at least once:

- each event has a stable `(site_id, epoch, sequence)` identity;
- the controller persists an event and its offset in one transaction before
  acknowledging it;
- duplicates are harmless because the identity is unique;
- gaps cause replay or a full snapshot request;
- the site retains a bounded durable outbox until acknowledgement.

Commands are also at least once:

- the controller persists the operation before delivery;
- the site durably records accepted idempotency keys through its Raft-backed state
  before executing a mutating operation;
- reconnecting resends only unacknowledged operations;
- a duplicate returns the original terminal result;
- expired, revoked, fenced, or unsupported commands fail without mutation.

Terminal results are immutable. Cancellation is a new operation, not deletion of
history. Read-only queries may use a shorter non-durable path when stale data is
acceptable, but must still be correlated and bounded.

### Reconnection and health

The client reconnects forever while enabled using exponential backoff with full
jitter, a configured maximum, and reset after a stable session. DNS is resolved
again on reconnect. IPv4 and IPv6 connection attempts use normal dual-stack
behavior without embedding an address family in the protocol.

Heartbeats report leadership, committed revision, queue depth, health summary,
clock offset, and last processed command/event sequence. The controller derives
`connected`, `degraded`, `stale`, and `offline` states from configured thresholds;
these states are not supplied by the site as authoritative truth.

Backpressure is explicit. Inventory snapshots, event batches, and command queues
have byte/count limits. A slow federation service may delay telemetry but must not
consume unbounded site memory or block the scheduler, Raft apply, accounting, or
site APIs.

### Transport abstraction

The application session uses one transport-neutral gRPC contract. A narrow
connector abstraction should expose connection establishment and metadata, not
reimplement framing:

- IPv4 TCP with TLS;
- IPv6 TCP with TLS;
- TCP with TLS routed over a WireGuard interface.

The selected transport is recorded in site/session metadata. All modes use mTLS;
WireGuard is an additional network boundary, not an authentication exemption.

For WireGuard:

- accept fixed `wg_address` and an optional explicitly configured endpoint;
- permit either IPv4 or IPv6 tunnel addresses;
- reuse `spur-net` parsing and durable peer helpers where management is requested;
- allow an externally managed interface without modifying it;
- do not allocate addresses or assume a federation-wide CIDR;
- do not add a site's compute mesh routes to another site;
- reject ambiguous or overlapping routes during preflight when the toolkit is
  responsible for route management.

## E. Persistent data model

PostgreSQL is the source of truth for federation state. IDs are UUIDs internally;
stable external IDs may also be stored where human-readable names are needed.
Tables should include creation/update timestamps and tenant keys where applicable.

### Core records

`organizations`
: tenant identity, display name, status, and policy defaults.

`sites`
: `site_id`, tenant, unique name, description, location/region, environment,
  Spur version, negotiated protocol, controller count, reported/derived health,
  last seen, connection/federation status, transport, capabilities, labels,
  registration time, and latest committed revision.

`site_endpoints`
: typed IPv4, IPv6, DNS, and WireGuard metadata. Endpoints are descriptive for
  pull sessions and diagnostics, not permission for the controller to dial a site.

`site_credentials`
: certificate/key identifiers, issuer, serial/fingerprint, validity interval,
  rotation relationship, status, and revocation metadata. Private keys are never
  stored.

`site_sessions`
: controller replica owner, session/fencing ID, connection times, heartbeat,
  leadership epoch, transport, peer metadata, and termination reason.

### State and event records

`site_snapshots`
: latest normalized inventory revision and snapshot metadata. Large inventories
  may be split into versioned site, controller, partition, node, and aggregate
  resource tables.

`site_events`
: immutable deduplicated event envelope, classification, source revision, and
  retention partition. Payloads are schema-versioned and filtered to avoid
  secrets or unnecessary user data.

`site_event_offsets`
: last contiguous persisted and acknowledged sequence per site/epoch.

`operations`
: requested action, actor, target, parameters or safe digest, idempotency key,
  policy decision, state, deadline, owning session, and terminal result.

`operation_attempts`
: each delivery/acknowledgement/result transition for troubleshooting without
  changing the operation's identity.

### Identity, policy, and audit

`principals`, `groups`, `roles`, `permissions`, and `role_bindings`
: OIDC/service identity mapping and scoped RBAC.

`policies` and `policy_versions`
: immutable evaluated versions with activation history.

`audit_events`
: append-only actor, action, scope, target, request/correlation ID, outcome,
  reason, source address, and safe before/after references.

`schema_history`
: database migration and compatibility metadata.

Use database constraints for tenant ownership, stable event IDs, unique
idempotency keys, credential serials, and one current site-session lease. Apply
row-level security where it adds defense in depth, while retaining explicit
tenant predicates in application queries.

Migrations are ordered, transactional where PostgreSQL permits, and safe for
concurrent controller startup. Schema changes follow expand/migrate/contract so
rolling versions can coexist. Retention policies are explicit for events and
audit, with audit defaulting to retained rather than silently purged.

## F. Control and failure flows

### Registration and first connection

1. An authorized administrator creates a pending site with tenant and policy.
2. The site generates its key and exchanges one-time enrollment proof for a
   certificate.
3. The current site leader connects outbound and sends `Hello`.
4. The federation replica authenticates and atomically acquires the site session.
5. Versions and capabilities are negotiated.
6. The controller requests resume from a durable offset or a full snapshot.
7. The site becomes healthy only after snapshot/event convergence and heartbeats.

### Inventory and event convergence

Initial sync sends a consistent snapshot tied to a committed Raft revision.
Subsequent state changes produce ordered, filtered events after commit. Reconnect
resumes from the controller's persisted offset. If the site's retained outbox no
longer covers that offset, the controller discards only its derived cache and
requests a new snapshot; registrations, credentials, policy, audit, and operation
history remain intact.

### Federated operation

1. The API authenticates the caller and resolves tenant/site scope.
2. RBAC and policy authorize a typed operation.
3. The controller stores the operation and audit intent transactionally.
4. If the site is online, its session receives the command; otherwise the
   operation remains pending only when the action is explicitly queueable.
5. The site validates fencing, deadline, capability, local authorization, and
   idempotency.
6. A mutating command enters the site's existing `ClusterManager`/Raft path.
7. The committed result is returned and persisted, then exposed to the caller.

Each operation type declares whether it is queueable offline, its expiry,
idempotency scope, cancellation behavior, and required local permission. Dangerous
or inherently non-idempotent actions are never queued by default.

### Failure cases

- **Federation unavailable:** the site drops or durably buffers bounded telemetry
  and continues normal local operation.
- **Site unavailable:** cached inventory is marked stale; queued operations retain
  deadlines and are never reported as applied.
- **Site leader changes:** the old leader's stream is fenced; the new leader
  reconnects with a newer epoch and resumes offsets.
- **Federation replica fails:** PostgreSQL lease expiry lets another replica own
  the next session; the site reconnects through the load balancer.
- **Duplicate event/command:** unique identities and idempotent results prevent
  duplicate effects.
- **Database unavailable:** replicas fail closed for mutation, keep bounded
  connections only if safe, and advertise not-ready. Sites remain independent.
- **Version mismatch:** session remains rejected or read-only according to the
  compatibility matrix; no unsupported command is sent.
- **Credential compromise:** revocation fences sessions and blocks reconnect;
  site-local operation remains available.
- **Clock skew:** deadlines use bounded skew checks and server-issued session
  timing; large skew degrades health and rejects unsafe mutations.
- **Network partition:** neither side assumes a lost response means failure;
  reconciliation queries the durable operation by idempotency key.

## G. Management API, CLI, and web experience

### Federation API

Expose a separately versioned API such as `/api/v1`, not a Slurm REST version.
Initial resources:

- organizations, principals, groups, roles, and bindings;
- sites, registration, credentials, labels, capabilities, health, and inventory;
- operations and terminal results;
- policies and policy versions;
- events and audit queries;
- version, compatibility, readiness, and diagnostics.

List APIs require pagination, stable ordering, server-side filtering, bounded page
sizes, and tenant scoping. Mutating APIs accept idempotency keys. Error responses
carry stable codes, correlation IDs, and actionable compatibility or policy
reasons without exposing secrets.

The gRPC site-session listener and HTTPS management listener may use different
ports and certificate policies. Public health endpoints disclose only aggregate
readiness; detailed health requires authorization.

### CLI responsibility

Federation business administration may be added to `spur-cli` as versioned API
calls, for example listing sites or inspecting an operation. Site-local Slurm
commands retain their current behavior.

Installation and lifecycle commands belong to `ROCm/spur-toolkit`, not
`spur-cli` or the controller binary. The toolkit should follow its existing
command naming after that repository is inspected. This plan uses
`spur federation ...` only as the desired administrator experience, not as a
prescription for an unverified toolkit layout.

### Web UI

The first UI should be a thin client of the same API and OIDC/RBAC rules. It
provides tenant/site inventory, health and staleness, operation progress,
credential expiry, policy decisions, and audit search. The UI must not have
private administration endpoints or embed OIDC client secrets.

## H. Configuration

### Site configuration in Spur

Continue using `/etc/spur/spur.conf` TOML and additive/defaulted fields. A new
section should contain:

- enabled flag;
- stable site ID and federation endpoint;
- preferred transport and optional fixed WireGuard address/interface metadata;
- certificate, key, and trust-bundle file paths;
- server name and expected federation identity;
- protocol range;
- heartbeat, reconnect, queue, and snapshot limits;
- enrollment configuration used only until a permanent credential exists.

Secrets are referenced by file and loaded with permission checks. Configuration
must not contain a shared fleet credential. Old `[[federation.clusters]]` entries
continue to parse until their separately documented retirement.

### Controller deployment configuration

`spur-toolkit` owns a declarative desired-state YAML document:

```
apiVersion: spur.rocm.dev/v1
kind: FederationController
metadata:
  name: production-federation
```

Its schema covers deployment mode, explicit artifact version/digest, replica
count, management and session listeners, PostgreSQL, OIDC, TLS/mTLS, registration,
audit retention, backups, health policy, and optional externally managed
WireGuard. Passwords, keys, and OIDC client secrets are file or secret-provider
references by default.

The toolkit validates and renders a smaller runtime configuration for the Rust
binary. The public desired-state schema and binary runtime schema have explicit,
independent versions. This keeps orchestration logic out of the controller and
business logic out of the toolkit.

Configuration migration is additive where possible. `config validate` reports
unknown fields, incompatible combinations, missing secret files, unsafe
development TLS in production mode, and version/schema incompatibility.
`config migrate` writes a reviewable new document without overwriting the source
unless explicitly requested.

## I. High availability, lifecycle, and release contract

### Runtime HA

Production uses at least three stateless controller replicas behind a
dual-stack-capable load balancer and an HA PostgreSQL service. PostgreSQL is the
coordination and durable state system; the federation replicas do not form or
join a site Raft group. Replica health reports database connectivity, migration
compatibility, OIDC/JWKS freshness, TLS validity, session service readiness,
event lag, and owned sessions.

Site sessions reconnect automatically during rolling replacement. Session leases
and fencing guarantee that two replicas cannot concurrently command one site.
The load balancer must use connection draining but must not require sticky
sessions after reconnect.

### Version contract

Every immutable release artifact contains or accompanies:

- controller semantic version and source revision;
- supported federation protocol range;
- database schema version and migration set;
- runtime and desired-state configuration schema versions;
- supported Spur site-version range;
- artifact digest/signature;
- upgrade edges, downgrade edges, and irreversible migration declarations;
- release and operator notes.

The API and `version`/`status` output expose installed version, protocol, database
schema, config schema, replica health, and connected-site compatibility. Never use
`latest` as the only deployment reference.

### Installation and idempotence

All deployment, configuration rendering, package/container installation, systemd
units, certificates, secret wiring, validation, and lifecycle automation are
implemented in `ROCm/spur-toolkit`. Reuse that repository's Ansible roles,
inventory, artifact handling, service management, and health conventions after
inspection.

Repeated install converges rather than recreating state:

- preserve identities and externally supplied certificates;
- create database objects through idempotent migrations;
- update, rather than duplicate, services and role bindings;
- compare rendered configuration before replacement;
- restart only replicas affected by a change;
- never overwrite data or secrets merely because install is rerun.

Support systemd/bare-metal deployment without Kubernetes. Development mode may
run one replica with explicitly development-grade TLS and a small local database
option if the controller supports it; production requires PostgreSQL, external
OIDC, mTLS, persistent audit storage, and HA expectations. Offline installation
consumes verified local packages, archives, images, and compatibility metadata
without contacting the internet.

### Upgrade

The toolkit upgrade flow:

1. validates host, configuration, artifact signature/digest, and target version;
2. reads current binary, protocol, config, and database versions;
3. validates the published compatibility/upgrade edge;
4. checks ports, resources, DNS, database, TLS, OIDC, transport, and site mix;
5. creates and verifies the configured backup;
6. acquires the migration lock and applies supported migrations;
7. rolls one replica at a time, waiting for readiness before proceeding;
8. verifies API, gRPC, RBAC/SSO, audit, event processing, protocol negotiation,
   and site reconnection;
9. records the deployed release only after validation.

Expand/contract database migrations allow old and new replicas to overlap. A
release that cannot do this must declare a maintenance window and expected site
reconnect impact.

### Downgrade and rollback

Downgrade is a declared release edge, not replacement of a binary. Preflight
compares binary, protocol, configuration, and database schema compatibility and
reports one of:

- supported in place;
- supported only by restoring a named pre-upgrade backup;
- unsupported, with the incompatible migration or protocol reason.

Irreversible migrations prevent automatic downgrade. The toolkit never silently
drops columns, rewrites audit history, or purges newer state to make an old binary
start.

Failed upgrades first stop further rollout. If the old binary remains compatible
with the expanded schema, replicas roll back in place. Otherwise the explicit
rollback procedure restores controller version, configuration, database backup,
and service state together. Validation runs again before success is reported.

### Other lifecycle operations

- `--dry-run` shows versions, config changes, migrations, restarts, compatibility,
  backups, and expected connection impact without mutation.
- `health` separately reports API, session gRPC, database, OIDC, TLS, RBAC,
  replica availability, site connections, protocol compatibility, and event lag.
- `uninstall --keep-data` removes software/services only.
- Data, audit, credentials, and registrations are destroyed only with an explicit
  purge option and interactive confirmation or an equally explicit automation
  acknowledgement.
- Certificate rotation is a first-class convergent operation and validates the
  overlap before retiring an old certificate.

## J. Observability, audit, and data governance

Use Spur's tracing and Prometheus conventions. Metrics should include:

- sessions by state, transport, protocol, and site version;
- reconnects, authentication failures, fencing events, and heartbeat age;
- event ingest rate, duplicate count, offset lag, snapshot duration, and queue
  saturation;
- operations by type/state, delivery attempts, latency, expiry, and denial;
- database pool/migration health, API latency/error class, OIDC/JWKS health, and
  certificate expiry;
- per-replica readiness and session ownership.

Metric labels must be bounded; site IDs and users belong in logs or sampled traces,
not unconstrained metric dimensions. Structured logs include correlation,
operation, tenant, site, and session identifiers but exclude tokens, certificates,
private addresses when policy forbids them, job environment, and scripts.

Audit is append-only at the application layer and records enrollment, credential
changes, sign-in/admin changes, policy/RBAC changes, site commands, lifecycle
metadata changes, exports, and destructive actions. Audit writes for security or
mutation events are transactional with the requested state change. If durable
audit cannot be written, sensitive mutations fail closed.

Define retention and export independently for telemetry, inventory, operations,
and audit. Minimize replicated job/user data, encrypt storage and backups, support
tenant export/deletion policy, and document which fields remain for security and
compliance after a site is disabled.

## K. Repository changes and phased delivery

### `ROCm/spur`

Planned implementation areas:

- `proto/federation.proto`: isolated site-session wire contract.
- `crates/spur-proto`: generate and export the new package without changing
  `package slurm`.
- `crates/spur-core/src/config.rs`: additive site-client configuration with
  serde defaults and compatibility tests.
- `crates/spurctld/src/federation_client.rs`: leader-gated outbound session,
  reconnect, event outbox, command validation, and result handling.
- `crates/spurctld/src/cluster.rs`: narrowly scoped committed snapshot/event and
  idempotent operation hooks; all mutations continue through `WalOperation`.
- `crates/spurctld/src/raft.rs`: expose only the leadership epoch and committed
  revision needed to fence sessions.
- `crates/spur-net`: reuse or minimally generalize address and WireGuard helpers;
  federation protocol code must not depend on WireGuard.
- new `crates/spur-federation-controller`: management/session servers, OIDC,
  federation RBAC, PostgreSQL store/migrations, policy, audit, health, metrics,
  and web assets.
- `crates/spur-cli`: optional business-administration client commands only.
- `docs`: site enrollment, security, protocol compatibility, failure semantics,
  administration, and upgrade compatibility.

Do not put federation tables into site accounting, make federation state a
`WalOperation`, expose Raft internals over federation, or route federation through
the Slurm REST/FFI schema.

### `ROCm/spur-toolkit`

This separate repository owns:

- desired-state YAML schema and configuration migration;
- inventory, Ansible/deployment roles, systemd/container templates;
- artifact download or offline import, signature/digest verification, and pinning;
- secret and certificate placement/permissions/rotation;
- preflight, install, status, health, upgrade, downgrade, rollback, and uninstall;
- PostgreSQL backup/restore orchestration and migration invocation;
- rolling deployment and post-operation validation;
- operator documentation and complete lifecycle tests.

Exact command names and paths remain provisional until `ROCm/spur-toolkit` is
inspected. No toolkit code should duplicate protocol handling, RBAC decisions,
event processing, or database business rules.

### Delivery phases

**Phase 0 — contracts and threat model**

- Approve trust boundaries, tenancy, operation allowlist, data classification,
  failure semantics, compatibility policy, and ADRs.
- Define protocol/version metadata and toolkit/controller artifact contract.
- Replace the legacy direct-forwarding roadmap with an explicit deprecation path,
  without removing its config or behavior yet.

**Phase 1 — protocol and persistent foundation**

- Add isolated protobuf generation, controller crate skeleton, version endpoints,
  PostgreSQL migrations, site/credential/session records, and configuration.
- Add mTLS enrollment, rotation/revocation model, session fencing, and protocol
  negotiation.
- Provide unit, migration, compatibility, and security tests.

**Phase 2 — outbound site connectivity and read-only inventory**

- Add the leader-gated `spurctld` client, reconnect/backoff, heartbeats, snapshot,
  durable event offsets, bounded outbox, stale/offline derivation, and dual-stack
  transport.
- Add externally managed and fixed-address WireGuard coverage.
- Prove site operation is unaffected by federation and database outages.

**Phase 3 — OIDC, tenant RBAC, API, audit, and UI**

- Add human/service identity, tenant isolation, scoped roles, management API,
  append-only audit, health/metrics, and read-only UI.
- Complete authorization matrix and cross-tenant isolation tests before enabling
  any remote mutation.

**Phase 4 — safe federated operations**

- Add a small typed allowlist of idempotent commands through `ClusterManager` and
  Raft.
- Add durable operation state, retries, expiry, cancellation, reconciliation,
  actor propagation, local authorization, and policy evaluation.
- Expand only after failure-injection tests prove exactly-once effects over the
  at-least-once transport.

**Phase 5 — policy and global workflows**

- Add approved placement/policy workflows, fleet views, and accounting summaries.
- Any cross-site job routing uses explicit ownership and compensation states; it
  does not revive best-effort peer forwarding or weaken site autonomy.

**Phase 6 — production lifecycle in `spur-toolkit`**

- Implement convergent install, offline artifacts, production/development modes,
  preflight, dry-run, health, certificate rotation, and uninstall.
- Implement rolling upgrade, migration, backup, rollback, and guarded downgrade.
- Publish and continuously test the site/protocol/database/config compatibility
  matrix.

Phases 1–3 may build lifecycle scaffolding in parallel, but production deployment
is not complete until the toolkit can manage the full supported lifecycle.

### Test strategy and acceptance gates

`ROCm/spur` tests:

- protobuf compatibility/golden fixtures across supported protocol versions;
- mTLS identity binding, bootstrap replay, expiry, rotation, and revocation;
- OIDC validation, RBAC matrix, policy denial, and tenant isolation;
- event deduplication, gaps, replay, snapshot replacement, and retention;
- command idempotency, fencing, deadlines, reconnect, result reconciliation, and
  leader changes;
- IPv4, IPv6, fixed/external WireGuard, NAT interruption, DNS change, and TLS
  rotation;
- controller replica and PostgreSQL failure injection;
- old snapshot/Raft-log compatibility for every site-state field added;
- bounded-memory/backpressure and sensitive-data redaction.

`ROCm/spur-toolkit` tests:

- clean and repeated installation;
- preflight and dry-run;
- v1 install, site registration, v2 rolling upgrade, validation, supported
  downgrade/restore, and revalidation;
- failed migration/deployment and rollback;
- configuration migration, secret permissions, certificate rotation, uninstall
  keep/purge behavior, and offline artifact installation;
- development and production modes over IPv4, IPv6, and WireGuard.

Release gates:

- a disconnected federation has no material effect on site scheduling latency or
  availability;
- no operation can cross tenant/site scope or bypass local Raft and authorization;
- duplicate delivery cannot duplicate a committed effect;
- a controller replica can fail without losing durable offsets or operations;
- all supported mixed versions pass the published matrix;
- upgrades preserve registrations, credentials, RBAC, SSO configuration, audit,
  event offsets, policies, pending operations, and cached metadata;
- unsupported downgrade paths fail before changing the system.

## L. Risks, decisions, and approval gates

### Security issues to address before implementation

The current direct peer forwarding path lacks durable idempotency and strong peer
identity, so it must not be exposed outside a trusted test network or reused for
central federation. Internal Raft gRPC also has a different trust boundary and
must not be exposed as a federation shortcut. Federation listeners require TLS
and explicit authentication even if existing site APIs are configured in a
permissive mode.

### Principal risks

- A central control plane increases blast radius unless tenant isolation and the
  site-side allowlist are independently enforced.
- Long-lived streams make stale-session fencing and load-balancer behavior
  correctness requirements, not optimizations.
- Global job routing can create ambiguous ownership and billing after partitions;
  it should follow read-only federation and bounded operations.
- Persisting too much job or user detail creates privacy, scale, and retention
  liabilities; start with aggregates and explicit field classification.
- Database migrations determine whether rolling upgrades and downgrades are real.
  Each release must declare compatibility rather than relying on convention.
- Managing WireGuard centrally can conflict with independent site networks.
  Externally managed mode and fixed addresses are first-class requirements.

### Decisions required before Phase 1

Approve:

1. PostgreSQL as the only production durable store and coordination mechanism.
2. An embedded, leader-gated `spurctld` client rather than a separate agent.
3. Per-site mTLS with CSR enrollment and external-CA support.
4. The initial protocol/version and site identity formats.
5. The tenant/resource authorization model and first command allowlist.
6. Inventory/event data classification, retention, and audit guarantees.
7. The initial supported Spur-site and federation upgrade matrix.
8. Whether development mode may use a local database or must also use PostgreSQL.
9. The toolkit's existing conventions after a dedicated `ROCm/spur-toolkit`
   inspection.

No implementation should begin until these boundaries and Phase 1 acceptance
criteria are approved. Each later phase should ship as a reviewable, independently
operable increment rather than one federation-wide change.

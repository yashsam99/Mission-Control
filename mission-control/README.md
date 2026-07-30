# Mission Control

A tiny distributed task-dispatch system: a `commander` accepts missions over
HTTP and hands them to a fleet of `soldier` workers over RabbitMQ. Soldiers
never expose a port — they pull work, never receive it.

## Architecture

Three services, wired together by Docker Compose:

```
                 POST /missions                 orders_queue
   client  ───────────────────────▶  commander  ─────────────▶  rabbitmq
                 GET /missions/{id}       ▲                          │
   client  ◀────────────────────────      │                          │
                                          │      status_queue        │
                                          └──────────────────────────┤
                                                                     │
                                                     orders_queue    ▼
                                          soldier(s) ◀────────────────
                                          (no exposed ports)
```

- **`commander`** — the only service with an HTTP listener. Accepts new
  missions, stores mission state in memory, publishes orders to
  `orders_queue`, and consumes `status_queue` to update mission state as
  soldiers report progress. Also issues short-lived JWTs.
- **`soldier`** — zero-ingress. It never listens on a port; it only dials out
  to `commander` (for JWTs) and `rabbitmq` (for orders/status). Runs a fixed
  pool of workers that pull orders from `orders_queue`, "execute" the
  mission (simulated work), and publish status updates to `status_queue`.
  Scale it horizontally by running more replicas — they all compete for the
  same `orders_queue`.
- **`rabbitmq`** — the broker, with the management plugin enabled for the
  admin UI at `:15672`.

Because soldiers have no ingress, the attack surface for compromising a
worker is limited to what it can reach outbound (the broker and the
commander's `/auth` endpoint).

## Prerequisites

- Docker
- Docker Compose (v2, the `docker compose` subcommand — this was verified
  against Docker Compose v5.3.1 / Docker 29.6.2)

No local Go toolchain is required to run the system (it builds inside
Docker), but `go 1.26` is required to run the unit tests directly on the
host.

## Quickstart

From the `mission-control/` directory:

```bash
docker compose up --build --scale soldier=3
```

This builds the `commander` and `soldier` images, starts RabbitMQ, waits for
it to report healthy, then starts `commander` and three `soldier` replicas.
`commander` publishes on `localhost:8080`; RabbitMQ's management UI is on
`localhost:15672` (user `mission` / pass `control`, from
`docker-compose.yml`).

Verified live: `docker compose up --build --scale soldier=3 -d` brought up
`rabbitmq-1`, `commander-1`, and `soldier-1..3`, with `commander-1` reporting
`(healthy)` within its healthcheck window.

Tear down with:

```bash
docker compose down -v
```

## API Reference

All requests/responses are JSON. `commander` listens on `HTTP_PORT`
(default `8080`).

### `POST /missions`

Queue a new mission. Returns `202 Accepted` with the generated mission ID.
The mission starts in `QUEUED` and an order is published to `orders_queue`
immediately.

```bash
curl -s -X POST http://localhost:8080/missions \
  -H 'Content-Type: application/json' \
  -d '{"objective":"recon sector 7"}'
```

```json
{"mission_id":"41eafc54-72a6-4217-ab81-5061a7df749d"}
```

### `GET /missions/{id}`

Fetch current mission state. Returns `404` if the ID is unknown.

```bash
curl -s http://localhost:8080/missions/41eafc54-72a6-4217-ab81-5061a7df749d
```

```json
{
  "mission_id": "41eafc54-72a6-4217-ab81-5061a7df749d",
  "objective": "recon sector 7",
  "status": "COMPLETED",
  "created_at": "2026-07-27T15:29:08.091104504Z",
  "updated_at": "2026-07-27T15:29:13.098249757Z"
}
```

### `POST /auth`

Exchange the shared bootstrap secret for a short-lived JWT. This is what
soldiers call to bootstrap and to rotate their identity (see
[Security Model](#security-model)). Wrong secret returns `401`.

```bash
curl -s -X POST http://localhost:8080/auth \
  -H 'Content-Type: application/json' \
  -d '{"bootstrap_secret":"super-secret-bootstrap"}'
```

```json
{"expires_in":30,"token":"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...."}
```

### `GET /health`

Liveness/readiness probe used by the compose healthcheck (and internally by
`commander healthcheck`, the entrypoint's health-check subcommand). Always
`200 OK` with body `ok` once the process is up.

```bash
curl -s -i http://localhost:8080/health
```

```
HTTP/1.1 200 OK
Content-Type: text/plain; charset=utf-8

ok
```

## Mission State Machine & Resilience

```
QUEUED ──▶ IN_PROGRESS |──▶ COMPLETED
                       └──▶ FAILED
```

- **`QUEUED`** — set by `commander` the instant `POST /missions` publishes the order; before any soldier has picked it up.
- **`IN_PROGRESS`** — set when a soldier worker dequeues the order and starts executing it (published as the first status message).
- **`COMPLETED`** / **`FAILED`** — terminal. Set when the worker finishes; outcome is randomized (~90% success) to simulate real mission risk.

### State Transition Validation
`MissionStore.UpdateStatus` enforces an explicit state transition table under a mutex lock to guarantee resilience against out-of-order, duplicate, or stale status messages:
- `QUEUED` → `IN_PROGRESS`, `COMPLETED`, or `FAILED` (allows skipping `IN_PROGRESS` if network reordering drops the initial update).
- `IN_PROGRESS` → `COMPLETED` or `FAILED`.
- `COMPLETED` & `FAILED` are **terminal** — once reached, no subsequent status updates are accepted.
- Out-of-order or duplicate transitions (e.g. `COMPLETED` → `IN_PROGRESS`) are rejected safely without mutating state.

### Background Goroutine Panic Safety
Background message consumers (such as `commander`'s status consumer and `soldier`'s order workers) execute outside standard HTTP handler panic recovery middleware. To prevent an unexpected payload error or panic from taking down the entire service process, the shared `internal/broker` client executes delivery callbacks inside a `safeInvoke` wrapper that catches panics, logs stack traces via `slog`, and safely Nacks the delivery.

## Security & Identity Model

Two independent, robust security layers:

1. **Broker Authentication (AMQP):** `rabbitmq` requires credentials passed in `RABBITMQ_URL` (`mission`/`control` default). This gates physical connection to the broker.

2. **Instance-Bound Rotating JWTs:**
   - **Worker Instance Binding:** Each `soldier` process generates a unique instance UUID at startup. During `POST /auth`, it sends its `instance_id`. `commander` binds issued JWTs to the requesting soldier instance via a standard `sub` (subject) claim.
   - **Verification:** `commander` validates that incoming status updates carry a valid signature, unexpired TTL, and a valid UUID `sub` claim. An invalid, expired, or malformed token triggers a security breach alert and drops the message.
   - **Replay Window Protection:** Tokens carry a short 30-second TTL (`tokenTTL`) and are automatically rotated every 25s by the worker's background `TokenStore`. Even if a valid token is captured during transit, its utility is constrained to the remaining seconds of its 30s window and restricted to status reports originating from that verified worker instance.

### Secrets Management in Production
In production, dev secrets (`BOOTSTRAP_SECRET`, `JWT_SECRET`, and `RABBITMQ_URL` credentials) must be managed outside of version control:
- Inject credentials at runtime using secret managers (HashiCorp Vault, AWS Secrets Manager, Kubernetes Secrets, or Docker Secrets).
- Environment variables should be supplied via git-ignored `.env` files (`env_file:` in Docker Compose) with distinct keys per environment.

## Shared Architecture & Code Organization

To avoid code duplication between `commander` and `soldier`, all AMQP connection management, automatic reconnection, channel recovery, and panic-safe delivery handling are centralized in `internal/broker`.

- **Unified Client:** `internal/broker` provides a single reconnecting broker implementation configured via callbacks, eliminating ~180 lines of duplicated reconnect logic.
- **Concurrent Publish Locking:** AMQP frame publishing is serialized across concurrent goroutines using a dedicated publish mutex (`pubMu`), preventing channel corruption under heavy concurrent worker loads.

## State Persistence & Scalability Considerations

### 1. Commander State Persistence & Multi-Instance HA
Currently, `MissionStore` maintains mission state in an in-memory map protected by `sync.RWMutex`.
- **Process Restart:** On service restart, in-memory state is lost.
- **Horizontal Scaling (Multi-Instance Commander):** To run multiple `commander` instances behind a load balancer, `MissionStore` would be backed by a shared persistent database (e.g. Redis for fast status updates / key-value lookups, or PostgreSQL for persistent mission audit trails). Pub/sub (or Redis keyspace notifications) would synchronize real-time status updates across Commander nodes.

### 2. Worker Fleet Bottlenecks & Scale Limits
The current design scales horizontally by adding `soldier` replicas. However, at high worker counts (e.g., 500+ workers):
- **Single Queue Contention:** All workers compete for deliveries on a single `orders_queue`. AMQP queue lock contention inside RabbitMQ becomes a bottleneck.
- **QoS Prefetch Tuning:** Workers prefetch orders based on `WORKER_POOL_SIZE`. Tuning prefetch sizes prevents fast workers from starving while slow workers hold buffered messages.
- **AMQP Socket Serialization:** A single shared AMQP connection per soldier process can bottleneck on socket I/O under extreme message throughput; multi-connection pooling is recommended for massive scale.

## Running the Unit Tests

```bash
go test ./... -race
```

Verified live: 14 tests in `commander` (mission store, state machine transitions, JWT sub binding, HTTP handlers, status consumer) and 7 in `soldier` (token store, auth client, worker execution, worker pool), plus unit tests in `internal/broker` — all pass under `-race`:

```
ok   mission-control/commander 1.637s
ok   mission-control/internal/broker 1.485s
ok   mission-control/soldier 1.470s
```

## Running the Integration Script

`test_missions.sh` drives a running stack end-to-end over four scenarios:
single mission lifecycle, a 20-mission concurrency flood, and a 35-second
audit that a soldier's log shows at least one `Token Rotated` event. It
takes about a minute (mostly the 35s rotation-audit wait) and expects the
stack already up with `commander` on `localhost:8080`.

```bash
docker compose up --build --scale soldier=3 -d
./test_missions.sh
```

Verified live (fresh 3-replica stack): all four scenarios passed in
~61 seconds —

```
== 1. Health check ==
commander healthy: PASS
== 2. Single mission lifecycle ==
initial (IN_PROGRESS): PASS
terminal (COMPLETED): PASS
== 3. Concurrency flood (20 missions) ==
missions IN_PROGRESS concurrently: 12
concurrency: PASS
== 4. Identity rotation audit (35s window) ==
Token Rotated count: 4
rotation: PASS
== All scenarios passed ==
```

## Environment Variables

### `commander`

| Variable            | Default                              | Purpose                                                              |
|-------------------  |-------------------------------------------------------------------------------------------------------      |
| `HTTP_PORT`         | `8080`                               | Port the REST API listens on.                                        |
| `RABBITMQ_URL`      | `amqp://guest:guest@localhost:5672/` | AMQP connection string (includes broker credentials).                |
| `BOOTSTRAP_SECRET`  | `bootstrap`                          | Shared secret `POST /auth` requires to mint a JWT.                   |
| `JWT_SECRET`        | `jwt-secret`                         | HS256 signing key for status-message JWTs. Held only by `commander`. |

### `soldier`

| Variable              | Default                              | Purpose                                                                   |
|---------------------- |--------------------------------------|---------------------------------------------------------------------------|
| `COMMANDER_URL`       | `http://localhost:8080`              | Base URL used to call `POST /auth` for bootstrap and rotation.            |
| `BOOTSTRAP_SECRET`    | `bootstrap`                          | Must match `commander`'s value, or `/auth` calls are rejected.            |
| `RABBITMQ_URL`        | `amqp://guest:guest@localhost:5672/` | AMQP connection string (includes broker credentials).                     |
| `WORKER_POOL_SIZE`    | `10`                                 | Number of concurrent worker goroutines (also sets AMQP QoS prefetch).     |

All four are also set explicitly in `docker-compose.yml`; the defaults above
apply when running a binary standalone.

## AI Usage

In accordance with project guidelines, AI tooling was utilized transparently to accelerate system design and documentation formatting.

### 1

**Tool Used:** Gemini

Application: Used as a Principal Architectural sounding board to compare message broker trade-offs (RabbitMQ vs. NATS), validate the thread-safe atomic.Value strategy for JWT rotation in Go, and generate the Mermaid.js architecture diagrams.

**Prompts Used:**

"Act as Principal Technical Architect with proficiency in golang, event-driven architectures, system design and distributed systems. Your task is to understand the requirements and functionality of the Project and come up with the design document on the most viable and efficient approach..."

"from this we have to first create create important design/architecture diagrams for this project"

"yes (Can you generate the complete final README.md file integrating these diagrams and the previous design rationale?)"

### 2

Application: Used for bug fixing of AMQP reconnect silently stops order consuming and status recording and optimizing the code structure, use of constants.go and models.go

**Tool Used:** claude-code

**Prompts Used:**

"Act as principal technical architect proficient in golang, distributed systems, event driven architecture and best coding practices in golang, I see that we have 2 bugs related to AMQP reconnect, basically we have to fix this by handling the ConsumeOrders and ConsumeStatus. Review the code and apply the fix for this"

"Act as principal technical architect proficient in golang, distributed systems, event driven architecture and best coding practices in golang, Can we create a proper production code structure for this project like moving constants to constants.go and creating models.go for main structures. Only make changes that does not require extensive work and easy to implement without existing working flow"

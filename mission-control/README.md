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

## Mission State Machine

```
QUEUED ──▶ IN_PROGRESS |──▶ COMPLETED
                       └──▶ FAILED
```

- **`QUEUED`** — set by `commander` the instant `POST /missions` publishes
  the order; before any soldier has picked it up.
- **`IN_PROGRESS`** — set when a soldier worker dequeues the order and
  starts executing it (published as the first status message).
- **`COMPLETED`** / **`FAILED`** — terminal. Set when the worker finishes;
  outcome is randomized (~90% success) to simulate real mission risk.

Every transition after `QUEUED` is driven by a `StatusMessage` a soldier
publishes to `status_queue`, which `commander` consumes and applies via
`MissionStore.UpdateStatus`.

## Security Model

Two independent, unrelated security layers:

1. **Broker authentication (AMQP).** `rabbitmq` requires the credentials
   baked into `RABBITMQ_URL` (`RABBITMQ_DEFAULT_USER` /
   `RABBITMQ_DEFAULT_PASS` in `docker-compose.yml`, default
   `mission`/`control`). This is static, standard AMQP auth — it gates who
   can connect to the broker at all, not who is allowed to report mission
   status.

2. **Rotating JWT on status messages.** Every status message a soldier
   publishes carries a JWT in its AMQP `authorization` header. `commander`
   signs HS256 JWTs with `JWT_SECRET` (a secret only `commander` ever holds)
   and validates every incoming status message against it before trusting
   the payload; an invalid/expired/wrongly-signed token is logged as a
   `SECURITY BREACH` and the message is nacked without requeue (dropped).
   Tokens are minted via `POST /auth`, gated by a separate shared
   `BOOTSTRAP_SECRET` (not the JWT secret — soldiers never see
   `JWT_SECRET`).

   - **TTL: 30s fixed** (`tokenTTL` in `commander/main.go`).
   - **Rotation: every 25s** (`rotationInterval` in `soldier/main.go`) — a
     soldier re-authenticates 5s before its current token would expire, so
     there is no window where every held token has expired. If a rotation
     call fails (e.g. commander briefly unreachable), the soldier logs a
     warning and keeps using its previous (still-valid-for-a-few-more-
     seconds) token rather than blocking.

   This means a leaked broker credential lets an attacker connect to
   RabbitMQ, but not forge mission status — that needs a live, signed,
   unexpired JWT, and JWTs expire in 30 seconds.

## Running the Unit Tests

```bash
go test ./... -race
```

Verified live: 14 tests in `commander` (mission store, JWT sign/validate,
HTTP handlers, status consumer) and 7 in `soldier` (token store, auth
client, worker execution, worker pool) — all pass under `-race`:

```
ok   mission-control/commander 2.156s
ok   mission-control/soldier 1.649s
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

## Known Limitations

These were found and confirmed during implementation review — noted here so
operators know what to expect, not as hypothetical risks:

1. **Dev secrets are plaintext in `docker-compose.yml`.**
   `BOOTSTRAP_SECRET`, `JWT_SECRET`, and the credentials embedded in
   `RABBITMQ_URL` (`mission`/`control`) are committed directly in the
   compose file. That's fine for local development and this demo, but
   before any real deployment they should move out of version control —
   e.g. into a git-ignored `.env` file consumed via Compose's `env_file:`
   / variable substitution, or a proper secrets manager (Docker secrets,
   Vault, cloud KMS, etc.) — with `RABBITMQ_URL`'s embedded username/
   password rotated at the same time.

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

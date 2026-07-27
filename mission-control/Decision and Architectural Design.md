# Architectural Design Document: Mission Control System

## 1. Executive Summary

This document outlines the architecture for a secure, asynchronous, and resilient military command and control system. The core constraint of this architecture is strict zero-trust network ingress for field units: the Soldier Worker must never expose an open port or listen for inbound connections. To achieve this, the system leverages a centralized Message Broker (Communications Hub) to decouple the Commander's Camp Service from the field units, employing an asynchronous publish/subscribe model combined with aggressive short-lived identity rotation.

The technology stack centers on **Go (Golang)** for its native concurrency primitives (`goroutines`, channels) and minimal footprint, ensuring the Soldier Worker runs efficiently on constrained field hardware.

---

## 2. Core Architectural Decisions & Trade-off Analysis

To meet the robust reliability and zero-inbound constraints, we must evaluate our core dependencies carefully.

### 2.1 The Central Communications Hub (Message Queue)

We need a broker to handle `orders_queue` and `status_queue`.

| Option | Pros | Cons | Decision |
| --- | --- | --- | --- |
| **RabbitMQ** | Native AMQP, excellent routing, robust acknowledgment models, built-in dead-lettering. | Slightly heavier memory footprint than NATS. | **Chosen.** Best fit for reliable, distinct task queuing and manual acknowledgments in Go. |
| **NATS (Core)** | Extremely lightweight, highly performant, simple deployment. | At-most-once delivery by default; requires JetStream for persistence. | **Rejected.** Core NATS risks message loss if a soldier unit goes offline. |
| **Kafka** | Perfect for event-sourcing, infinite replayability. | High operational complexity, consumer-group offset management is overkill here. | **Rejected.** Too heavy for discrete command/response architecture. |

**Rationale:** RabbitMQ guarantees at-least-once delivery through manual consumer acknowledgments. If a Soldier Worker crashes mid-mission, the unacknowledged message will requeue and be picked up by another worker.

### 2.2 Datastore for Commander Service

The Commander Service must track mission states (`QUEUED`, `IN_PROGRESS`, `COMPLETED`, `FAILED`).

| Option | Pros | Cons | Decision |
| --- | --- | --- | --- |
| **In-Memory Go Map** | Zero operational overhead, native to Go, extremely fast. | State is lost on container restart. | **Chosen (for V1).** Fits the "in-memory" requirement perfectly, minimizes Docker footprint. |
| **Redis** | Ephemeral but out-of-process state, handles high concurrency. | Adds another container dependency. | **Alternative.** Easy to swap in later if persistence becomes a requirement. |

**Rationale:** We will use a Go `map[string]Mission` protected by a `sync.RWMutex`. This provides thread-safe, concurrent reads and writes while fulfilling the minimal container dependency requirement.

---

## 3. High-Level System Architecture

The system consists of three distinct network boundaries modeled via Docker Compose:

1. **Commander's Camp (Service):** Exposes a REST API (`/missions`, `/missions/{id}`, `/auth`). It connects to RabbitMQ as a Publisher (Orders) and Consumer (Status).
2. **Communications Hub (RabbitMQ):** The secure intermediary broker. Requires authentication to connect.
3. **Battlefield (Soldier Worker):** Runs in an isolated network space with *no open ports*. It connects outward to RabbitMQ as a Consumer (Orders) and Publisher (Status), and outward to the Commander's API for token rotation.

---

## 4. Component Design & Concurrency Strategy

### 4.1 Commander's Camp Service (Go)

This service acts as both an HTTP server and an AMQP consumer.

* **Ingress (`POST /missions`):** Generates a UUID `mission_id`, records the status as `QUEUED` in the thread-safe map, and publishes the JSON payload to RabbitMQ's `orders_queue`. Returns `202 Accepted` immediately.
* **Status Ingestion (AMQP Consumer):** A background goroutine listens to `status_queue`. As messages arrive, it validates the JWT token in the message header. If valid, it acquires a write-lock on the in-memory map and updates the mission state.
* **Query (`GET /missions/{mission_id}`):** Acquires a read-lock on the in-memory map and returns the current state.

### 4.2 Soldier Worker Service (Go)

This service requires a robust concurrency model to handle multiple missions simultaneously without blocking the queue consumer.

* **Queue Polling:** A single goroutine continuously consumes from `orders_queue`.
* **Worker Pool:** Upon receiving a message, the consumer dispatches the mission to a buffered Go channel (`chan Mission`). A fixed pool of worker goroutines (e.g., 10 workers) listens to this channel.
* **Mission Execution:**

1. Worker receives the mission, publishes `IN_PROGRESS` to `status_queue` (attaching its current JWT).
2. Simulates work via `time.Sleep(time.Duration(rand.Intn(10)+5) * time.Second)`.
3. Randomly determines success (90%) via `rand.Float32() < 0.90`.
4. Publishes `COMPLETED` or `FAILED` to the `status_queue`.
5. Acknowledges (ACKs) the original AMQP message on the `orders_queue` to remove it from RabbitMQ.

> **Resiliency Note:** By only ACKing the RabbitMQ message *after* the simulated delay finishes, we ensure that if the Soldier container is violently killed mid-mission, RabbitMQ will redeliver the order to a surviving worker.

---

## 5. Security: Authentication & Identity Rotation

Field units operating in hostile territory run a high risk of capture. Static credentials are a critical vulnerability. We will implement short-lived JSON Web Tokens (JWT) rotating every 30 seconds.

### The Rotation Mechanism

1. **Bootstrapping:** The Soldier Worker boots with a highly restricted, long-lived "Bootstrap Secret" (provided via Docker environment variables).
2. **Token Acquisition:** On startup, the Soldier makes an outbound `POST /auth` HTTP call to the Commander Service, providing the Bootstrap Secret. It receives a JWT valid for exactly 30 seconds.
3. **Thread-Safe Storage:** The Soldier stores this JWT in an `atomic.Value` (Go's lock-free atomic pointer).
4. **Usage:** Whenever a worker goroutine publishes to the `status_queue`, it reads the token from the `atomic.Value` and injects it into the AMQP message headers.
5. **Background Rotation:** A dedicated goroutine runs a `time.Ticker` set to 25 seconds (giving a 5-second buffer). Every tick, it calls `POST /auth`, receives a new token, logs "Token Rotated", and atomically updates the `atomic.Value`.
6. **Commander Validation:** The Commander's `status_queue` consumer uses standard JWT signature validation. If a token is expired, the message is logged as a security breach and dropped.

---

## 6. Project Deliverables & Repository Structure

Repository should be structured as follows:

```text
mission-control/
├── commander/
│   ├── main.go
│   ├── Dockerfile
│   └── (go code for API, AMQP, Auth)
├── soldier/
│   ├── main.go
│   ├── Dockerfile
│   └── (go code for worker pool, token rotation)
├── docker-compose.yml
├── test_missions.sh
└── README.md

```

### 6.1 Docker Orchestration (`docker-compose.yml`)

The compose file will define three services:

1. **rabbitmq:** Using the `rabbitmq:3-management` alpine image (exposes 5672 for AMQP, 15672 for UI).
2. **commander:** Built from `./commander/Dockerfile`. Depends on RabbitMQ. Exposes port 8080.
3. **soldier:** Built from `./soldier/Dockerfile`. Depends on RabbitMQ and Commander. Can be scaled via `docker-compose up --scale soldier=3`.

### 6.2 Automation & Testing (`test_missions.sh`)

The shell script will use `curl`, `jq` (for JSON parsing), and standard bash loops to prove system viability.

1. **Health Check:** Ensure infrastructure is ready.
Wait for Commander HTTP `200 OK` on `/health` and RabbitMQ AMQP port to accept connections.

2. **Single Mission Verification:**
`POST /missions`, parse the `mission_id`. Immediately `GET /missions/{id}` to verify `QUEUED` status. Wait 6 seconds, poll again to verify `IN_PROGRESS`, and wait 10 more seconds to verify terminal state (`COMPLETED` / `FAILED`).

3. **Concurrency Flood:**
Run a bash `for` loop to `POST` 20 missions simultaneously. Poll all 20 IDs to show multiple entering `IN_PROGRESS` concurrently, proving the Soldier's goroutine worker pool is operating correctly.

4. **Identity Rotation Audit:**
Use `docker logs <soldier_container>` and `grep` for the "Token Rotated" log entry over a 35-second window to mathematically prove the 30-second TTL logic executed successfully without dropping active task processing.

# Implementation Notes: Lab Automation Scheduler

## Overview
This document outlines the architectural decisions, concurrency model, and trade-offs made while building the lab automation scheduler. The scheduler is designed to execute Directed Acyclic Graphs (DAGs) of workflow steps reliably, support parallel execution across independent hardware devices, handle hardware contention, and recover gracefully from driver refusals or instrument failures.

---

## 1. Concurrency & State Safety
* **Atomic State Evaluations (`asyncio.Lock`)**: 
  To prevent race conditions—such as two simultaneous driver callbacks attempting to transition or evaluate the DAG concurrently—all state evaluations and database updates inside the `Scheduler` are protected by an `asyncio.Lock`. This ensures strict serialization of critical sections (DAG checks and dispatches) while allowing I/O operations to remain non-blocking.
* **Concurrent Event Handling**: 
  Incoming result messages from the NATS bus are spawned as independent asynchronous background tasks (`asyncio.create_task`). This ensures that a slow callback or heavy computation from one instrument driver never stalls message consumption or blocks other device responses.

---

## 2. DAG Evaluation & Parallel Execution
* **Set-Based Prerequisite Checking**: 
  The scheduler queries the database for all steps in a run, categorizing them into `completed` and `running` sets. A step is eligible for dispatch only when:
  1. It has not already completed or started.
  2. Its `depends_on` prerequisites form a subset of the `completed` set.
  3. Its designated hardware device is not currently busy.
* **Maximizing Parallelism**: 
  By evaluating the entire list of steps rather than processing them strictly sequentially, independent assay branches (e.g., preparing reagent and sample plates simultaneously) execute in parallel across different devices.

---

## 3. Hardware Contention & Driver Refusals
* **Device Lock/Busy Checks**: 
  Before dispatching a command to a driver via `NATSBus.send_command`, the scheduler queries the database (`is_device_busy`) to verify that no other step is currently occupying that physical instrument (`STEP_DISPATCHED`).
* **Handling Refusals**: 
  Drivers can immediately refuse a command if they are currently busy processing another instruction. When a non-accepted acknowledgment (`CommandAck(accepted=False)`) is received, the scheduler catches the refusal, logs the reason, and safely rolls the step status back to `pending` so it can be picked up in subsequent evaluation cycles.

---

## 4. Error Handling & Failure Propagation
* **Instrument Failures**: 
  When a driver reports a step completion with an error string via `StepResult`, the scheduler intercepts the failure, logs the error, marks the specific step as `failed`, and immediately transitions the entire workflow run to a `failed` state. This safely halts further dispatches and fulfills strict safety invariants for laboratory hardware automation.

---

## 5. Design Trade-offs
* **Database as the Source of Truth**: 
  Rather than maintaining complex in-memory state caches that risk drifting from reality during container restarts or network blips, the scheduler treats PostgreSQL as the authoritative source of truth for step and run statuses.
* **Polling/Trigger Evaluation Loop**: 
  The DAG is re-evaluated after every successful step completion or startup event. While simple, this reactive loop ensures that any newly unblocked dependencies are dispatched with minimal latency without requiring heavy background polling timers.

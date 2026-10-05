# Design

## Assumptions

One Postgres instance, a few hundred tasks a second at most, and workers that reach the API over a trusted network: the `X-Worker-Id` header identifies them and proves nothing.

## Storage

Postgres, through `pg`. Row locks with `SKIP LOCKED` give an atomic claim without a coordinator, a unique key gives atomic dedupe, and a task and its dedupe row commit together. Redis would be faster, but its durability depends on configuration, and a queue that must not lose tasks should not. SQLite would serialise every writer.

## How `claim` works

One statement: a CTE selects ready rows `FOR UPDATE SKIP LOCKED`, and the update marks them in flight, counts the attempt and sets the lease. A concurrent claim skips rows another has locked instead of waiting, so it can never select the same task, and claims do not queue behind each other. A select followed by a separate update would let two claims read one row before either wrote.

## The lease model

- **Duration:** `lease_ms` per queue, 30 seconds by default.
- **Renewal:** the harness calls `extend` at half the lease. `extend` works only for the holder, only before expiry.
- **Expiry:** a task's lease is over when `lease_until <= now()`, by the database's clock, so workers' clocks do not matter.
- **Recovery query:** `sweep` runs every second and before each claim. It returns expired tasks to ready, or moves them to the DLQ past their last attempt. Two sweeps at once are safe: the second finds nothing left to update.

## A worker dies holding a task

1. `ready`: nothing to recover.
2. `claim`: `in_flight`, attempts 1, leased to the worker.
3. The worker dies, and heartbeats stop.
4. The lease runs out. The task is still `in_flight`, and no other worker can get it yet.
5. `sweep`: `ready` again, attempts unchanged, `last_error` "lease expired".
6. Another worker claims it: attempts 2.
7. If the dead worker was only partitioned and comes back, its ack is refused with 409, because `worker_id` no longer matches. If it had already run the side effect, the task runs twice. That is the at-least-once cost.

A task that keeps killing its worker uses up its attempts and lands in the DLQ like any other.

## Dispatch and isolation

`Worker.register(type, handler)` fills a map, and each claimed task is routed by its `type`. An unknown type is a permanent failure, not a dropped task.

- **`js`** runs in a child process under Node's permission model and a fresh `vm` context: no files, no child processes, no `require`, and a 10 second timeout. A crash or a hang takes down only the child.
- **`http`** runs in the worker, but only waits on the network. The abort signal stops it if the lease is lost.
- **`llm`** does too, behind the `LlmProvider` interface.

## Retries

Exponential backoff, doubling from 1 second up to 60 seconds, half of it jittered: attempt 3 waits 2 to 4 seconds. Doubling backs off quickly from a service that is down, the cap keeps recovery prompt, and jitter stops tasks that failed together from retrying together. Five attempts by default, all per queue.

| Type | Retries | Does not |
|---|---|---|
| `http` | 5xx, 429, network failures | any other 4xx |
| `js` | runtime errors, timeouts | `SyntaxError` |
| `llm` | 429, 5xx, network failures | other provider refusals |

The line is whether the same request can succeed later. A `503` is about the server's moment; a `SyntaxError` will be there on every attempt. Runtime errors in scripts are the judgement call: they may be deterministic, but they may depend on data, and a few wasted attempts cost less than a lost task.

## Idempotency end to end

**Enqueue:** the primary key on `dedupe(queue, key)` decides a race. The second enqueue blocks until the first commits, then finds a live row and returns the same task with a 200. Past the window, the conflict clause takes the row over and a new task is made.

**Consumer:** at-least-once means handlers can run twice. The task's `id` is stable across attempts, so handlers pass it as an idempotency key to whatever they call. The queue cannot do this for them. The `http` handler forwards the headers it is given, and a producer that needs it sets an `Idempotency-Key` there.

## What comes next, and what was cut

Next: retention for finished tasks and expired dedupe rows, which grow without bound today, then metrics.

Cut: `SKIP LOCKED` fairness between queues, which the brief leaves out; a React page, as a single HTML file does the job; and offset pagination is used on the DLQ rather than keyset, since it is an operator's view that stays small.

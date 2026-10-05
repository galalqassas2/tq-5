# Task queue

A small task queue on Postgres: an HTTP API, a worker harness with `llm`, `js` and `http` handlers, and a page showing each queue and its DLQ.

## Run it

Needs Node 24 and Docker.

```bash
npm install
npm start
```

That starts Postgres and the API on http://localhost:3000, which also serves the page. In a second terminal:

```bash
npm run worker
curl -X POST localhost:3000/queues/default/tasks -H 'content-type: application/json' \
  -d '{"type":"js","payload":{"script":"return 1 + 1"}}'
```

## Test it

Tests run against real Postgres, so start it first:

```bash
npm run db
npm test
```

`test/concurrent-claim.test.ts` enqueues 50 mixed tasks, runs 4 workers against them, and asserts that every task is processed and no two workers hold one at once.

## What is implemented

Everything in the brief: the eight endpoints, leases with heartbeats, retries with per-queue backoff, a DLQ with requeue, idempotent enqueue, the three handlers and the page. Beyond the brief, `PUT /queues/:name` sets a queue's settings, `GET /queues` lists them for the page, and `GET /tasks/:id` reads a result.

## What is not

Everything the brief leaves out, and: real LLM providers (the stub sits behind `LlmProvider`), metrics, and cleaning up old finished tasks and dedupe rows.

## Why not exactly-once

A worker can finish a task and die before its ack arrives. The queue cannot tell that from a worker that died halfway, so it has to run the task again or risk losing it. We chose to run it again: delivery is at least once, and a handler with side effects must be idempotent. [DESIGN.md](DESIGN.md) covers how.

## Where the time went

About 8 hours: 3 on the store and its concurrency tests, 2 on the harness and handlers, 1 on the API, 1 on the page, 1 on the docs.

## With another day

Retention for finished tasks, a real provider behind `LlmProvider`, and metrics on claim latency and queue depth.

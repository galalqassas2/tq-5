-- This file runs as one transaction. The lock stops servers starting together from racing here.
SELECT pg_advisory_xact_lock(7316);

CREATE TABLE IF NOT EXISTS queues (
  name text PRIMARY KEY,
  max_attempts int NOT NULL DEFAULT 5 CHECK (max_attempts > 0),
  backoff_base_ms int NOT NULL DEFAULT 1000 CHECK (backoff_base_ms >= 0),
  backoff_factor real NOT NULL DEFAULT 2 CHECK (backoff_factor >= 1),
  backoff_max_ms int NOT NULL DEFAULT 60000 CHECK (backoff_max_ms >= 0),
  lease_ms int NOT NULL DEFAULT 30000 CHECK (lease_ms > 0),
  dedupe_window_ms int NOT NULL DEFAULT 300000 CHECK (dedupe_window_ms > 0)
);

CREATE TABLE IF NOT EXISTS tasks (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  queue text NOT NULL REFERENCES queues (name),
  type text NOT NULL CHECK (type IN ('llm', 'js', 'http')),
  payload jsonb NOT NULL,
  status text NOT NULL DEFAULT 'ready' CHECK (status IN ('ready', 'in_flight', 'done', 'dead')),
  attempts int NOT NULL DEFAULT 0,
  run_at timestamptz NOT NULL DEFAULT now(),
  worker_id text,
  lease_until timestamptz,
  result jsonb,
  last_error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz
);

CREATE INDEX IF NOT EXISTS tasks_ready ON tasks (queue, run_at) WHERE status = 'ready';
CREATE INDEX IF NOT EXISTS tasks_leased ON tasks (lease_until) WHERE status = 'in_flight';
CREATE INDEX IF NOT EXISTS tasks_dead ON tasks (queue, finished_at DESC) WHERE status = 'dead';

CREATE TABLE IF NOT EXISTS dedupe (
  queue text NOT NULL REFERENCES queues (name),
  key text NOT NULL,
  task_id uuid NOT NULL,
  expires_at timestamptz NOT NULL,
  PRIMARY KEY (queue, key)
);

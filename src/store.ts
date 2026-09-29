import type pg from "pg";

import { backoffMs } from "./backoff.ts";

export const TASK_TYPES = ["llm", "js", "http"] as const;
export type TaskType = (typeof TASK_TYPES)[number];

export type NewTask = {
	type: TaskType;
	payload: unknown;
	delay?: number;
	dedupeKey?: string;
};

export type ClaimedTask = {
	id: string;
	type: TaskType;
	payload: unknown;
	attempts: number;
	leaseUntil: string;
};

export type QueueConfig = {
	maxAttempts?: number;
	backoffBaseMs?: number;
	backoffFactor?: number;
	backoffMaxMs?: number;
	leaseMs?: number;
	dedupeWindowMs?: number;
};

export class HttpError extends Error {
	readonly status: number;
	readonly code: string;

	constructor(status: number, code: string, message: string) {
		super(message);
		this.status = status;
		this.code = code;
	}
}

const ENSURE_QUEUE = "INSERT INTO queues (name) VALUES ($1) ON CONFLICT DO NOTHING";

export class Store {
	readonly #pool: pg.Pool;

	constructor(pool: pg.Pool) {
		this.#pool = pool;
	}

	async #transaction<T>(work: (client: pg.PoolClient) => Promise<T>): Promise<T> {
		const client = await this.#pool.connect();
		try {
			await client.query("BEGIN");
			const result = await work(client);
			await client.query("COMMIT");
			return result;
		} catch (error) {
			await client.query("ROLLBACK");
			throw error;
		} finally {
			client.release();
		}
	}

	/** Sets a queue's retry, lease and dedupe settings, creating it if needed. */
	async configure(queue: string, config: QueueConfig): Promise<void> {
		await this.#pool.query(ENSURE_QUEUE, [queue]);
		await this.#pool.query(
			`UPDATE queues SET
				max_attempts = COALESCE($2, max_attempts),
				backoff_base_ms = COALESCE($3, backoff_base_ms),
				backoff_factor = COALESCE($4, backoff_factor),
				backoff_max_ms = COALESCE($5, backoff_max_ms),
				lease_ms = COALESCE($6, lease_ms),
				dedupe_window_ms = COALESCE($7, dedupe_window_ms)
			WHERE name = $1`,
			[
				queue,
				config.maxAttempts,
				config.backoffBaseMs,
				config.backoffFactor,
				config.backoffMaxMs,
				config.leaseMs,
				config.dedupeWindowMs,
			],
		);
	}

	/**
	 * Adds a task. The primary key on dedupe(queue, key) decides a race: of two enqueues with one
	 * key, the second waits for the first to commit, then finds its live row and adds nothing.
	 */
	async enqueue(queue: string, task: NewTask): Promise<{ id: string; deduped: boolean }> {
		return this.#transaction(async (client) => {
			await client.query(ENSURE_QUEUE, [queue]);
			const { rows } = await client.query<{ id: string }>("SELECT gen_random_uuid() AS id");
			const id = rows[0].id;
			if (task.dedupeKey !== undefined) {
				const claimed = await client.query(
					`INSERT INTO dedupe (queue, key, task_id, expires_at)
					SELECT $1, $2, $3, now() + dedupe_window_ms * interval '1 millisecond'
					FROM queues WHERE name = $1
					ON CONFLICT (queue, key) DO UPDATE
						SET task_id = EXCLUDED.task_id, expires_at = EXCLUDED.expires_at
						WHERE dedupe.expires_at <= now()`,
					[queue, task.dedupeKey, id],
				);
				if (claimed.rowCount === 0) {
					const existing = await client.query<{ task_id: string }>(
						"SELECT task_id FROM dedupe WHERE queue = $1 AND key = $2",
						[queue, task.dedupeKey],
					);
					return { id: existing.rows[0].task_id, deduped: true };
				}
			}
			await client.query(
				`INSERT INTO tasks (id, queue, type, payload, run_at)
				VALUES ($1, $2, $3, $4, now() + $5 * interval '1 millisecond')`,
				[id, queue, task.type, JSON.stringify(task.payload), task.delay ?? 0],
			);
			return { id, deduped: false };
		});
	}

	/**
	 * Leases up to `max` ready tasks in one statement. FOR UPDATE SKIP LOCKED makes concurrent
	 * claims pass over each other's rows, so no two can select the same task.
	 */
	async claim(queue: string, workerId: string, max: number): Promise<ClaimedTask[]> {
		await this.sweep();
		const { rows } = await this.#pool.query<ClaimedTask>(
			`WITH next AS (
				SELECT id FROM tasks
				WHERE queue = $1 AND status = 'ready' AND run_at <= now()
				ORDER BY run_at, created_at
				LIMIT $3
				FOR UPDATE SKIP LOCKED
			)
			UPDATE tasks SET
				status = 'in_flight',
				attempts = attempts + 1,
				worker_id = $2,
				lease_until = now() + (SELECT lease_ms FROM queues WHERE name = $1) * interval '1 millisecond'
			FROM next WHERE tasks.id = next.id
			RETURNING tasks.id, type, payload, attempts, lease_until AS "leaseUntil"`,
			[queue, workerId, max],
		);
		return rows;
	}

	/** Recovers tasks whose lease ran out: back to ready, or to the DLQ past their last attempt. */
	async sweep(): Promise<number> {
		const { rowCount } = await this.#pool.query(
			`UPDATE tasks SET
				status = CASE WHEN attempts >= queues.max_attempts THEN 'dead' ELSE 'ready' END,
				finished_at = CASE WHEN attempts >= queues.max_attempts THEN now() END,
				run_at = now(),
				worker_id = NULL,
				lease_until = NULL,
				last_error = 'lease expired'
			FROM queues
			WHERE queues.name = tasks.queue AND status = 'in_flight' AND lease_until <= now()`,
		);
		return rowCount ?? 0;
	}

	/** Why a request on a task it does not hold failed: gone, or no longer this worker's. */
	async #refusal(id: string): Promise<HttpError> {
		const { rowCount } = await this.#pool.query("SELECT 1 FROM tasks WHERE id = $1", [id]);
		if (rowCount === 0) return new HttpError(404, "task-not-found", `No task ${id}`);
		return new HttpError(409, "lease-lost", `This worker does not hold task ${id}`);
	}

	async ack(id: string, workerId: string, result: unknown): Promise<void> {
		const { rowCount } = await this.#pool.query(
			`UPDATE tasks SET status = 'done', result = $3, worker_id = NULL, lease_until = NULL,
				finished_at = now()
			WHERE id = $1 AND status = 'in_flight' AND worker_id = $2`,
			[id, workerId, JSON.stringify(result ?? null)],
		);
		if (rowCount === 0) throw await this.#refusal(id);
	}

	/** Schedules the next attempt by the queue's backoff, or moves the task to the DLQ. */
	async nack(id: string, workerId: string, reason: string, retryable: boolean): Promise<void> {
		const refused = await this.#transaction(async (client) => {
			const { rows } = await client.query(
				`SELECT attempts, max_attempts, backoff_base_ms, backoff_factor, backoff_max_ms
				FROM tasks JOIN queues ON queues.name = tasks.queue
				WHERE id = $1 AND status = 'in_flight' AND worker_id = $2
				FOR UPDATE OF tasks`,
				[id, workerId],
			);
			const held = rows.at(0);
			if (held === undefined) return true;
			if (!retryable || held.attempts >= held.max_attempts) {
				await client.query(
					`UPDATE tasks SET status = 'dead', last_error = $2, worker_id = NULL, lease_until = NULL,
						finished_at = now()
					WHERE id = $1`,
					[id, reason],
				);
				return false;
			}
			const delay = backoffMs(held.attempts, {
				baseMs: held.backoff_base_ms,
				factor: held.backoff_factor,
				maxMs: held.backoff_max_ms,
			});
			await client.query(
				`UPDATE tasks SET status = 'ready', last_error = $2, worker_id = NULL, lease_until = NULL,
					run_at = now() + $3 * interval '1 millisecond'
				WHERE id = $1`,
				[id, reason, delay],
			);
			return false;
		});
		if (refused) throw await this.#refusal(id);
	}

	/** Renews the lease, only for the worker holding it and only while it has not run out. */
	async extend(id: string, workerId: string): Promise<{ leaseUntil: string }> {
		const { rows } = await this.#pool.query<{ leaseUntil: string }>(
			`UPDATE tasks SET lease_until = now() + queues.lease_ms * interval '1 millisecond'
			FROM queues
			WHERE queues.name = tasks.queue AND id = $1 AND status = 'in_flight' AND worker_id = $2
				AND lease_until > now()
			RETURNING lease_until AS "leaseUntil"`,
			[id, workerId],
		);
		const extended = rows.at(0);
		if (extended === undefined) throw await this.#refusal(id);
		return extended;
	}

	async task(id: string) {
		const { rows } = await this.#pool.query(
			`SELECT id, queue, type, payload, status, attempts, result, last_error AS "lastError"
			FROM tasks WHERE id = $1`,
			[id],
		);
		const task = rows.at(0);
		if (task === undefined) throw await this.#refusal(id);
		return task;
	}

	async stats(queue: string): Promise<{ ready: number; inFlight: number; dlq: number }> {
		const { rows } = await this.#pool.query(
			`SELECT
				count(*) FILTER (WHERE status = 'ready')::int AS ready,
				count(*) FILTER (WHERE status = 'in_flight')::int AS "inFlight",
				count(*) FILTER (WHERE status = 'dead')::int AS dlq
			FROM tasks WHERE queue = $1`,
			[queue],
		);
		return rows[0];
	}

	async queues() {
		const { rows } = await this.#pool.query(
			`SELECT queues.name,
				count(*) FILTER (WHERE status = 'ready')::int AS ready,
				count(*) FILTER (WHERE status = 'in_flight')::int AS "inFlight",
				count(*) FILTER (WHERE status = 'dead')::int AS dlq
			FROM queues LEFT JOIN tasks ON tasks.queue = queues.name
			GROUP BY queues.name ORDER BY queues.name`,
		);
		return rows;
	}

	async dlq(queue: string, limit: number, offset: number) {
		const { rows } = await this.#pool.query(
			`SELECT id, type, payload, attempts, last_error AS "lastError", finished_at AS "finishedAt",
				count(*) OVER ()::int AS total
			FROM tasks WHERE queue = $1 AND status = 'dead'
			ORDER BY finished_at DESC, id
			LIMIT $2 OFFSET $3`,
			[queue, limit, offset],
		);
		const total = rows.at(0)?.total ?? 0;
		return { total, items: rows.map(({ total: _, ...item }) => item) };
	}

	/** Replays a DLQ task from its first attempt. */
	async requeue(id: string): Promise<void> {
		const { rowCount } = await this.#pool.query(
			`UPDATE tasks SET status = 'ready', attempts = 0, run_at = now(), finished_at = NULL
			WHERE id = $1 AND status = 'dead'`,
			[id],
		);
		if (rowCount !== 0) return;
		const refusal = await this.#refusal(id);
		if (refusal.status === 404) throw refusal;
		throw new HttpError(409, "task-not-dead", `Task ${id} is not in the DLQ`);
	}
}

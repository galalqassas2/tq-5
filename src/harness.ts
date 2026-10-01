import { setTimeout as sleep } from "node:timers/promises";

import type { ClaimedTask, TaskType } from "./store.ts";

/** A failure no retry can fix: the task goes straight to the DLQ. */
export class PermanentError extends Error {}

export type HandlerContext = { task: ClaimedTask; signal: AbortSignal };
export type Handler = (payload: unknown, context: HandlerContext) => Promise<unknown>;

export type WorkerOptions = {
	baseUrl: string;
	queue: string;
	workerId: string;
	/** How many tasks it runs at once. */
	concurrency?: number;
	/** How long it waits when the queue is empty, in ms. */
	pollMs?: number;
};

const DEFAULT_CONCURRENCY = 4;
const DEFAULT_POLL_MS = 500;

/** Claims tasks over the API, runs each by its type, and acks or nacks what comes of it. */
export class Worker {
	readonly #handlers = new Map<string, Handler>();
	readonly #running = new Set<Promise<void>>();
	readonly #options: Required<WorkerOptions>;
	#stopped = false;
	#loop: Promise<void> = Promise.resolve();

	constructor(options: WorkerOptions) {
		this.#options = {
			concurrency: DEFAULT_CONCURRENCY,
			pollMs: DEFAULT_POLL_MS,
			...options,
		};
	}

	register(type: TaskType, handler: Handler): this {
		this.#handlers.set(type, handler);
		return this;
	}

	start(): void {
		this.#loop = this.#run();
	}

	/** Stops claiming and waits for the tasks in hand to finish. */
	async stop(): Promise<void> {
		this.#stopped = true;
		await this.#loop;
		await Promise.all(this.#running);
	}

	async #run(): Promise<void> {
		while (!this.#stopped) {
			const free = this.#options.concurrency - this.#running.size;
			const tasks = free > 0 ? await this.#claim(free) : [];
			for (const task of tasks) {
				const running: Promise<void> = this.#process(task)
					.catch(console.error)
					.finally(() => this.#running.delete(running));
				this.#running.add(running);
			}
			if (tasks.length === 0) await sleep(this.#options.pollMs);
		}
	}

	async #claim(max: number): Promise<ClaimedTask[]> {
		try {
			const { workerId, queue } = this.#options;
			const { tasks } = await this.#post(`/queues/${queue}/claim`, { workerId, max });
			return tasks;
		} catch (error) {
			console.error("claim failed", error);
			return [];
		}
	}

	async #process(task: ClaimedTask): Promise<void> {
		const lost = new AbortController();
		const heartbeat = this.#heartbeat(task, lost);
		try {
			const handler = this.#handlers.get(task.type);
			if (handler === undefined) throw new PermanentError(`No handler for type ${task.type}`);
			const result = await handler(task.payload, { task, signal: lost.signal });
			await this.#post(`/tasks/${task.id}/ack`, { result });
		} catch (error) {
			const reason = error instanceof Error ? error.message : String(error);
			const retryable = !(error instanceof PermanentError);
			await this.#post(`/tasks/${task.id}/nack`, { reason, retryable });
		} finally {
			clearInterval(heartbeat);
		}
	}

	/** Renews the lease at half its length; a refusal means another worker may hold it now. */
	#heartbeat(task: ClaimedTask, lost: AbortController): NodeJS.Timeout {
		const every = (new Date(task.leaseUntil).getTime() - Date.now()) / 2;
		const { workerId } = this.#options;
		return setInterval(() => {
			this.#post(`/tasks/${task.id}/extend`, { workerId }).catch((error) => {
				console.error(`lost the lease on ${task.id}`, error);
				lost.abort();
			});
		}, every);
	}

	async #post(path: string, body: unknown) {
		const response = await fetch(new URL(path, this.#options.baseUrl), {
			method: "POST",
			headers: { "content-type": "application/json", "x-worker-id": this.#options.workerId },
			body: JSON.stringify(body),
		});
		const text = await response.text();
		if (!response.ok) throw new Error(`${path} answered ${response.status}: ${text}`);
		return text === "" ? null : JSON.parse(text);
	}
}

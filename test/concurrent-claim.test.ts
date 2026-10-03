import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { setTimeout as sleep } from "node:timers/promises";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { httpHandler } from "../src/handlers/http.ts";
import { jsHandler } from "../src/handlers/js.ts";
import { llmHandler, StubProvider } from "../src/handlers/llm.ts";
import { type Handler, Worker } from "../src/harness.ts";
import type { NewTask, TaskType } from "../src/store.ts";
import { newQueue, realApi, until } from "./helpers.ts";

const TASKS = 50;
const WORKERS = 4;
const TYPES: TaskType[] = ["llm", "js", "http"];

const api = realApi();

const echo = createServer((_request, response) => response.end("ok"));

beforeAll(() => new Promise<void>((resolve) => echo.listen(0, resolve)));
afterAll(() => {
	echo.close();
});

/** Task `n`, cycling through the three types. */
function newTask(n: number): NewTask {
	const type = TYPES[n % TYPES.length];
	const origin = `http://127.0.0.1:${(echo.address() as AddressInfo).port}`;
	if (type === "http") return { type, payload: { url: `${origin}/${n}` } };
	if (type === "js") return { type, payload: { script: `return ${n}` } };
	return { type, payload: { model: "m", messages: [{ role: "user", content: `${n}` }] } };
}

describe("concurrent-claim", () => {
	it("gives each of 50 mixed tasks to one of 4 workers at a time, and loses none", async () => {
		const queue = newQueue();
		const ids = await Promise.all(
			Array.from({ length: TASKS }, async (_, n) => (await api.store.enqueue(queue, newTask(n))).id),
		);

		/** Who holds each task now, and every overlap and completion seen, by task. */
		const holders = new Map<string, string>();
		const overlaps: string[] = [];
		const processed = new Map<string, number>();

		const watched = (workerId: string, handler: Handler): Handler => async (payload, context) => {
			const { id } = context.task;
			if (holders.has(id)) overlaps.push(id);
			holders.set(id, workerId);
			await sleep(5);
			const result = await handler(payload, context);
			holders.delete(id);
			processed.set(id, (processed.get(id) ?? 0) + 1);
			return result;
		};

		const workers = Array.from({ length: WORKERS }, (_, n) => {
			const workerId = `worker-${n}`;
			return new Worker({ baseUrl: api.url, queue, workerId, pollMs: 5 })
				.register("llm", watched(workerId, llmHandler(new StubProvider())))
				.register("js", watched(workerId, jsHandler))
				.register("http", watched(workerId, httpHandler));
		});
		for (const worker of workers) worker.start();

		await until(async () => (await api.store.stats(queue)).ready === 0 && processed.size === TASKS, 60_000);
		await Promise.all(workers.map((worker) => worker.stop()));

		expect(overlaps).toEqual([]);
		for (const id of ids) expect(processed.get(id)).toBeGreaterThanOrEqual(1);
		expect(await api.store.stats(queue)).toEqual({ ready: 0, inFlight: 0, dlq: 0 });
	}, 90_000);

	it("never hands one task to two of 10 simultaneous claims", async () => {
		const queue = newQueue();
		await Promise.all(Array.from({ length: TASKS }, (_, n) => api.store.enqueue(queue, newTask(n))));

		const claims = await Promise.all(
			Array.from({ length: 10 }, (_, n) => api.store.claim(queue, `worker-${n}`, 10)),
		);

		const claimed = claims.flat().map(({ id }) => id);
		expect(new Set(claimed).size).toBe(claimed.length);
	});
});

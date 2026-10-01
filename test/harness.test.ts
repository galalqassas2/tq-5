import { setTimeout as sleep } from "node:timers/promises";
import { describe, expect, it } from "vitest";

import { Worker } from "../src/harness.ts";
import { newQueue, realApi, until } from "./helpers.ts";

const api = realApi();

const worker = (queue: string, workerId = "w1") =>
	new Worker({ baseUrl: api.url, queue, workerId, pollMs: 10 });

const JS = { type: "js" as const, payload: { script: "return 1" } };

describe("Worker", () => {
	it("routes a task by its type, and stores what the handler returns", async () => {
		const queue = newQueue();
		const { id } = await api.store.enqueue(queue, JS);
		const running = worker(queue).register("js", async () => "done");
		running.start();
		await until(async () => (await api.store.task(id)).status === "done");
		await running.stop();
		expect((await api.store.task(id)).result).toBe("done");
	});

	it("fails a task of a type it has no handler for", async () => {
		const queue = newQueue();
		const { id } = await api.store.enqueue(queue, JS);
		const running = worker(queue);
		running.start();
		await until(async () => (await api.store.task(id)).status === "dead");
		await running.stop();
		expect((await api.store.task(id)).lastError).toContain("No handler");
	});

	it("nacks a thrown error, then retries until it passes", async () => {
		const queue = newQueue();
		await api.store.configure(queue, { backoffBaseMs: 1, backoffMaxMs: 1 });
		const { id } = await api.store.enqueue(queue, JS);
		let calls = 0;
		const running = worker(queue).register("js", async () => {
			calls += 1;
			if (calls < 3) throw new Error("not yet");
			return calls;
		});
		running.start();
		await until(async () => (await api.store.task(id)).status === "done");
		await running.stop();
		expect(await api.store.task(id)).toMatchObject({ attempts: 3, result: 3 });
	});

	it("keeps the lease of a handler that outlasts it", async () => {
		const queue = newQueue();
		await api.store.configure(queue, { leaseMs: 100 });
		const { id } = await api.store.enqueue(queue, JS);
		const slow = worker(queue, "slow").register("js", async () => {
			await sleep(400);
			return "slow";
		});
		const other = worker(queue, "other").register("js", async () => "stolen");
		slow.start();
		await until(async () => (await api.store.task(id)).status === "in_flight");
		other.start();
		await until(async () => (await api.store.task(id)).status === "done");
		await Promise.all([slow.stop(), other.stop()]);
		expect(await api.store.task(id)).toMatchObject({ attempts: 1, result: "slow" });
	});
});

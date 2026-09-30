import { describe, expect, it } from "vitest";

import { newQueue, realApi } from "./helpers.ts";

const api = realApi();

const LLM = { type: "llm", payload: { model: "m", messages: [{ role: "user", content: "hi" }] } };

async function post(path: string, payload: object, workerId = "w1") {
	return api.app.inject({ method: "POST", url: path, payload, headers: { "x-worker-id": workerId } });
}

async function enqueue(queue: string, body: object = LLM) {
	const response = await post(`/queues/${queue}/tasks`, body);
	return { status: response.statusCode, ...response.json() };
}

async function claim(queue: string, workerId = "w1", max = 1) {
	const response = await post(`/queues/${queue}/claim`, { workerId, max });
	return response.json().tasks;
}

async function stats(queue: string) {
	return (await api.app.inject({ url: `/queues/${queue}/stats` })).json();
}

describe("the API", () => {
	it("refuses a bad request with its error shape", async () => {
		const response = await post(`/queues/${newQueue()}/tasks`, { type: "sql", payload: {} });
		expect(response.statusCode).toBe(400);
		expect(response.json().error.code).toBe("invalid-request");
	});

	it("collapses two enqueues with one dedupe key, even at once", async () => {
		const queue = newQueue();
		const body = { ...LLM, dedupeKey: "order-1" };
		const [first, second] = await Promise.all([enqueue(queue, body), enqueue(queue, body)]);
		expect(first.id).toBe(second.id);
		expect([first.status, second.status].sort()).toEqual([200, 201]);
		expect((await stats(queue)).ready).toBe(1);
	});

	it("adds a new task once the dedupe window has passed", async () => {
		const queue = newQueue();
		await api.app.inject({ method: "PUT", url: `/queues/${queue}`, payload: { dedupeWindowMs: 1 } });
		const first = await enqueue(queue, { ...LLM, dedupeKey: "k" });
		await new Promise((resolve) => setTimeout(resolve, 20));
		const second = await enqueue(queue, { ...LLM, dedupeKey: "k" });
		expect(second.id).not.toBe(first.id);
	});

	it("holds back a delayed task", async () => {
		const queue = newQueue();
		await enqueue(queue, { ...LLM, delay: 60_000 });
		expect(await claim(queue)).toEqual([]);
	});

	it("claims at most max, each with a lease, and counts the attempt", async () => {
		const queue = newQueue();
		await Promise.all([enqueue(queue), enqueue(queue), enqueue(queue)]);
		const tasks = await claim(queue, "w1", 2);
		expect(tasks).toHaveLength(2);
		expect(tasks[0].attempts).toBe(1);
		expect(new Date(tasks[0].leaseUntil).getTime()).toBeGreaterThan(Date.now());
		expect(await stats(queue)).toMatchObject({ ready: 1, inFlight: 2 });
	});

	it("keeps the result after an ack, and refuses another worker's", async () => {
		const queue = newQueue();
		await enqueue(queue);
		const [task] = await claim(queue);
		expect((await post(`/tasks/${task.id}/ack`, { result: 1 }, "w2")).statusCode).toBe(409);
		expect((await post(`/tasks/${task.id}/ack`, { result: { answer: 42 } })).statusCode).toBe(204);
		const read = await api.app.inject({ url: `/tasks/${task.id}` });
		expect(read.json()).toMatchObject({ status: "done", result: { answer: 42 } });
	});

	it("answers 404 for an unknown task", async () => {
		const response = await post("/tasks/00000000-0000-4000-8000-000000000000/ack", {});
		expect(response.statusCode).toBe(404);
	});

	it("schedules a retry by the backoff on a nack", async () => {
		const queue = newQueue();
		await api.app.inject({ method: "PUT", url: `/queues/${queue}`, payload: { backoffBaseMs: 60_000, backoffMaxMs: 60_000 } });
		await enqueue(queue);
		const [task] = await claim(queue);
		await post(`/tasks/${task.id}/nack`, { reason: "boom" });
		expect(await stats(queue)).toMatchObject({ ready: 1, inFlight: 0, dlq: 0 });
		expect(await claim(queue)).toEqual([]);
	});

	it("moves a task past its last attempt to the DLQ, and replays it from the start", async () => {
		const queue = newQueue();
		await api.app.inject({ method: "PUT", url: `/queues/${queue}`, payload: { maxAttempts: 1 } });
		await Promise.all([enqueue(queue), enqueue(queue)]);
		for (const task of await claim(queue, "w1", 2))
			await post(`/tasks/${task.id}/nack`, { reason: "boom" });
		expect((await stats(queue)).dlq).toBe(2);

		const page = await api.app.inject({ url: `/queues/${queue}/dlq?limit=1` });
		expect(page.json()).toMatchObject({ total: 2, limit: 1 });
		const [dead] = page.json().items;
		expect(dead.lastError).toBe("boom");
		const next = await api.app.inject({ url: `/queues/${queue}/dlq?limit=1&offset=1` });
		expect(next.json().items[0].id).not.toBe(dead.id);

		expect((await post(`/tasks/${dead.id}/requeue`, {})).statusCode).toBe(204);
		expect((await post(`/tasks/${dead.id}/requeue`, {})).statusCode).toBe(409);
		const [again] = await claim(queue);
		expect(again).toMatchObject({ id: dead.id, attempts: 1 });
	});

	it("sends a task that cannot succeed straight to the DLQ", async () => {
		const queue = newQueue();
		await enqueue(queue);
		const [task] = await claim(queue);
		await post(`/tasks/${task.id}/nack`, { reason: "bad script", retryable: false });
		expect((await stats(queue)).dlq).toBe(1);
	});

	it("returns a dead worker's task to ready, and extends only for the holder", async () => {
		const queue = newQueue();
		await api.app.inject({ method: "PUT", url: `/queues/${queue}`, payload: { leaseMs: 50 } });
		await enqueue(queue);
		const [task] = await claim(queue, "dies");
		const refused = await post(`/tasks/${task.id}/extend`, { workerId: "other" });
		expect(refused.statusCode).toBe(409);
		const extended = await post(`/tasks/${task.id}/extend`, { workerId: "dies" });
		expect(extended.statusCode).toBe(200);

		await new Promise((resolve) => setTimeout(resolve, 80));
		const [recovered] = await claim(queue, "w2");
		expect(recovered).toMatchObject({ id: task.id, attempts: 2 });
		expect((await post(`/tasks/${task.id}/ack`, {}, "dies")).statusCode).toBe(409);
	});

	it("sends a task to the DLQ when its last lease runs out", async () => {
		const queue = newQueue();
		await api.app.inject({ method: "PUT", url: `/queues/${queue}`, payload: { leaseMs: 1, maxAttempts: 1 } });
		await enqueue(queue);
		await claim(queue);
		await new Promise((resolve) => setTimeout(resolve, 20));
		await api.store.sweep();
		expect(await stats(queue)).toMatchObject({ inFlight: 0, dlq: 1 });
	});
});

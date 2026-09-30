import { readFile } from "node:fs/promises";
import Fastify, { type FastifyInstance } from "fastify";
import { z } from "zod";

import { HttpError, type Store, TASK_TYPES } from "./store.ts";

const WORKER_HEADER = "x-worker-id";
const DEFAULT_PAGE = 20;
const MAX_PAGE = 100;
const MAX_CLAIM = 100;

const Name = z.string().regex(/^[a-z0-9-]{1,64}$/);
const QueueParams = z.strictObject({ name: Name });
const TaskParams = z.strictObject({ id: z.uuid() });
const WorkerId = z.string().min(1).max(128);

const Enqueue = z.strictObject({
	type: z.enum(TASK_TYPES),
	payload: z.record(z.string(), z.unknown()),
	delay: z.number().int().nonnegative().optional(),
	dedupeKey: z.string().min(1).max(256).optional(),
});
const Claim = z.strictObject({
	workerId: WorkerId,
	max: z.number().int().min(1).max(MAX_CLAIM),
});
const Ack = z.strictObject({ result: z.unknown().optional() });
const Nack = z.strictObject({
	reason: z.string().min(1),
	retryable: z.boolean().default(true),
});
const Extend = z.strictObject({ workerId: WorkerId });
const Page = z.strictObject({
	limit: z.coerce.number().int().min(1).max(MAX_PAGE).default(DEFAULT_PAGE),
	offset: z.coerce.number().int().nonnegative().default(0),
});
const Configure = z.strictObject({
	maxAttempts: z.number().int().positive().optional(),
	backoffBaseMs: z.number().int().nonnegative().optional(),
	backoffFactor: z.number().min(1).optional(),
	backoffMaxMs: z.number().int().nonnegative().optional(),
	leaseMs: z.number().int().positive().optional(),
	dedupeWindowMs: z.number().int().positive().optional(),
});

/** The API, every error answered as { error: { code, message } }. */
export function buildApi(store: Store): FastifyInstance {
	const app = Fastify();

	app.setErrorHandler((error, _request, reply) => {
		if (error instanceof z.ZodError)
			return reply
				.status(400)
				.send({ error: { code: "invalid-request", message: z.prettifyError(error) } });
		if (error instanceof HttpError)
			return reply.status(error.status).send({ error: { code: error.code, message: error.message } });
		console.error(error);
		return reply.status(500).send({ error: { code: "internal", message: "Something failed" } });
	});

	/** The worker named by the static header that acks and nacks must carry. */
	const workerOf = (headers: Record<string, unknown>) => WorkerId.parse(headers[WORKER_HEADER]);

	app.get("/", async (_request, reply) => {
		const page = await readFile(new URL("../public/index.html", import.meta.url), "utf8");
		return reply.type("text/html").send(page);
	});

	app.get("/queues", async () => ({ queues: await store.queues() }));

	app.put("/queues/:name", async (request, reply) => {
		const { name } = QueueParams.parse(request.params);
		await store.configure(name, Configure.parse(request.body));
		return reply.status(204).send();
	});

	app.post("/queues/:name/tasks", async (request, reply) => {
		const { name } = QueueParams.parse(request.params);
		const task = await store.enqueue(name, Enqueue.parse(request.body));
		return reply.status(task.deduped ? 200 : 201).send(task);
	});

	app.post("/queues/:name/claim", async (request) => {
		const { name } = QueueParams.parse(request.params);
		const { workerId, max } = Claim.parse(request.body);
		return { tasks: await store.claim(name, workerId, max) };
	});

	app.get("/queues/:name/stats", async (request) => {
		const { name } = QueueParams.parse(request.params);
		return store.stats(name);
	});

	app.get("/queues/:name/dlq", async (request) => {
		const { name } = QueueParams.parse(request.params);
		const { limit, offset } = Page.parse(request.query);
		return { ...(await store.dlq(name, limit, offset)), limit, offset };
	});

	app.get("/tasks/:id", async (request) => {
		const { id } = TaskParams.parse(request.params);
		return store.task(id);
	});

	app.post("/tasks/:id/ack", async (request, reply) => {
		const { id } = TaskParams.parse(request.params);
		const { result } = Ack.parse(request.body ?? {});
		await store.ack(id, workerOf(request.headers), result);
		return reply.status(204).send();
	});

	app.post("/tasks/:id/nack", async (request, reply) => {
		const { id } = TaskParams.parse(request.params);
		const { reason, retryable } = Nack.parse(request.body);
		await store.nack(id, workerOf(request.headers), reason, retryable);
		return reply.status(204).send();
	});

	app.post("/tasks/:id/extend", async (request) => {
		const { id } = TaskParams.parse(request.params);
		const { workerId } = Extend.parse(request.body);
		return store.extend(id, workerId);
	});

	app.post("/tasks/:id/requeue", async (request, reply) => {
		const { id } = TaskParams.parse(request.params);
		await store.requeue(id);
		return reply.status(204).send();
	});

	return app;
}

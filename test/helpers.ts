import { randomUUID } from "node:crypto";
import type { AddressInfo } from "node:net";
import type { FastifyInstance } from "fastify";
import type pg from "pg";
import { afterAll, beforeAll } from "vitest";

import { buildApi } from "../src/api.ts";
import { createPool, migrate } from "../src/db.ts";
import { Store } from "../src/store.ts";

/** A queue no other test touches, as tests share the database. */
export const newQueue = () => `test-${randomUUID()}`;

/** A real database and API, listening, for the file's tests. */
export function realApi() {
	let pool: pg.Pool;
	let app: FastifyInstance;
	const context = { store: null as unknown as Store, app: null as unknown as FastifyInstance, url: "" };

	beforeAll(async () => {
		pool = createPool();
		await migrate(pool);
		context.store = new Store(pool);
		app = buildApi(context.store);
		await app.listen({ port: 0 });
		context.app = app;
		context.url = `http://127.0.0.1:${(app.server.address() as AddressInfo).port}`;
	});

	afterAll(async () => {
		await app.close();
		await pool.end();
	});

	return context;
}

/** Waits until `done` holds, or fails the test. */
export async function until(done: () => Promise<boolean>, timeoutMs = 15_000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		if (await done()) return;
		await new Promise((resolve) => setTimeout(resolve, 20));
	}
	throw new Error("Timed out waiting");
}

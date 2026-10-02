import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { httpHandler } from "../src/handlers/http.ts";
import { jsHandler } from "../src/handlers/js.ts";
import { llmHandler, ProviderError, StubProvider } from "../src/handlers/llm.ts";
import { type HandlerContext, PermanentError } from "../src/harness.ts";

const context = { signal: new AbortController().signal } as HandlerContext;

describe("jsHandler", () => {
	it("captures the script's return value", async () => {
		expect(await jsHandler({ script: "return 1 + 1" }, context)).toBe(2);
		expect(await jsHandler({ script: "return await Promise.resolve('x')" }, context)).toBe("x");
	});

	it("does not retry a script that does not compile", async () => {
		await expect(jsHandler({ script: "return (" }, context)).rejects.toBeInstanceOf(PermanentError);
	});

	it("retries a runtime error", async () => {
		const failed = jsHandler({ script: "throw new Error('boom')" }, context);
		await expect(failed).rejects.toThrow("boom");
		await expect(failed).rejects.not.toBeInstanceOf(PermanentError);
	});

	it("keeps the script away from the worker's process", async () => {
		await expect(jsHandler({ script: "return process.pid" }, context)).rejects.toThrow("process");
		await expect(jsHandler({ script: "return require('node:fs')" }, context)).rejects.toThrow("require");
	});
});

describe("httpHandler", () => {
	const server = createServer((request, response) => {
		response.statusCode = Number(request.url?.slice(1));
		response.end(`answered ${request.method}`);
	});
	let origin = "";

	beforeAll(async () => {
		await new Promise<void>((resolve) => server.listen(0, resolve));
		origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
	});

	afterAll(() => {
		server.close();
	});

	it("captures the status and the body", async () => {
		const result = await httpHandler({ method: "POST", url: `${origin}/201`, body: "x" }, context);
		expect(result).toEqual({ status: 201, body: "answered POST" });
	});

	it("retries a 503 and a 429, not a 404", async () => {
		for (const status of [503, 429]) {
			const failed = httpHandler({ url: `${origin}/${status}` }, context);
			await expect(failed).rejects.not.toBeInstanceOf(PermanentError);
		}
		await expect(httpHandler({ url: `${origin}/404` }, context)).rejects.toBeInstanceOf(PermanentError);
	});
});

describe("llmHandler", () => {
	const payload = { model: "m", messages: [{ role: "user", content: "hi" }] };
	const failing = (status: number) =>
		llmHandler({
			complete: async () => {
				throw new ProviderError(status, `answered ${status}`);
			},
		});

	it("captures the completion", async () => {
		const result = await llmHandler(new StubProvider())(payload, context);
		expect(result).toEqual({ text: "[m] hi", tokens: 2 });
	});

	it("retries a rate limit, not a refused request", async () => {
		await expect(failing(429)(payload, context)).rejects.not.toBeInstanceOf(PermanentError);
		await expect(failing(400)(payload, context)).rejects.toBeInstanceOf(PermanentError);
	});

	it("refuses a payload with no messages", async () => {
		const handler = llmHandler(new StubProvider());
		await expect(handler({ model: "m", messages: [] }, context)).rejects.toBeInstanceOf(PermanentError);
	});
});

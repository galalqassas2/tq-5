import { randomUUID } from "node:crypto";

import { httpHandler } from "./handlers/http.ts";
import { jsHandler } from "./handlers/js.ts";
import { llmHandler, StubProvider } from "./handlers/llm.ts";
import { Worker } from "./harness.ts";

const worker = new Worker({
	baseUrl: process.env.QUEUE_URL ?? "http://localhost:3000",
	queue: process.env.QUEUE_NAME ?? "default",
	workerId: `worker-${randomUUID()}`,
})
	.register("llm", llmHandler(new StubProvider()))
	.register("js", jsHandler)
	.register("http", httpHandler);

worker.start();

for (const signal of ["SIGINT", "SIGTERM"] as const)
	process.once(signal, () => void worker.stop().then(() => process.exit(0)));

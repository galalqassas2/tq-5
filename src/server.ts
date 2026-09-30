import { buildApi } from "./api.ts";
import { createPool, migrate } from "./db.ts";
import { Store } from "./store.ts";

const SWEEP_EVERY_MS = 1000;

const pool = createPool();
await migrate(pool);

const store = new Store(pool);
setInterval(() => void store.sweep().catch(console.error), SWEEP_EVERY_MS);

const port = Number(process.env.PORT ?? 3000);
await buildApi(store).listen({ port, host: "0.0.0.0" });
console.info(`queue listening on http://localhost:${port}`);

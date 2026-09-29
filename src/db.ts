import { readFile } from "node:fs/promises";
import pg from "pg";

const LOCAL_DATABASE = "postgres://queue:queue@localhost:5433/queue";

export function createPool(): pg.Pool {
	return new pg.Pool({ connectionString: process.env.DATABASE_URL ?? LOCAL_DATABASE });
}

/** Creates what is missing; safe to run on every start. */
export async function migrate(pool: pg.Pool): Promise<void> {
	await pool.query(await readFile(new URL("../schema.sql", import.meta.url), "utf8"));
}

import { spawn } from "node:child_process";
import { z } from "zod";

import { type Handler, PermanentError } from "../harness.ts";

const Payload = z.strictObject({ script: z.string().min(1) });

const SCRIPT_TIMEOUT_MS = 10_000;

/** Runs in the child: compiles the script read from stdin, and prints one JSON line of what came. */
const RUNNER = `
const vm = require("node:vm");
let source = "";
process.stdin.on("data", (chunk) => { source += chunk; });
process.stdin.on("end", async () => {
	const print = (outcome) => process.stdout.write(JSON.stringify(outcome));
	try {
		const script = new vm.Script("(async () => {" + source + "\\n})()");
		print({ value: await script.runInNewContext({}) });
	} catch (error) {
		print({ error: { name: error.name, message: error.message } });
	}
});
`;

const Outcome = z.union([
	z.strictObject({ value: z.unknown() }),
	z.strictObject({ error: z.strictObject({ name: z.string(), message: z.string() }) }),
]);

/**
 * Runs the script in its own process under Node's permission model, in a fresh context: no files,
 * no child processes, no require. A crash or a hang there takes nothing down here.
 */
function run(script: string, signal: AbortSignal): Promise<string> {
	return new Promise((resolve, reject) => {
		const child = spawn(process.execPath, ["--permission", "-e", RUNNER], {
			signal,
			timeout: SCRIPT_TIMEOUT_MS,
			stdio: ["pipe", "pipe", "inherit"],
		});
		let output = "";
		child.stdout.on("data", (chunk) => {
			output += chunk;
		});
		child.on("error", reject);
		child.on("close", (code) => {
			if (code === 0) resolve(output);
			else reject(new Error(`The script's process ended with ${code}`));
		});
		child.stdin.end(script);
	});
}

/** A script that does not compile fails the same every time; a runtime error may not. */
export const jsHandler: Handler = async (payload, { signal }) => {
	const parsed = Payload.safeParse(payload);
	if (!parsed.success) throw new PermanentError(z.prettifyError(parsed.error));
	const outcome = Outcome.parse(JSON.parse(await run(parsed.data.script, signal)));
	if (!("error" in outcome)) return outcome.value;
	const message = `${outcome.error.name}: ${outcome.error.message}`;
	if (outcome.error.name === "SyntaxError") throw new PermanentError(message);
	throw new Error(message);
};

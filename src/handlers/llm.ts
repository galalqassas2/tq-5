import { z } from "zod";

import { type Handler, PermanentError } from "../harness.ts";

const Payload = z.strictObject({
	model: z.string().min(1),
	messages: z.array(z.strictObject({ role: z.string(), content: z.string() })).min(1),
});

export type Completion = { text: string; tokens: number };

export interface LlmProvider {
	complete(request: z.infer<typeof Payload>, signal: AbortSignal): Promise<Completion>;
}

/** What a provider throws on a refused request, with the status it answered. */
export class ProviderError extends Error {
	readonly status: number;

	constructor(status: number, message: string) {
		super(message);
		this.status = status;
	}
}

const TOO_MANY_REQUESTS = 429;
const SERVER_ERRORS = 500;

/** Answers without a network; swap it for a real provider behind the same interface. */
export class StubProvider implements LlmProvider {
	async complete({ model, messages }: z.infer<typeof Payload>): Promise<Completion> {
		const last = messages.at(-1)?.content ?? "";
		return { text: `[${model}] ${last}`, tokens: last.length };
	}
}

/** Rate limits and provider outages pass; a request the provider refused will be refused again. */
export function llmHandler(provider: LlmProvider): Handler {
	return async (payload, { signal }) => {
		const parsed = Payload.safeParse(payload);
		if (!parsed.success) throw new PermanentError(z.prettifyError(parsed.error));
		try {
			return await provider.complete(parsed.data, signal);
		} catch (error) {
			if (
				error instanceof ProviderError &&
				error.status !== TOO_MANY_REQUESTS &&
				error.status < SERVER_ERRORS
			)
				throw new PermanentError(error.message);
			throw error;
		}
	};
}

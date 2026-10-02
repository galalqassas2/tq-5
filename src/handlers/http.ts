import { z } from "zod";

import { type Handler, PermanentError } from "../harness.ts";

const Payload = z.strictObject({
	method: z.string().default("GET"),
	url: z.url(),
	headers: z.record(z.string(), z.string()).optional(),
	body: z.string().optional(),
});

const TOO_MANY_REQUESTS = 429;
const SERVER_ERRORS = 500;
const CLIENT_ERRORS = 400;

/**
 * Makes the request and captures the status and body. A 5xx, a 429 or a network failure may pass,
 * so it retries; any other 4xx will answer the same next time, so it does not.
 */
export const httpHandler: Handler = async (payload, { signal }) => {
	const parsed = Payload.safeParse(payload);
	if (!parsed.success) throw new PermanentError(z.prettifyError(parsed.error));
	const { url, ...request } = parsed.data;
	const response = await fetch(url, { ...request, signal });
	const body = await response.text();
	if (response.status >= SERVER_ERRORS || response.status === TOO_MANY_REQUESTS)
		throw new Error(`${url} answered ${response.status}`);
	if (response.status >= CLIENT_ERRORS)
		throw new PermanentError(`${url} answered ${response.status}`);
	return { status: response.status, body };
};

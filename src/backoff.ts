export type BackoffPolicy = {
	baseMs: number;
	factor: number;
	maxMs: number;
};

/**
 * Exponential backoff with half of it jittered, so tasks that failed together do not retry
 * together. With a 1000 ms base and a factor of 2, attempt 3 waits between 2000 and 4000 ms.
 */
export function backoffMs(attempt: number, policy: BackoffPolicy, random = Math.random): number {
	const ceiling = Math.min(policy.maxMs, policy.baseMs * policy.factor ** (attempt - 1));
	return Math.round(ceiling / 2 + (random() * ceiling) / 2);
}

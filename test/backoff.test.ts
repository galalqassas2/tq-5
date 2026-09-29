import { describe, expect, it } from "vitest";

import { backoffMs } from "../src/backoff.ts";

const policy = { baseMs: 1000, factor: 2, maxMs: 8000 };

describe("backoffMs", () => {
	it("doubles with each attempt, half of it jittered", () => {
		expect(backoffMs(1, policy, () => 0)).toBe(500);
		expect(backoffMs(1, policy, () => 1)).toBe(1000);
		expect(backoffMs(3, policy, () => 0)).toBe(2000);
		expect(backoffMs(3, policy, () => 1)).toBe(4000);
	});

	it("stops growing at the cap", () => {
		expect(backoffMs(20, policy, () => 1)).toBe(8000);
	});
});

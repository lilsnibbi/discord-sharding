import { describe, expect, test } from "bun:test";
import { validateLcovCoverage } from "../../scripts/coverage-report";

const passingReport = `TN:
SF:src/one.ts
FNF:2
FNH:1
LF:4
LH:4
end_of_record
SF:src/two.ts
FNF:3
FNH:3
LF:6
LH:4
end_of_record
`;

describe("LCOV aggregate validation", () => {
	test("sums records before enforcing line and function coverage", () => {
		const result = validateLcovCoverage(passingReport);

		expect(result).toEqual({
			functions: { found: 5, hit: 4, ratio: 0.8 },
			lines: { found: 10, hit: 8, ratio: 0.8 },
		});
		expect(Object.isFrozen(result)).toBe(true);
		expect(Object.isFrozen(result.functions)).toBe(true);
		expect(Object.isFrozen(result.lines)).toBe(true);
	});

	test("rejects aggregate coverage below the required ratio", () => {
		expect(() => validateLcovCoverage(passingReport, 0.81)).toThrow("Function coverage 80.00%");
	});

	test.each([
		["", "no source records"],
		["SF:src/example.ts\nFNF:1\nFNH:2\nLF:1\nLH:1\nend_of_record\n", "function hits exceed"],
		["SF:src/example.ts\nFNF:one\nFNH:0\nLF:1\nLH:1\nend_of_record\n", "FNF must be"],
		["SF:src/example.ts\nFNF:1\nFNH:1\nLF:1\nend_of_record\n", "missing LH"],
	] as const)("rejects malformed reports", (report, message) => {
		expect(() => validateLcovCoverage(report)).toThrow(message);
	});

	test("validates the configured minimum", () => {
		expect(() => validateLcovCoverage(passingReport, Number.NaN)).toThrow(TypeError);
		expect(() => validateLcovCoverage(passingReport, 1.01)).toThrow(TypeError);
	});
});

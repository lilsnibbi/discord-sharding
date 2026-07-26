interface CoverageRecord {
	functionsFound?: number;
	functionsHit?: number;
	linesFound?: number;
	linesHit?: number;
	source?: string;
}

interface CoverageMetric {
	readonly found: number;
	readonly hit: number;
	readonly ratio: number;
}

/**
 * Validates aggregate line and function coverage from an LCOV report.
 *
 * Bun applies its built-in line and function thresholds to each file. This
 * check intentionally measures the package-wide totals reported to users.
 *
 * @param report - Complete LCOV report text.
 * @param minimum - Inclusive aggregate ratio required for each metric.
 * @returns Validated aggregate line and function totals.
 */
export function validateLcovCoverage(
	report: string,
	minimum = 0.8,
): { readonly functions: CoverageMetric; readonly lines: CoverageMetric } {
	if (!Number.isFinite(minimum) || minimum < 0 || minimum > 1) {
		throw new TypeError("Coverage minimum must be a finite ratio from 0 through 1.");
	}

	let record: CoverageRecord = {};
	let records = 0;
	let functionsFound = 0;
	let functionsHit = 0;
	let linesFound = 0;
	let linesHit = 0;

	const finishRecord = (): void => {
		if (record.source === undefined) {
			if (Object.keys(record).length > 0) throw new Error("LCOV record is missing SF.");
			return;
		}
		const currentFunctionsFound = requireCount(record.functionsFound, "FNF", record.source);
		const currentFunctionsHit = requireCount(record.functionsHit, "FNH", record.source);
		const currentLinesFound = requireCount(record.linesFound, "LF", record.source);
		const currentLinesHit = requireCount(record.linesHit, "LH", record.source);
		assertCoveredCount(currentFunctionsHit, currentFunctionsFound, "function", record.source);
		assertCoveredCount(currentLinesHit, currentLinesFound, "line", record.source);
		functionsFound += currentFunctionsFound;
		functionsHit += currentFunctionsHit;
		linesFound += currentLinesFound;
		linesHit += currentLinesHit;
		records += 1;
		record = {};
	};

	for (const rawLine of report.split(/\r?\n/)) {
		const line = rawLine.trim();
		if (line.length === 0 || line.startsWith("TN:")) continue;
		if (line === "end_of_record") {
			finishRecord();
			continue;
		}
		const separator = line.indexOf(":");
		if (separator < 0) continue;
		const key = line.slice(0, separator);
		const value = line.slice(separator + 1);
		switch (key) {
			case "SF":
				if (record.source !== undefined) throw new Error(`LCOV record ${record.source} is missing end_of_record.`);
				if (value.length === 0) throw new Error("LCOV SF must name a source file.");
				record.source = value;
				break;
			case "FNF":
				record.functionsFound = parseCount(value, key);
				break;
			case "FNH":
				record.functionsHit = parseCount(value, key);
				break;
			case "LF":
				record.linesFound = parseCount(value, key);
				break;
			case "LH":
				record.linesHit = parseCount(value, key);
				break;
		}
	}
	finishRecord();

	if (records === 0) throw new Error("LCOV report contains no source records.");
	const functions = metric(functionsHit, functionsFound, "function");
	const lines = metric(linesHit, linesFound, "line");
	assertMinimum(functions, minimum, "Function");
	assertMinimum(lines, minimum, "Line");
	return Object.freeze({
		functions: Object.freeze(functions),
		lines: Object.freeze(lines),
	});
}

function parseCount(value: string, name: string): number {
	if (!/^(?:0|[1-9]\d*)$/u.test(value)) throw new Error(`LCOV ${name} must be a non-negative integer.`);
	const count = Number(value);
	if (!Number.isSafeInteger(count)) throw new Error(`LCOV ${name} exceeds the safe integer range.`);
	return count;
}

function requireCount(value: number | undefined, name: string, source: string): number {
	if (value === undefined) throw new Error(`LCOV record ${source} is missing ${name}.`);
	return value;
}

function assertCoveredCount(hit: number, found: number, name: string, source: string): void {
	if (hit > found) throw new Error(`LCOV ${name} hits exceed the total for ${source}.`);
}

function metric(hit: number, found: number, name: string): CoverageMetric {
	if (found === 0) throw new Error(`LCOV report contains no ${name} coverage entries.`);
	return { found, hit, ratio: hit / found };
}

function assertMinimum(metricValue: CoverageMetric, minimum: number, label: string): void {
	if (metricValue.ratio + Number.EPSILON < minimum) {
		throw new Error(
			`${label} coverage ${formatPercent(metricValue.ratio)} is below the required ${formatPercent(minimum)}.`,
		);
	}
}

function formatPercent(value: number): string {
	return `${(value * 100).toFixed(2)}%`;
}

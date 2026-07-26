import { validateLcovCoverage } from "./coverage-report";
import { BUN_EXECUTABLE, ROOT_DIRECTORY } from "./repository";
import { runInherited } from "./run";

await runInherited(
	[
		BUN_EXECUTABLE,
		"test",
		"--path-ignore-patterns",
		"tests/performance/**",
		"--coverage",
		"--coverage-reporter=text",
		"--coverage-reporter=lcov",
	],
	ROOT_DIRECTORY,
);

const reportPath = `${ROOT_DIRECTORY}/coverage/lcov.info`;
if (!(await Bun.file(reportPath).exists())) throw new Error("Bun did not create the expected LCOV report.");
const coverage = validateLcovCoverage(await Bun.file(reportPath).text());
console.log(
	`Aggregate coverage passed: ${(coverage.lines.ratio * 100).toFixed(2)}% lines, ` +
		`${(coverage.functions.ratio * 100).toFixed(2)}% functions.`,
);

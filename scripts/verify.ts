import { BUN_EXECUTABLE, ROOT_DIRECTORY } from "./repository";
import { runInherited } from "./run";

for (const script of [
	"typecheck",
	"check:docs",
	"check:examples",
	"check:imports",
	"check:jsdoc",
	"check",
	"test",
	"test:coverage",
	"test:performance",
	"release:verify",
	"pack:check",
]) {
	await runInherited([BUN_EXECUTABLE, "run", script], ROOT_DIRECTORY);
}

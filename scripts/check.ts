import { checkMaintainedFileLengths } from "./check-lines";
import { BUN_EXECUTABLE, ROOT_DIRECTORY } from "./repository";
import { runInherited } from "./run";

await checkMaintainedFileLengths();
await runInherited([BUN_EXECUTABLE, "x", "--bun", "--no-install", "biome", "check", "."], ROOT_DIRECTORY);

import { describe, expect, test } from "bun:test";
import { $ } from "bun";
import { removeDirectChildDirectory } from "../../scripts/cleanup";
import { ROOT_DIRECTORY } from "../../scripts/repository";

const temporaryParent = `${ROOT_DIRECTORY}/.tmp`;

describe("repository cleanup", () => {
	test("removes one validated direct child", async () => {
		const directory = `${temporaryParent}/cleanup-test-${crypto.randomUUID()}`;
		await $`mkdir -p ${directory}`.quiet();
		await Bun.write(`${directory}/marker.txt`, "cleanup");

		await removeDirectChildDirectory(directory, temporaryParent);

		expect(await containsEntry(temporaryParent, directory.split("/").at(-1) ?? "")).toBe(false);
	});

	test("rejects nested and traversal targets", async () => {
		await expect(removeDirectChildDirectory(`${temporaryParent}/first/second`, temporaryParent)).rejects.toThrow(
			"not a direct child",
		);
		await expect(removeDirectChildDirectory(`${temporaryParent}/../outside`, temporaryParent)).rejects.toThrow(
			"must not contain traversal",
		);
	});
});

async function containsEntry(parent: string, name: string): Promise<boolean> {
	for await (const _entry of new Bun.Glob(name).scan({ cwd: parent, onlyFiles: false })) return true;
	return false;
}

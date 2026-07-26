import { removeDirectChildDirectory } from "./cleanup";
import { ROOT_DIRECTORY } from "./repository";

const targets = [`${ROOT_DIRECTORY}/.tmp`, `${ROOT_DIRECTORY}/coverage`, `${ROOT_DIRECTORY}/release-assets`];

for (const target of targets) {
	await removeDirectChildDirectory(target, ROOT_DIRECTORY);
}

import { ROOT_DIRECTORY } from "./repository";

interface BrokenLink {
	readonly file: string;
	readonly reason: string;
	readonly target: string;
}

const markdownFiles = new Set<string>();
for (const pattern of ["*.md", "docs/**/*.md"]) {
	for await (const path of new Bun.Glob(pattern).scan({
		absolute: true,
		cwd: ROOT_DIRECTORY,
		onlyFiles: true,
	})) {
		markdownFiles.add(normalizePath(path));
	}
}

const brokenLinks: BrokenLink[] = [];
const anchorCache = new Map<string, ReadonlySet<string>>();
for (const file of [...markdownFiles].sort()) {
	const markdown = await Bun.file(file).text();
	for (const target of localLinkTargets(markdown)) {
		const { fragment, path } = splitLocalTarget(target);
		const resolved = path.length === 0 ? file : resolveRelativePath(file, decodeURIComponent(path));
		if (!(await Bun.file(resolved).exists())) {
			brokenLinks.push({ file, reason: "missing target", target });
			continue;
		}
		if (fragment.length === 0 || !resolved.toLowerCase().endsWith(".md")) continue;
		const decodedFragment = decodeURIComponent(fragment);
		let anchors = anchorCache.get(resolved);
		if (anchors === undefined) {
			anchors = markdownAnchors(await Bun.file(resolved).text());
			anchorCache.set(resolved, anchors);
		}
		if (!anchors.has(decodedFragment)) brokenLinks.push({ file, reason: "missing Markdown anchor", target });
	}
}

if (brokenLinks.length > 0) {
	for (const link of brokenLinks) {
		console.error(`${link.file}: broken local documentation link (${link.reason}) ${link.target}`);
	}
	throw new Error(`${brokenLinks.length} local documentation link${brokenLinks.length === 1 ? "" : "s"} are broken.`);
}

console.log(`Validated local links across ${markdownFiles.size} Markdown files.`);

function localLinkTargets(markdown: string): readonly string[] {
	const targets: string[] = [];
	const inlineLink = /!?\[[^\]]*]\(\s*(?:<([^>]+)>|([^\s)]+))(?:\s+["'][^"']*["'])?\s*\)/g;
	for (const match of markdown.matchAll(inlineLink)) {
		const target = match[1] ?? match[2];
		if (target !== undefined && isLocalTarget(target)) targets.push(target);
	}
	const referenceLink = /^\s*\[[^\]]+]:\s*(?:<([^>]+)>|(\S+))/gm;
	for (const match of markdown.matchAll(referenceLink)) {
		const target = match[1] ?? match[2];
		if (target !== undefined && isLocalTarget(target)) targets.push(target);
	}
	return targets;
}

function isLocalTarget(target: string): boolean {
	return (
		!target.startsWith("/") &&
		!target.startsWith("http://") &&
		!target.startsWith("https://") &&
		!target.startsWith("mailto:")
	);
}

function splitLocalTarget(target: string): { readonly fragment: string; readonly path: string } {
	const hashIndex = target.indexOf("#");
	const queryIndex = target.indexOf("?");
	const pathEnd = [hashIndex, queryIndex].filter((index) => index >= 0).sort((left, right) => left - right)[0];
	const path = pathEnd === undefined ? target : target.slice(0, pathEnd);
	const fragment = hashIndex < 0 ? "" : target.slice(hashIndex + 1, queryIndex > hashIndex ? queryIndex : undefined);
	return { fragment, path };
}

function markdownAnchors(markdown: string): ReadonlySet<string> {
	const anchors = new Set<string>();
	const slugCounts = new Map<string, number>();
	let fence: { readonly character: string; readonly length: number } | undefined;
	for (const line of markdown.split(/\r?\n/)) {
		const fenceMatch = /^ {0,3}(`{3,}|~{3,})/.exec(line);
		const marker = fenceMatch?.[1];
		if (marker !== undefined) {
			if (fence === undefined) {
				fence = { character: marker[0] ?? "", length: marker.length };
			} else if (marker[0] === fence.character && marker.length >= fence.length) {
				fence = undefined;
			}
			continue;
		}
		if (fence !== undefined) continue;

		for (const match of line.matchAll(/\bid\s*=\s*["']([^"']+)["']/gi)) {
			const id = match[1];
			if (id !== undefined && id.length > 0) anchors.add(id);
		}

		const headingMatch = /^ {0,3}#{1,6}[ \t]+(.+?)[ \t]*#*[ \t]*$/.exec(line);
		const heading = headingMatch?.[1];
		if (heading === undefined) continue;
		const base = githubHeadingSlug(heading);
		if (base.length === 0) continue;
		const duplicate = slugCounts.get(base) ?? 0;
		slugCounts.set(base, duplicate + 1);
		anchors.add(duplicate === 0 ? base : `${base}-${duplicate}`);
	}
	return anchors;
}

function githubHeadingSlug(heading: string): string {
	return heading
		.replace(/!\[([^\]]*)]\([^)]*\)/g, "$1")
		.replace(/\[([^\]]+)]\([^)]*\)/g, "$1")
		.replace(/<[^>]*>/g, "")
		.toLowerCase()
		.replace(/[^\p{L}\p{N}\s_-]/gu, "")
		.trim()
		.replace(/\s+/g, "-");
}

function resolveRelativePath(sourceFile: string, target: string): string {
	const rootParts = normalizePath(ROOT_DIRECTORY).split("/");
	const parts = normalizePath(sourceFile).split("/");
	if (!rootParts.every((segment, index) => parts[index] === segment)) {
		throw new Error(`Documentation source is outside the repository: ${sourceFile}`);
	}
	parts.pop();
	for (const segment of normalizePath(target).split("/")) {
		if (segment.length === 0 || segment === ".") continue;
		if (segment === "..") {
			if (parts.length <= rootParts.length) {
				throw new Error(`Documentation link escapes the repository: ${target}`);
			}
			parts.pop();
			continue;
		}
		parts.push(segment);
	}
	return parts.join("/");
}

function normalizePath(path: string): string {
	return path.replaceAll("\\", "/");
}

import * as ts from "typescript/unstable/ast";
import type { Diagnostic, Project } from "typescript/unstable/sync";
import { createSyncApi, ROOT_DIRECTORY } from "./repository";

interface DocumentationExample {
	readonly block: number;
	readonly documentationFile: string;
	readonly origin: "jsdoc" | "markdown";
	readonly source: string;
	readonly virtualFile: string;
}

interface ExamplePolicyFailure {
	readonly example: DocumentationExample;
	readonly reason: string;
	readonly specifier: string;
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

const examples: DocumentationExample[] = [];
for (const markdownFile of [...markdownFiles].sort()) {
	const markdown = await Bun.file(markdownFile).text();
	const sources = extractTypeScriptFences(markdown, markdownFile);
	for (const [block, source] of sources.entries()) {
		examples.push({
			block,
			documentationFile: markdownFile,
			origin: "markdown",
			source,
			virtualFile: `${ROOT_DIRECTORY}/.sharding-doc-example-${examples.length}.ts`,
		});
	}
}

const packageSourceFiles = new Set<string>();
for await (const path of new Bun.Glob("src/**/*.ts").scan({
	absolute: true,
	cwd: ROOT_DIRECTORY,
	onlyFiles: true,
})) {
	packageSourceFiles.add(normalizePath(path));
}
for (const packageSourceFile of [...packageSourceFiles].sort()) {
	const sourceFile = await Bun.file(packageSourceFile).text();
	const sources = extractJSDocExamples(sourceFile, packageSourceFile);
	for (const [block, source] of sources.entries()) {
		examples.push({
			block,
			documentationFile: packageSourceFile,
			origin: "jsdoc",
			source,
			virtualFile: `${ROOT_DIRECTORY}/.sharding-doc-example-${examples.length}.ts`,
		});
	}
}

if (examples.length === 0) throw new Error("No TypeScript documentation examples were found.");

const configFile = `${ROOT_DIRECTORY}/.sharding-doc-examples.tsconfig.json`;
const virtualFiles = new Map<string, string>();
for (const example of examples) {
	virtualFiles.set(normalizeLookupPath(example.virtualFile), `${example.source}\nexport {};\n`);
}
virtualFiles.set(
	normalizeLookupPath(configFile),
	JSON.stringify({
		compilerOptions: { noEmit: true },
		extends: "./tsconfig.json",
		files: examples.map((example) => `./${basename(example.virtualFile)}`),
		include: [],
	}),
);

const api = await createSyncApi({
	cwd: ROOT_DIRECTORY,
	fs: {
		fileExists(path) {
			return virtualFiles.has(normalizeLookupPath(path)) ? true : undefined;
		},
		readFile(path) {
			return virtualFiles.get(normalizeLookupPath(path));
		},
	},
});
const snapshot = api.updateSnapshot({ openProjects: [configFile] });

try {
	const project = snapshot
		.getProjects()
		.find((candidate) => normalizeLookupPath(candidate.configFileName) === normalizeLookupPath(configFile));
	if (project === undefined) throw new Error("Unable to load the virtual documentation-example TypeScript project.");

	const policyFailures = validateExamplePolicy(examples, project);
	if (policyFailures.length > 0) {
		for (const failure of policyFailures) {
			console.error(`${exampleLocation(failure.example)}: ${failure.reason}: ${failure.specifier}`);
		}
		throw new Error(
			`${policyFailures.length} documentation import-policy failure${policyFailures.length === 1 ? "" : "s"} found.`,
		);
	}

	const diagnostics = [
		...project.program.getConfigFileParsingDiagnostics(),
		...project.program.getProgramDiagnostics(),
		...project.program.getGlobalDiagnostics(),
		...project.program.getSyntacticDiagnostics(),
		...project.program.getBindDiagnostics(),
		...project.program.getSemanticDiagnostics(),
	];
	if (diagnostics.length > 0) {
		for (const diagnostic of diagnostics) console.error(formatDiagnostic(diagnostic, examples));
		throw new Error(
			`${diagnostics.length} TypeScript documentation diagnostic${diagnostics.length === 1 ? "" : "s"} found.`,
		);
	}
} finally {
	snapshot.dispose();
	api.close();
}

const markdownExampleCount = examples.filter((example) => example.origin === "markdown").length;
const jsdocExampleCount = examples.length - markdownExampleCount;
console.log(
	`Type-checked ${examples.length} isolated TypeScript examples ` +
		`(${markdownExampleCount} Markdown fences and ${jsdocExampleCount} JSDoc @example fences).`,
);

function basename(path: string): string {
	return normalizePath(path).split("/").at(-1) ?? path;
}

function extractTypeScriptFences(markdown: string, markdownFile: string): readonly string[] {
	const lines = markdown.split(/\r?\n/);
	const sources: string[] = [];
	for (let index = 0; index < lines.length; index++) {
		const line = lines[index];
		if (line === undefined) continue;
		const opening = /^ {0,3}(`{3,}|~{3,})[ \t]*(?:ts|typescript)\b[^\r\n]*$/i.exec(line);
		if (opening === null) continue;
		const marker = opening[1];
		if (marker === undefined) continue;
		const markerCharacter = marker[0];
		const source: string[] = [];
		let closed = false;
		for (index++; index < lines.length; index++) {
			const sourceLine = lines[index];
			if (sourceLine === undefined) continue;
			const closing = /^ {0,3}(`{3,}|~{3,})[ \t]*$/.exec(sourceLine);
			const closingMarker = closing?.[1];
			if (
				closingMarker !== undefined &&
				closingMarker[0] === markerCharacter &&
				closingMarker.length >= marker.length
			) {
				closed = true;
				break;
			}
			source.push(sourceLine);
		}
		if (!closed) {
			throw new Error(`${relativePath(markdownFile)} contains an unclosed TypeScript fence.`);
		}
		sources.push(source.join("\n"));
	}
	return sources;
}

function extractJSDocExamples(source: string, sourceFile: string): readonly string[] {
	const sources: string[] = [];
	for (const match of source.matchAll(/\/\*\*[\s\S]*?\*\//g)) {
		const comment = match[0];
		if (comment === undefined) continue;
		const normalized = comment
			.split(/\r?\n/)
			.map((line) =>
				line
					.replace(/^\s*\/\*\*?/, "")
					.replace(/\*\/\s*$/, "")
					.replace(/^\s*\*\s?/, ""),
			)
			.join("\n");
		if (!/(?:^|\s)@example(?:\s|$)/.test(normalized)) continue;
		const examples = extractTypeScriptFences(normalized, sourceFile);
		if (examples.length === 0) {
			throw new Error(`${relativePath(sourceFile)} contains a JSDoc @example without a TypeScript fence.`);
		}
		sources.push(...examples);
	}
	return sources;
}

function exampleLocation(example: DocumentationExample): string {
	const label = example.origin === "markdown" ? "TypeScript block" : "JSDoc @example";
	return `${relativePath(example.documentationFile)} (${label} ${example.block + 1})`;
}

function formatDiagnostic(diagnostic: Diagnostic, candidates: readonly DocumentationExample[]): string {
	if (diagnostic.fileName === undefined) return `TS${diagnostic.code}: ${diagnostic.text}`;
	const example = candidates.find(
		(candidate) => normalizeLookupPath(candidate.virtualFile) === normalizeLookupPath(diagnostic.fileName ?? ""),
	);
	const location = example === undefined ? normalizePath(diagnostic.fileName) : exampleLocation(example);
	return `${location}: TS${diagnostic.code}: ${diagnostic.text}`;
}

function validateExamplePolicy(
	candidates: readonly DocumentationExample[],
	project: Project,
): readonly ExamplePolicyFailure[] {
	const failures: ExamplePolicyFailure[] = [];
	const prohibitedPackages = new Set([
		"@elysiajs/eden",
		"arktype",
		"drizzle-kit",
		"drizzle-orm",
		"elysia",
		"prism-media",
		"redis",
		"ts-mixer",
	]);
	const transpiler = new Bun.Transpiler({ loader: "ts" });
	for (const example of candidates) {
		let scannedImports: ReturnType<Bun.Transpiler["scanImports"]>;
		try {
			scannedImports = transpiler.scanImports(example.source);
		} catch (cause) {
			throw new Error(`${exampleLocation(example)} could not be scanned for imports.`, { cause });
		}
		const specifiers = new Set(scannedImports.map((imported) => imported.path));
		const sourceFile = project.program.getSourceFile(example.virtualFile);
		if (sourceFile === undefined) {
			throw new Error(`Unable to load virtual documentation example: ${relativePath(example.documentationFile)}.`);
		}
		collectTypeScriptSpecifiers(sourceFile, specifiers);
		for (const specifier of specifiers) {
			const bareSpecifier = specifier.split(/[?#]/, 1)[0] ?? specifier;
			if (isNodeBuiltinImport(bareSpecifier)) {
				failures.push({ example, reason: "Node built-in imports are prohibited", specifier });
			} else if (isProhibitedPackage(bareSpecifier, prohibitedPackages)) {
				failures.push({ example, reason: "Removed dependency import is prohibited", specifier });
			} else if (isPackageSpecifier(bareSpecifier) && !isAllowedPackageImport(bareSpecifier)) {
				failures.push({ example, reason: "Unreviewed package import is prohibited", specifier });
			} else if (isRelativeSpecifier(bareSpecifier) && /\.(?:[cm]?[jt]s|[jt]sx)$/.test(bareSpecifier)) {
				failures.push({
					example,
					reason: "Imports and re-exports must be extensionless",
					specifier,
				});
			}
		}
	}
	return failures;
}

function collectTypeScriptSpecifiers(sourceFile: ts.SourceFile, specifiers: Set<string>): void {
	const visit = (node: ts.Node): void => {
		if (
			(ts.isImportDeclaration(node) || ts.isExportDeclaration(node)) &&
			node.moduleSpecifier !== undefined &&
			ts.isStringLiteral(node.moduleSpecifier)
		) {
			specifiers.add(node.moduleSpecifier.text);
		} else if (
			ts.isImportTypeNode(node) &&
			ts.isLiteralTypeNode(node.argument) &&
			ts.isStringLiteral(node.argument.literal)
		) {
			specifiers.add(node.argument.literal.text);
		} else if (
			ts.isExternalModuleReference(node) &&
			node.expression !== undefined &&
			ts.isStringLiteral(node.expression)
		) {
			specifiers.add(node.expression.text);
		}
		node.forEachChild(visit);
	};
	sourceFile.forEachChild(visit);
}

function isProhibitedPackage(specifier: string, prohibitedPackages: ReadonlySet<string>): boolean {
	if (prohibitedPackages.has(specifier)) return true;
	for (const packageName of prohibitedPackages) {
		if (specifier.startsWith(`${packageName}/`)) return true;
	}
	return false;
}

function isNodeBuiltinImport(specifier: string): boolean {
	if (specifier.startsWith("node:")) return true;
	if (isRelativeSpecifier(specifier) || specifier.startsWith("/") || specifier.includes(":")) return false;
	try {
		return Bun.resolveSync(specifier, ROOT_DIRECTORY).startsWith("node:");
	} catch {
		return false;
	}
}

function isRelativeSpecifier(specifier: string): boolean {
	return specifier === "." || specifier === ".." || specifier.startsWith("./") || specifier.startsWith("../");
}

function isPackageSpecifier(specifier: string): boolean {
	return !isRelativeSpecifier(specifier) && !specifier.startsWith("/");
}

function isAllowedPackageImport(specifier: string): boolean {
	return (
		specifier === "bun" ||
		specifier.startsWith("bun:") ||
		specifier === "discord.js" ||
		specifier === "@lilsnibbi/discord-sharding" ||
		specifier === "@lilsnibbi/discord-sharding/package.json"
	);
}

function normalizeLookupPath(path: string): string {
	return normalizePath(path).toLowerCase();
}

function normalizePath(path: string): string {
	return path.replaceAll("\\", "/");
}

function relativePath(path: string): string {
	const normalizedRoot = normalizePath(ROOT_DIRECTORY);
	const normalized = normalizePath(path);
	return normalized.startsWith(`${normalizedRoot}/`) ? normalized.slice(normalizedRoot.length + 1) : normalized;
}

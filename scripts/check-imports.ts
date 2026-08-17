import * as ts from "typescript/unstable/ast";
import { API } from "typescript/unstable/sync";
import { ROOT_DIRECTORY } from "./repository";

interface PolicyFailure {
	readonly file: string;
	readonly line: number;
	readonly reason: string;
	readonly specifier?: string;
}

const sourceFiles = new Set<string>();
for (const pattern of ["src/**/*.ts", "scripts/**/*.ts", "tests/**/*.ts", "examples/**/*.ts"]) {
	for await (const path of new Bun.Glob(pattern).scan({
		absolute: true,
		cwd: ROOT_DIRECTORY,
		onlyFiles: true,
	})) {
		sourceFiles.add(normalizePath(path));
	}
}

const failures: PolicyFailure[] = [];
const prohibitedPackages = new Set([
	"@elysiajs/eden",
	"drizzle-kit",
	"drizzle-orm",
	"elysia",
	"prism-media",
	"redis",
	"ts-mixer",
]);
const transpiler = new Bun.Transpiler({ loader: "ts" });
const api = new API({ cwd: ROOT_DIRECTORY });
const snapshot = api.updateSnapshot({ openProjects: [`${ROOT_DIRECTORY}/tsconfig.json`] });
const project = snapshot
	.getProjects()
	.find((candidate) => normalizePath(candidate.configFileName) === `${normalizePath(ROOT_DIRECTORY)}/tsconfig.json`);

try {
	if (project === undefined) throw new Error("Unable to load the root TypeScript project for policy validation.");
	for (const file of [...sourceFiles].sort()) {
		const source = await Bun.file(file).text();
		const specifiers = new Set(transpiler.scanImports(source).map((imported) => imported.path));
		const sourceFile = project.program.getSourceFile(file);
		if (sourceFile === undefined) throw new Error(`Unable to load TypeScript source for policy validation: ${file}`);
		collectTypeScriptSpecifiers(sourceFile, specifiers);
		for (const specifier of specifiers) {
			const bareSpecifier = specifier.split(/[?#]/, 1)[0] ?? specifier;
			if (isNodeBuiltinImport(bareSpecifier)) {
				failures.push({
					file,
					line: importLine(sourceFile, specifier),
					reason: "Node built-in imports are prohibited",
					specifier,
				});
			} else if (isProhibitedPackage(bareSpecifier)) {
				failures.push({
					file,
					line: importLine(sourceFile, specifier),
					reason: "Non-native dependency import is prohibited",
					specifier,
				});
			} else if (isPackageSpecifier(bareSpecifier) && !isAllowedPackageImport(file, bareSpecifier)) {
				failures.push({
					file,
					line: importLine(sourceFile, specifier),
					reason: "Unreviewed package import is prohibited",
					specifier,
				});
			} else if (isRelativeSpecifier(bareSpecifier) && /\.(?:[cm]?[jt]s|[jt]sx)$/.test(bareSpecifier)) {
				failures.push({
					file,
					line: importLine(sourceFile, specifier),
					reason: "Relative imports and re-exports must be extensionless",
					specifier,
				});
			}
		}
		collectSyntaxFailures(sourceFile, failures);
		collectSuppressionFailures(source, file, failures);
	}
} finally {
	snapshot.dispose();
	api.close();
}

failures.sort(
	(left, right) =>
		left.file.localeCompare(right.file) ||
		left.line - right.line ||
		left.reason.localeCompare(right.reason) ||
		(left.specifier ?? "").localeCompare(right.specifier ?? ""),
);
if (failures.length > 0) {
	for (const failure of failures) {
		const suffix = failure.specifier === undefined ? "" : `: ${failure.specifier}`;
		console.error(`${failure.file}:${failure.line} ${failure.reason}${suffix}`);
	}
	throw new Error(`${failures.length} source policy violation${failures.length === 1 ? "" : "s"} found.`);
}

console.log(`Validated import and type policy across ${sourceFiles.size} TypeScript files.`);

function collectSyntaxFailures(sourceFile: ts.SourceFile, target: PolicyFailure[]): void {
	const visit = (node: ts.Node): void => {
		if (node.kind === ts.SyntaxKind.AnyKeyword) {
			recordNodeFailure(target, sourceFile, node, "Explicit any types are prohibited");
		}
		if (ts.isAssertionExpression(node) && isUnknownAssertion(unwrapParentheses(node.expression))) {
			recordNodeFailure(target, sourceFile, node, "Double casts through unknown are prohibited");
		}
		node.forEachChild(visit);
	};
	sourceFile.forEachChild(visit);
}

function collectSuppressionFailures(source: string, file: string, target: PolicyFailure[]): void {
	const pattern = /@ts-(?:ignore|nocheck|expect-error)\b/g;
	for (const match of source.matchAll(pattern)) {
		const index = match.index;
		target.push({
			file,
			line: index === undefined ? 1 : lineAt(source, index),
			reason: "TypeScript suppression directives are prohibited",
		});
	}
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

function importLine(sourceFile: ts.SourceFile, specifier: string): number {
	const index = sourceFile.text.indexOf(specifier);
	return index < 0 ? 1 : sourceFile.getLineAndCharacterOfPosition(index).line + 1;
}

function isProhibitedPackage(specifier: string): boolean {
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

function isAllowedPackageImport(file: string, specifier: string): boolean {
	if (specifier === "bun" || specifier.startsWith("bun:")) return true;
	const normalizedFile = normalizePath(file);
	const normalizedRoot = normalizePath(ROOT_DIRECTORY);
	if (normalizedFile.startsWith(`${normalizedRoot}/examples/`)) {
		return (
			specifier === "discord.js" ||
			specifier === "@lilsnibbi/discord-sharding" ||
			specifier === "@lilsnibbi/discord-sharding/package.json"
		);
	}
	if (normalizedFile.startsWith(`${normalizedRoot}/tests/`)) return specifier === "discord.js";
	if (normalizedFile.startsWith(`${normalizedRoot}/scripts/`)) {
		return specifier === "typescript/unstable/ast" || specifier === "typescript/unstable/sync";
	}
	return normalizedFile.startsWith(`${normalizedRoot}/src/`) && specifier === "arktype";
}

function isUnknownAssertion(node: ts.Expression): boolean {
	return ts.isAssertionExpression(node) && node.type.kind === ts.SyntaxKind.UnknownKeyword;
}

function unwrapParentheses(node: ts.Expression): ts.Expression {
	let current = node;
	while (ts.isParenthesizedExpression(current)) current = current.expression;
	return current;
}

function recordNodeFailure(target: PolicyFailure[], sourceFile: ts.SourceFile, node: ts.Node, reason: string): void {
	target.push({
		file: normalizePath(sourceFile.fileName),
		line: sourceFile.getLineAndCharacterOfPosition(node.getStart(sourceFile)).line + 1,
		reason,
	});
}

function lineAt(source: string, index: number): number {
	let line = 1;
	for (let position = 0; position < index; position++) {
		if (source.charCodeAt(position) === 10) line++;
	}
	return line;
}

function normalizePath(path: string): string {
	return path.replaceAll("\\", "/");
}

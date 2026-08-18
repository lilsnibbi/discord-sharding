import * as ts from "typescript/unstable/ast";
import { SymbolFlags } from "typescript/unstable/sync";
import { createSyncApi, ROOT_DIRECTORY } from "./repository";

interface DocumentationFailure {
	readonly file: string;
	readonly line: number;
	readonly name: string;
	readonly reason: string;
}

const failures: DocumentationFailure[] = [];
const checkedDeclarations = new Set<string>();
const checkedDocumentationNodes = new Set<string>();
const normalizedRoot = normalizePath(ROOT_DIRECTORY);
const sourceRoot = `${normalizedRoot}/src`;
const publicTypesRoot = `${sourceRoot}/types/`;
const entryPoint = `${sourceRoot}/index.ts`;
const api = await createSyncApi({ cwd: ROOT_DIRECTORY });
const snapshot = api.updateSnapshot({ openProjects: [`${ROOT_DIRECTORY}/tsconfig.json`] });
const project = snapshot
	.getProjects()
	.find((candidate) => normalizePath(candidate.configFileName) === `${normalizedRoot}/tsconfig.json`);

try {
	if (project === undefined) throw new Error("Unable to load the root TypeScript project for JSDoc validation.");
	const sourceFile = project.program.getSourceFile(entryPoint);
	if (sourceFile === undefined) throw new Error(`Unable to load the public package entry point: ${entryPoint}`);
	const moduleSymbol = project.checker.getSymbolAtLocation(sourceFile);
	if (moduleSymbol === undefined) throw new Error("Unable to resolve the public package entry point.");
	const checkedClassSurfaces = new Set<string>();
	const checkClassHierarchy = (declaration: ts.ClassDeclaration, owner: string): void => {
		const declarationKey = `${normalizePath(declaration.getSourceFile().fileName)}:${declaration.pos}`;
		if (checkedClassSurfaces.has(declarationKey)) return;
		checkedClassSurfaces.add(declarationKey);
		for (const member of declaration.members) {
			if (!isPublicClassMember(member)) continue;
			checkDocumentation(member, `${owner}.${memberName(member)}`);
		}
		for (const clause of declaration.heritageClauses ?? []) {
			if (clause.token !== ts.SyntaxKind.ExtendsKeyword) continue;
			for (const type of clause.types) {
				let symbol = project.checker.getSymbolAtLocation(type.expression);
				if (symbol === undefined) continue;
				if ((symbol.flags & SymbolFlags.Alias) !== 0) symbol = project.checker.getAliasedSymbol(symbol);
				for (const handle of symbol.declarations) {
					const baseDeclaration = handle.resolve(project);
					if (baseDeclaration !== undefined && ts.isClassDeclaration(baseDeclaration)) {
						checkClassHierarchy(baseDeclaration, owner);
					}
				}
			}
		}
	};

	for (const exportedSymbol of project.checker.getExportsOfModule(moduleSymbol)) {
		const symbol =
			(exportedSymbol.flags & SymbolFlags.Alias) !== 0
				? project.checker.getAliasedSymbol(exportedSymbol)
				: exportedSymbol;
		for (const handle of symbol.declarations) {
			const declaration = handle.resolve(project);
			if (declaration === undefined) continue;
			if (!isSourcePath(declaration.getSourceFile().fileName)) {
				recordFailure(
					failures,
					declaration,
					exportedSymbol.name,
					"public API declarations must be owned by this package",
				);
				continue;
			}
			if (ts.isInterfaceDeclaration(declaration) || ts.isTypeAliasDeclaration(declaration)) {
				checkPublicTypeLocation(declaration, exportedSymbol.name);
			}
			const declarationKey = `${normalizePath(declaration.getSourceFile().fileName)}:${declaration.pos}:${declaration.kind}`;
			if (checkedDeclarations.has(declarationKey)) continue;
			checkedDeclarations.add(declarationKey);
			checkDocumentation(declaration, exportedSymbol.name);
			if (ts.isClassDeclaration(declaration)) {
				checkClassHierarchy(declaration, exportedSymbol.name);
			} else {
				checkNestedPublicSurface(declaration, exportedSymbol.name);
			}
		}
	}
} finally {
	snapshot.dispose();
	api.close();
}

failures.sort(
	(left, right) =>
		left.file.localeCompare(right.file) ||
		left.line - right.line ||
		left.name.localeCompare(right.name) ||
		left.reason.localeCompare(right.reason),
);

if (failures.length > 0) {
	for (const failure of failures) {
		console.error(`${failure.file}:${failure.line} ${failure.name}: ${failure.reason}`);
	}
	throw new Error(`${failures.length} public API documentation violation${failures.length === 1 ? "" : "s"} found.`);
}

console.log(`Validated meaningful JSDoc for ${checkedDocumentationNodes.size} public API declarations and members.`);

function checkPublicTypeLocation(declaration: ts.InterfaceDeclaration | ts.TypeAliasDeclaration, name: string): void {
	if (!name.startsWith("$")) {
		recordFailure(failures, declaration, name, "exported type names must start with $");
	}
	const path = normalizePath(declaration.getSourceFile().fileName);
	if (!path.startsWith(publicTypesRoot)) {
		recordFailure(failures, declaration, name, "public exported types must be declared under src/types");
	}
}

function checkNestedPublicSurface(declaration: ts.Node, owner: string): void {
	if (ts.isClassDeclaration(declaration)) {
		for (const member of declaration.members) {
			if (!isPublicClassMember(member)) continue;
			checkDocumentation(member, `${owner}.${memberName(member)}`);
		}
		return;
	}
	if (ts.isInterfaceDeclaration(declaration)) {
		for (const member of declaration.members) {
			checkDocumentation(member, `${owner}.${memberName(member)}`);
			checkNestedType(nodeType(member), `${owner}.${memberName(member)}`);
		}
		return;
	}
	if (ts.isTypeAliasDeclaration(declaration)) checkNestedType(declaration.type, owner);
}

function checkNestedType(node: ts.TypeNode | undefined, owner: string): void {
	if (node === undefined) return;
	if (ts.isParenthesizedTypeNode(node)) {
		checkNestedType(node.type, owner);
		return;
	}
	if (ts.isUnionTypeNode(node) || ts.isIntersectionTypeNode(node)) {
		for (const type of node.types) checkNestedType(type, owner);
		return;
	}
	if (!ts.isTypeLiteralNode(node)) return;
	for (const member of node.members) {
		checkDocumentation(member, `${owner}.${memberName(member)}`);
		checkNestedType(nodeType(member), `${owner}.${memberName(member)}`);
	}
}

function checkDocumentation(node: ts.Node, name: string): void {
	checkedDocumentationNodes.add(`${normalizePath(node.getSourceFile().fileName)}:${node.pos}:${node.kind}`);
	const documentation = documentationText(node);
	if (documentation === undefined) {
		recordFailure(failures, node, name, "missing JSDoc");
		return;
	}
	const summary = documentationSummary(documentation);
	if (!isMeaningfulSummary(summary)) {
		recordFailure(failures, node, name, "JSDoc needs a clear, non-placeholder summary");
	}
	const documentedParameters = documentedParameterNames(documentation);
	for (const parameter of nodeParameters(node)) {
		if (!ts.isIdentifier(parameter.name)) continue;
		if (!documentedParameters.has(parameter.name.text)) {
			recordFailure(failures, parameter, `${name}.${parameter.name.text}`, "missing @param documentation");
		}
	}
}

function documentationText(node: ts.Node): string | undefined {
	const direct = node.jsDoc;
	if (direct !== undefined && direct.length > 0) {
		return direct.map((documentation) => documentation.getText(node.getSourceFile())).join("\n");
	}
	if (!ts.isVariableDeclaration(node)) return undefined;
	const declarationList = node.parent;
	const statement = declarationList.parent;
	const inherited = ts.isVariableStatement(statement) ? statement.jsDoc : undefined;
	return inherited === undefined || inherited.length === 0
		? undefined
		: inherited.map((documentation) => documentation.getText(node.getSourceFile())).join("\n");
}

function documentationSummary(documentation: string): string {
	const lines = documentation
		.replace(/^\/\*\*|\*\/$/g, "")
		.split(/\r?\n/)
		.map((line) => line.replace(/^\s*\*\s?/, "").trim());
	const summary: string[] = [];
	for (const line of lines) {
		if (line.startsWith("@")) break;
		if (line.length > 0) summary.push(line);
	}
	return summary.join(" ").replace(/\s+/g, " ").trim();
}

function isMeaningfulSummary(summary: string): boolean {
	if (summary.length < 12 || !/[A-Za-z]{3}/.test(summary) || !/\s/.test(summary)) return false;
	return !/\b(?:fixme|placeholder|tbd|todo)\b/i.test(summary);
}

function documentedParameterNames(documentation: string): ReadonlySet<string> {
	const names = new Set<string>();
	const pattern = /@param\s+(?:\{[^}]*\}\s*)?(\[?[$A-Z_a-z][$\w]*(?:=[^\]]+)?\]?)/g;
	for (const match of documentation.matchAll(pattern)) {
		const rawName = match[1];
		if (rawName === undefined) continue;
		names.add(rawName.replace(/^\[/, "").replace(/\]$/, "").split("=", 1)[0] ?? rawName);
	}
	return names;
}

function nodeParameters(node: ts.Node): readonly ts.ParameterDeclaration[] {
	if (
		ts.isFunctionDeclaration(node) ||
		ts.isMethodDeclaration(node) ||
		ts.isConstructorDeclaration(node) ||
		ts.isMethodSignatureDeclaration(node) ||
		ts.isCallSignatureDeclaration(node) ||
		ts.isConstructSignatureDeclaration(node) ||
		ts.isFunctionExpression(node) ||
		ts.isArrowFunction(node)
	) {
		return node.parameters;
	}
	return [];
}

function recordFailure(target: DocumentationFailure[], node: ts.Node, name: string, reason: string): void {
	const sourceFile = node.getSourceFile();
	const position = sourceFile.getLineAndCharacterOfPosition(node.getStart(sourceFile));
	target.push({
		file: normalizePath(sourceFile.fileName),
		line: position.line + 1,
		name,
		reason,
	});
}

function isPublicClassMember(member: ts.ClassElement): boolean {
	const name = nodeName(member);
	if (name !== undefined && ts.isPrivateIdentifier(name)) return false;
	const modifiers = nodeModifiers(member);
	return !modifiers?.some(
		(modifier) => modifier.kind === ts.SyntaxKind.PrivateKeyword || modifier.kind === ts.SyntaxKind.ProtectedKeyword,
	);
}

function memberName(member: ts.ClassElement | ts.TypeElement): string {
	if (ts.isConstructorDeclaration(member)) return "constructor";
	const name = nodeName(member);
	if (name === undefined) return ts.SyntaxKind[member.kind] ?? "member";
	return name.getText(member.getSourceFile());
}

function isSourcePath(fileName: string): boolean {
	const path = normalizePath(fileName);
	return path === entryPoint || path.startsWith(`${sourceRoot}/`);
}

function normalizePath(path: string): string {
	return path.replaceAll("\\", "/");
}

function nodeName(node: ts.Node): ts.Node | undefined {
	return hasName(node) ? node.name : undefined;
}

function nodeType(node: ts.Node): ts.TypeNode | undefined {
	return hasType(node) ? node.type : undefined;
}

function nodeModifiers(node: ts.Node): readonly ts.Node[] | undefined {
	return hasModifiers(node) ? node.modifiers : undefined;
}

function hasName(node: ts.Node): node is ts.Node & { readonly name: ts.Node } {
	return "name" in node && node.name !== undefined;
}

function hasType(node: ts.Node): node is ts.Node & { readonly type?: ts.TypeNode } {
	return "type" in node;
}

function hasModifiers(node: ts.Node): node is ts.Node & { readonly modifiers?: readonly ts.Node[] } {
	return "modifiers" in node;
}

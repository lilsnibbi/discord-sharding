<div align="center">

# Sharding Documentation

**Complete guides and references for Sharding Discord shard orchestration.**

[`Getting started`](getting-started.md) · [`API reference`](api-reference.md) · [`Architecture`](architecture.md) · [`Operations`](operations.md)

<br />

<code>Bun</code> <code>TypeScript</code> <code>Redis</code> <code>ArkType</code> <code>Discord.js</code>

</div>

> [!NOTE]
> The documentation hub provides links to setup, API reference, architecture, operations, and troubleshooting guides.

> [!WARNING]
> Always verify options and schemas against the latest [API reference](api-reference.md) before deploying updates.

## What is in the Documentation?

The Sharding documentation covers all operational aspects of multi-process Discord shard orchestration using Bun.

<table>
<tr>
<td width="33%" valign="top">

### [Getting Started](getting-started.md)

Step-by-step installation, Hub deployment, Bridge configuration, and Shard integration.

</td>
<td width="33%" valign="top">

### [API Reference](api-reference.md)

Detailed documentation of public classes, options, interfaces, methods, and administration endpoints.

</td>
<td width="33%" valign="top">

### [Architecture](architecture.md)

Control plane design, identify scheduling, sticky assignments, IPC routing, and Redis storage.

</td>
</tr>
</table>

## Documentation Map

<table>
<tr>
<td valign="top">

**Set up**

- [Getting started](getting-started.md)
- [Code examples](examples.md)

</td>
<td valign="top">

**Reference**

- [API reference](api-reference.md)
- [Architecture](architecture.md)
- [Architecture deep dive](architecture/overview.md)
- [Shard identity](shard-identity.md)
- [Hub events](hub-events.md)

</td>
<td valign="top">

**Operate**

- [Operations](operations.md)
- [Troubleshooting](troubleshooting.md)
- [Performance](performance.md)
- [Testing](testing.md)
- [Known limitations](known-limitations.md)

</td>
</tr>
</table>

## Commands

<details>
<summary><strong>Documentation validation</strong></summary>

| Command | Purpose |
| --- | --- |
| `bun run check:docs` | Validate all local markdown file links |
| `bun run check:examples` | Typecheck markdown code fences |
| `bun run check:jsdoc` | Validate public JSDoc coverage |

</details>

## Community, security, and license

[Project README](../README.md) · [Contributing](../CONTRIBUTING.md) · [Security policy](../SECURITY.md) · [GitHub Issues](https://github.com/lilsnibbi/discord-sharding/issues)

Licensed under the [Apache License 2.0](../LICENSE).

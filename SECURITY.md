# Sharding security policy

## Reporting a vulnerability

Use [GitHub private vulnerability reporting](https://github.com/lilsnibbi/discord-sharding/security/advisories/new). Do not
include exploit details, credentials, private payloads, or deployment data in a public issue.

Include the affected package version, impact, reproduction conditions, and a minimal proof of concept. Security fixes
target the latest release; older pre-1.0 releases may require an upgrade.

## Scope

Reports are in scope when they affect:

- Bridge or administration authentication;
- WebSocket or Bun IPC validation and generation fencing;
- shard assignment, identify admission, routing, request correlation, or broadcast evaluation;
- Bun Redis or SQLite persistence, or analytics handling performed by this package;
- subprocess lifecycle, resource cleanup, backpressure, or packaged source integrity.

Vulnerabilities in Bun, Discord, `discord.js`, reverse proxies, application handlers, or deployment configuration
should be reported to the responsible project unless this package creates the exposure.

## Deployment hardening

- Use different high-entropy values for Bridge and administration tokens.
- Terminate TLS before public Hub traffic and restrict administration routes at the network boundary.
- Keep the Hub and Bridge SQLite files private to their deployment accounts, on storage that is not world-readable and
  not served by any public endpoint.
- Validate application payloads again at their domain boundary.
- Call `broadcastEval` only with trusted, developer-authored functions. Evaluator source executes on every ready shard;
  protocol validation does not sandbox it, so never construct it from user or network input.
- Avoid logging bot tokens, authentication headers, database paths, complete payloads, or guild data.
- Keep Bun and package versions current and test token rotation and graceful shutdown procedures.

# Claude Code Integration

## Overview

SEMOSS includes a built-in integration with [Claude Code](https://docs.anthropic.com/en/docs/claude-code), Anthropic's agentic coding tool. The Claude Agent SDK (`claude-agent-sdk`) is embedded directly into the Python layer of the platform, and SEMOSS ships with a bundled version of Claude Code — no separate installation is required.

This integration allows users to invoke Claude Code as an autonomous coding agent within the context of a SEMOSS project. Claude Code can read and write files, execute shell commands, search the web, and perform other tool-based actions — all routed through SEMOSS's model proxy and security layer.

## How It Works

When a Claude Code request is made, the Java layer (`ClaudeCodeManager`) spins up a dedicated Python TCP server process and initializes a `ClaudeCodeClient` instance. This client wraps the Claude Agent SDK and communicates with the Anthropic API through SEMOSS's internal model endpoint (`/Monolith/api/model/anthropic`), ensuring that all requests flow through the platform's authentication and access control mechanisms.

Key aspects of the architecture:

- **Authentication** is handled automatically. SEMOSS generates temporary access/secret key pairs per request based on the users credentials, which are passed to Claude Code as environment variables.
- **Project scoping** — Claude Code operates within the assets folder of the specified SEMOSS project, giving it a working directory for file operations.
- **Room-based conversation tracking** — each interaction is tied to a room ID for audit and session management.

## Pixel Usage

Claude Code is invoked via a Pixel call using the `ClaudeCode` reactor:

```
ClaudeCode(
    engine="engine_id",
    project="project_id",
    context="system_prompt",
    command="prompt",
    roomId="room_id",
    allowedTools=["Bash", "Glob", "Read", "Write", "Edit", "Grep", "WebSearch", "WebFetch", "AskUserQuestion"],
    permissionMode="acceptEdits"
)
```

### Parameters

| Parameter | Required | Description |
|---|---|---|
| `engine` | Yes | The engine ID of the registered Anthropic model to use. |
| `project` | Yes | The SEMOSS project ID. Claude Code will operate within this project's assets directory. |
| `command` | Yes | The user prompt — the task or question you want Claude Code to perform. |
| `context` | No | An optional system prompt to guide Claude Code's behavior. |
| `roomId` | No | A room ID for conversation tracking. A new room is created if one does not already exist. |
| `allowedTools` | No | A list of tools Claude Code is permitted to use. Defaults to a standard set including Bash, file operations, and web access. |
| `permissionMode` | No | Controls how Claude Code handles tool permissions. Defaults to `acceptEdits`. |

### Allowed Tools

The following tools can be granted to Claude Code:

- **Bash** — Execute shell commands
- **Glob** — Search for files by pattern
- **Read** — Read file contents
- **Write** — Create or overwrite files
- **Edit** — Make targeted edits to existing files
- **Grep** — Search file contents
- **WebSearch** — Search the web
- **WebFetch** — Fetch content from URLs
- **AskUserQuestion** — Prompt the user for input

### Permission Modes

| Mode | Description |
|---|---|
| `default` | Standard behavior — prompts for permission on first use of each tool. |
| `acceptEdits` | Automatically accepts file edit permissions for the session. |
| `plan` | Plan Mode — Claude can analyze but not modify files or execute commands. |
| `dontAsk` | Auto-denies tools unless pre-approved via `/permissions` or `permissions.allow` rules. |
| `bypassPermissions` | Skips all permission prompts. **Use only in safe, sandboxed environments.** |

## Current Limitations

- **One-off messages only** — The integration currently supports single prompt-response interactions. Multi-turn conversational sessions with Claude Code are not yet supported; each `ClaudeCode` Pixel call is an independent invocation.
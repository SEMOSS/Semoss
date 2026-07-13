---
name: build-and-publish
description: Use when compiling, building, or publishing the React app after making changes to source files — at the end of any turn that edited client code, or when the user asks to build, rebuild, or deploy. Invoke the BuildAndPublishApp tool with the project id. Do not attempt to run node, npm, pnpm, or any JavaScript build command via Bash — the sandbox blocks node execution, and BuildAndPublishApp is the only supported build path.
---

# Build and publish

After making changes to source files in the React app, compile and publish by invoking the `BuildAndPublishApp` tool. This is the only supported path — node, npm, pnpm, and other JS build tooling are not available in the sandbox.

## When to build

Build at the end of any turn that modified files in the client source tree. Do not build for read-only operations (viewing files, answering questions about the code, searching the codebase).

## How to build

Invoke the `BuildAndPublishApp` tool with the project id:

```
BuildAndPublishApp(project="<PROJECT_ID>")
```

The project id is provided in the system prompt. On success, the built output is written to `assets/portals/` and the project is published automatically.

## Do not

- Do not run `npm run build`, `pnpm build`, `node ...`, or any JS toolchain via the Bash tool. These will fail — the sandbox does not include node.
- Do not try to inspect `node_modules/` or run type-checking separately. `BuildAndPublishApp` handles the full build pipeline.
- Do not build after every small edit within a multi-step task. Build once at the end, after all file changes are complete.

## If the build fails

`BuildAndPublishApp` returns build errors in its response. Read the errors, edit the relevant source files, and invoke `BuildAndPublishApp` again. Do not fall back to Bash to diagnose — the build logs in the tool response are the canonical source.

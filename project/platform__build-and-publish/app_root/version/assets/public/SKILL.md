---
name: build-and-publish
description: Use when compiling, building, or publishing the React app after making changes to source files - at the end of any turn that edited client code, or when the user asks to build, rebuild, or deploy. Invoke the BuildAndPublishApp tool with the project id when client source must be compiled. For already-runnable portal assets, invoke PublishProject(project, release=true). Do not attempt to run node, npm, pnpm, or any JavaScript build command via Bash - the sandbox blocks node execution, and BuildAndPublishApp is the only supported build path.
---

# Build and publish

After making changes to source files in the React app, compile and publish by invoking the `BuildAndPublishApp` tool. This is the only supported compilation path - node, npm, pnpm, and other JS build tooling are not available in the sandbox.

## When to build

Build at the end of any turn that modified files in the client source tree. Do not build for read-only operations (viewing files, answering questions about the code, searching the codebase).

## How to build

Invoke the `BuildAndPublishApp` tool with the project id:

```
BuildAndPublishApp(project="<PROJECT_ID>")
```

The project id is provided in the system prompt. On success, the built output is written to `assets/portals/` and the project is published automatically.

## When to publish without building

When runnable assets already exist in `assets/portals` and do not need compilation, such as a plain `index.html` app, publish them directly:

```
PublishProject(project="<PROJECT_ID>", release=true)
```

## Do not

- Do not run `npm run build`, `pnpm build`, `node ...`, or any JS toolchain via the Bash tool. These will fail - the sandbox does not include node.
- Do not try to inspect `node_modules/` or run type-checking separately. `BuildAndPublishApp` handles the full build pipeline.
- Do not build after every small edit within a multi-step task. Build once at the end, after all file changes are complete.

## If the build fails

`BuildAndPublishApp` returns build errors in its response. Read the errors, edit the relevant source files, and invoke `BuildAndPublishApp` again. Do not fall back to Bash to diagnose - the build logs in the tool response are the canonical source.

## If the build succeeds but is not published

`BuildAndPublishApp` inspects the output before publishing, so a compile that succeeds can still be held back. The response says which check stopped it.

**"it produced absolute asset urls"** - `portals/index.html` references an asset as `/assets/...`. A portal is served from a project-scoped path rather than the server root, so those urls 404 and the app loads as a blank page. Add `base: "./"` to `client/vite.config.ts` and build again. The previously published portal stays live until a clean build replaces it, so the app is not left broken while you fix this.

Treat any held-back build as a failure: the source is wrong, and rerunning the tool unchanged returns the same response. See the `app-bootstrap` skill for the config invariants behind these checks.

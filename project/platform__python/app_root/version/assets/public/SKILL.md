---
name: python
description: Use when writing Python in a platform app — `py/mcp_driver.py`, helper modules, or any code that runs inside the platform Python runtime. Covers reading per-execution paths with `smss_get_runtime_var` (`ROOT`, `APP_ROOT`, `USER_ROOT`) rather than a bare injected name, what to import from `ai_server` / `semoss` / `smssutil`, the `@mcp_metadata` decorator for exposing MCP tools, and how Python reaches Pixel via `Insight().run_pixel(...)`. For what Pixel commands to actually run, see the `database`, `model`, and `vector` skills. Do not use for Java reactor authoring or frontend `@semoss/sdk` calls.
---

# Python on the platform

Python code in a platform app runs in a managed runtime backed by a TCP server proxy. This skill covers the Python-side glue: what's injected, what to import, and how to bridge to Pixel. For the actual Pixel command syntax (LLM, SqlQuery, VectorDatabaseQuery, etc.), see the matching skill — Python doesn't change those, you just pass them as strings to `Insight().run_pixel(...)`.

## What the platform injects vs what you import

| Symbol                                                                             | Source                                                            | Notes                                                                        |
| ---------------------------------------------------------------------------------- | ----------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| `smss_get_runtime_var`                                                             | `from smssutil import smss_get_runtime_var`                       | Reads a per-execution variable passed in from Java: `ROOT`, `APP_ROOT`, `USER_ROOT`. |
| `Insight`                                                                          | `from semoss import Insight` (or `from ai_server import Insight`) | Pixel runner.                                                                |
| `ModelEngine`, `DatabaseEngine`, `StorageEngine`, `VectorEngine`, `FunctionEngine` | `from ai_server import ...`                                       | Python wrappers around the matching engines — alternative to running pixels. |
| `mcp_metadata`                                                                     | `from smssutil import mcp_metadata`                               | Decorator for exposing functions as MCP tools.                               |

## Reaching the platform from Python

Two options:

1. **Engine wrappers** (`ModelEngine`, `DatabaseEngine`, `VectorEngine`, `StorageEngine`, `FunctionEngine`) — typed Python calls. Prefer these for LLM, SQL, and vector work.
2. **`Insight().run_pixel(pixel_string)`** — for anything an engine wrapper doesn't expose (system pixels, schema lookups, custom reactors, etc.). The Pixel string is the same one the frontend uses — see the `database`, `model`, `vector` skills for syntax.

## File I/O via `smss_get_runtime_var`

Paths are passed in per execution and read with `smss_get_runtime_var(key, default=None)`:

```python
import os
from smssutil import smss_get_runtime_var

root = smss_get_runtime_var("ROOT")
out_path = os.path.join(root, "report.txt")
with open(out_path, "w", encoding="utf-8") as f:
    f.write(content)
```

Available keys:

| Key | Points to |
| --- | --- |
| `ROOT` | the active insight/room folder |
| `APP_ROOT` | the app's `assets/` folder |
| `USER_ROOT` | the current user's space; absent (returns the default) when there is no user context |

**Do not reference a bare `ROOT` name.** It is not injected into the execution globals, so it raises `NameError` — a bare `ROOT` in existing code is why you sometimes see a `# noqa: F821` suppression next to it. Use the accessor instead.

The accessor reads thread-local state rather than shared globals, which is the point: several executions can run concurrently against the same insight, and a value written into the shared globals could be overwritten by another thread mid-run.

`ROOT` is the insight folder, not the project folder. `APP_ROOT` is the app's `assets/` folder, so prefer it over calling `GetProjectAssetsFolder(project='...')` through `run_pixel` when you just need the current app's path.

Because it is a plain import from `smssutil`, it also works inside helper modules, not only in the entry script.

## Exposing Python as an MCP tool

Decorate a top-level function in `py/mcp_driver.py` with `@mcp_metadata`. The platform discovers it automatically:

```python
import json
import traceback
from smssutil import mcp_metadata

@mcp_metadata({
    "execution": "auto",            # "auto" | "ask" | "disabled"
    "loadingMessage": "Processing...",
    "displayLocation": "inline",    # "inline" | "sidebar" | "hidden"
    "resourceURI": None,            # e.g. "/#/your-route" for a custom UI
})
def get_inventory(category: str) -> str:
    """Return the current stock for a product category."""
    try:
        return json.dumps({"category": category, "count": 12})
    except Exception as e:
        return json.dumps({"success": False, "error": str(e), "traceback": traceback.format_exc()})
```

- Every parameter needs a type hint — that's how the platform builds the input schema.
- Function name → tool title; docstring → description.
- Return a JSON string (`json.dumps(...)`), not a raw dict.
- Wrap the body in try/except and return `traceback.format_exc()` on failure — much easier to debug from the playground.

After adding, renaming, or removing a tool, the user needs to run the `MakePythonMCP()` pixel from the playground to regenerate `mcp/py_mcp.json` — that's what makes the new/updated tools callable by the LLM. Do not hand-edit that file.

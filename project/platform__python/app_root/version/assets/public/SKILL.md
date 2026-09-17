---
name: python
description: Use when executing inline Python with `ExecutePythonCode` or writing Python in a platform app, including `py/mcp_driver.py` and helper modules. Covers the managed-runtime `ROOT`, `APP_ROOT`, and `USER_ROOT` paths, when bare names are available versus when to use `smss_get_runtime_var`, platform imports, MCP metadata, and calling Pixel from Python. For Pixel command syntax, see the `database`, `model`, and `vector` skills. Do not use for Java reactor authoring or frontend `@semoss/sdk` calls.
---

# Python on the platform

Python code in a platform app runs in a managed runtime backed by a TCP server proxy. This skill covers the Python-side glue: what's injected, what to import, and how to bridge to Pixel. For the actual Pixel command syntax (LLM, SqlQuery, VectorDatabaseQuery, etc.), see the matching skill - Python doesn't change those, you just pass them as strings to `Insight().run_pixel(...)`.

## Choose the execution route

- Use `ExecutePythonCode` for inline Python during a RunAgent task. It executes through the same managed runtime as `PyReactor`; the last expression is returned.
- Use a Python MCP function for a reusable, typed capability that agents should call repeatedly.
- Use `BashCommand` to run a packaged `.py` file only when that script does not need managed-runtime variables. A separate shell Python process does not receive `ROOT`, `APP_ROOT`, or `USER_ROOT`.

`ExecutePythonCode` and direct `PyReactor` calls inject bare runtime names, so concise agent snippets work:

```python
import os
sorted(os.listdir(USER_ROOT))
```

For RunAgent, `ROOT` is the active working directory selected by `space` and `subdir`. `USER_ROOT` is the authenticated user's asset-app root even when `ROOT` targets an insight or editable project. `APP_ROOT` follows the current platform app context and is absent when the insight does not have one.

## What the platform injects vs what you import

| Symbol                                                                             | Source                                                            | Notes                                                                        |
| ---------------------------------------------------------------------------------- | ----------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| `smss_get_runtime_var`                                                             | `from smssutil import smss_get_runtime_var`                       | Reads a per-execution variable passed in from Java: `ROOT`, `APP_ROOT`, `USER_ROOT`. |
| `Insight`                                                                          | `from semoss import Insight` (or `from ai_server import Insight`) | Pixel runner.                                                                |
| `ModelEngine`, `DatabaseEngine`, `StorageEngine`, `VectorEngine`, `FunctionEngine` | `from ai_server import ...`                                       | Python wrappers around the matching engines - alternative to running pixels. |
| `mcp_metadata`                                                                     | `from smssutil import mcp_metadata`                               | Decorator for exposing functions as MCP tools.                               |

## Reaching the platform from Python

Two options:

1. **Engine wrappers** (`ModelEngine`, `DatabaseEngine`, `VectorEngine`, `StorageEngine`, `FunctionEngine`) - typed Python calls. Prefer these for LLM, SQL, and vector work.
2. **`Insight().run_pixel(pixel_string)`** - for anything an engine wrapper doesn't expose (system pixels, schema lookups, custom reactors, etc.). The Pixel string is the same one the frontend uses - see the `database`, `model`, `vector` skills for syntax.

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
| `ROOT` | the active execution folder; for RunAgent, the target selected by `space` and `subdir` |
| `APP_ROOT` | the current app's `assets/` folder; absent when the insight has no app context |
| `USER_ROOT` | the current user's space; absent (returns the default) when there is no user context |

Inside MCP functions and imported helper modules, use the accessor rather than depending on a bare runtime name. Those paths use thread-local runtime values and do not guarantee that a bare `ROOT` name is present. Bare names are supported for `ExecutePythonCode` and direct `PyReactor` snippets for compatibility and convenience.

The accessor reads thread-local state rather than shared globals, which is the point: several executions can run concurrently against the same insight, and a value written into the shared globals could be overwritten by another thread mid-run.

Outside RunAgent, `ROOT` is normally the insight folder. In RunAgent, it is the selected target working directory. `APP_ROOT` is the current app's `assets/` folder when an app context exists, so prefer it over calling `GetProjectAssetsFolder(project='...')` through `run_pixel` when you just need that app's path.

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

- Every parameter needs a type hint - that's how the platform builds the input schema.
- Function name -> tool title; docstring -> description.
- Return a JSON string (`json.dumps(...)`), not a raw dict.
- Wrap the body in try/except and return `traceback.format_exc()` on failure - much easier to debug from the playground.

After adding, renaming, or removing a tool, the user needs to run the `MakePythonMCP()` pixel from the playground to regenerate `mcp/py_mcp.json` - that's what makes the new/updated tools callable by the LLM. Do not hand-edit that file.

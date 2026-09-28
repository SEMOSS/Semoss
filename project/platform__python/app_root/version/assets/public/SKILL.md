---
name: python
description: Use when executing inline Python with `ExecutePythonCode` or writing Python in a platform app, including `py/mcp_driver.py` and helper modules. Covers the standard image's Python libraries, per-execution paths through `smss_get_runtime_var()`, refreshing cached app imports with `smss_clear_app_imports()`, platform imports, MCP metadata, and calling Pixel from Python. For Pixel command syntax, see the `database`, `model`, and `vector` skills. Do not use for Java reactor authoring or frontend `@semoss/sdk` calls.
---

# Python on the platform

Python code in a platform app runs in a managed runtime backed by a TCP server proxy. This skill covers the Python-side glue: what's injected, what to import, and how to bridge to Pixel. Import `Insight` explicitly before calling Pixel:

```python
from semoss import Insight
```

Then pass a Pixel command string to `Insight().run_pixel(...)`. For the command syntax (LLM, SqlQuery, VectorDatabaseQuery, etc.), see the matching skill; Python does not change the Pixel syntax.

## Libraries in the standard Python image

The standard Python runtime includes the packages below for common analysis tasks. Availability and versions can vary by deployment, so check the active runtime before relying on a package. Prefer these libraries when they support the task:

| Task | Packages / Python imports |
| --- | --- |
| Tables and numerical analysis | `pandas`, `numpy` |
| Charts | `matplotlib.pyplot`, `seaborn` |
| Machine learning and preprocessing | `scikit-learn` (import `sklearn`) |
| Arrow and Parquet files | `pyarrow`, plus pandas readers/writers |
| Excel files | `openpyxl`, `xlrd`, plus pandas readers |
| SQL over data frames | `pandasql` |
| PDF and document data | `pdfplumber`, `pypdf`, `python-docx` (import `docx`), `python-pptx` (import `pptx`) |
| Text analysis | `nltk`, `thefuzz`, `sentence-transformers` (import `sentence_transformers`) |
| Progress and validation | `tqdm`, `pydantic` |

Python's standard library also provides `json`, `csv`, `pathlib`, `datetime`, `statistics`, and `sqlite3`. PyTorch packages are installed through the image's CPU/GPU extras; do not infer that CUDA or a GPU is available. Model packages do not guarantee that pretrained weights, NLTK data, or other downloaded resources are already present.

Check package availability and versions in the managed runtime before relying on an optional library. `plotly`, `polars`, and `statsmodels` are not explicit dependencies in the baseline; a particular image may include them transitively or through local customization. For example:

```python
from importlib.metadata import PackageNotFoundError, version

def installed_version(distribution):
    try:
        return version(distribution)
    except PackageNotFoundError:
        return None

{name: installed_version(name) for name in ["pandas", "numpy", "matplotlib", "scikit-learn"]}
```

You are unable to install Python packages in the managed runtime. Use the packages already available and avoid downloading large model assets for routine analysis when the baseline supports the task. If a required dependency is missing, explain the limitation and use an installed alternative when possible; otherwise ask the user to have it added through the deployment's supported installation process.

## Choose the execution route

- Use `ExecutePythonCode` for inline Python during a RunAgent task. It executes through the same managed runtime as `PyReactor`; the last expression is returned and user-defined state is retained for later calls in the same room during the current login session while the worker remains alive.
- Use a Python MCP function for a reusable, typed capability that agents should call repeatedly.
- Use `BashCommand` to run a packaged `.py` file only when that script does not need managed-runtime variables. A separate shell Python process does not receive `ROOT`, `APP_ROOT`, or `USER_ROOT`.

Use `smss_get_runtime_var()` by default for runtime paths in inline snippets, MCP functions, and imported helper methods:

```python
from pathlib import Path
from smssutil import smss_get_runtime_var

user_root = smss_get_runtime_var("USER_ROOT")
if not user_root:
    raise RuntimeError("USER_ROOT is unavailable for this execution")
sorted(path.name for path in Path(user_root).iterdir())
```

For RunAgent, `ROOT` is the active working directory selected by `space` and `subdir`. `USER_ROOT` is the authenticated user's asset-app root even when `ROOT` targets an insight or editable project. `APP_ROOT` follows the current platform app context and is absent when the insight does not have one.

## Runtime state and isolation

`ExecutePythonCode` uses a stateful Python session scoped to the room within the current login session. Variables, functions, and imports created by one call are available to later calls from the same room. A different room, including a subagent's child room, receives a different namespace. The namespace is in memory only: restarting the managed Python worker, logging out, or restarting the platform clears it. A new login receives a new namespace. Persist anything that must survive those events to a file under the directory returned by `smss_get_runtime_var("ROOT")`.

The room namespace belongs only to `ExecutePythonCode`. Python MCP tools, app helpers, direct `PyReactor` calls, and engine-owned Python continue to use their existing Insight- or engine-scoped namespaces. Do not depend on an MCP function seeing variables created by `ExecutePythonCode`, or the reverse.

If a later call changes the RunAgent `space` or `subdir`, the room namespace remains, but runtime paths are refreshed for that execution. Avoid retaining open file handles or derived paths when switching targets; call `smss_get_runtime_var()` again for the current execution.

## What the platform injects vs what you import

| Symbol                                                                             | Source                                                            | Notes                                                                        |
| ---------------------------------------------------------------------------------- | ----------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| `smss_get_runtime_var`                                                             | `from smssutil import smss_get_runtime_var`                       | Reads a per-execution variable passed in from Java: `ROOT`, `APP_ROOT`, `USER_ROOT`. |
| `smss_clear_app_imports`                                                           | `from smssutil import smss_clear_app_imports`                     | Clears cached modules for the execution's active app asset paths and cached MCP driver aliases. Also injected into managed inline execution globals. |
| `Insight`                                                                          | `from semoss import Insight` (or `from ai_server import Insight`) | Pixel runner.                                                                |
| `ModelEngine`, `DatabaseEngine`, `StorageEngine`, `VectorEngine`, `FunctionEngine` | `from ai_server import ...`                                       | Python wrappers around the matching engines - alternative to running pixels. |
| `mcp_metadata`                                                                     | `from smssutil import mcp_metadata`                               | Decorator for exposing functions as MCP tools.                               |

## Reaching the platform from Python

Two options:

1. **Engine wrappers** (`ModelEngine`, `DatabaseEngine`, `VectorEngine`, `StorageEngine`, `FunctionEngine`) - typed Python calls. Prefer these for LLM, SQL, and vector work.
2. **`Insight().run_pixel(pixel_string)`** - for anything an engine wrapper doesn't expose (system pixels, schema lookups, custom reactors, etc.). The Pixel string is the same one the frontend uses - see the `database`, `model`, `vector` skills for syntax.

## File I/O via `smss_get_runtime_var`

Paths are passed in per execution and read with `smss_get_runtime_var(key, default=None)`. Import the accessor once, but resolve the path inside the function or method each time it is called:

```python
from pathlib import Path
from smssutil import smss_get_runtime_var

def write_report(content: str) -> str:
    root = smss_get_runtime_var("ROOT")
    if not root:
        raise RuntimeError("ROOT is unavailable for this execution")
    out_path = Path(root) / "report.txt"
    out_path.write_text(content, encoding="utf-8")
    return str(out_path)
```

Available keys:

| Key | Points to |
| --- | --- |
| `ROOT` | the active execution folder; for RunAgent, the target selected by `space` and `subdir` |
| `APP_ROOT` | the current app's `assets/` folder; absent when the insight has no app context |
| `USER_ROOT` | the current user's space; absent (returns the default) when there is no user context |

Use the accessor for all new code, including inline snippets. Some execution routes also inject bare `ROOT`, `APP_ROOT`, and `USER_ROOT` names for compatibility; imported functions and methods should not depend on those globals. Do not cache resolved paths at module import time or in function default arguments, because the module may be reused in another execution context.

The accessor reads thread-local state rather than shared globals, which is the point: several executions can run concurrently against the same insight, and a value written into the shared globals could be overwritten by another thread mid-run.

Outside RunAgent, `ROOT` is normally the insight folder. In RunAgent, it is the selected target working directory. `APP_ROOT` is the current app's `assets/` folder when an app context exists, so prefer it over calling `GetProjectAssetsFolder(project='...')` through `run_pixel` when you just need that app's path.

The accessor works inside imported helpers and methods running on the managed execution thread. A new thread or separate Python process does not automatically inherit that context. When a required key is unavailable, report the missing context instead of falling back to an unrelated working directory.

## Refreshing app imports after Python file changes

Use `smss_clear_app_imports()` when edited app Python files are still served from the import cache. Call it from a managed execution with that app's asset paths active, before importing the app modules again. It takes no arguments; there is no app-ID or filename parameter.

```python
from smssutil import smss_clear_app_imports

cleared = smss_clear_app_imports()
# Import the app modules again here, after clearing.
cleared
```

The function is also injected into managed inline execution globals. The explicit import above makes it available inside imported helpers and methods as well; it still needs the current managed execution context.

It evicts the cached, namespaced Python modules associated with the execution's active app asset paths. It also removes all cached MCP driver aliases (`__smss_mcp_*`) in the current execution namespace, so a later tool call can load its driver again. The return value is a dictionary with `modules` and `mcp_aliases` lists naming the cache entries removed; either list can be empty.

This clears import caches, not files on disk or ordinary analysis variables. It leaves third-party packages and modules for inactive app paths in the module cache. Existing references to imported functions, modules, or class instances still point to the old objects: re-import the modules and recreate affected objects after clearing. Clearing the cache does not replace code already executing.

Changing tool names, parameters, or metadata still requires `MakePythonMCP()` to regenerate the tool catalog, as described below.

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

Load the `mcp` skill for calling these tools through the frontend SDK, configuring their published UI routes, receiving arguments from Playground, and returning results or approval decisions from a page.

import hashlib
import sys
import threading

# Thread-local populated by handle_python before each exec() and cleared after.
# active_paths  - list of asset path strings for per-project import isolation
# insight_globals - reference to the current insight's globals dict
_asset_thread_local = threading.local()


def _asset_ns_key(path: str) -> str:
    """Stable sys.modules namespace prefix for a given asset path."""
    return "_smss_" + hashlib.md5(path.encode()).hexdigest()[:12]


def smss_get_runtime_var(key, default=None):
    """Retrieve a runtime variable passed from Java for the current execution."""
    return (getattr(_asset_thread_local, "runtime_vars", None) or {}).get(key, default)


def smss_clear_app_imports():
    """
    Evicts all cached project-local module imports for the current app's asset paths
    and clears any cached MCP driver aliases in the current insight's globals.

    Call this at the top of your app entry script to force Python to pick up
    any .py file changes without restarting the server.

    Returns:
        dict with keys:
            "modules"     - list of sys.modules keys that were deleted
            "mcp_aliases" - list of insight_globals keys that were deleted
    """
    active_paths = getattr(_asset_thread_local, "active_paths", None) or []
    ig = getattr(_asset_thread_local, "insight_globals", None)
    cleared_modules = []
    cleared_mcp_aliases = []
    for path in active_paths:
        pfx = _asset_ns_key(path) + "_"
        for k in [k for k in list(sys.modules.keys()) if k.startswith(pfx)]:
            del sys.modules[k]
            cleared_modules.append(k)
    if ig is not None:
        for k in [k for k in list(ig.keys()) if k.startswith("__smss_mcp_")]:
            del ig[k]
            cleared_mcp_aliases.append(k)
    return {"modules": cleared_modules, "mcp_aliases": cleared_mcp_aliases}

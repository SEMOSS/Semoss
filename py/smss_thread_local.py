import threading
import sys
from typing import Optional, Callable, Any

# This object is globally accessible, but its attributes are unique to each thread
_storage = threading.local()


def set_smss_stream(func: Callable) -> None:
    """Sets the smss_stream function for the current thread.
    Wrap the provided sink so UnicodeEncodeError during streaming won't crash the
    process. The wrapper will sanitize string data by replacing non-encodable
    characters with '?' and retry once.
    """
    try:
        _storage.smss_stream = _safe_wrap_smss_stream(func)
    except Exception:
        _storage.smss_stream = func


def get_smss_stream() -> Optional[Callable]:
    """Gets the smss_stream function for the current thread, if it exists."""
    return getattr(_storage, "smss_stream", None)


def clear_smss_stream() -> None:
    """Clears the smss_stream function for the current thread."""
    if hasattr(_storage, "smss_stream"):
        del _storage.smss_stream


# Uncomment the below if debugging and need the smss_stream defined
# def smss_stream_func(data: Any, stream_type: str = "content", interim: bool = True):
#     structured_output = {"stream_type": stream_type, "data": data}
#     print(structured_output)


# set_smss_stream(smss_stream_func)


# Minimal helpers: try to set UTF-8 for the process and sanitize strings for stdout encoding.
def _ensure_utf8_process() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass


def _sanitize_str_for_stdout(s: str) -> str:
    enc = getattr(sys.stdout, "encoding", None) or "utf-8"
    try:
        s.encode(enc)
        return s
    except UnicodeEncodeError:
        out = []
        for ch in s:
            try:
                ch.encode(enc)
                out.append(ch)
            except UnicodeEncodeError:
                out.append("?")
        return "".join(out)


def _sanitize_obj(obj: Any) -> Any:
    if isinstance(obj, str):
        return _sanitize_str_for_stdout(obj)
    if isinstance(obj, dict):
        return {k: _sanitize_obj(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_obj(x) for x in obj]
    if isinstance(obj, tuple):
        return tuple(_sanitize_obj(x) for x in obj)
    if isinstance(obj, set):
        return set(_sanitize_obj(x) for x in obj)
    return obj


def _safe_wrap_smss_stream(orig_smss_stream):
    _ensure_utf8_process()

    def _wrapped(data, stream_type: str = "content", interim: bool = True):
        try:
            return orig_smss_stream(data, stream_type=stream_type, interim=interim)
        except UnicodeEncodeError:
            try:
                safe_data = _sanitize_obj(data)
                return orig_smss_stream(safe_data, stream_type=stream_type, interim=interim)
            except Exception:
                return None
        except Exception:
            try:
                safe_data = _sanitize_obj(data)
                return orig_smss_stream(safe_data, stream_type=stream_type, interim=interim)
            except Exception:
                return None

    return _wrapped

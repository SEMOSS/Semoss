import threading
from typing import Optional, Callable, Any

# This object is globally accessible, but its attributes are unique to each thread
_storage = threading.local()


def set_smss_stream(func: Callable) -> None:
    """Sets the smss_stream function for the current thread."""
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

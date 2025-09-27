import threading
from typing import Optional, Callable

# This object is globally accessible, but its attributes are unique to each thread
_storage = threading.local()

def set_smss_stream(func: Callable) -> None:
    """Sets the smss_stream function for the current thread."""
    _storage.smss_stream = func

def get_smss_stream() -> Optional[Callable]:
    """Gets the smss_stream function for the current thread, if it exists."""
    return getattr(_storage, 'smss_stream', None)

def clear_smss_stream() -> None:
    """Clears the smss_stream function for the current thread."""
    if hasattr(_storage, 'smss_stream'):
        del _storage.smss_stream

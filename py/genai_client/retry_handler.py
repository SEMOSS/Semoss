import time
import re
from typing import Callable, Any, Optional
from functools import wraps


class RetryHandler:
    def __init__(
        self,
        max_retries: int = 1,
        delay_factor: float = 2.0,
    ):
        self.max_retries = max_retries
        self.delay_factor = delay_factor

        if (
            self.max_retries < 0
            or self.max_retries > 6
            or not isinstance(self.max_retries, int)
        ):
            self.max_retries = 1

    def _get_error_code(self, error: Exception) -> Optional[int]:
        """Extract HTTP status code from exception."""
        for attr in ["code", "status_code", "status", "grpc_status_code"]:
            if hasattr(error, attr):
                code = getattr(error, attr)
                if isinstance(code, int):
                    return code

        error_str = str(error)
        match = re.search(r"\b([45]\d{2})\b", error_str)
        if match:
            return int(match.group(1))

        return None

    def _is_retriable_error(self, error: Exception) -> bool:
        """Determine if an error should be retried."""
        error_code = self._get_error_code(error)
        if error_code:
            if error_code == 429:
                return True
            if 500 <= error_code < 600:
                return True
            if 400 <= error_code < 500 and error_code != 404:
                return True

        return False

    def retry(self, func: Callable):
        """Decorator to wrap any method with retry logic."""

        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            total_attempts = self.max_retries + 1

            for attempt in range(1, total_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as error:
                    if not self._is_retriable_error(error):
                        raise

                    if attempt >= total_attempts:
                        print(f"Max retries reached for {func.__name__}: {error}")
                        raise

                    sleep_time = self.delay_factor**attempt
                    print(
                        f"Retry {attempt}/{self.max_retries} for {func.__name__} "
                        f"in {sleep_time:.1f}s due to {type(error).__name__}: {error}"
                    )
                    time.sleep(sleep_time)

        return wrapper

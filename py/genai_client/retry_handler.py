import time
from typing import Callable, Any, Type, List
from functools import wraps

class RetryHandler:
    def __init__(
        self,
        max_retries: int = 1,
        delay_factor: float = 2.0,
        retriable_exceptions: tuple[Type[Exception], ...] = (),
        transient_pattern_exceptions: List = None,
    ):
        self.max_retries = max_retries
        self.delay_factor = delay_factor
        self.retriable_exceptions = retriable_exceptions
        self.transient_pattern_exceptions = [pattern.lower() for pattern in (transient_pattern_exceptions or [])]

    def retry(self, func: Callable):
        """Decorator to wrap any method with retry logic."""
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            for attempt in range(1, self.max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as error:
                    error_str = str(error).lower()
                    is_retriable = isinstance(error, self.retriable_exceptions)
                    matches_transient = any(pattern in error_str for pattern in self.transient_pattern_exceptions)

                    if not (is_retriable or matches_transient): # Raising the error, if its not retriable and transient error
                        raise

                    if attempt >= self.max_retries: # checking the attempt count
                        print(f"Max retries reached for {func.__name__}: {error}")
                        raise

                    sleep_time = self.delay_factor ** attempt
                    print(
                        f"Retry {attempt}/{self.max_retries} for {func.__name__} "
                        f"in {sleep_time:.1f}s due to {type(error).__name__}: {error}"
                    )
                    time.sleep(sleep_time)

        return wrapper
 
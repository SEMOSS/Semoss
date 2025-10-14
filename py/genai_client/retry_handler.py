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
        self.transient_pattern_exceptions = transient_pattern_exceptions

    def retry(self, func: Callable):
        """Decorator to wrap any method with retry logic."""
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            attempt = 0
            while attempt < self.max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as error:
                    if isinstance(error, self.retriable_exceptions) or any(pattern in str(error).lower() for pattern in self.transient_pattern_exceptions):
                        attempt += 1
                        if attempt >= self.max_retries:
                            print(f"Max retries reached for {func.__name__}: {error}")
                            raise
                        sleep_time = self.delay_factor ** attempt
                        print(
                            f"Retry {attempt}/{self.max_retries} for {func.__name__} after {sleep_time}s due to {type(error).__name__}: {error}"
                        )
                        time.sleep(sleep_time)
                    else:
                        raise error
        return wrapper
 
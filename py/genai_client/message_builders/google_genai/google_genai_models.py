from ...utils import StringEnum
from google.api_core import exceptions as google_exceptions


class GoogleRoles(StringEnum):
    USER = "user"
    MODEL = "model"


GOOGLE_RETRIABLE_EXCEPTIONS = (
    google_exceptions.ServiceUnavailable,
    google_exceptions.InternalServerError,
    google_exceptions.DeadlineExceeded,
    google_exceptions.ResourceExhausted,
    google_exceptions.Aborted,
    google_exceptions.Cancelled,
    ConnectionError,
    TimeoutError,
)

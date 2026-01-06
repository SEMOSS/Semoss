from typing import Any
from pydantic import BaseModel
from anthropic import BadRequestError


class ErrorDetails(BaseModel):
    message: str
    error_type: str
    code: int
    client: str
    model: str
    messageType: str = "ERROR"


class SemossException:
    """Base error for all SEMOSS-related exceptions."""

    def __init__(
        self,
        error: Any,
        client: str,
        model: str,
    ):
        self.error = error
        self.client = client
        self.model = model

    def parse_error(self) -> ErrorDetails:
        if self.client.lower() == "anthropic":
            return self._parse_anthropic_error()
        else:
            return ErrorDetails(
                message=str(self.error),
                code=500,
                error_type="Unknown",
                client=self.client,
                model=self.model,
            )

    def _parse_anthropic_error(self) -> ErrorDetails:
        if isinstance(self.error, BadRequestError):
            return self._parse_anthropic_bad_request(self.error)
        else:
            return ErrorDetails(
                message=str(self.error),
                code=400,
                error_type="Unknown",
                client=self.client,
                model=self.model,
            )

    def _parse_anthropic_bad_request(self, error: BadRequestError) -> ErrorDetails:
        status_code = error.status_code
        body = error.body if isinstance(error.body, dict) else {}

        error_inner = body.get("error", {})

        if isinstance(error_inner, dict):
            message = error_inner.get(
                "message", "An unexpected Anthropic error occurred."
            )
            error_type = error_inner.get("type", "invalid_request_error")
            if error_type == "invalid_request_error":
                error_type = "Invalid Request Error"
        else:
            message = str(error.message)
            error_type = "Unknown"

        return ErrorDetails(
            message=message,
            code=status_code,
            error_type=error_type,
            client=self.client,
            model=self.model,
        )

from typing import Any
from pydantic import BaseModel
from anthropic import APIStatusError, APIConnectionError, APITimeoutError


class AnthropicRefusalError(RuntimeError):
    """Raised when Anthropic returns stop_reason='refusal'."""


class ErrorDetails(BaseModel):
    message: str
    error_type: str
    code: int
    client: str
    model: str
    messageType: str = "ERROR"


class ModelEngineException:
    def __init__(self, error: Any, client: str, model: str):
        self.error = error
        self.client = client
        self.model = model

    def parse_error(self) -> ErrorDetails:
        if self.client.lower() == "anthropic":
            return self._parse_anthropic_error()

        return ErrorDetails(
            message=str(self.error),
            code=500,
            error_type="Internal Server Error",
            client=self.client,
            model=self.model,
        )

    def _parse_anthropic_error(self) -> ErrorDetails:

        if isinstance(self.error, AnthropicRefusalError):
            return ErrorDetails(
                message="The model refused to complete the request due to safety or policy constraints.",
                code=403,
                error_type="Model Refusal",
                client=self.client,
                model=self.model,
            )

        if isinstance(self.error, APIStatusError):
            return self._parse_anthropic_status_error(self.error)

        if isinstance(self.error, APITimeoutError):
            return ErrorDetails(
                message="The request to Anthropic timed out.",
                code=408,
                error_type="Timeout Error",
                client=self.client,
                model=self.model,
            )

        if isinstance(self.error, APIConnectionError):
            return ErrorDetails(
                message="Could not connect to Anthropic servers.",
                code=502,
                error_type="Connection Error",
                client=self.client,
                model=self.model,
            )

        return ErrorDetails(
            message=str(self.error),
            code=500,
            error_type="Unknown Anthropic Error",
            client=self.client,
            model=self.model,
        )

    def _parse_anthropic_status_error(self, error: APIStatusError) -> ErrorDetails:
        """
        Handles any Anthropic error that has an HTTP status code.
        This includes BadRequestError, RateLimitError, InternalServerError, etc.
        """
        status_code = error.status_code
        body = error.body if isinstance(error.body, dict) else {}
        error_inner = body.get("error", {})

        if isinstance(error_inner, dict):
            message = error_inner.get("message", error.message)
            raw_type = error_inner.get("type", "api_error")
            error_type = raw_type.replace("_", " ").title()
        else:
            message = error.message
            error_type = "API Status Error"

        return ErrorDetails(
            message=message,
            code=status_code,
            error_type=error_type,
            client=self.client,
            model=self.model,
        )

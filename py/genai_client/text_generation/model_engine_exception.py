from typing import Any
import traceback, re, json
from pydantic import BaseModel
from anthropic import APIStatusError, APIConnectionError, APITimeoutError


class AnthropicRefusalError(RuntimeError):
    """Raised when Anthropic returns stop_reason='refusal'."""


class ErrorDetails(BaseModel):
    messageType: str = "ERROR"
    message: str
    error_type: str
    code: int
    client: str
    model: str
    traceback: str


class ModelEngineException:
    def __init__(self, error: Any, client: str, model: str):
        self.error = error
        self.client = client
        self.model = model
        self.traceback = traceback.format_exc()

    def parse_error(self) -> ErrorDetails:
        if self.client == "anthropic":
            return self._parse_anthropic_error()
        elif self.client in ["google", "vertex", "gemini"]:
            return self._parse_google_error()

        return ErrorDetails(
            message=str(self.error),
            code=500,
            error_type="Internal Server Error",
            client=self.client,
            model=self.model,
            traceback=self.traceback,
        )

    def _parse_google_error(self) -> ErrorDetails:
        """
        Handles Google GenAI errors which often look like:
        400 INVALID_ARGUMENT. {'error': {'code': 400, 'message': '...', 'status': '...'}}
        """
        error_str = str(self.error)
        code = 500
        error_type = "Google API Error"
        message = error_str

        if hasattr(self.error, "code"):
            code = self.error.code() if callable(self.error.code) else self.error.code

        try:
            if "{" in error_str:
                json_part = error_str[error_str.find("{") :]
                json_part = json_part.replace("'", '"')
                data = json.loads(json_part)

                inner_error = data.get("error", {})
                if isinstance(inner_error, dict):
                    message = inner_error.get("message", message)
                    code = inner_error.get("code", code)
                    error_type = (
                        inner_error.get("status", "GOOGLE_ERROR")
                        .replace("_", " ")
                        .title()
                    )
        except Exception:
            pass

        clean_message = re.sub(r"^\d+\s+[A-Z_]+\.\s*", "", message)

        return ErrorDetails(
            message=clean_message,
            code=code,
            error_type=error_type,
            client=self.client,
            model=self.model,
            traceback=self.traceback,
        )

    def _parse_anthropic_error(self) -> ErrorDetails:

        if isinstance(self.error, AnthropicRefusalError):
            return ErrorDetails(
                message="The model refused to complete the request due to safety or policy constraints.",
                code=403,
                error_type="Model Refusal",
                client=self.client,
                model=self.model,
                traceback=self.traceback,
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
                traceback=self.traceback,
            )

        if isinstance(self.error, APIConnectionError):
            return ErrorDetails(
                message="Could not connect to Anthropic servers.",
                code=502,
                error_type="Connection Error",
                client=self.client,
                model=self.model,
                traceback=self.traceback,
            )

        return ErrorDetails(
            message=str(self.error),
            code=500,
            error_type="Unknown Anthropic Error",
            client=self.client,
            model=self.model,
            traceback=self.traceback,
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
            traceback=self.traceback,
        )

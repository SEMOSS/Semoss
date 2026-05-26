from typing import Any
import traceback, re, json
import openai
from pydantic import BaseModel
from anthropic import APIStatusError, APIConnectionError, APITimeoutError
from botocore.exceptions import ClientError, BotoCoreError


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
        elif self.client in ["openai", "azure"]:
            return self._parse_openai_error()
        elif self.client == "bedrock":
            return self._parse_bedrock_error()

        # Generic fallback
        return self._create_error_details(
            message=str(self.error),
            code=500,
            error_type="Internal Server Error",
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

        return self._create_error_details(
            message=clean_message,
            code=int(str(code)),
            error_type=error_type,
        )

    def _parse_anthropic_error(self) -> ErrorDetails:

        if isinstance(self.error, AnthropicRefusalError):
            return self._create_error_details(
                message="The model refused to complete the request due to safety or policy constraints.",
                code=403,
                error_type="Model Refusal",
            )

        if isinstance(self.error, APIStatusError):
            return self._parse_anthropic_status_error(self.error)

        if isinstance(self.error, APITimeoutError):
            return self._create_error_details(
                message="The request to Anthropic timed out.",
                code=408,
                error_type="Timeout Error",
            )

        if isinstance(self.error, APIConnectionError):
            return self._create_error_details(
                message="Could not connect to Anthropic servers.",
                code=502,
                error_type="Connection Error",
            )

        return self._create_error_details(
            message=str(self.error),
            code=500,
            error_type="Unknown Anthropic Error",
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

        return self._create_error_details(
            message=message,
            code=status_code,
            error_type=error_type,
        )

    def _parse_openai_error(self) -> ErrorDetails:
        if isinstance(self.error, openai.ContentFilterFinishReasonError):
            return self._create_error_details(
                "Request rejected by safety content filter.", 403, "Content Filter"
            )

        if isinstance(self.error, openai.LengthFinishReasonError):
            return self._create_error_details(
                "Model reached maximum output length limit.", 400, "Max Tokens Reached"
            )

        if isinstance(self.error, openai.APIError):
            status_code = getattr(self.error, "status_code", 500)

            raw_type = self.error.code or self.error.type or "api_error"
            error_type = str(raw_type).replace("_", " ").title()

            return self._create_error_details(
                message=self.error.message, code=status_code, error_type=error_type
            )

        if isinstance(self.error, openai.APITimeoutError):
            return self._create_error_details(
                "OpenAI request timed out.", 408, "Timeout"
            )

        if isinstance(self.error, openai.APIConnectionError):
            return self._create_error_details(
                "Connection to OpenAI failed.", 502, "Connection Failure"
            )

        return self._default_fallback()

    def _parse_bedrock_error(self) -> ErrorDetails:
        """
        Handles AWS Bedrock ClientErrors.
        Structure: ClientError('An error occurred (ExceptionName) ...')
        Metadata lives in error.response['Error']
        """
        message = str(self.error)
        code = 500
        error_type = "Bedrock API Error"

        if isinstance(self.error, ClientError):
            response = self.error.response.get("Error", {})

            raw_code_str = response.get("Code", "BedrockError")
            error_type = raw_code_str.replace("_", " ").title()

            message = response.get("Message", message)

            code = self.error.response.get("ResponseMetadata", {}).get(
                "HTTPStatusCode", 400
            )

        elif isinstance(self.error, BotoCoreError):
            error_type = "AWS Connection Error"
            code = 500

        return self._create_error_details(
            message=message, code=code, error_type=error_type
        )

    def _create_error_details(
        self, message: str, code: int, error_type: str
    ) -> ErrorDetails:
        """Helper to keep the object creation consistent."""
        return ErrorDetails(
            message=message,
            code=code,
            error_type=error_type,
            client=self.client,
            model=self.model,
            traceback=self.traceback,
        )

    def _default_fallback(self) -> ErrorDetails:
        return self._create_error_details(
            message=str(self.error),
            code=500,
            error_type="Internal Server Error",
        )

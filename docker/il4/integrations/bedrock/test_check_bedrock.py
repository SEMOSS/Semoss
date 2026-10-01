import contextlib
import io
import json
import os
import unittest
from unittest.mock import MagicMock, patch

import check_bedrock as check


class BedrockCheckTests(unittest.TestCase):
    def setUp(self):
        self.adapter = MagicMock()
        self.adapter.client.meta.endpoint_url = (
            "https://bedrock-runtime-fips.us-gov-west-1.amazonaws.com"
        )
        self.adapter.client._endpoint.http_session._verify = True
        self.adapter.ask_call.return_value = check.AskModelEngineResponse2(
            response="READY", prompt_tokens=18, response_tokens=5,
        )
        self.credentials = {
            "AccessKeyId": "test-id", "SecretAccessKey": "test-secret",
            "SessionToken": "test-session-token",
        }
        self.output = io.StringIO()
        for patcher in (
            patch.dict(os.environ, {}, clear=True),
            patch.object(check, "BedrockClient", return_value=self.adapter),
            patch.object(check, "Stubber", autospec=True),
            patch.object(check.boto3, "setup_default_session"),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)

    def run_check(self, live=True):
        with patch.object(check.sys, "stdin", io.StringIO(json.dumps(self.credentials))):
            with contextlib.redirect_stdout(self.output):
                check.run(live, "us-gov-west-1", "test-model")

    def test_live_default_chain_supports_session_credentials_without_logging_them(self):
        self.run_check()
        check.boto3.setup_default_session.assert_called_once_with(
            aws_access_key_id="test-id", aws_secret_access_key="test-secret",
            aws_session_token="test-session-token", region_name="us-gov-west-1",
        )
        result = json.loads(self.output.getvalue())
        self.assertEqual(result["mode"], "LIVE")
        self.assertEqual(result["response_tokens"], 5)
        for secret in self.credentials.values():
            self.assertNotIn(secret, self.output.getvalue())
        self.adapter.client.close.assert_called_once()
        self.assertIsNone(check.boto3.DEFAULT_SESSION)
        self.assertEqual(os.environ["AWS_USE_FIPS_ENDPOINT"], "true")

    def test_offline_uses_stubber_and_checks_consumption(self):
        self.run_check(live=False)
        stub = check.Stubber.return_value
        stub.activate.assert_called_once()
        stub.assert_no_pending_responses.assert_called_once()
        stub.deactivate.assert_called_once()
        self.assertEqual(json.loads(self.output.getvalue())["mode"], "OFFLINE")

    def test_missing_credentials_fail_before_adapter_construction(self):
        self.credentials = {}
        with self.assertRaisesRegex(ValueError, "usable credentials"):
            self.run_check()
        check.BedrockClient.assert_not_called()

    def test_non_fips_or_plaintext_endpoint_fails_before_request(self):
        for endpoint in (
            "http://bedrock-runtime-fips.us-gov-west-1.amazonaws.com",
            "https://bedrock-runtime.us-gov-west-1.amazonaws.com",
            "https://unexpected-fips.example.invalid",
        ):
            with self.subTest(endpoint=endpoint):
                self.adapter.client.meta.endpoint_url = endpoint
                with self.assertRaisesRegex(RuntimeError, "HTTPS FIPS"):
                    self.run_check()
        self.adapter.ask_call.assert_not_called()

    def test_disabled_tls_verification_fails_before_request(self):
        self.adapter.client._endpoint.http_session._verify = False
        with self.assertRaisesRegex(RuntimeError, "must not be disabled"):
            self.run_check()
        self.adapter.ask_call.assert_not_called()

    def test_adapter_error_response_is_not_success(self):
        self.adapter.ask_call.return_value = {"error": "synthetic failure"}
        with self.assertRaisesRegex(RuntimeError, "request failed"):
            self.run_check()
        self.adapter.client.close.assert_called_once()
        self.assertEqual(self.output.getvalue(), "")

    def test_unexpected_output_or_token_count_fails(self):
        for text, tokens in (("NOT READY", 5), ("READY", 0), ("READY", 17)):
            with self.subTest(text=text, tokens=tokens):
                self.adapter.ask_call.return_value = check.AskModelEngineResponse2(
                    response=text, response_tokens=tokens,
                )
                with self.assertRaisesRegex(RuntimeError, "Unexpected Claude"):
                    self.run_check()

    def test_request_guard_enforces_content_and_token_budget(self):
        self.run_check()
        hook = self.adapter.client.meta.events.register.call_args.args[1]
        params = {
            "inferenceConfig": {"maxTokens": 16},
            "messages": [{"role": "user", "content": [{"text": check.PROMPT}]}],
        }
        hook(params)
        params["inferenceConfig"]["maxTokens"] = 17
        with self.assertRaisesRegex(RuntimeError, "token budget"):
            hook(params)
        params["inferenceConfig"]["maxTokens"] = 16
        params["messages"] = []
        with self.assertRaisesRegex(RuntimeError, "Unexpected request"):
            hook(params)


if __name__ == "__main__":
    unittest.main()

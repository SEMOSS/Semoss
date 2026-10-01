"""Exercise the packaged SEMOSS Bedrock adapter; live mode reads CLI credentials on stdin."""
import argparse
import json
import os
import sys

sys.path.insert(0, "/opt/semosshome/py")

import boto3
from botocore.stub import Stubber
from genai_client import BedrockClient
from genai_client.constants import AskModelEngineResponse2


PROMPT = "Reply with exactly the word READY and nothing else."


def run(live, region, model):
    os.environ["AWS_USE_FIPS_ENDPOINT"] = "true"
    os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
    credentials = json.load(sys.stdin) if live else {
        "AccessKeyId": "offline-test", "SecretAccessKey": "offline-test",
    }
    if not credentials.get("AccessKeyId") or not credentials.get("SecretAccessKey"):
        raise ValueError("AWS CLI did not supply usable credentials")
    boto3.setup_default_session(
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials.get("SessionToken"),
        region_name=region,
    )
    del credentials
    adapter = BedrockClient(
        modelId=model, region=region, max_completion_tokens=16,
        retry_config={"mode": "standard", "max_attempts": 0},
    )
    endpoint = adapter.client.meta.endpoint_url
    expected_endpoint = "https://bedrock-runtime-fips." + region + ".amazonaws.com"
    if endpoint != expected_endpoint:
        raise RuntimeError("Bedrock did not resolve to an HTTPS FIPS endpoint")
    if adapter.client._endpoint.http_session._verify is False:
        raise RuntimeError("AWS TLS certificate verification must not be disabled")

    def check_request(params, **kwargs):
        if params["inferenceConfig"]["maxTokens"] != 16:
            raise RuntimeError("Smoke request exceeded its configured output-token budget")
        if params["messages"] != [{"role": "user", "content": [{"text": PROMPT}]}]:
            raise RuntimeError("Unexpected request content; refusing to send")

    adapter.client.meta.events.register(
        "before-parameter-build.bedrock-runtime.Converse", check_request
    )
    message_json = json.dumps([{
        "type": "INPUT_TEXT", "io": "INPUT", "schemaVersion": 2,
        "parts": [{"type": "TEXT", "text": PROMPT}],
    }])
    stub = Stubber(adapter.client)
    if not live:
        stub.add_response("converse", {
            "output": {"message": {"role": "assistant", "content": [{"text": "READY"}]}},
            "stopReason": "end_turn",
            "usage": {"inputTokens": 10, "outputTokens": 1, "totalTokens": 11},
            "metrics": {"latencyMs": 1},
        })
        stub.activate()
    try:
        result = adapter.ask_call(message_json=message_json, stream=False, max_completion_tokens=16)
        if not isinstance(result, AskModelEngineResponse2):
            raise RuntimeError("SEMOSS Bedrock request failed: " + repr(result))
        if result.response.strip() != "READY" or not 0 < result.response_tokens <= 16:
            raise RuntimeError("Unexpected Claude smoke-test response")
        if not live:
            stub.assert_no_pending_responses()
        print(json.dumps({
            "mode": "LIVE" if live else "OFFLINE",
            "adapter": "SEMOSS BedrockClient.ask_call",
            "region": region, "model": model, "endpoint": endpoint,
            "response": result.response, "prompt_tokens": result.prompt_tokens,
            "response_tokens": result.response_tokens,
            "tls_certificate_verification": True,
        }, indent=2))
    finally:
        stub.deactivate()
        adapter.client.close()
        boto3.DEFAULT_SESSION = None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--region", default="us-gov-west-1")
    parser.add_argument("--model", default="us-gov.anthropic.claude-sonnet-4-5-20250929-v1:0")
    arguments = parser.parse_args()
    run(arguments.live, arguments.region, arguments.model)

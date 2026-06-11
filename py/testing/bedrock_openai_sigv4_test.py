"""
Smoke-test for the OpenAI-compatible Bedrock endpoint using SigV4 auth.

Requires the IAM user/role to have the `bedrock-mantle:CreateInference` permission.

Usage:
    python bedrock_openai_sigv4_test.py
"""

import os

import boto3
import httpx
from openai import OpenAI
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

# ── Config ────────────────────────────────────────────────────────────────────
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-2")
SERVICE = "bedrock-mantle"

MODEL = "openai.gpt-5.4"
# ──────────────────────────────────────────────────────────────────────────────


class BedrockSigV4Auth(httpx.Auth):
    """httpx auth handler that SigV4-signs every request for AWS bedrock-mantle."""

    def __init__(self, service: str, region: str, credentials):
        self.service = service
        self.region = region
        self.credentials = credentials

    def auth_flow(self, request: httpx.Request):
        aws_request = AWSRequest(
            method=request.method,
            url=str(request.url),
            data=request.content,
            headers={"host": request.url.host},
        )
        SigV4Auth(self.credentials, self.service, self.region).add_auth(aws_request)
        for k, v in aws_request.headers.items():
            request.headers[k] = v
        yield request


def build_client(access_key: str, secret_key: str, region: str) -> OpenAI:
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
    )
    creds = session.get_credentials().get_frozen_credentials()
    return OpenAI(
        api_key="dummy",  # satisfies SDK constructor; overridden by SigV4 auth
        base_url=f"https://{SERVICE}.{region}.api.aws/openai/v1",
        http_client=httpx.Client(auth=BedrockSigV4Auth(SERVICE, region, creds)),
    )


if __name__ == "__main__":
    print(f"Testing model: {MODEL} in {REGION}")
    client = build_client(AWS_ACCESS_KEY, AWS_SECRET_KEY, REGION)
    response = client.responses.create(model=MODEL, input="What is 2+2?")
    print("Response:", response.output_text)

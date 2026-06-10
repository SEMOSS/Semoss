from typing import TYPE_CHECKING, Generator

import httpx

if TYPE_CHECKING:
    from botocore.credentials import Credentials


class BedrockSigV4Auth(httpx.Auth):
    # Mantle uses SigV4, not bearer tokens — overwrite the OpenAI SDK's Authorization header.
    # Hold the live Credentials object (not a frozen snapshot) so refreshable
    # creds (EC2 role, assumed role, SSO) auto-renew between requests.
    requires_request_body = True

    def __init__(
        self,
        credentials: "Credentials",
        service: str = "bedrock-mantle",
        region: str = "us-east-1",
    ) -> None:
        self._credentials = credentials
        self._service = service
        self._region = region

    def auth_flow(
        self, request: httpx.Request
    ) -> Generator[httpx.Request, httpx.Response, None]:
        from botocore.auth import SigV4Auth
        from botocore.awsrequest import AWSRequest

        aws_request = AWSRequest(
            method=request.method,
            url=str(request.url),
            data=request.content,
            headers={"host": request.url.host},
        )
        SigV4Auth(self._credentials, self._service, self._region).add_auth(aws_request)
        for key, value in aws_request.headers.items():
            request.headers[key] = value
        yield request

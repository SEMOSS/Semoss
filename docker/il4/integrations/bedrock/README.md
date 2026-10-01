# Bedrock integration validation

The **actual packaged SEMOSS 5.4 `BedrockClient.ask_call` adapter** passed a live,
non-streaming Claude request from the final UBI 10.2 / Python 3.14 image:

| Check | Observed |
| --- | --- |
| Region | `us-gov-west-1` |
| Inference profile | `us-gov.anthropic.claude-sonnet-4-5-20250929-v1:0` |
| Endpoint | `https://bedrock-runtime-fips.us-gov-west-1.amazonaws.com` |
| TLS certificate verification | Enabled |
| Response | `READY` |
| Input / output tokens | 18 / 5 |
| Output limit / retries | 16 tokens / no retries |

Only a fixed synthetic prompt was sent. No workspace content or production data
was sent. Existing AWS CLI authentication was used without changing IAM, account
configuration, or the original credential files. Credentials passed through stdin
into process memory, not command arguments, Docker environment configuration,
image layers, or exported credential files. The disposable container was removed.
This is an adapter test, **not yet a registered SEMOSS ModelEngine/UI conversation**.
Streaming, tools, guardrails, and audio are not covered by this live check.
Eight hermetic checker tests passed with 92.2% statement coverage measured by
Python's standard-library `trace`; the 32 assembly/HTTP/Python tests also passed.
The live call preceded the Java JDBC driver overlay; the unchanged Python
adapter and its offline checks were verified again against the rebuilt image.

## Repeat the checks

From the workspace root, run the hermetic unit tests and actual adapter stub:

```bash
docker run --rm --platform linux/amd64 --network none --read-only \
  --cap-drop=ALL --security-opt=no-new-privileges \
  --tmpfs /tmp:rw,noexec,nosuid,nodev \
  --mount "type=bind,src=$PWD/integrations/bedrock,dst=/checks,readonly" \
  --entrypoint /opt/semoss-python/bin/python \
  semoss:5.4.0-ubi10-python314-bcfips \
  -m unittest discover -s /checks -p 'test_*.py'

docker run --rm --platform linux/amd64 --network none --read-only \
  --cap-drop=ALL --security-opt=no-new-privileges \
  --tmpfs /tmp:rw,noexec,nosuid,nodev \
  --mount "type=bind,src=$PWD/integrations/bedrock,dst=/checks,readonly" \
  --entrypoint /opt/semoss-python/bin/python \
  semoss:5.4.0-ubi10-python314-bcfips /checks/check_bedrock.py
```

The following makes **one billable request** using the current AWS CLI profile.
Do not enable shell tracing, dump the credential export, or run on an untrusted
Docker host. A FIPS service endpoint does not validate the Python cryptographic
modules; Python TLS is not routed through Java Bouncy Castle.

```bash
set -o pipefail
aws configure export-credentials --format process |
docker run --rm -i --platform linux/amd64 --read-only \
  --cap-drop=ALL --security-opt=no-new-privileges --memory=2g --pids-limit=128 \
  --tmpfs /tmp:rw,noexec,nosuid,nodev \
  --mount "type=bind,src=$PWD/integrations/bedrock,dst=/checks,readonly" \
  --entrypoint /opt/semoss-python/bin/python \
  semoss:5.4.0-ubi10-python314-bcfips /checks/check_bedrock.py --live
```

The checker fails for adapter error objects, unexpected response content, disabled
TLS verification, or an endpoint other than the expected regional AWS FIPS URL.
Custom endpoints and other DNS partitions are intentionally not supported by this
checker. Before sending, it verifies that the
message contains only the fixed synthetic prompt and that `maxTokens` is 16.

## Deployment requirements

- Prefer short-lived workload-role credentials through boto3's default credential
  chain. The tested CLI identity did not provide a session token or expiration;
  it is not a substitute for production workload identity.
- Set `AWS_USE_FIPS_ENDPOINT=true` in the deployed worker environment. Keep TLS
  verification enabled; use an approved PEM CA bundle via `AWS_CA_BUNDLE` if
  required by your environment. Do not use `verify=False`.
- Use the default credential chain rather than SEMOSS's explicit `access_key` /
  `secret_key` constructor options: that constructor has no session-token option.
- Authorize only required `bedrock:InvokeModel` resources, including the inference
  profile and its destination model resources. Streaming additionally requires
  `bedrock:InvokeModelWithResponseStream`. Do not grant blanket Bedrock admin.
- Review the inference profile's destination regions against your data-residency
  requirements before production use. This check does not alter profile routing.
- Configure model permissions, guardrails, auditing, and cost controls before
  exposing a registered model to users.
- The packaged adapter requires `message_json` with SEMOSS message parts.
  Older examples using only `question=` do not exercise this release correctly.

No persistent model registration or secrets backend is configured by this checker.

import os
import sys
from dotenv import load_dotenv

os.chdir("/workspace/Semoss/py")
print(os.getcwd())
sys.path.append(os.getcwd())

import genai_client

load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"))

claude4 = genai_client.AnthropicClient(
    model_name="claude-sonnet-4-5@20250929",
    max_tokens=8192,
    context_window=200000,
    project="us-gcp-ame-adv-a66-npd-1",
    region="us-east5",
    service_account_credentials={
        "type": "service_account",
        "project_id": "us-gcp-ame-adv-a66-npd-1",
        "private_key_id": os.getenv("PRIVATE_KEY_ID"),
        "private_key": os.getenv("PRIVATE_KEY"),
        "client_email": "semoss-bucket-sa@us-gcp-ame-adv-a66-npd-1.iam.gserviceaccount.com",
        "client_id": "110498862546389535985",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/semoss-bucket-sa%40us-gcp-ame-adv-a66-npd-1.iam.gserviceaccount.com",
        "universe_domain": "googleapis.com",
    },
    provider="google",
    use_beta_header="true",
    beta_feature_name="interleaved-thinking-2025-05-14",
)

x = claude4.ask(
    question="""""",
    max_completion_tokens=4000,
    message_json='[{"inputUIPrompt":"Are there an infinite number of prime numbers such that n mod 4 == 3?","inputPrompt":"Are there an infinite number of prime numbers such that n mod 4 == 3?","type":"INPUT_TEXT","imageInfos":[],"modelId":"b0d18f4b-ff2c-4563-8f9d-57efbff53d60","modelType":"VERTEX","messageId":"0199f28c-c2d4-70dd-a29a-1e18c7ef853d","tokens":0,"visible":true,"platform_generated":false,"dateCreated":"2025-10-17 14:22:15","ornaments":{}}]',
    tools=[],
    prefix="",
    stream=True,
    thinking={"type": "enabled", "budget_tokens": 2000},
)

print("==== THINKING BLOCK ====")
print(x.get("thinking", "No thinking block found."))

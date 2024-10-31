### To run this you need to be in the Semoss/py directory
### Then run the following command
### python -m unittest genai_client.tests.test_ner_client
import os
import unittest
import asyncio
from typing import Dict, List
from genai_client import NERRemoteClient
from dotenv import load_dotenv


class TestNERClient(unittest.TestCase):

    def test_model_init(self):
        load_dotenv()
        DEPLOYER_ENDPOINT = os.getenv("DEPLOYER_ENDPOINT")
        MODEL_ID = os.getenv("MODEL_ID")

        print(f"Using deployer endpoint: {DEPLOYER_ENDPOINT} with model id: {MODEL_ID}")

        model = NERRemoteClient(
            host="127.0.0.1:2181",  # Port forwarding to local
            deployer_endpoint=DEPLOYER_ENDPOINT,
            model_repo_name="urchade/gliner_multi-v2.1",
            model_name="gliner-multi-v2-1",
            model_id=MODEL_ID,
            is_dev=True,
        )

        model_status = model.initalize()

        print(f"MESSAGE: {model_status.message}")
        print(f"STATUS: {model_status.status}")
        print(f"CLUSTER IP: {model_status.cluster_ip}")

        self.assertIsInstance(model_status.status, str)
        self.assertIsInstance(model_status.message, str)

        if model_status.status == "active":
            self.assertIsInstance(model_status.cluster_ip, str)

    def test_predict(self):
        load_dotenv()
        DEPLOYER_ENDPOINT = os.getenv("DEPLOYER_ENDPOINT")
        MODEL_ID = os.getenv("MODEL_ID")
        try:
            model = NERRemoteClient(
                host="127.0.0.1:2181",  # Port forwarding to local
                deployer_endpoint=DEPLOYER_ENDPOINT,
                model_repo_name="urchade/gliner_multi-v2.1",
                model_name="gliner-multi-v2-1",
                model_id=MODEL_ID,
                is_dev=True,
            )

            prediction = model.predict(
                text="John Smith works at Microsoft in Seattle",
                entities=["PERSON", "ORGANIZATION", "LOCATION"],
                mask_entities=["PERSON", "ORGANIZATION"],
            )
            print("PREDICTION:", prediction)

            # This block is if the model is cold and needs to be started
            if prediction.get("message") is not None:
                status = prediction.get("status")
                message = prediction.get("message")
                cluster_ip = prediction.get("cluster_ip")
                print(
                    f"STATUS: {status}.. MESSAGE: {message}.. CLUSTER IP: {cluster_ip}"
                )
                self.assertIsInstance(status, str)
                self.assertIsInstance(message, str)
            # This block is if the model is hot and ready to make predictions
            else:
                self.assertIsInstance(prediction, Dict)

                output = prediction.get("output")
                self.assertIsInstance(output, str)
                raw_output = prediction.get("raw_output")
                self.assertIsInstance(raw_output, List)
                self.assertGreater(len(raw_output), 0)
                input = prediction.get("input")
                self.assertIsInstance(input, str)
                entities = prediction.get("entities")
                self.assertIsInstance(entities, List)
                self.assertGreater(len(entities), 0)
        finally:
            asyncio.run(model.close())

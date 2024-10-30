### To run this you need to be in the Semoss/py directory
### Then run the following command
### python -m unittest genai_client.tests.test_ner_client

import unittest
import asyncio
from typing import Dict, List
from genai_client import NERRemoteClient


class TestNERClient(unittest.TestCase):

    def test_model_init(self):

        model = NERRemoteClient(
            host="127.0.0.1:2181",  # Port forwarding to local
            deployer_endpoint="",  # Fill in with the endpoint
            model_repo_name="urchade/gliner_multi-v2.1",
            model_name="gliner-multi-v2-1",
            model_id="",  # Fill in with the model id
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
        try:
            model = NERRemoteClient(
                host="127.0.0.1:2181",  # Port forwarding to local
                deployer_endpoint="",
                model_repo_name="urchade/gliner_multi-v2.1",
                model_name="gliner-multi-v2-1",
                model_id="",
                is_dev=True,
            )

            prediction = model.predict(
                text="John Smith works at Microsoft in Seattle",
                entities=["PERSON", "ORGANIZATION", "LOCATION"],
                mask_entities=["PERSON", "ORGANIZATION"],
            )

            print(prediction)

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

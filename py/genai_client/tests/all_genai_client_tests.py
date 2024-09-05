### ATTENTION
### To run this you need to be in the Semoss_Dev/py directory
### Then run the following command
### py -3.10 -m unittest genai_client.tests.all_genai_client_tests

import unittest

from genai_client.tests.test_genai_text_generation import AskModelTests
from genai_client.tests.test_genai_embedders import EmbeddingsModelTests

if __name__ == "__main__":
    test_loader = unittest.TestLoader()
    test_suite = unittest.TestSuite()

    # tests for model asks
    test_text_generation = test_loader.loadTestsFromTestCase(AskModelTests)
    test_suite.addTest(test_text_generation)

    # tests for embeddings
    test_embeddings = test_loader.loadTestsFromTestCase(EmbeddingsModelTests)
    test_suite.addTest(test_embeddings)

    # run tests
    unittest.TextTestRunner(verbosity=2).run(test_suite)

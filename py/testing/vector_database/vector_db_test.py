"""
To run the tests, navigate to the Semoss_Dev/py directory and then run the following command:

    pytest testing/vector_database/vector_db_test.py
    pytest -s testing/vector_database/vector_db_test.py -> For enabling the print statements

Ensure the below items before run it,
    - Make sure you have the below model engines,
        - Dev_Testing_FAISS__1222b449-1bc6-4358-9398-1ed828e4f26a
        - TextEmbeddings BAAI-Large-En-V1.5__e4449559-bcff-4941-ae72-0e3f18e06660_engine
    - Make sure you have added the `dataset.pkl` and `vectors.pkl` in the test_files folder
        - test_files path - Semoss/py/testing/vector_database/test_files
        - Create the test_files folder if you don't have and add the pkl files into it from Dev_Testing_FAISS engine (Dev_Testing_FAISS_\Dev_Testing_FAISS_\schema\default)
    - Make sure you have a valid `.env.example` file with the below required keys:
        - SERVER_CLIENT_BASE
        - ACCESS_KEY
        - SECRET_KEY

Install pytest if not already installed:
    pip install pytest
"""

import os
import pytest
from dotenv import load_dotenv
from genai_client import get_tokenizer
from gaas_gpt_model import ModelEngine
import vector_database

# Load environment variables from .env.example file
load_dotenv("testing/.env.example")

SERVER_CLIENT_BASE = os.getenv("SERVER_CLIENT_BASE")
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")


@pytest.fixture
def embed_tokenizer():
    """Fixture to get the appropriate tokenizer for EMBEDDED type."""
    return get_tokenizer(
        tokenizer_name="BAAI/bge-large-en-v1.5",
        max_tokens=None,
        tokenizer_type="EMBEDDED",
    )


def test_nearestNeighbor_with_local_model(embed_tokenizer):
    """
    Test the nearestNeighbor function using local model engine using related functions and assertions.
        - Checking the `create_searcher` function by creating `default` searcher.
        - Checking the `load_dataset` by loading the `dataset.pkl` file.
        - Checking the `load_encoded_vectors` by loading the `vectors.pkl` file.
        - Checking the `nearestNeighbor` function by finding the nearest neightbor against the question.
        - Test the results of `nearestNeighbor` function
            - Checking the type of results
            - Checking the each elements type of results
    """
    # Create a local model engine by passing the engine id
    # To do this you might need to pass your semoss_dev_path, if your Semoss_Dev path is different than default
    #     - semoss_dev_path -> Defaults for windows: "C:/workspace/Semoss_Dev" & non-windows: "/opt/semosshome"
    #     - It is locating the smss file of the engine.
    #         - Keep your smss file in this location to fetch it -> "C:/workspace/Semoss_Dev/model/model_engine.smss"
    embedder_engine = ModelEngine(
        engine_id="e4449559-bcff-4941-ae72-0e3f18e06660",
        model_engine_class="LOCAL",
        semoss_dev_path="D:/Users/pamuniappa/AI_Jumpstart/Models/",
    )

    faiss_db = vector_database.FAISSDatabase(
        embedder_engine=embedder_engine,
        tokenizer=embed_tokenizer,
        distance_method="Squared Euclidean (L2) distance",
    )

    path_to_index_class = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "test_files"
    )

    faiss_db.create_searcher(searcher_name="default", base_path=path_to_index_class)
    faiss_db.searchers["default"].load_dataset(path_to_index_class + "/dataset.pkl")
    faiss_db.searchers["default"].load_encoded_vectors(
        path_to_index_class + "/vectors.pkl"
    )
    search_results = faiss_db.searchers["default"].nearestNeighbor(
        question="how is the president chosen?"
    )
    print("search_results - ", search_results)

    assert isinstance(search_results, list)

    for row in search_results:
        assert isinstance(row, dict)

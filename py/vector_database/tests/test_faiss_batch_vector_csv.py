import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

import numpy as np
import pandas as pd

from vector_database.faiss.faiss_client import FAISSSearcher


class RecordingEmbedder:
    def __init__(self):
        self.calls = []

    def embeddings(self, *, strings_to_embed, insight_id):
        self.calls.append((list(strings_to_embed), insight_id))
        vectors = np.arange(len(strings_to_embed) * 3, dtype=np.float32).reshape(-1, 3)
        return [{"response": vectors.tolist()}]


class RecordingKeywordEngine:
    def __init__(self):
        self.calls = []

    def keyword_extraction(self, *, input, insight_id, param_dict):
        self.calls.append((list(input), insight_id, dict(param_dict)))
        return [f"keywords:{value}" for value in input]


class FaissBatchVectorCsvTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.indexed_files = (
            Path(self.temporary_directory.name) / "schema" / "default" / "indexed_files"
        )
        self.indexed_files.mkdir(parents=True)
        self.document = self.indexed_files / "batch.csv"
        self.document.write_text("unused", encoding="utf-8")

    def searcher(self, dataset, embedder=None, keyword_engine=None):
        searcher = object.__new__(FAISSSearcher)
        searcher.embeddings_engine = embedder or RecordingEmbedder()
        searcher.keyword_engine = keyword_engine or RecordingKeywordEngine()
        searcher.tokenizer = object()
        searcher.encoded_vectors = None
        searcher.vector_dimensions = None
        searcher._load_dataset = Mock(return_value=dataset.copy())
        searcher.createMasterFiles = Mock(
            return_value=(
                [
                    str(self.indexed_files.parent / "vectors.npy"),
                    str(self.indexed_files.parent / "dataset.parquet"),
                ],
                [],
            )
        )
        return searcher

    def test_embeds_one_csv_once_and_slices_stable_source_artifacts(self):
        dataset = pd.DataFrame(
            {
                "Source": ["alpha.java", "alpha.java", "beta.java"],
                "Modality": ["text", "text", "text"],
                "Divider": ["a#L1-L1", "a#L2-L2", "b#L1-L1"],
                "Part": [1, 2, 1],
                "Tokens": [2, 2, 2],
                "Content": ["alpha one", "alpha two", "beta one"],
            }
        )
        embedder = RecordingEmbedder()
        searcher = self.searcher(dataset, embedder=embedder)

        response = searcher._vector_addDocument(
            [str(self.document)],
            ["Content"],
            [],
            "text",
            "|",
            {},
            "insight-1",
        )

        self.assertEqual(len(embedder.calls), 1)
        self.assertEqual(
            embedder.calls[0],
            (["alpha one|", "alpha two|", "beta one|"], "insight-1"),
        )
        alpha_vectors = np.load(
            self.indexed_files / "alpha.java_vectors.npy", allow_pickle=False
        )
        beta_vectors = np.load(
            self.indexed_files / "beta.java_vectors.npy", allow_pickle=False
        )
        np.testing.assert_array_equal(alpha_vectors, np.arange(6).reshape(2, 3))
        np.testing.assert_array_equal(beta_vectors, np.arange(6, 9).reshape(1, 3))
        self.assertTrue((self.indexed_files / "alpha.java_dataset.parquet").is_file())
        self.assertTrue((self.indexed_files / "beta.java_dataset.parquet").is_file())
        self.assertEqual(searcher.createMasterFiles.call_count, 1)
        searcher.createMasterFiles.assert_called_once_with(
            path_to_files=str(self.indexed_files.parent)
        )
        self.assertIn(
            str(self.indexed_files / "alpha.java_dataset.parquet"),
            response["createdDocuments"],
        )
        self.assertIn(
            str(self.indexed_files / "alpha.java_vectors.npy"),
            response["createdDocuments"],
        )

    def test_oversized_single_source_is_embedded_in_one_call(self):
        row_count = 368
        dataset = pd.DataFrame(
            {
                "Source": ["Parser.java"] * row_count,
                "Content": [f"chunk {index}" for index in range(row_count)],
            }
        )
        embedder = RecordingEmbedder()
        searcher = self.searcher(dataset, embedder=embedder)

        searcher._vector_addDocument(
            [str(self.document)], ["Content"], [], "text", "|", {}, "insight-1"
        )

        self.assertEqual(len(embedder.calls), 1)
        self.assertEqual(len(embedder.calls[0][0]), row_count)
        vectors = np.load(
            self.indexed_files / "Parser.java_vectors.npy", allow_pickle=False
        )
        self.assertEqual(vectors.shape, (row_count, 3))

    def test_invalid_embedding_row_count_writes_no_source_artifacts(self):
        class ShortEmbedder:
            def embeddings(self, *, strings_to_embed, insight_id):
                del insight_id
                return [{"response": [[1.0, 2.0]] * (len(strings_to_embed) - 1)}]

        dataset = pd.DataFrame(
            {"Source": ["alpha.java", "beta.java"], "Content": ["alpha", "beta"]}
        )
        searcher = self.searcher(dataset, embedder=ShortEmbedder())

        with self.assertRaisesRegex(ValueError, "shape does not match"):
            searcher._vector_addDocument(
                [str(self.document)], ["Content"], [], "text", "|", {}, "insight-1"
            )

        self.assertEqual(list(self.indexed_files.glob("*_dataset.parquet")), [])
        self.assertEqual(list(self.indexed_files.glob("*_vectors.npy")), [])
        searcher.createMasterFiles.assert_not_called()

    def test_keyword_search_remains_a_per_source_fallback(self):
        dataset = pd.DataFrame(
            {
                "Source": ["alpha.java", "alpha.java", "beta.java"],
                "Content": ["alpha one", "alpha two", "beta one"],
            }
        )
        embedder = RecordingEmbedder()
        keyword_engine = RecordingKeywordEngine()
        searcher = self.searcher(
            dataset, embedder=embedder, keyword_engine=keyword_engine
        )

        searcher._vector_addDocument(
            [str(self.document)],
            ["Content"],
            [],
            "text",
            "|",
            {"keywordSearch": True, "limit": 4},
            "insight-1",
        )

        self.assertEqual(len(keyword_engine.calls), 2)
        self.assertEqual(len(embedder.calls), 2)
        self.assertEqual(keyword_engine.calls[0][2], {"limit": 4})
        self.assertEqual(keyword_engine.calls[1][2], {"limit": 4})

    def test_per_source_artifact_names_remain_removal_compatible(self):
        dataset = pd.DataFrame({"Source": ["alpha.java"], "Content": ["alpha one"]})
        searcher = self.searcher(dataset)
        searcher._vector_addDocument(
            [str(self.document)], ["Content"], [], "text", "|", {}, "insight-1"
        )

        dataset_path = self.indexed_files / "alpha.java_dataset.parquet"
        vector_path = self.indexed_files / "alpha.java_vectors.npy"
        self.assertTrue(dataset_path.is_file())
        self.assertTrue(vector_path.is_file())
        os.remove(dataset_path)
        os.remove(vector_path)
        self.assertFalse(dataset_path.exists())
        self.assertFalse(vector_path.exists())


if __name__ == "__main__":
    unittest.main()

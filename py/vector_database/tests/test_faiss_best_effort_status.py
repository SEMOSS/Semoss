import logging
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

import numpy as np
import pandas as pd

from vector_database.faiss.faiss_client import FAISSSearcher


class RecordingEmbedder:
    def __init__(self, fail_for=()):
        self.calls = []
        self.fail_for = set(fail_for)

    def embeddings(self, *, strings_to_embed, insight_id):
        self.calls.append((list(strings_to_embed), insight_id))
        if self.fail_for.intersection(strings_to_embed):
            raise RuntimeError("embedding engine rejected the request")
        vectors = np.arange(len(strings_to_embed) * 3, dtype=np.float32).reshape(-1, 3)
        return [{"response": vectors.tolist()}]


class BestEffortStatusTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.indexed_files = (
            Path(self.temporary_directory.name) / "schema" / "default" / "indexed_files"
        )
        self.indexed_files.mkdir(parents=True)

    def document(self, name):
        path = self.indexed_files / name
        path.write_text("unused", encoding="utf-8")
        return str(path)

    def searcher(self, datasets_by_document, embedder=None, corrupted_file_sets=()):
        searcher = object.__new__(FAISSSearcher)
        searcher.class_logger = logging.getLogger(__name__)
        searcher.embeddings_engine = embedder or RecordingEmbedder()
        searcher.keyword_engine = Mock()
        searcher.tokenizer = object()
        searcher.encoded_vectors = None
        searcher.vector_dimensions = None
        searcher._load_dataset = Mock(
            side_effect=lambda dataset_location: datasets_by_document[
                Path(dataset_location).name
            ].copy()
        )
        searcher.createMasterFiles = Mock(
            return_value=(
                [str(self.indexed_files.parent / "dataset.parquet")],
                list(corrupted_file_sets),
            )
        )
        return searcher

    def add(self, searcher, documents):
        return searcher._vector_addDocument(
            documents, ["Content"], [], "text", "|", {}, "insight-1"
        )

    def test_failed_document_is_reported_and_the_rest_still_embed(self):
        datasets = {
            "good.csv": pd.DataFrame(
                {"Source": ["alpha.java"], "Content": ["alpha one"]}
            ),
            "bad.csv": pd.DataFrame({"Source": ["beta.java"], "Content": ["boom"]}),
        }
        embedder = RecordingEmbedder(fail_for=["boom|"])
        searcher = self.searcher(datasets, embedder=embedder)

        response = self.add(
            searcher, [self.document("good.csv"), self.document("bad.csv")]
        )

        self.assertEqual(
            [status["status"] for status in response["documentStatuses"]],
            ["SUCCESS", "FAILED"],
        )
        self.assertEqual(
            [status["fileName"] for status in response["documentStatuses"]],
            ["good.csv", "bad.csv"],
        )
        failed = response["documentStatuses"][1]
        self.assertEqual(failed["insertedRecords"], 0)
        self.assertEqual(failed["totalRecords"], 1)
        self.assertIn("embedding engine rejected", failed["error"])
        self.assertTrue((self.indexed_files / "alpha.java_dataset.parquet").is_file())
        self.assertTrue((self.indexed_files / "alpha.java_vectors.npy").is_file())
        self.assertFalse((self.indexed_files / "beta.java_dataset.parquet").exists())
        self.assertIn(
            str(self.indexed_files / "alpha.java_dataset.parquet"),
            response["createdDocuments"],
        )
        self.assertNotIn(
            str(self.indexed_files / "beta.java_dataset.parquet"),
            response["createdDocuments"],
        )

    def test_failed_document_rolls_back_in_memory_vectors_and_artifacts(self):
        datasets = {
            "good.csv": pd.DataFrame(
                {"Source": ["alpha.java"], "Content": ["alpha one"]}
            ),
            # two sources: the first embeds via the keyword-free batch path in one
            # call, so make the whole batch fail after nothing was appended, then a
            # keyword-free mixed failure is covered by the load failure below
            "bad.csv": pd.DataFrame({"Source": ["beta.java"], "Content": ["boom"]}),
        }
        embedder = RecordingEmbedder(fail_for=["boom|"])
        searcher = self.searcher(datasets, embedder=embedder)

        self.add(searcher, [self.document("good.csv"), self.document("bad.csv")])

        self.assertEqual(searcher.encoded_vectors.shape, (1, 3))
        self.assertEqual(searcher.vector_dimensions, (1, 3))

    def test_load_failure_reports_failed_with_zero_totals(self):
        datasets = {
            "good.csv": pd.DataFrame(
                {"Source": ["alpha.java"], "Content": ["alpha one"]}
            ),
        }
        searcher = self.searcher(datasets)
        searcher._load_dataset = Mock(
            side_effect=lambda dataset_location: (
                (_ for _ in ()).throw(ValueError("unreadable csv"))
                if Path(dataset_location).name == "bad.csv"
                else datasets["good.csv"].copy()
            )
        )

        response = self.add(
            searcher, [self.document("bad.csv"), self.document("good.csv")]
        )

        self.assertEqual(
            [status["status"] for status in response["documentStatuses"]],
            ["FAILED", "SUCCESS"],
        )
        failed = response["documentStatuses"][0]
        self.assertEqual(failed["totalRecords"], 0)
        self.assertIn("unreadable csv", failed["error"])

    def test_all_documents_succeed(self):
        datasets = {
            "one.csv": pd.DataFrame({"Source": ["a.java"], "Content": ["a"]}),
            "two.csv": pd.DataFrame(
                {"Source": ["b.java", "b.java"], "Content": ["b1", "b2"]}
            ),
        }
        searcher = self.searcher(datasets)

        response = self.add(
            searcher, [self.document("one.csv"), self.document("two.csv")]
        )

        self.assertEqual(
            response["documentStatuses"],
            [
                {
                    "fileName": "one.csv",
                    "status": "SUCCESS",
                    "insertedRecords": 1,
                    "failedRecords": 0,
                    "totalRecords": 1,
                },
                {
                    "fileName": "two.csv",
                    "status": "SUCCESS",
                    "insertedRecords": 2,
                    "failedRecords": 0,
                    "totalRecords": 2,
                },
            ],
        )

    def test_all_documents_fail_without_raising(self):
        datasets = {
            "bad.csv": pd.DataFrame({"Source": ["beta.java"], "Content": ["boom"]}),
        }
        embedder = RecordingEmbedder(fail_for=["boom|"])
        searcher = self.searcher(datasets, embedder=embedder)

        response = self.add(searcher, [self.document("bad.csv")])

        self.assertEqual(
            [status["status"] for status in response["documentStatuses"]], ["FAILED"]
        )
        self.assertEqual(searcher.createMasterFiles.call_count, 1)

    def test_corrupted_artifacts_downgrade_document_status(self):
        datasets = {
            "mixed.csv": pd.DataFrame(
                {
                    "Source": ["alpha.java", "beta.java"],
                    "Content": ["alpha one", "beta one"],
                }
            ),
        }
        corrupted_pair = [
            str(self.indexed_files / "beta.java_dataset.parquet"),
            str(self.indexed_files / "beta.java_vectors.npy"),
        ]
        searcher = self.searcher(datasets, corrupted_file_sets=[corrupted_pair])

        response = self.add(searcher, [self.document("mixed.csv")])

        status = response["documentStatuses"][0]
        self.assertEqual(status["status"], "PARTIAL")
        self.assertEqual(status["insertedRecords"], 1)
        self.assertEqual(status["failedRecords"], 1)
        self.assertIn("failed validation", status["error"])
        for corrupted_path in corrupted_pair:
            self.assertNotIn(corrupted_path, response["createdDocuments"])

    def test_corruption_of_every_source_marks_document_failed(self):
        datasets = {
            "single.csv": pd.DataFrame(
                {"Source": ["alpha.java"], "Content": ["alpha one"]}
            ),
        }
        corrupted_pair = [
            str(self.indexed_files / "alpha.java_dataset.parquet"),
            str(self.indexed_files / "alpha.java_vectors.npy"),
        ]
        searcher = self.searcher(datasets, corrupted_file_sets=[corrupted_pair])

        response = self.add(searcher, [self.document("single.csv")])

        status = response["documentStatuses"][0]
        self.assertEqual(status["status"], "FAILED")
        self.assertEqual(status["insertedRecords"], 0)


if __name__ == "__main__":
    unittest.main()

import unittest

from vector_database.faiss.faiss_client import FAISSSearcher


class FaissRankFusionTests(unittest.TestCase):

    def setUp(self):
        self.searcher = FAISSSearcher.__new__(FAISSSearcher)
        self.vector_results = [
            {"idx": 1, "Score": 0.2},
            {"idx": 2, "Score": 0.4},
        ]
        self.bm25_results = [
            {"idx": 1, "BM25_Score": 2.0},
            {"idx": 3, "BM25_Score": 1.0},
        ]

    def test_rank_fusion_returns_combined_score(self):
        results = self.searcher._rank_fusion(
            self.vector_results, self.bm25_results
        )

        self.assertEqual(1, results[0]["idx"])
        self.assertAlmostEqual(2 / 61, results[0]["RRF_Score"])

    def test_weighted_rank_fusion_returns_combined_score(self):
        results = self.searcher._weighted_rank_fusion(
            self.vector_results,
            self.bm25_results,
            vector_weight=0.3,
            bm25_weight=0.7,
        )

        self.assertEqual(1, results[0]["idx"])
        self.assertAlmostEqual(1 / 61, results[0]["Weighted_RRF_Score"])


if __name__ == "__main__":
    unittest.main()

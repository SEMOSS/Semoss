import bm25s
import os
import pandas as pd
import logging
from typing import List, Tuple, Optional, Dict
import json


class BM25Searcher:
    """
    A self-contained class for BM25 keyword search. This class builds an
    index from a corpus of texts and returns search results as tuples of
    (document_index, score).
    """

    def __init__(self, base_path: str):
        self.class_logger = logging.getLogger(__name__)
        self.bm25_index = None
        self.bm25_corpus = None

        self.bm25_index_path = os.path.join(base_path, "bm25_index")
        self.bm25_corpus_path = os.path.join(base_path, "bm25_corpus.json")

        try:
            import Stemmer

            self.stemmer = Stemmer.Stemmer("english")
        except ImportError:
            self.class_logger.warning(
                "PyStemmer not found. BM25 will not use stemming."
            )
            self.stemmer = None

        self._load_bm25_index()

    def _load_bm25_index(self):
        """Load existing BM25 index and corpus if they exist"""
        try:
            if os.path.exists(self.bm25_index_path) and os.path.exists(
                self.bm25_corpus_path
            ):
                self.bm25_index = bm25s.BM25.load(
                    self.bm25_index_path, load_corpus=True
                )

                with open(self.bm25_corpus_path, "r", encoding="utf-8") as f:
                    self.bm25_corpus = json.load(f)

        except Exception as e:
            self.class_logger.warning(f"Failed to load BM25 index: {e}")
            self.bm25_index = None
            self.bm25_corpus = None

    def _save_bm25_index(self):
        """Save BM25 index and corpus to disk"""
        try:
            if self.bm25_index is not None:
                self.bm25_index.save(self.bm25_index_path, corpus=self.bm25_corpus)

                with open(self.bm25_corpus_path, "w", encoding="utf-8") as f:
                    json.dump(self.bm25_corpus, f, ensure_ascii=False, indent=2)

        except Exception as e:
            self.class_logger.error(f"Failed to save BM25 index: {e}")

    def generate_and_load_bm25_index(self, ds: Optional[pd.DataFrame]):
        """Load existing BM25 index and corpus if they exist"""
        try:
            if (
                not os.path.exists(self.bm25_index_path)
                or not os.path.exists(self.bm25_corpus_path)
            ) and ds is not None:
                self.build_bm25_index(list(ds["Content"]))
            elif os.path.exists(self.bm25_index_path) and os.path.exists(
                self.bm25_corpus_path
            ):
                self._load_bm25_index()

        except Exception as e:
            self.class_logger.warning(f"Failed to generate and load BM25 index: {e}")
            self.bm25_index = None
            self.bm25_corpus = None

    def build_bm25_index(self, texts: List[str]):
        """Build BM25 index from text corpus"""
        try:
            # Coerce to a plain list so downstream JSON serialization works whether the
            # caller passed a list, a pandas Series, or any other iterable of strings.
            texts = list(texts)
            corpus_tokens = bm25s.tokenize(texts, stopwords="en", stemmer=self.stemmer)

            self.bm25_index = bm25s.BM25(method="lucene")
            self.bm25_index.index(corpus_tokens)
            self.bm25_corpus = texts
            self._save_bm25_index()

        except Exception as e:
            self.class_logger.error(f"Failed to build BM25 index: {e}")
            self.bm25_index = None
            self.bm25_corpus = None

    def update_bm25_index(self, new_texts: List[str]):
        """Update BM25 index with new texts"""
        try:
            if self.bm25_corpus is None:
                self.bm25_corpus = []

            all_texts = self.bm25_corpus + new_texts
            self.build_bm25_index(all_texts)

        except Exception as e:
            self.class_logger.error(f"Failed to update BM25 index: {e}")

    def classic_search(self, query: str, top_k: int = 10) -> List[Tuple[int, float]]:
        """
        Perform a BM25 search and return a list of (doc_index, score) tuples.
        """
        if self.bm25_index is None:
            self.class_logger.warning(
                "Cannot search: BM25 index is not built or loaded."
            )
            return []

        try:
            query_tokens = bm25s.tokenize(query, stopwords="en", stemmer=self.stemmer)

            # The retrieve method returns document indices and scores
            doc_indices, scores = self.bm25_index.retrieve(
                query_tokens,
                k=min(
                    top_k, len(self.bm25_corpus)
                ),  # Ensure k is not larger than corpus
            )

            # The results are nested in a list for each query; we only have one query.
            results = list(zip(doc_indices[0], scores[0]))
            results.sort(key=lambda x: x[1], reverse=True)

            return results
        except Exception as e:
            self.class_logger.error(f"BM25 search failed: {e}", exc_info=True)
            return []

    def search_with_data(
        self,
        query: str,
        top_k: int = 10,
        ds: Optional[pd.DataFrame] = None,
        columns_to_return: Optional[List[str]] = None,
    ) -> List[Dict]:
        """Perform BM25 search and return document indices with scores"""
        if self.bm25_index is None or self.bm25_corpus is None:
            return []

        if columns_to_return is None:
            columns_to_return = list(ds.columns)

        try:
            query_tokens = bm25s.tokenize([query], stopwords="en", stemmer=self.stemmer)

            documents, scores = self.bm25_index.retrieve(
                query_tokens, k=min(top_k, len(self.bm25_corpus))
            )

            results = []
            for i, (documents_array, scores_array) in enumerate(zip(documents, scores)):
                for document, score in zip(documents_array, scores_array):
                    doc_idx = document["id"]
                    similarity_score = float(score)

                    if doc_idx != -1:
                        output = {"BM25_Score": similarity_score, "idx": doc_idx}
                        data_row = ds.iloc[doc_idx]
                        output.update({col: data_row[col] for col in columns_to_return})
                        results.append(output)

            results.sort(key=lambda x: x["BM25_Score"], reverse=True)

            return results

        except Exception as e:
            self.class_logger.error(f"BM25 search failed: {e}")
            return []

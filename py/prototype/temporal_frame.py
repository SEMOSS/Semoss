from typing import List, Dict, Any

import numpy as np
from datasets import Dataset, concatenate_datasets
from copy import deepcopy


class FaissFrame:
    """
    A class to manage a temporary frame with FAISS indexing for efficient similarity search of embeddings.

    Attributes:
        ds (`Dataset | None`): The Hugging Face dataset object containing the data and embeddings.
        columns (`List[str]`): List of column names in the dataset.
        vector_length (`int | None`): The length of the embedding vectors.
    """

    EMBEDDINGS_COLUMN = "embedding"

    def __init__(self) -> None:
        self.ds = None
        self.columns = []
        self.vector_length = None

    def add_rows(self, row_list: List) -> None:
        """
        Adds rows to the dataset from a list of dictionaries. Initializes the dataset if it's empty,
        otherwise appends to it. Automatically creates a FAISS index for the embedding column.

        Parameters:
            row_list (`List[Dict[str, Any]]`): List of rows to add, each represented as a dictionary.

        Raises:
            Exception: Propagates any exceptions that occur during the process.
        """
        backup_ds = deepcopy(self.ds)  # create a backup in case an exception occurs
        try:
            if self.ds is None:
                init_dict = {
                    key: [row[key] for row in row_list] for key in row_list[0].keys()
                }
                self.ds = Dataset.from_dict(init_dict)
                self.vector_length = len(self.ds[FaissFrame.EMBEDDINGS_COLUMN][0])
            else:
                for row in row_list:
                    self.ds = self.ds.add_item(row)

            # TODO allow users to define faiss metric type
            self.ds.add_faiss_index(column=FaissFrame.EMBEDDINGS_COLUMN)
        except Exception as e:
            self.ds = backup_ds
            raise e

    def remove_rows(self, indices: List[int]) -> None:
        """
        Removes rows from the dataset based on their indices.

        Parameters:
            indices (List[int]): List of indices of the rows to remove.
        """
        if self.ds is not None:
            self.ds = self.ds.select(
                [i for i in range(len(self.ds)) if i not in indices]
            )

    def search(
        self, search_embeddings: List[float], k: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Searches for the k nearest embeddings in the dataset to the given search embeddings.

        Parameters:
            search_embeddings (List[float]): The embedding vector to search for.
            k (int): The number of nearest neighbors to retrieve.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, each representing a search result with
                                   its score and original data.
        """
        if not self.ds:
            raise ValueError("Dataset is empty or not initialized.")

        scores, retrieved_examples = self.ds.get_nearest_examples(
            FaissFrame.EMBEDDINGS_COLUMN, np.array(search_embeddings), k=k
        )

        search_results = [
            {
                "Score": scores[i],
                **{
                    col: retrieved_examples[col][i] for col in retrieved_examples.keys()
                },
            }
            for i in range(k)
        ]

        return search_results

    def stack_frames(self, other_ds):
        """
        Stacks another dataset with the current one.

        Parameters:
            other_ds (Dataset): Another dataset to stack with the current one.
        """
        if self.ds is None:
            self.ds = other_ds
        else:
            self.ds = concatenate_datasets([self.ds, other_ds])

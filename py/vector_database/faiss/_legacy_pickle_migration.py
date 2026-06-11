"""
One-time migration of legacy pickle (.pkl) index files to safe formats.

Background:
    Older FAISS indexes wrote `dataset.pkl` (HuggingFace Dataset / pandas DataFrame)
    and `vectors.pkl` (numpy ndarray) using `pickle.dump`. `pickle.load` is unsafe
    because it can execute arbitrary code, so the loader path was migrated to use
    Parquet (datasets) and `.npy` (vectors), which are pure-data formats.

    This module is the *only* remaining place in the codebase that calls
    `pickle.load`. It runs once per `base_path`, converts each `.pkl` in place,
    and deletes the original after a successful conversion. After the migration
    completes there is no further pickle exposure at runtime.
"""

import glob
import logging
import os
import pickle  # noqa: S403 - isolated to legacy migration; see module docstring
from typing import Tuple

import numpy as np
import pandas as pd
from datasets import Dataset

_logger = logging.getLogger(__name__)

_VECTOR_PKL_SUFFIXES = ("vectors.pkl",)
_DATASET_PKL_SUFFIXES = ("dataset.pkl",)


def migrate_pickle_to_safe(base_path: str) -> Tuple[int, int]:
    """
    Convert any legacy `.pkl` files under ``base_path`` to safe formats.

    The directory layout produced by FAISSSearcher is::

        base_path/
            dataset.pkl              -> dataset.parquet
            vectors.pkl              -> vectors.npy
            indexed_files/
                <name>_dataset.pkl   -> <name>_dataset.parquet
                <name>_vectors.pkl   -> <name>_vectors.npy

    Returns:
        Tuple ``(converted, failed)`` counting files processed.
    """
    if not base_path or not os.path.isdir(base_path):
        return (0, 0)

    candidates = []
    for name in ("dataset.pkl", "vectors.pkl"):
        path = os.path.join(base_path, name)
        if os.path.exists(path):
            candidates.append(path)

    indexed_files_dir = os.path.join(base_path, "indexed_files")
    if os.path.isdir(indexed_files_dir):
        candidates.extend(glob.glob(os.path.join(indexed_files_dir, "*_dataset.pkl")))
        candidates.extend(glob.glob(os.path.join(indexed_files_dir, "*_vectors.pkl")))

    if not candidates:
        return (0, 0)

    _logger.warning(
        "Legacy pickle index files detected under %s - migrating %d file(s) to safe formats (.parquet/.npy). "
        "This runs once; pickle files will be deleted after successful conversion.",
        base_path,
        len(candidates),
    )

    converted = 0
    failed = 0
    for pkl_path in candidates:
        try:
            if pkl_path.endswith(_VECTOR_PKL_SUFFIXES):
                _convert_vector_pkl(pkl_path)
            elif pkl_path.endswith(_DATASET_PKL_SUFFIXES):
                _convert_dataset_pkl(pkl_path)
            else:
                continue
            os.remove(pkl_path)
            converted += 1
        except Exception:
            failed += 1
            _logger.exception("Failed to migrate legacy pickle file: %s", pkl_path)

    _logger.warning(
        "Pickle migration complete for %s: converted=%d, failed=%d",
        base_path,
        converted,
        failed,
    )
    return (converted, failed)


def _convert_vector_pkl(pkl_path: str) -> None:
    npy_path = pkl_path[: -len(".pkl")] + ".npy"
    if os.path.exists(npy_path):
        return
    with open(pkl_path, "rb") as f:
        vectors = pickle.load(f)  # noqa: S301 - legacy migration only
    if not isinstance(vectors, np.ndarray):
        raise TypeError(
            f"Expected numpy.ndarray in {pkl_path}, got {type(vectors).__name__}"
        )
    np.save(npy_path, vectors, allow_pickle=False)


def _convert_dataset_pkl(pkl_path: str) -> None:
    parquet_path = pkl_path[: -len(".pkl")] + ".parquet"
    if os.path.exists(parquet_path):
        return
    with open(pkl_path, "rb") as f:
        obj = pickle.load(f)  # noqa: S301 - legacy migration only
    if isinstance(obj, Dataset):
        obj.to_parquet(parquet_path)
    elif isinstance(obj, pd.DataFrame):
        obj.to_parquet(parquet_path, index=False)
    else:
        raise TypeError(
            f"Expected Dataset or pandas.DataFrame in {pkl_path}, got {type(obj).__name__}"
        )

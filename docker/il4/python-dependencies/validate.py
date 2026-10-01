"""Offline validation of the locked CPython 3.14 CPU environment."""

import importlib
import importlib.metadata
import json
import platform
import sys
import traceback
from pathlib import Path


MODULES = [
    "numpy", "pandas", "pyarrow", "datasets", "dill", "multiprocess",
    "torch", "torchvision", "torchaudio", "transformers", "sentence_transformers",
    "pipecat", "numba", "onnxruntime", "soxr", "annoy", "pandasql", "swifter",
    "faiss", "gliner", "chonky", "guidance", "langchain", "enchant", "timm",
    "detoxify", "semantic_text_splitter",
]


def main():
    results = []

    def check(name, operation):
        try:
            operation()
            results.append({"check": name, "ok": True})
            print("PASS", name, flush=True)
        except Exception as error:
            results.append({"check": name, "ok": False, "error": repr(error)})
            traceback.print_exc()
            print("FAIL", name, repr(error), flush=True)

    for module in MODULES:
        check("import " + module, lambda module=module: importlib.import_module(module))

    def datasets_roundtrip():
        from datasets import Dataset

        dataset = Dataset.from_dict({"value": [1, 2]})
        assert dataset.map(lambda row: {"value": row["value"] + 1})["value"] == [2, 3]
        assert dataset.to_pandas()["value"].tolist() == [1, 2]
        assert dataset.map(
            lambda row: {"value": row["value"] + 1}, num_proc=2
        )["value"] == [2, 3]

    def cpu_operations():
        import torch
        import torchaudio.functional
        import torchvision.ops
        from annoy import AnnoyIndex

        assert torch.version.cuda is None
        assert not torch.cuda.is_available()
        assert torch.ones(2).sum().item() == 2
        audio = torchaudio.functional.resample(torch.ones(1, 160), 16000, 8000)
        assert audio.shape == (1, 80)
        boxes = torch.tensor([[0.0, 0.0, 1.0, 1.0]])
        assert torchvision.ops.nms(boxes, torch.ones(1), 0.5).tolist() == [0]
        index = AnnoyIndex(2, "euclidean")
        index.add_item(0, [1.0, 0.0])
        index.add_item(1, [0.0, 1.0])
        index.build(2)
        assert index.get_nns_by_item(0, 1) == [0]

    check("datasets Arrow22 create/map/to_pandas/multiprocess", datasets_roundtrip)
    check("CPU torch/audio/vision and Annoy operations", cpu_operations)
    report = {
        "python": platform.python_version(),
        "machine": platform.machine(),
        "packages": {
            distribution.metadata["Name"]: distribution.version
            for distribution in importlib.metadata.distributions()
        },
        "results": results,
    }
    Path("validation.json").write_text(json.dumps(report, indent=2) + "\n")
    return 0 if all(result["ok"] for result in results) else 1


if __name__ == "__main__":
    sys.exit(main())

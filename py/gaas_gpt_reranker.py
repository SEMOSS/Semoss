from transformers import AutoModelForSequenceClassification, AutoTokenizer
import torch

from typing import Optional, Dict, Any


class Reranker:

    def __init__(self, reranker_name="BAAI/bge-reranker-base", **kwargs):
        self.reranker_gaas_model = reranker_name
        self.reranker_tok = AutoTokenizer.from_pretrained(self.reranker_gaas_model)
        self.reranker_model = AutoModelForSequenceClassification.from_pretrained(
            self.reranker_gaas_model
        )

    # send input as an array like this
    # ['hello world', 'goodbye world']

    def model(
        self,
        input: Any,
        insight_id: Optional[str] = None,
        param_dict: Optional[Dict] = None,
        **kwargs
    ):
        tok_args = {
            "padding": True,
            "truncation": True,
            "return_tensors": "pt",
            "max_length": 512,
        }
        pair = input
        if "tok_args" in kwargs:
            tok_args = kwargs.pop("tok_args")
        with torch.no_grad():
            inputs = self.reranker_tok(pair, **tok_args)
            scores = list(
                self.reranker_model(**inputs, return_dict=True)
                .logits.view(
                    -1,
                )
                .float()
                .detach()
                .numpy()
            )
            return scores[0]

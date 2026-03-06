from transformers import AutoModelForSequenceClassification, AutoTokenizer, pipeline


class PromptInjectionClassifier:

  def __init__(self, model_id=None, trust_remote_code=False, use_cuda=None, device=None, **kwargs):
    if model_id is None or str(model_id).strip() == "":
      raise ValueError("model_id is required")

    import torch

    trust_remote_code = bool(trust_remote_code)
    device = self._resolve_device(device=device, use_cuda=use_cuda, torch=torch)

    tokenizer = AutoTokenizer.from_pretrained(model_id, trust_remote_code=trust_remote_code)
    model = AutoModelForSequenceClassification.from_pretrained(
      model_id, trust_remote_code=trust_remote_code, **kwargs
    )
    self.pipe = pipeline(
      "text-classification", model=model, tokenizer=tokenizer, device=device
    )

  def classify(self, text=None, truncation=True, max_length=512):
    if text is None:
      text = ""

    outputs = None
    try:
      outputs = self.pipe(text, truncation=truncation, max_length=max_length, top_k=None)
    except TypeError:
      outputs = self.pipe(text, truncation=truncation, max_length=max_length, return_all_scores=True)

    scores = self._extract_scores(outputs)
    top = None
    for item in scores:
      if top is None or item.get("score", 0) > top.get("score", 0):
        top = item

    if top is None:
      top = {"label": None, "score": None}

    scores_by_label = {}
    for item in scores:
      label = item.get("label")
      score = item.get("score")
      if label is not None and score is not None:
        scores_by_label[label] = float(score)

    return {
      "top_label": top.get("label"),
      "top_score": float(top.get("score")) if top.get("score") is not None else None,
      "scores": scores,
      "scores_by_label": scores_by_label,
    }

  def _resolve_device(self, device=None, use_cuda=None, torch=None):
    if device is not None:
      if isinstance(device, int):
        return device
      if isinstance(device, str):
        device_str = device.strip().lower()
        if device_str in ("cpu", "-1"):
          return -1
        if device_str.isdigit() or (device_str.startswith("-") and device_str[1:].isdigit()):
          return int(device_str)
        if device_str == "cuda":
          return 0 if torch is not None and torch.cuda.is_available() else -1
        if device_str.startswith("cuda:"):
          idx = device_str.split(":", 1)[1]
          if idx.isdigit():
            return int(idx)
          return 0
      return 0 if torch is not None and torch.cuda.is_available() else -1

    if use_cuda is None:
      return 0 if torch is not None and torch.cuda.is_available() else -1

    if bool(use_cuda) and torch is not None and torch.cuda.is_available():
      return 0
    return -1

  def _extract_scores(self, outputs):
    if outputs is None:
      return []

    if isinstance(outputs, list):
      if len(outputs) == 0:
        return []
      # transformers may return:
      # - list[dict] (single example, multiple labels when top_k=None)
      # - list[list[dict]] (single example, multiple labels when return_all_scores=True)
      if isinstance(outputs[0], dict):
        return outputs
      if isinstance(outputs[0], list):
        if len(outputs[0]) == 0:
          return []
        if isinstance(outputs[0][0], dict):
          return outputs[0]
    if isinstance(outputs, dict):
      return [outputs]

    return []

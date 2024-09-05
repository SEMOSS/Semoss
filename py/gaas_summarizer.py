from transformers import AutoTokenizer, AutoModel, AutoModelForSeq2SeqLM, pipeline
import transformers


class Summarizer:
    def __init__(
        self,
        tokenizer_model="google/pegasus-xsum",
        model_model="google/pegasus-xsum",
        tokenizer_loader=transformers.AutoTokenizer,
        model_loader=transformers.AutoModelForSeq2SeqLM,
    ):
        self.tokenizer = tokenizer_loader.from_pretrained(
            tokenizer_model, max_length=512, add_special_tokens=True, truncation=True
        )
        self.model = model_loader.from_pretrained(model_model, max_length=60)
        self.pipe = pipeline(
            "summarization", model=self.model, tokenizer=self.tokenizer
        )

    def summarize(self, text, max_length=150):
        # print(f"Length {len(text)} and max_length : {max_length}")
        text = text[:512]
        if len(text) > max_length * 2:
            return self.pipe(text, max_length=max_length)
        else:
            return text

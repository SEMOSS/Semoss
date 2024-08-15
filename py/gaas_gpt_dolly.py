class Dolly:

    def init(self, llama_model="EleutherAI/gpt-j-6B"):
        from transformers import AutoTokenizer, AutoModelForCausalLM
        from peft import PeftModel
        from transformers import AutoTokenizer, GPTJForCausalLM, GenerationConfig

        self.tokenizer = AutoTokenizer.from_pretrained(llama_model)
        self.tokenizer.pad_token = self.tokenizer.eos_token
        self.tokenizer.pad_token_id = self.tokenizer.eos_token_id

        self.model = GPTJForCausalLM.from_pretrained(
            "EleutherAI/gpt-j-6B",
            load_in_8bit=True,
            device_map="auto",
        )
        self.model = PeftModel.from_pretrained(self.model, "samwit/dolly-lora")
        return self.model, self.tokenizer

    def set_model(self, model=None):
        assert model is not None
        self.model = model

    def set_tokenizer(self, tokenizer=None):
        assert tokenizer is not None
        self.tokenizer = tokenizer

    def ask(self, instruction, model=None, tokenizer=None):
        if tokenizer is None:
            tokenizer = self.tokenizer
        if model is None:
            model = self.model
        if tokenizer == None or model == None:
            print(
                "Please initialize the model and pass model and tokenizer to continue"
            )
            return
        from transformers import AutoTokenizer, GPTJForCausalLM, GenerationConfig

        inputs = tokenizer(
            instruction,
            return_tensors="pt",
        )
        input_ids = inputs["input_ids"].cuda()
        generation_config = GenerationConfig(
            temperature=0.6,
            top_p=0.95,
            repetition_penalty=1.2,
        )

        print("Generating...")
        generation_output = model.generate(
            input_ids=input_ids,
            generation_config=generation_config,
            return_dict_in_generate=True,
            output_scores=True,
            max_new_tokens=128,
            pad_token_id=0,
            eos_token_id=50256,
        )

        for s in generation_output.sequences:
            print(tokenizer.decode(s))

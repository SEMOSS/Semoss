# Adventure - alpaca.init(llama_model="decapoda-research/llama-13b-hf", alpaca_model="samwit/alpaca13B-lora")
# Vicuna lmsys/vicuna-13b-delta-v0
# alpaca.init(llama_model="decapoda-research/llama-13b-hf", alpaca_model="lmsys/vicuna-13b-delta-v0")
# https://github.com/lm-sys/FastChat#vicuna-weights
# alpaca.init(llama_model="decapoda-research/llama-7b-hf", alpaca_model="lmsys/vicuna-7b-delta-v0")
# https://github.com/d8ahazard/sd_dreambooth_extension/issues/7

#!pip install bitsandbytes
#!pip install -q datasets loralib sentencepiece
#!pip install -q git+https://github.com/zphang/transformers@c3dc391
#!pip install -q git+https://github.com/huggingface/peft.git

# import sys
# sys.path.append("c:/users/pkapaleeswaran/workspacej3/semossdev/py")
# import gaas_gpt_alpaca as al
# model, tokenizer = al.init()
# al.evaluate("what is sun?", model=model, tokenizer=tokenizer)

# If we want to load alpaca native - the results are not great.. infact it is worse thatn the default mentioned here
# from transformers import AutoTokenizer, AutoModelForCausalLM
# tokenizer = AutoTokenizer.from_pretrained("chavinlo/gpt4-x-alpaca")
# model = AutoModelForCausalLM.from_pretrained("chavinlo/gpt4-x-alpaca")

# alpaca_model = "C:/Users/pkapaleeswaran/workspacej3/simple-llm-finetuner/lora/decapoda-research_llama-7b-hf_unhelpful2"

# t.generate(prompt, temperature=1.07, repitition_penalty=1.5, num_beams=1,top_p=0.3, top_k=40, do_sample=True, max_new_tokens=80)


class LoraGPT:

    def load_base(self, llama_model="decapoda-research/llama-7b-hf"):
        from peft import PeftModel
        from transformers import (
            LlamaTokenizer,
            LlamaForCausalLM,
            GenerationConfig,
            AutoModelForCausalLM,
        )

        self.tokenizer = LlamaTokenizer.from_pretrained(llama_model)
        self.model = AutoModelForCausalLM.from_pretrained(
            llama_model, load_in_8bit=True, device_map="auto"
        )
        # this for dolly, alpaca 13b
        # model = PeftModel.from_pretrained(model, alpaca_model,device_map={'': 0})
        # model = PeftModel.from_pretrained(model, alpaca_model)
        return self.model, self.tokenizer

    def load_lora(self, model=None, lora=None):
        from peft import PeftModel
        from transformers import (
            LlamaTokenizer,
            LlamaForCausalLM,
            GenerationConfig,
            AutoModelForCausalLM,
        )

        # this for dolly, alpaca 13b
        self.model = PeftModel.from_pretrained(model, lora, device_map={"": 0})
        # model = PeftModel.from_pretrained(model, alpaca_model)
        return self.model

    # this is better.. alpaca_model="plncmm/guanaco-lora-13b" <-- this is qlora
    # decapoda-research/llama-13b-hf
    def init(
        self,
        llama_model="decapoda-research/llama-7b-hf",
        alpaca_model="tloen/alpaca-lora-7b",
    ):
        from peft import PeftModel
        from transformers import (
            LlamaTokenizer,
            LlamaForCausalLM,
            GenerationConfig,
            AutoModelForCausalLM,
        )

        print("Loading Model " + llama_model + " PEFT " + alpaca_model)

        self.tokenizer = LlamaTokenizer.from_pretrained(llama_model)
        self.model = AutoModelForCausalLM.from_pretrained(
            llama_model, load_in_8bit=True, device_map="auto"
        )
        # this for dolly, alpaca 13b
        self.model = PeftModel.from_pretrained(
            self.model, alpaca_model, device_map={"": 0}
        )
        # model = PeftModel.from_pretrained(model, alpaca_model)
        return self.model, self.tokenizer

    def generate_prompt(self, instruction, input=None):
        if input:
            return f"""Below is an instruction that describes a task, paired with an input that provides further context. Write a response that appropriately completes the request.

  ### Instruction:
  {instruction}

  ### Input:
  {input}

  ### Response:"""
        else:
            return f"""Below is an instruction that describes a task. Write a response that appropriately completes the request.

  ### Instruction:
  {instruction}

  ### Response:"""

    # replace this with the iterator logic in gaas_t2t
    def ask(self, instruction, input=None, **kwargs):
        if self.tokenizer == None or self.model == None:
            print(
                "Please initialize the model and pass model and tokenizer to continue"
            )
            return
        from transformers import LlamaTokenizer, LlamaForCausalLM, GenerationConfig

        generation_config = GenerationConfig(
            # temperature=0.92,
            # top_p=0.75,
            # num_beams=1,
            **kwargs
        )

        prompt = self.generate_prompt(instruction, input)
        inputs = self.tokenizer(prompt, return_tensors="pt", **kwargs)
        input_ids = inputs["input_ids"].cuda()
        generation_output = self.model.generate(
            input_ids=input_ids,
            generation_config=generation_config,
            return_dict_in_generate=True,
            output_scores=True,
            # max_new_tokens=80,
        )
        for s in generation_output.sequences:
            output = self.tokenizer.decode(s)
            print("Response:", output.split("### Response:")[1].strip())

            return output

    def ask_raw(self, instruction, input=None, **kwargs):
        if self.tokenizer == None or self.model == None:
            print(
                "Please initialize the model and pass model and tokenizer to continue"
            )
            return
        from transformers import LlamaTokenizer, LlamaForCausalLM, GenerationConfig

        generation_config = GenerationConfig(
            # temperature=0.92,
            # top_p=0.75,
            # num_beams=4,
            **kwargs
        )

        # prompt = generate_prompt(instruction, input)
        prompt = instruction
        inputs = self.tokenizer(prompt, return_tensors="pt", **kwargs)
        input_ids = inputs["input_ids"].cuda()
        generation_output = self.model.generate(
            input_ids=input_ids,
            generation_config=generation_config,
            return_dict_in_generate=True,
            output_scores=True,
            # max_new_tokens=80,
            **kwargs,
        )
        for s in generation_output.sequences:
            output = self.tokenizer.decode(s)
            # print("Response:", output.split("### Response:")[1].strip())

            return output

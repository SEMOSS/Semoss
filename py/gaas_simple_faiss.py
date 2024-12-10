import transformers
from datasets import Dataset, concatenate_datasets
import pandas as pd
import faiss
from sentence_transformers import SentenceTransformer
from transformers import (
    AutoTokenizer,
    AutoModel,
    AutoModelForSeq2SeqLM,
    AutoModelForQuestionAnswering,
    pipeline,
)
import numpy as np
import smssutil

# https://raw.githubusercontent.com/yashprakash13/datasets/master/arxiv_short.csv
# from transformers import DPRContextEncoder, DPRContextEncoderTokenizer
# ctx_encoder = DPRContextEncoder.from_pretrained("facebook/dpr-ctx_encoder-single-nq-base")
# ctx_tokenizer = DPRContextEncoderTokenizer.from_pretrained("facebook/dpr-ctx_encoder-single-nq-base")

# initialize for DPRContextEncoder
#  f2 = fa.FAISSSearcher(df=df, tokenizer_model="facebook/dpr-ctx_encoder-single-nq-base", model_model="facebook/dpr-ctx_encoder-single-nq-base", tokenizer_loader=transformers.DPRContextEncoderTokenizer, model_loader=transformers.DPRContextEncoder, dpr=True)

# initialize for generic
#  f = fa.FAISSSearcher(df=df)


# encoder models
# - sentence-transformers/all-mpnet-base-v2 - Decent - 384 tokens
# multi-qa-mpnet-base-dot-v1 - Trained on 215M not so great.. would not use - also 512 tokens
# - paraphrase-mpnet-base-v2 - 512 tokens
# sentence-transformers/facebook-dpr-ctx_encoder-single-nq-base - 509 tokens
# all-distilroberta-v1 - 512 tokens trained on 1B - Similar to others - Kind of similar to DPR
# "sentence-transformers/multi-qa-mpnet-base-dot-v1"

# text pattern = '\\s\\[1-9]+\\.\\d+[-]?\\d+'


class FAISSSearcher:

    def __init__(
        self,
        df=None,
        ds=None,
        tokenizer_model=None,
        model_model=None,
        tokenizer_loader=transformers.AutoTokenizer,
        model_loader=transformers.AutoModel,
        dpr=False,
    ):
        # if df is None and ds is None:
        #  return "Both dataframe and dataset cannot be none"
        if tokenizer_model is not None:
            self.tokenizer = tokenizer_loader.from_pretrained(tokenizer_model)
        if model_model is not None:
            self.model = model_loader.from_pretrained(model_model)
            if not dpr:
                self.model.to(self.device)
        self.init_device()
        if df is not None:
            self.ds = Dataset.from_pandas(df)
            if "__index_level_0__" in self.ds.column_names:
                self.ds = self.ds.remove_columns("__index_level_0__")
        if ds is not None:
            self.ds = ds
        self.dpr = dpr
        self.faiss_encoder_loaded = False
        self.lfqa_loaded = False
        self.qa_loaded = False
        self.summarizer_loaded = False
        self.encoded_vectors = None
        self.encoder_name = None

    def concatenate_columns(
        self, row, columns_to_index=None, target_column=None, separator="\n"
    ):
        text = ""
        # print(row)
        # print(columns_to_index)
        for col in columns_to_index:
            # print(row)
            text += str(row[col])
            text += separator
        return {target_column: text}

    def cls_pooling(self, model_output):
        return model_output.last_hidden_state[:, 0]

    def get_embeddings(self, text_list):
        # lambda example: {'embeddings': ctx_encoder(**ctx_tokenizer(example["line"], return_tensors="pt"))[0][0].numpy()}
        encoded_input = self.tokenizer(
            text_list, padding=True, truncation=True, return_tensors="pt"
        )
        encoded_input = {k: v.to(self.device) for k, v in encoded_input.items()}
        model_output = self.model(**encoded_input)
        return self.cls_pooling(model_output)

    def get_embeddings_dpr(self, text_list):
        encoded_input = self.tokenizer(
            text_list, padding=True, truncation=True, return_tensors="pt"
        )
        # encoded_input = {k: v for k, v in encoded_input.items()}
        model_output = self.model(**encoded_input)[0][0].numpy()
        return model_output

    def init_device(self):
        import torch

        if torch.cuda.is_available():
            self.device = torch.device("cuda")
            # print("Using GPU.")
        else:
            # print("No GPU available, using the CPU instead.")
            self.device = torch.device("cpu")

    ###############################################################################
    # FAISS
    ###############################################################################

    # the other paraphraser - "paraphrase-mpnet-base-v2"
    # sentence-transformers/facebook-dpr-ctx_encoder-single-nq-base
    def custom_faiss_index(
        self,
        columns_to_index=None,
        target_column="text",
        embedding_column="embeddings",
        encoder_name="paraphrase-mpnet-base-v2",
        separator="\n",
    ):
        # take the columns and concatenate them together
        if columns_to_index is None:
            columns_to_index = list(self.ds.features)
        # concatenate columns
        # print(self.ds)
        self.ds = self.ds.map(
            self.concatenate_columns,
            fn_kwargs={
                "columns_to_index": columns_to_index,
                "target_column": target_column,
                "separator": separator,
            },
        )
        # get the column to index
        self.load_faiss_encoder(encoder_name=encoder_name)
        vectors = self.faiss_encoder.encode(self.ds[target_column])
        self.encoded_vectors = np.copy(vectors)
        vector_dimension = vectors.shape[1]
        self.index = faiss.IndexFlatL2(vector_dimension)
        faiss.normalize_L2(vectors)
        self.index.add(vectors)

    def appendToIndex(
        self, dataObj=None, target_column="text", columns_to_index=None, separator="\n"
    ):
        if columns_to_index is None:
            columns_to_index = list(self.ds.features)

        # previous_encoded_vector = np.load(filePath)
        appendDs = Dataset.from_dict({})
        if isinstance(dataObj, pd.DataFrame):
            appendDs = Dataset.from_pandas(dataObj)
            if "__index_level_0__" in appendDs.column_names:
                appendDs = appendDs.remove_columns("__index_level_0__")
        elif isinstance(dataObj, Dataset):
            appendDs = dataObj
        elif isinstance(dataObj, dict):
            appendDs = Dataset.from_dict(dataObj)
        else:
            raise ValueError(
                "Undefined class check: dataObj is of an unrecognized type"
            )

        appendDs = appendDs.map(
            self.concatenate_columns,
            fn_kwargs={
                "columns_to_index": columns_to_index,
                "target_column": target_column,
                "separator": separator,
            },
        )

        self.load_faiss_encoder()
        new_vector = self.faiss_encoder.encode(appendDs[target_column])

        assert self.encoded_vectors.shape[1] == new_vector.shape[1]

        self.ds = concatenate_datasets([self.ds, appendDs])
        conc_vector = np.concatenate((self.encoded_vectors, new_vector), axis=0)
        vector_dimension = conc_vector.shape[1]
        self.index = faiss.IndexFlatL2(vector_dimension)
        self.encoded_vectors = np.copy(conc_vector)
        faiss.normalize_L2(conc_vector)
        self.index.add(conc_vector)

    def load_faiss_encoder(self, encoder_name="paraphrase-mpnet-base-v2"):
        if not self.faiss_encoder_loaded:
            self.faiss_encoder = SentenceTransformer(encoder_name)
            self.faiss_encoder_loaded = True
            self.encoder_name = encoder_name

    def get_result_faiss(
        self,
        question,
        results=5,
        target_columns=None,
        json=True,
        print_result=False,
        index=None,
        ds=None,
    ):
        if ds is None:
            ds = self.ds
        if target_columns is None:
            target_columns = list(ds.features)
        self.load_faiss_encoder()
        search_vector = self.faiss_encoder.encode(question)
        _vector = np.array([search_vector])
        faiss.normalize_L2(_vector)
        if index is None:
            index = self.index
        distances, ann = index.search(_vector, k=results)
        # print("results.. ")
        samples_df = pd.DataFrame({"distances": distances[0], "ann": ann[0]})
        samples_df.sort_values("distances", ascending=False, inplace=True)
        # print(samples_df)

        final_output = []
        docs = []
        for _, row in samples_df.iterrows():
            output = {}
            output.update({"Score": row["distances"]})
            # print("-"*30)
            # print(row['ann'])
            data_row = ds[int(row["ann"])]
            # print(f"Score : {row['distances']}")
            for col in target_columns:
                if print_result:
                    print(f"{col} : {data_row[col]}")
                output.update({col: data_row[col]})
                docs.append(f"{col}:{data_row[col]}")
            final_output.append(output)
        if json:
            return final_output
        else:
            return docs

    def load_index(self, index_location):
        self.load_faiss_encoder()
        self.index = faiss.read_index(index_location)

    def save_index(self, index_location):
        faiss.write_index(self.index, index_location)

    ###############################################################################
    # Long Form QA
    ###############################################################################

    # DPR works great with LFQA

    def load_lfqa(self, lfqa_model_name="vblagoje/bart_lfqa"):
        if not self.lfqa_loaded:
            self.lfqa_tokenizer = AutoTokenizer.from_pretrained(lfqa_model_name)
            self.lfqa_model = AutoModelForSeq2SeqLM.from_pretrained(lfqa_model_name)
            self.lfqa_loaded = True

    def lfqa(
        self, question, results=5, lfqa_model_name="vblagoje/bart_lfqa", num_returns=1
    ):
        docs = self.get_result_faiss(question, results=results, json=False)
        # print("-o-"*30)
        # print(docs)
        device = self.init_device()
        self.load_lfqa(lfqa_model_name)
        self.lfqa_model = self.lfqa_model.to(device)

        conditioned_doc = "<P> " + " <P> ".join([str(d) for d in docs])
        query_and_docs = "question: {} context: {}".format(question, conditioned_doc)
        model_input = self.lfqa_tokenizer(
            query_and_docs, truncation=True, padding=True, return_tensors="pt"
        )
        generated_answers_encoded = self.lfqa_model.generate(
            input_ids=model_input["input_ids"].to(device),
            attention_mask=model_input["attention_mask"].to(device),
            min_length=64,
            max_length=256,
            do_sample=False,
            early_stopping=True,
            num_beams=8,
            temperature=0.1,
            top_k=None,
            top_p=None,
            eos_token_id=self.lfqa_tokenizer.eos_token_id,
            no_repeat_ngram_size=3,
            num_return_sequences=num_returns,
        )
        output = self.lfqa_tokenizer.batch_decode(
            generated_answers_encoded,
            skip_special_tokens=True,
            clean_up_tokenization_spaces=True,
        )
        print(output)

    ###############################################################################
    # Simple QA
    ###############################################################################
    # trial - sentence-transformers/all-mpnet-base-v2
    # also decent - deepset/deberta-v3-base-squad2

    def load_qa(self, qa_model_name="deepset/roberta-base-squad2"):
        if not self.lfqa_loaded:
            self.qa_tokenizer = AutoTokenizer.from_pretrained(qa_model_name)
            self.qa_model = AutoModelForQuestionAnswering.from_pretrained(qa_model_name)
            self.qa_loaded = True

    # trying to make this stateless
    def qa(
        self,
        question,
        results=5,
        qa_model_name="deepset/roberta-base-squad2",
        summarize=False,
        add_source=True,
        print_result=False,
        pattern=None,
        index=None,
        ds=None,
        stringify=True,
    ):
        if summarize:
            print("Summarization is in experiment.. please do not depend on it fully")
            self.load_summarizer()
        if index is None:
            index = self.index
        if ds is None:
            ds = self.ds
        docs = self.get_result_faiss(
            question,
            results=results,
            json=True,
            print_result=print_result,
            index=index,
            ds=ds,
        )
        self.load_qa(qa_model_name)
        nlp = pipeline(
            "question-answering",
            model=self.qa_model,
            tokenizer=self.qa_tokenizer,
            max_seq_len=200,
        )

        outputs = []
        answers = []
        for doc in docs:
            if print_result:
                print(doc)
            QA_input = {"question": f"{question}", "context": f"{str(doc)}"}
            res = nlp(QA_input)
            if summarize:
                summary = self.summarizer.summarize(str(doc))
                res.update({"summary": summary})
            if add_source:
                res.update({"source": doc})
            if pattern is not None:
                import re

                citations = list(
                    dict.fromkeys(map(str.strip, re.findall(pattern, str(doc))))
                )
                res.update({"citations": citations})
            # add other metadata from the doc
            answers.append(res["answer"])
            # print(res)
            outputs.append(res)
        if stringify:
            outputs = str(outputs)
        print(answers)
        return outputs

    def remove_non_ascii(self, string):
        # Encode the string as ASCII and ignore any characters that can't be represented
        # Decode the resulting bytes back into a string
        string = string.encode("ascii", "ignore").decode("ascii")
        return string

    # trying to make this stateless
    def qaLLM(
        self,
        question,
        results=3,
        print_result=False,
        pattern=None,
        index=None,
        ds=None,
        endpoint=None,
    ):
        if index is None:
            index = self.index
        if ds is None:
            ds = self.ds
        docs = self.get_result_faiss(
            question,
            results=results,
            json=True,
            print_result=print_result,
            index=index,
            ds=ds,
        )
        prompt = self.generate_prompt(docs, question)
        # print(prompt)
        prompt = self.remove_non_ascii(prompt)
        from text_generation import Client

        summaryClient = Client(endpoint)
        summaryClient.timeout = 60
        summary = smssutil.chat_guanaco(
            context=None,
            question=prompt,
            client=summaryClient,
            max_new_tokens=300,
            temperature=0.001,
        )
        returnString = summary["response"].strip()
        return returnString

    def generate_prompt(self, docs, question):
        prompt_template = """Use the following pieces of context to answer the question at the end. If you don't know the answer, just say that you don't know, don't try to make up an answer.

Context: 

{Content}

Question: {Question} """
        prompt = ""
        content = ""
        for doc in docs:
            content += doc["Content"] + "\n\n"
        prompt = prompt_template.format(
            Content=content.lstrip(), Question=question.strip()
        )
        return prompt

    ## update the below to be less restrictive
    #   def generate_prompt(self,docs, question):
    #       prompt_template = """Below is information that a user is asking a question about. Use the information below as context to answer the question presented at the end. As an AI assistant, you must only use the information present in the context to answer and no other outside knowledge.  Carefully determine if the user's question can be answered from the context and with reasoning provide a response to the user's question. If the question is not relevant to the information or cannot be answered from the context, provide reasoning as why the question cannot be answered.

    # CONTEXT :::

    # {Content}

    # QUESTION ::: {Question} """
    #       prompt = ""
    #       content = ""
    #       for doc in docs:
    #           content += doc['Content'] + '\n\n'
    #       prompt = prompt_template.format(Content=content.lstrip(), Question=question.strip())
    #       #print(prompt)
    #       return prompt

    # Need a way to keep the configuration - I dont know how to do multiple files
    # but all in good times
    ###############################################################################
    # Summarizer
    ###############################################################################

    def load_summarizer(self):
        import gaas_summarizer

        if not self.summarizer_loaded:
            self.summarizer = gaas_summarizer.Summarizer()
            self.summarizer_loaded = True

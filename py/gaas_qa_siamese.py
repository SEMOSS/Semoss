def create_model(
    folder_name="random",
    sent_ckpt="msmarco-distilbert-base-v4",
    encoding="iso-8859-1",
    separator="=x=x=x=",
    content_column="Content",
):
    import os

    folder_name = folder_name.replace(os.sep, "/")
    model_file_name = f"{folder_name}/model/siamese.pt"
    import pathlib
    import torch
    from sentence_transformers import SentenceTransformer, util
    from transformers import pipeline

    model_file = pathlib.Path(model_file_name)

    if model_file.exists():
        document_embeddings = torch.load(model_file_name)
        return document_embeddings
    else:
        model_dir = pathlib.Path(model_file.parent)
        print(model_dir)
        if not model_dir.exists():
            os.mkdir(f"{folder_name}/model")

    bi_encoder = SentenceTransformer(sent_ckpt)
    bi_encoder.max_seq_length = 256
    # picks all of the text files here
    # converts it to model
    import glob

    file_list = glob.glob(f"{folder_name}/processed/*.csv")
    import numpy as np
    import pandas as pd

    master_document = []
    all_files = ""
    for i, file in enumerate(file_list):
        # index this file
        # encoding windows encoding windows-1252
        # print("processing file ", file)
        input_file = pathlib.Path(file)
        if i == 0:
            all_files = f"{input_file.stem}"
        else:
            all_files = f"{all_files}, {input_file.stem}"
        csv_file = pd.read_csv(file, encoding=encoding)
        csv_file = csv_file.fillna(value="NA")
        csv_dict = csv_file.to_dict(orient="records")
        csv_dict = convert_pd_to_list(csv_dict, content_column)
        # print(text)
        # separate it by some separator, the default is
        documents = np.array(csv_dict)
        master_document.extend(documents)

    # index all of them
    document_embeddings = bi_encoder.encode(
        master_document, convert_to_tensor=True, show_progress_bar=True
    )
    # write it to the folder
    import torch

    torch.save(document_embeddings, model_file_name)
    return document_embeddings


def hydrate_model(folder_name=None):
    import os

    folder_name = folder_name.replace(os.sep, "/")
    from sentence_transformers import SentenceTransformer, util
    from transformers import pipeline
    import torch

    print("Loading project model")
    model_file_name = f"{folder_name}/model/siamese.pt"
    import pathlib

    model_file = pathlib.Path(model_file_name)
    if model_file.exists():
        model = torch.load(model_file_name)
        print("Loaded Project Model")
        return model
    else:
        return None


def delete_model(folder_name=None):
    import shutil
    import os

    # should I just delete the siamese model
    folder_name = folder_name.replace(os.sep, "/")
    shutil.rmtree(f"{folder_name}/model")


def delete_processed(folder_name=None):
    import shutil
    import os

    folder_name = folder_name.replace(os.sep, "/")
    shutil.rmtree(f"{folder_name}/processed")


def get_master_document(
    folder_name=None, encoding="iso-8859-1", content_column="Content"
):
    import glob

    file_list = glob.glob(f"{folder_name}/processed/*.csv")
    import numpy as np
    import pandas as pd

    master_document = []
    all_files = ""
    import pathlib

    print("Loading Corpus for project")
    for i, file in enumerate(file_list):
        # index this file
        # encoding windows encoding windows-1252
        input_file = pathlib.Path(file)
        if i == 0:
            all_files = f"{input_file.stem}"
        else:
            all_files = f"{all_files}, {input_file.stem}"
        csv_file = pd.read_csv(file, encoding=encoding)
        csv_file = csv_file.fillna(value="NA")

        csv_dict = csv_file.to_dict(orient="records")
        csv_dict = convert_pd_to_list(csv_dict, content_column)
        # print(text)
        # separate it by some separator, the default is
        documents = np.array(csv_dict)
        master_document.extend(documents)
    return master_document


# Moose ( command = "docqa:what is firm fixed price ?" , filePath = "qa_models" , model = "siamese" ) ;
# Moose ( command = "docqa:what is firm fixed price ?" , filePath = "qa_models" , model = "siamese" , project = "9f78e44f-64cc-456a-925f-b5062783315f" ) ;
def search(
    folder_name=None,
    sent_ckpt="msmarco-distilbert-base-v4",
    qa_ckpt="deepset/roberta-base-squad2",
    encoding="windows-1252",
    separator="=x=x=x=",
    model=None,
    query=None,
    threshold=0.2,
    result_count=3,
    source=False,
    master_document=None,
):
    import os

    folder_name = folder_name.replace(os.sep, "/")
    from sentence_transformers import SentenceTransformer, util
    from transformers import pipeline
    import torch

    print("Starting Search..")
    if model is None:
        return []
    print("Loading Sentence Model")
    bi_encoder = SentenceTransformer(sent_ckpt)
    bi_encoder.max_seq_length = 256
    # picks all of the text files here
    # converts it to model
    print("Indexing Document")
    if master_document is None:
        master_document = get_master_document(folder_name)
    # index all of them
    # write it to the folder
    print("Loading Document Model for Query")
    nlp = pipeline(
        "question-answering", model=qa_ckpt, tokenizer=qa_ckpt, max_length=10
    )
    question_embedding = bi_encoder.encode(query, convert_to_tensor=True)
    print("Performing Search.. ")
    hits = util.semantic_search(question_embedding, model, top_k=result_count)[0]
    return_data = []
    # print(hits)
    # print(type(hits))
    # print(len(master_document))
    print("Formatting Result.. ")
    for i, hit in enumerate(hits):
        # print(f'Document {i+1} Cos Sim {hit["score"]:.3f}:\n\r {documents[hit["corpus_id"]]}')
        item = {}
        # print(hit)
        if hit["score"] > threshold:
            nlp_output = nlp(query, str(master_document[hit["corpus_id"]]))
            fd = master_document[hit["corpus_id"]]
            if source:
                item.update({"full_document": fd["content"]})
            item.update({"meta": fd["meta"]})
            item.update({"source_document_id": hit["corpus_id"]})
            item.update({"start": nlp_output["start"]})
            item.update({"end": nlp_output["end"]})
            item.update({"answer": nlp_output["answer"]})
            item.update({"score": nlp_output["score"]})
            return_data.append(item)

    return return_data


def convert_pd_to_list(incoming_dict, content_column_name):
    doc_list = []
    for i in range(0, len(incoming_dict) - 1):
        new_data = {}
        content = incoming_dict[i].pop(content_column_name)
        new_data.update({"content": content})
        new_data.update({"meta": incoming_dict[i]})
        doc_list.append(new_data)
    return doc_list

### haystack
##
#!pip install transformers
#!pip install farm-haystack
#!pip install farm-haystack[faiss]
#
# ONLY FOR COLAB - !pip install Ipython --upgrade
# %load_ext autoreload
# %autoreload 2


def create_model(
    folder_name="random",
    sent_ckpt="msmarco-distilbert-base-v4",
    encoding="iso-8859-1",
    separator="=x=x=x=",
    content_column="Content",
):
    model_file_name = f"{folder_name}/model/haystack.db"
    import pathlib

    model_file = pathlib.Path(model_file_name)
    print("loaded document store")
    from haystack.document_stores import SQLDocumentStore

    if model_file.exists():
        # load and return it
        # document_store = SQLDocumentStore(f"sqlite:///{model_file_name}")
        return hydrate_model(folder_name=folder_name)
    else:
        import os

        model_dir = pathlib.Path(model_file.parent)
        # print(model_dir)
        if not model_dir.exists():
            os.mkdir(f"{folder_name}/model")

    # create the document store
    document_store = SQLDocumentStore(f"sqlite:///{model_file_name}")
    # if not do the indexing and such
    from haystack.utils import convert_files_to_docs

    # this assumes all of these are text files
    # if not this will lead to other issues for now
    # this needs to be more file based
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
        import pandas as pd

        csv_file = pd.read_csv(file, encoding=encoding)
        csv_dict = csv_file.to_dict(orient="records")
        csv_dict = convert_pd_to_list(csv_dict, content_column)
        master_document.extend(csv_dict)
    document_store.write_documents(master_document)
    from haystack.pipelines.standard_pipelines import TextIndexingPipeline

    ip = TextIndexingPipeline(document_store)
    return hydrate_model(folder_name=folder_name)


def hydrate_model(folder_name=None, qa_model_ckpt="deepset/roberta-base-squad2"):
    model_file_name = f"{folder_name}/model/haystack.db"
    import pathlib

    model_file = pathlib.Path(model_file_name)
    if model_file.exists():
        from haystack.document_stores import SQLDocumentStore

        document_store = SQLDocumentStore(f"sqlite:///{model_file_name}")
        # model=hydrate_model(document_store=document_store)
        from haystack.nodes import BM25Retriever, EmbeddingRetriever, TfidfRetriever

        retriever = TfidfRetriever(document_store=document_store)
        from haystack.nodes import FARMReader

        reader = FARMReader(model_name_or_path=qa_model_ckpt, use_gpu=True)
        from haystack.pipelines import ExtractiveQAPipeline, Pipeline

        pipe = ExtractiveQAPipeline(reader, retriever)
        return pipe
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
    if model is None:
        # try to hydrate the model
        model = hydrate_model(folder_name)
        if model is None:
            return []  # giving you an empty set

    # all set do the work of predicting
    prediction = model.run(
        query=query, params={"Retriever": {"top_k": 3}, "Reader": {"top_k": 3}}
    )
    # need to compose this back into return data
    return_data = []
    for ans in prediction["answers"]:
        item = {}
        if source:
            item.update({"full_document": ans.context})
        # print(ans.meta)
        # corpus = f"{ans.meta['Source']}->{ans.meta['Page']}"
        # item.update({"source_document_id": corpus})
        item.update({"meta": ans.meta})
        item.update({"answer": ans.answer})
        item.update({"start": ans.offsets_in_document[0].start})
        item.update({"end": ans.offsets_in_document[0].end})
        item.update({"score": ans.score})
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


def get_master_document(
    folder_name=None, encoding="iso-8859-1", content_column="Content"
):  # dummy call
    return 1

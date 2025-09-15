import logging
import ast
import json
import datetime
import os
import time
from typing import Optional

logger = logging.getLogger("SocketServer")

# callback link
executorExceptionCallback = None


def setExecutorExceptionCallback(callback):
    global executorExceptionCallback
    executorExceptionCallback = callback


# custom exception class to be used with callback


class InterpreterError(Exception):
    pass


# all of the util functions go here


def test_file_encoding(file_path):
    """
    Detects the encoding of a file using chardet by reading the first 4096 bytes.

    Args:
        file_path (str): The path to the file.

    Returns:
        str: The detected encoding, or None if detection fails.
    """
    import chardet

    try:
        with open(file_path, "rb") as f:
            raw_data = f.read(-1)
            if not raw_data:
                return None  # Handle empty file case
            result = chardet.detect(raw_data)
            return result["encoding"]
    except:
        import ntpath

        logger.info(
            "Unable to determine the encoding type for file "
            + ntpath.basename(file_path)
        )
        return None


def getfunctions(file):
    import inspect

    print("Loading file", file)
    obj1 = loadScript("rand", file)
    members = [obj for obj in dir(obj1) if not obj.startswith("__")]
    return members


def loadScript(module_name, file):
    import importlib.util

    spec = importlib.util.spec_from_file_location(module_name, file)
    loader = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loader)
    return loader


def findlibraries(file):
    loadScript("random", file)
    from modulefinder import ModuleFinder

    finder = ModuleFinder()
    finder.run_script(file)
    return finder.modules.keys()


def getalllibraries():
    import pkg_resources

    dists = [str(d).replace(" ", "==") for d in pkg_resources.working_set]
    k = []
    for item in dists:
        keyval = item.split("==")
        k.append(keyval[0])
    return k


def getalllibraries2():
    import sys

    dists = sys.modules.keys()
    k = []
    for item in dists:
        keyval = item.split("==")
        k.append(keyval[0])
    return k


def findlibraries2(file):
    import findimports

    output = findimports.find_imports(file)
    k = []
    for item in output:
        k.append(item.name.split(".")[0])
    return k


def canLoad(file):
    liblist = findlibraries2(file)
    alllist = getalllibraries2()
    import numpy as np

    finalList = list(set(liblist) - set(alllist))

    return finalList


def runwrapper(file, output, error, g):
    import contextlib
    import sys
    import traceback

    global executorExceptionCallback

    foundErr = None
    with (
        open(output, "w", buffering=1) as ofile,
        open(error, "w", buffering=1) as efile,
        contextlib.redirect_stdout(ofile),
        contextlib.redirect_stderr(ofile),
        open(file, "r") as datafile,
    ):
        try:
            exec(datafile.read(), g)
        except SyntaxError as err:
            foundErr = err
            error_class = err.__class__.__name__
            detail = err.args[0]
            line_number = err.lineno
        except Exception as err:
            foundErr = err
            error_class = err.__class__.__name__
            detail = err.args[0]
            cl, exc, tb = sys.exc_info()
            line_number = traceback.extract_tb(tb)[-1][1]
        else:
            return

        if foundErr is not None:
            errorMessage = "%s at line %d of source string: %s" % (
                error_class,
                line_number,
                detail,
            )
            logger.error(errorMessage)
            if executorExceptionCallback is not None:
                executorExceptionCallback.throwPython(errorMessage, foundErr)
            else:
                raise InterpreterError(errorMessage)


def runwrapper_semoss_console(command, output, error, g):
    import contextlib

    ofile = output
    efile = error
    with contextlib.redirect_stdout(ofile), contextlib.redirect_stderr(ofile):
        # datafile = open(file, "r")
        # print(f'found the trigger {jout}')
        try:
            exec(command, g)
        except Exception as e:
            logger.error(e)
    ofile.close()
    efile.close()


def runwrappereval(file, output, error, g):
    global executorExceptionCallback
    import contextlib
    import sys
    import traceback

    foundErr = None
    with (
        open(output, "w", buffering=1) as ofile,
        open(error, "w", buffering=1) as efile,
        contextlib.redirect_stdout(ofile),
        contextlib.redirect_stderr(ofile),
        open(file, "r") as datafile,
    ):
        command = datafile.read()
        try:
            output_obj = eval(command, g)
            if output_obj is not None:
                print(output_obj)
        except Exception as e:
            try:
                exec(command, g)
            except SyntaxError as err:
                foundErr = err
                error_class = err.__class__.__name__
                detail = err.args[0]
                line_number = err.lineno
            except Exception as err:
                foundErr = err
                error_class = err.__class__.__name__
                detail = err.args[0]
                cl, exc, tb = sys.exc_info()
                line_number = traceback.extract_tb(tb)[-1][1]
            else:
                return

            if foundErr is not None:
                print(foundErr)
                errorMessage = "%s at line %d of source string: %s" % (
                    error_class,
                    line_number,
                    detail,
                )
                logger.error(errorMessage)
                if executorExceptionCallback is not None:
                    executorExceptionCallback.throwPython(errorMessage, foundErr)
                else:
                    raise InterpreterError(errorMessage)


def runwrappereval_semoss_console(command, output, error, g):
    import contextlib
    import io
    import sys
    import os

    ofile = output
    efile = error
    with contextlib.redirect_stdout(ofile), contextlib.redirect_stderr(ofile):
        # datafile = open(file, "r")
        # command = datafile.read()
        try:
            output_obj = eval(command, g)
            if output_obj is not None:
                print(output_obj)
                return output_obj
        except Exception as e:
            try:
                exec(command, g)
            except Exception as e:
                logger.error(e)
    ofile.close()
    efile.close()


# same as run wrapper eval but will also return the output instead of printing it and
# will not exec it
# since I need the return value
# - Updating the progress bar - https://stackoverflow.com/questions/45808140/using-tqdm-progress-bar-in-a-while-loop


def runwrappereval_return(command, output, error, g):
    global executorExceptionCallback
    import contextlib
    import sys
    import traceback

    foundErr = None
    with (
        open(output, "w", buffering=1) as ofile,
        open(error, "w", buffering=1) as efile,
        contextlib.redirect_stdout(ofile),
        contextlib.redirect_stderr(ofile),
    ):
        from tqdm import tqdm

        with tqdm(total=100) as pbar:
            pbar.update(10)
            try:
                pbar.update(20)
                output_obj = eval(command, g)
                pbar.update(50)
                if output_obj is not None:
                    pbar.update(10)
                    print(output_obj)
                    return output_obj
            except SyntaxError as err:
                foundErr = err
                error_class = err.__class__.__name__
                detail = err.args[0]
                line_number = err.lineno
            except Exception as err:
                foundErr = err
                error_class = err.__class__.__name__
                detail = err.args[0]
                cl, exc, tb = sys.exc_info()
                line_number = traceback.extract_tb(tb)[-1][1]

                # if we didn't hit the above return
                # then there was definitely an error
                pbar.update(10)
                if foundErr is not None:
                    print(foundErr)
                    errorMessage = "%s at line %d of source string: %s" % (
                        error_class,
                        line_number,
                        detail,
                    )
                    logger.error(errorMessage)
                    if executorExceptionCallback is not None:
                        executorExceptionCallback.throwPython(errorMessage, foundErr)
                    else:
                        raise InterpreterError(errorMessage)

                return None


# used by empty py direct
def run_empty_wrapper(file, g):
    global executorExceptionCallback
    import sys
    import traceback

    foundErr = None
    with open(file) as f:
        try:
            exec(f.read(), g)
        except SyntaxError as err:
            foundErr = err
            error_class = err.__class__.__name__
            detail = err.args[0]
            line_number = err.lineno
        except Exception as err:
            foundErr = err
            error_class = err.__class__.__name__
            detail = err.args[0]
            cl, exc, tb = sys.exc_info()
            line_number = traceback.extract_tb(tb)[-1][1]
        else:
            return

        if foundErr is not None:
            print(foundErr)
            errorMessage = "%s at line %d of source string: %s" % (
                error_class,
                line_number,
                detail,
            )
            logger.error(errorMessage)
            if executorExceptionCallback is not None:
                executorExceptionCallback.throwPython(errorMessage, foundErr)
            else:
                raise InterpreterError(errorMessage)


# Attribution = https://github.com/bosswissam/pysize/blob/master/pysize.py
# this thing is so slow, I am not sure it would even come back
def get_size(obj, seen=None):
    import sys
    import inspect

    """Recursively finds size of objects in bytes"""
    size = sys.getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    # Important mark as seen *before* entering recursion to gracefully handle
    # self-referential objects
    seen.add(obj_id)
    if hasattr(obj, "__dict__"):
        for cls in obj.__class__.__mro__:
            if "__dict__" in cls.__dict__:
                d = cls.__dict__["__dict__"]
                if inspect.isgetsetdescriptor(d) or inspect.ismemberdescriptor(d):
                    size += get_size(obj.__dict__, seen)
                break
    if isinstance(obj, dict):
        size += sum((get_size(v, seen) for v in obj.values()))
        size += sum((get_size(k, seen) for k in obj.keys()))
    elif hasattr(obj, "__iter__") and not isinstance(obj, (str, bytes, bytearray)):
        size += sum((get_size(i, seen) for i in obj))

    if hasattr(obj, "__slots__"):  # can have __slots__ with __dict__
        size += sum(
            get_size(getattr(obj, s), seen) for s in obj.__slots__ if hasattr(obj, s)
        )

    return size


def install_py(packageName):
    from pip._internal import main as pipmain

    pipmain(["install", packageName])


def load_hugging_face_model(modelName, typeOfModel, cacheFolder):
    from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
    import torch

    tokenizer = AutoTokenizer.from_pretrained(modelName)
    model = AutoModelForSeq2SeqLM.from_pretrained(modelName, cache_dir=cacheFolder)
    cuda = torch.cuda.is_available()
    if cuda:
        print("loading on cuda")
        from transformers import pipeline

        device = torch.device("cuda")
        model = model.to(device)
        pipe = pipeline(
            "text2text-generation", model=model, tokenizer=tokenizer, device=0
        )
        return pipe
    else:
        # need to check for kuda
        print("loading on non cuda")
        from transformers import pipeline

        pipe = pipeline(typeOfModel, model=model, tokenizer=tokenizer)
    return pipe


def get_function_signature(func_name):
    from inspect import signature
    from enum import Enum
    import types

    dict = signature(func_name).parameters.copy()
    keys = list(dict.keys())
    finalList = {}

    # add the current name as function name
    finalList.update({"function_name": func_name.__name__})
    # first elementis the type
    # second element is the default value
    # third item is if this is optional
    # if there is a default value it is optional
    value = []
    for item in keys:
        key = dict[item].name
        param_type = dict[item].annotation
        # print(item)
        thisValue = dict[item].default

        # first figure out the types
        # handle the enumeration
        processed = False

        if param_type.__class__ == Enum.__class__:
            dropdown = {}
            inner_dict_list = list(dict[item].annotation.__members__.copy().values())
            for i in inner_dict_list:
                dropdown.update({i.name: i.value})
            value.append("PixelDataType.Multi")
            value.append(dropdown)
            value.append("False")
            finalList.update({key: value})
            processed = True
            # print(f"handled as enum {key}: {value} {~processed}")
            value = []

        # handle function
        if param_type.__class__ == types.FunctionType:
            # print("handled as function")
            value.append("PixelDataType.Function")
            value.append(get_function_signature(param_type))
            value.append("False")
            finalList.update({key: value})
            processed = True
            value = []

        if param_type == dict[item].empty and not processed:
            # print("param type is empty")
            # impute from the default value if you can
            # if it is empty - that is a string value no default
            # if it is none it is a int value no default
            # else this is optional
            # also check to see if the type is already there
            if thisValue == dict[item].empty:
                # need to check the pixel data type
                value.append("PixelDataType.Str")
                value.append("")
                value.append("False")
                processed = True
            else:
                if thisValue == None:
                    value.append("PixelDataType.Int")
                    value.append("")
                    value.append("False")
                else:
                    # check to see if this starts with quotes
                    if type(thisValue) == (bool):
                        value.append("PixelDataType.Boolean")
                    else:
                        if isinstance(thisValue, (int, float)):
                            value.append("PixelDataType.bool")
                        else:
                            if ~processed:
                                # turn everything else into string
                                # if(isinstance(thisValue, (str))):
                                value.append("PixelDataType.Str")
                    value.append(str(thisValue))
                    value.append("True")
            finalList.update({key: value})
            value = []
        else:
            if not processed:
                # use the param type to fill the data
                if param_type == int:
                    value.append("PixelDataType.Int")
                    # check again to see if it is empty
                    if thisValue == dict[item].empty:
                        value.append("")
                        value.append("False")
                    else:
                        value.append(thisValue)
                        value.append("True")
                else:
                    if param_type == bool:
                        value.append("PixelDataType.Boolean")
                        if thisValue == dict[item].empty:
                            value.append("")
                            value.append("False")
                        else:
                            value.append(thisValue)
                            value.append("True")
                    else:
                        # Everything else is stringif(param_type == bool):
                        value.append("PixelDataType.Str")
                        if thisValue == dict[item].empty:
                            value.append("")
                            value.append("False")
                        else:
                            value.append(thisValue)
                            value.append("True")
                finalList.update({key: value})
                value = []

    # last item is return
    key = "return_value"
    returns = func_name.__annotations__
    ret_type = "unknown"
    if "return" in returns:
        ret_type = returns["return"].__name__
    value.append(f"PixelDataType.{ret_type}")
    value.append(ret_type)
    value.append("NA")
    finalList.update({key: value})
    return finalList


def run_gpt_3(nl_query, max_tokens_value):
    # import os
    from openai import OpenAI

    client = OpenAI()
    response = client.completions.create(
        model="code-davinci-002",
        prompt=nl_query,
        temperature=0,
        max_tokens=max_tokens_value,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0,
        stop=["#", ";"],
    )
    query = " SELECT " + response.choices[0].text
    print(query)
    return query


def chat_gpt_3(nl_query, max_tokens_value):
    # import os
    from openai import OpenAI

    client = OpenAI()
    response = client.completions.create(
        model="code-davinci-002",
        prompt=nl_query,
        temperature=0,
        max_tokens=max_tokens_value,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0,
        stop=["#", ";"],
    )
    query = " SELECT " + response.choices[0].text
    print(query)
    return query


def run_alpaca(nl_query, max_tokens_value, api_base, model_name="alpaca-13b-lora-int4"):
    # import os
    from openai import OpenAI

    client = OpenAI(api_key="Non Existent API Key", base_url=api_base)
    # response = client.completions.create(model="alpaca-30b-lora", prompt=nl_query, temperature=0, max_tokens=max_tokens_value, top_p=1, frequency_penalty=0, presence_penalty=0,stop=["#", ";"])
    response = client.completions.create(
        model=model_name,
        prompt=nl_query,
        temperature=0,
        max_tokens=max_tokens_value,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0,
        stop=["#", ";"],
    )
    # response = client.completions.create(model="alpaca-lora-7b", prompt=nl_query, temperature=0, max_tokens=max_tokens_value, top_p=1, frequency_penalty=0, presence_penalty=0,stop=["#", ";"])
    query = response.choices[0].text
    print(query)
    return query


def run_guanaco(
    nl_query,
    max_tokens_value,
    api_base,
    stop_sequences=["#", ";"],
    temperature_val=0.01,
    top_p_val=0.5,
    **kwargs,
):
    from text_generation import Client

    client = Client(api_base)
    client.timeout = 60
    text = ""
    # for response in client.generate_stream(compose_prompt(context, question), max_new_tokens=max_new_tokens, temperature=0.2,top_p=0.5):
    for response in client.generate_stream(
        compose_sql_prompt(nl_query),
        temperature=temperature_val,
        top_p=top_p_val,
        max_new_tokens=max_tokens_value,
        stop_sequences=stop_sequences,
        **kwargs,
    ):
        if not response.token.special:
            text += response.token.text
    text = text.replace("\r", " ").replace("\n", " ").replace("`", "")
    print(text)
    return text


def chat_alpaca(
    context, nl_query, max_tokens_value, api_base, model_name="guanaco-33b", long=False
):
    # import os
    from openai import OpenAI

    client = OpenAI(api_key="Non Existent API Key", base_url=api_base)

    query = ""
    if context is None:
        context = ""
    if not long:
        query = f"Below is an instruction that describes a task. Write a response that appropriately completes the request.\n\n### Instruction: {nl_query}\n\n### Response:"
    else:
        query = f"A chat between a curious human and an artificial intelligence assistant. The assistant gives helpful, detailed, and polite answers to the user's questions. Based on the following paragraphs, answer the human's question:\n\n{context}.\n\n### Questions:\n\n{nl_query}\n\n### Response:"
    print(query)

    # response = client.completions.create(model="alpaca-30b-lora", prompt=nl_query, temperature=0, max_tokens=max_tokens_value, top_p=1, frequency_penalty=0, presence_penalty=0,stop=["#", ";"])
    response = client.completions.create(
        model=model_name,
        prompt=query,
        temperature=0,
        max_tokens=max_tokens_value,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0,
        stop=["#", ";"],
    )
    # response = client.completions.create(model="alpaca-lora-7b", prompt=nl_query, temperature=0, max_tokens=max_tokens_value, top_p=1, frequency_penalty=0, presence_penalty=0,stop=["#", ";"])
    query = response.choices[0].text
    print(query)
    return query


def compose_sql_prompt(question=None):
    assert question is not None
    query = f"A chat between a curious human and an artificial intelligence assistant. The assistant gives helpful, detailed, and polite answers to the user's questions. Based on the table and columns defined below, create a sql statement that answers the human's question:### Question:\n\n{question}\n\n### SQL:"
    print(query)
    return query


def compose_prompt(context=None, question=None):
    assert question is not None
    if context is None:
        query = f"Below is an instruction that describes a task. Write a response that appropriately completes the request.\n\n### Instruction: {question}\n\n### Response:"
    else:
        query = f"A chat between a curious human and an artificial intelligence assistant. The assistant gives helpful, detailed, and polite answers to the user's questions. Based on the following paragraphs, answer the human's question:\n\n{context}.\n\n### Questions:\n\n{question}\n\n### Response:"
    # print(query)
    return query


def compose_prompt_wizard(context=None, question=None):
    assert question is not None
    if context is None:
        query = f"Below is an instruction that describes a task. Write a response that appropriately completes the request.\n\n### Instruction: {question}\n\n### Response:"
    else:
        query = f"A chat between a curious human and an artificial intelligence assistant. The assistant gives helpful, detailed, and polite answers to the user's questions. USER: {context}. {question} ? ASSISTANT:"
    # print(query)
    return query


def compose_prompt_qa(context=None, question=None):
    prompt = f"Context information is below. \n---------------------\n{context}\n---------------------\nGiven the context information and not prior knowledge, answer the question: {question}\n"
    return prompt


def chat_guanaco(
    context=None,
    question=None,
    client=None,
    max_new_tokens=200,
    prev_response=None,
    stop_sequences=["###", "#", ";"],
    **kwargs,
):
    if context == "":
        context = None
    text = ""
    finish_reason = ""
    input_text = compose_prompt(context, question)
    final_output = {}
    if prev_response is not None:
        input_text = f"{input_text} {prev_response}"
    # for response in client.generate_stream(compose_prompt(context, question), max_new_tokens=max_new_tokens, temperature=0.2,top_p=0.5):
    for response in client.generate_stream(
        compose_prompt(context, question),
        max_new_tokens=max_new_tokens,
        stop_sequences=stop_sequences,
        **kwargs,
    ):
        # for response in client.generate_stream(compose_prompt_qa(context, question), max_new_tokens=max_new_tokens, stop_sequences=stop_sequences, **kwargs):
        if not response.token.special:
            text += response.token.text
        if response.details is not None:
            detail = response.details
            from text_generation.types import FinishReason

            # print(f"Finished with {detail.finish_reason}")
            if (
                detail.finish_reason != "stop_sequence"
                and detail.finish_reason != "eos_token"
            ):
                finish_reason = f"... <Unable to complete request, please try by increasing token size from {max_new_tokens}>"
                final_output.update({"meta": finish_reason})
    # print(client.generated_stream.finish_reason)
    # print(f"{text}")
    final_output.update({"response": f"{text}"})
    return final_output


def chat_guanaco_code(
    context=None,
    question=None,
    client=None,
    prev_response=None,
    max_new_tokens=100,
    stop_sequences=["###", ";"],
    incremental=False,
    **kwargs,
):
    text = ""
    # code starts with ``` and ends with ```
    if context == "":
        context = None
    input_text = compose_prompt(context, question)
    if prev_response is not None:
        input_text = f"{input_text} {prev_response}"
    # print(input_text)
    response_available = False
    str_start = -1
    str_end = -1
    cur_new_tokens = 100
    total_tokens = 100
    response_so_far = ""
    final_answer = ""
    finish_reason = ""
    final_output = {}
    while not response_available and total_tokens < max_new_tokens:
        # for response in client.generate_stream(compose_prompt(context, question), max_new_tokens=max_new_tokens, temperature=0.2,top_p=0.5):
        # print("running while loop")
        for response in client.generate_stream(
            input_text,
            max_new_tokens=cur_new_tokens,
            stop_sequences=stop_sequences,
            **kwargs,
        ):
            # for response in client.generate_stream(compose_prompt_qa(context, question), max_new_tokens=max_new_tokens, stop_sequences=stop_sequences, **kwargs):
            # print("in for loop")
            if not response.token.special:
                text += response.token.text

        # if(len(response_so_far) == 0):
        # print(f"text is >>>> {text} <<<<<")
        # response_so_far = text

        if str_start < 0:
            str_start = text.find("```")

        if str_start > 0 and str_end < 0:
            str_end = text.find("```", str_start + 3)

        if str_start > 0 and str_end > 0:
            response_available = True
            # print("found both start and end")
            final_answer = text[str_start + 3 : str_end]
            final_output.update({"response": f"{final_answer}"})
            # print(f" Response so far {response_so_far}")
        else:
            input_text = f"{compose_prompt(context, question)} {text}"
            total_tokens = total_tokens + 100
            if incremental:
                print(f"{text}")
            # print(f" Response so far {response_so_far}")
            # print(f"{input_text}")

        # print(f"Start : {str_start}, End : {str_end}")
        # print(f"Response available ? {response_available}")
    if str_start < 0:
        final_answer = text
        finish_reason = ".. <Unable to complete request>"
        final_output.update({"meta": finish_reason})
        final_output.update({"response": f"{final_answer}"})
    elif str_start > 0 and str_end < 0:
        final_answer = text
        finish_reason = "... <Unable to get the full data. Please try by increasing max_tokens or resubmit the request>"
        final_output.update({"meta": finish_reason})
        final_output.update({"response": f"{final_answer}"})
    print(f"{final_answer}")
    # print(f"{text}")
    return final_output


def convert_pdf_to_text(document_location):
    import PyPDF2
    import pathlib
    import os

    inputFile = pathlib.Path(document_location)
    if not inputFile.exists():
        return "No Such File"
    if not inputFile.suffix == ".pdf":
        return "Not a PDF File, unable to process"

    parentFolder = str(inputFile.parent)
    outputLocation = f"{inputFile.parent}/{inputFile.stem}.txt"
    # check to see if this file is there
    # and if the date is after the current files date
    outputFile = pathlib.Path(outputLocation)
    if outputFile.exists() and os.path.getmtime(outputFile) > os.path.getmtime(
        inputFile
    ):
        return True

    pdfFileObj = open(inputFile, "rb")
    pdfReader = PyPDF2.PdfFileReader(pdfFileObj)
    outputFile = open(outputFile, "w")
    for i in range(pdfReader.numPages):
        prefix = f"{inputFile.stem}::Page={i}::"
        page_text = pdfReader.getPage(i).extractText()
        one_line = f"{prefix}{page_text}"
        print(one_line)
        outputFile.write(one_line)
        outputFile.write("\r\n\r\n")
        outputFile.flush()
    return True


def parse_sentence(text):
    from openie import StanfordOpenIE

    # https://stanfordnlp.github.io/CoreNLP/openie.html#api
    # Default value of openie.affinity_probability_cap was 1/3.
    properties = {
        "openie.affinity_probability_cap": 2 / 3,
    }
    client = StanfordOpenIE(properties=properties)
    return client.annotate(text)


def parse_paragraph(para):
    from nltk.tokenize import sent_tokenize

    sentences = sent_tokenize(para)
    all_triples = []
    for s in sentences:
        all_triples.append(parse_sentence(s))
    return all_triples


def run_gptj_causallm(prompt):
    from transformers import AutoModelForCausalLM, AutoTokenizer

    model = AutoModelForCausalLM.from_pretrained("EleutherAI/gpt-j-6B")
    tokenizer = AutoTokenizer.from_pretrained("EleutherAI/gpt-j-6B")
    input_ids = tokenizer(prompt, return_tensors="pt").input_ids
    gen_tokens = model.generate(
        input_ids,
        do_sample=True,
        temperature=0.1,
        max_length=200,
    )
    gen_text = tokenizer.batch_decode(gen_tokens)[0]


def hasTrigger(l, output, error):
    import contextlib
    import io
    import sys
    import os

    ofile = open(output, "w")
    efile = open(error, "w")
    with contextlib.redirect_stdout(ofile), contextlib.redirect_stderr(ofile):
        print("hello ")
        return "trigger" in l


# https://huggingface.co/psmathur/orca_mini_3b


def compose_prompt_orca(
    system="You are an AI assistant that follows instruction extremely well. Help as much as you can.",
    instruction=None,
    input=None,
):
    prompt = ""
    if input:
        prompt = f"### System:\n{system}\n\n### User:\n{instruction}\n\n### Input:\n{input}\n\n### Response:\n"
    else:
        prompt = f"### System:\n{system}\n\n### User:\n{instruction}\n\n### Response:\n"

    return prompt


def load_module_from_file(module_name=None, file_path=None, search=None):
    import importlib.util
    import sys

    # delete the module if it exists
    prev_module = module_name
    try:
        # sys.modules.pop(module_name)
        del prev_module
    except Exception as e:
        pass
    if search is not None:
        sys.path.append(search)
    spec = importlib.util.spec_from_file_location(
        module_name, file_path, submodule_search_locations=search
    )
    module = importlib.util.module_from_spec(spec)
    # sys.modules[module_name] = module
    spec.loader.exec_module(module)
    # reset the path
    if search is not None:
        sys.path.remove(search)
    return module
    # import module_name


def generate_mcp(
    src_file: str = None,
    function_name: Optional[str] = None,
    function_name_to_cell: Optional[dict] = None,
) -> dict:
    """
    Generate a MCP JSON from the functions in src_file
    If optional function_name is passed, it will only generate the JSON for that single function

    Args:
        src_file (str): Path to the python file
        function_name (Optional[str]): Optional filter to a specific function. If None or '*' value, all functions will generate a mcp tool
        function_name_to_cell (Optional[dict]): Optional dict for the notebook cell id to be used as _meta for the function (only applicable for no-code apps)
    """
    mcp_json = {}
    _meta = {}

    # Add metadata
    todays_date_utc = datetime.datetime.now(datetime.timezone.utc).date()
    date_format = "%Y-%m-%d"
    file_last_mod_date_utc = datetime.datetime.fromtimestamp(
        os.path.getmtime(src_file), tz=datetime.timezone.utc
    )
    _meta.update({"last_modified_date": todays_date_utc.strftime(date_format)})
    _meta.update(
        {"file_last_modified_date": file_last_mod_date_utc.strftime(date_format)}
    )
    _meta.update({"source_file": src_file})
    mcp_json.update({"_meta": _meta})

    tools = []

    with open(src_file, "r") as file:
        tree = ast.parse(file.read())

    for node in tree.body:
        function = {}
        input_schema = {}

        if isinstance(node, ast.FunctionDef):
            function_return_type = "string"
            if node.returns is not None:
                function_return_type = node.returns.id
                function_return_type = map_py_to_mcp(function_return_type)

            this_function = node.name
            if (
                function_name is None
                or function_name == "*"
                or this_function == function_name
            ):
                function.update({"name": this_function})
                function.update({"title": format_to_title_case(this_function)})
                docstring = ast.get_docstring(node)
                if docstring is not None and len(docstring) > 0:
                    function.update({"description": docstring})

                # Parse docstring to extract parameter descriptions
                arg_descriptions = parse_docstring_args(docstring) if docstring else {}

                properties = {}
                required = []

                for arg in node.args.args:
                    this_arg = {}
                    arg_name = arg.arg
                    this_arg.update({"title": format_to_title_case(arg_name)})

                    # Add description if found in docstring
                    if arg_name in arg_descriptions:
                        this_arg.update({"description": arg_descriptions[arg_name]})

                    arg_type = "string"
                    if arg.annotation:
                        if isinstance(arg.annotation, ast.Name):
                            arg_type = arg.annotation.id
                        elif isinstance(arg.annotation, ast.Subscript):
                            if isinstance(arg.annotation.value, ast.Name):
                                arg_type = arg.annotation.value.id
                            else:
                                arg_type = "string"
                        else:
                            arg_type = "string"

                    arg_type = map_py_to_mcp(arg_type)
                    function_return_type = "object"

                    this_arg.update({"type": arg_type})
                    required.append(arg_name)
                    properties.update({arg_name: this_arg})

                input_schema.update({"properties": properties})
                input_schema.update({"required": required})
                input_schema.update(
                    {"title": f"{format_to_title_case(this_function)} Arguments"}
                )
                input_schema.update({"type": function_return_type})
                function.update({"inputSchema": input_schema})

                _function_meta = {"generated_on": todays_date_utc.strftime(date_format)}
                if function_name_to_cell is not None:
                    cell_id = function_name_to_cell.get(this_function)
                    if cell_id:
                        _function_meta["notebook_cell_id"] = cell_id
                function.update({"_meta": _function_meta})
                tools.append(function)

    mcp_json.update({"tools": tools})
    return mcp_json


def gen_mcp(
    src_file: str = None,
    dest_file: str = None,
    function_name_to_cell: Optional[dict] = None,
) -> dict:
    """
    Generate a MCP JSON from the functions in src_file and writes the json to dest_file

    Args:
        src_file (str): Path to the python file
        dest_file (str): Path to export the json
        function_name_to_cell (Optional[dict]): Optional dict for the notebook cell id to be used as _meta for the function (only applicable for no-code apps)
    """
    mcp_json = generate_mcp(src_file, "*", function_name_to_cell)
    # Write to file
    with open(dest_file, "w", encoding="utf-8") as f:
        json.dump(mcp_json, f, indent=4, ensure_ascii=False)
    return mcp_json


def add_function_to_mcp(
    src_file: str = None,
    dest_file: str = None,
    function_name: str = None,
    function_name_to_cell: Optional[dict] = None,
) -> dict:
    """
    Generate a MCP JSON from a specific function in src_file and appends to the existing dest_file if exists

    Args:
        src_file (str): Path to the python file
        dest_file (str): Path to append the MCP JSON
        function_name (str): Specific function in src_file to generate a MCP tool json for
        function_name_to_cell (Optional[dict]): Optional dict for the notebook cell id to be used as _meta for the function (only applicable for no-code apps)
    """
    mcp_json = generate_mcp(src_file, function_name, function_name_to_cell)
    existing_mcp_json = {}

    # Check if file exists
    if os.path.exists(dest_file):
        # Load existing JSON
        try:
            with open(dest_file, "r", encoding="utf-8") as f:
                existing_mcp_json = json.load(f)
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(f"Error reading exisitng MCP JSON file: {e}")
        except Exception as e:
            raise Exception(f"Error reading exisitng MCP JSON file: {e}")

        # combine the old tools to the new tool
        existing_tools = existing_mcp_json.get("tools")
        mcp_json.get("tools").extend(existing_tools)

    # Write to file
    with open(dest_file, "w", encoding="utf-8") as f:
        json.dump(mcp_json, f, indent=4, ensure_ascii=False)
    return mcp_json


def parse_docstring_args(docstring):
    """
    Parse a docstring to extract argument descriptions.
    Supports Google-style docstrings with Args: section.

    Returns a dictionary mapping parameter names to their descriptions.
    """
    if not docstring:
        return {}

    lines = docstring.split("\n")
    args_descriptions = {}
    in_args_section = False
    current_arg = None
    current_description = []

    for line in lines:
        stripped = line.strip()

        # Check if we're entering the Args section
        if stripped.lower().startswith("args:"):
            in_args_section = True
            continue

        # Check if we're leaving the Args section (hit Returns:, Raises:, etc.)
        if in_args_section and stripped.lower().startswith(
            ("returns:", "return:", "raises:", "yields:", "note:", "example:")
        ):
            # Save the last argument if we have one
            if current_arg and current_description:
                args_descriptions[current_arg] = " ".join(current_description).strip()
            in_args_section = False
            break

        if in_args_section and stripped:
            # Check if this line defines a new argument (format: "param_name (type): description")
            if ":" in stripped and "(" in stripped and ")" in stripped:
                # Save previous argument if exists
                if current_arg and current_description:
                    args_descriptions[current_arg] = " ".join(
                        current_description
                    ).strip()

                # Parse new argument
                parts = stripped.split(":", 1)
                if len(parts) == 2:
                    arg_part = parts[0].strip()
                    # Extract parameter name (remove type annotation)
                    if "(" in arg_part:
                        current_arg = arg_part.split("(")[0].strip()
                    else:
                        current_arg = arg_part

                    # Start collecting description
                    current_description = [parts[1].strip()]
            elif current_arg:
                # This is a continuation of the current argument's description
                current_description.append(stripped)

    # Don't forget the last argument
    if current_arg and current_description:
        args_descriptions[current_arg] = " ".join(current_description).strip()

    return args_descriptions


def map_py_to_mcp(input):
    mapper = {
        "str": "string",
        "float": "number",
        "int": "number",
        "bool": "boolean",
    }
    if input in mapper:
        return mapper[input]
    else:
        return "object"


def map_mcp_to_py(input):
    mapper = {
        "string": "str",
        "number": "float",
        "boolean": "bool",
    }
    if input in mapper:
        return mapper[input]
    else:
        return "object"


def format_to_title_case(input_str):
    """
    Converts camelCase, PascalCase, or snake_case strings to title case with spaces
    Examples:
      "RunNER" -> "Run NER"
      "ToUpperCase" -> "To Upper Case"
      "simpleWord" -> "Simple Word"
      "XMLParser" -> "XML Parser"
      "get_stock_price" -> "Get Stock Price"
    """
    if not input_str:
        return input_str

    result = []
    capitalize_next = True  # Capitalize the first letter

    for i, char in enumerate(input_str):
        # Handle underscores - replace with space and capitalize next letter
        if char == "_":
            result.append(" ")
            capitalize_next = True
            continue

        # Add space before uppercase letters (except the first character)
        if i > 0 and char.isupper() and result and result[-1] != " ":
            # Check if previous character is lowercase or if next character is lowercase
            # This handles cases like "XMLParser" -> "XML Parser" correctly
            prev_char = input_str[i - 1]
            prev_is_lower = prev_char.islower()
            next_is_lower = i + 1 < len(input_str) and input_str[i + 1].islower()

            if prev_is_lower or next_is_lower:
                result.append(" ")
                capitalize_next = True

        # Apply capitalization logic
        if capitalize_next:
            result.append(char.upper())
            capitalize_next = False
        else:
            result.append(char)

    return "".join(result)


def get_function_name_from_code(code_string):
    """
    Extract the name of the first function defined in a Python code string.

    Args:
        code_string (str): A string containing valid Python code with a function definition

    Returns:
        str: The name of the first function found, or None if no function is found

    Raises:
        SyntaxError: If the code string contains invalid Python syntax
    """
    try:
        # Parse the code string into an Abstract Syntax Tree
        tree = ast.parse(code_string)

        # Walk through the AST nodes to find function definitions
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                return node.name

        # If no function definition is found
        return None

    except SyntaxError as e:
        raise SyntaxError(f"Invalid Python syntax: {e}")


def get_all_function_names_from_code(code_string):
    """
    Extract all function names from a Python code string.

    Args:
        code_string (str): A string containing valid Python code with function definitions

    Returns:
        list: A list of all function names found

    Raises:
        SyntaxError: If the code string contains invalid Python syntax
    """
    try:
        tree = ast.parse(code_string)
        function_names = []

        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                function_names.append(node.name)

        return function_names

    except SyntaxError as e:
        raise SyntaxError(f"Invalid Python syntax: {e}")


def remove_function_from_file(filepath: str, function_name: str) -> bool:
    """
    Remove a function (and any nested functions within it) from a Python file.

    Args:
        filepath (str): Path to the Python file
        function_name (str): Name of the function to remove

    Returns:
        bool: True if function was found and removed, False if function not found
    """

    # Check if file exists
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")

    # Read the original file
    with open(filepath, "r", encoding="utf-8") as f:
        original_code = f.read()

    try:
        # Parse the code into an AST
        tree = ast.parse(original_code)
    except SyntaxError as e:
        raise SyntaxError(f"Syntax error in {filepath}: {e}")

    # Find and remove the function
    function_found = False
    new_body = []

    for node in tree.body:
        # Check if this is the function we want to remove
        if isinstance(node, ast.FunctionDef) and node.name == function_name:
            function_found = True
            # Skip this node (effectively removing it)
            continue
        elif isinstance(node, ast.AsyncFunctionDef) and node.name == function_name:
            function_found = True
            # Skip this node (effectively removing it)
            continue
        else:
            # Keep this node
            new_body.append(node)

    if not function_found:
        return False

    # Create a new tree with the modified body
    tree.body = new_body
    # Convert the AST back to code
    new_code = ast.unparse(tree)

    # Write the modified code back to the file
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(new_code)

    return True

# all of the util functions go here
def getfunctions(file):
  import inspect
  print ("Loading file", file)
  obj1 = loadScript("rand", file)
  members = [obj for obj in dir(obj1) if not obj.startswith('__')]
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
  dists = [str(d).replace(" ","==") for d in pkg_resources.working_set]
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

def runwrapper(file, output, error,g):
  import contextlib, io, sys,os
  ofile = open(output, "w", buffering=1)
  efile = open(error, "w", buffering=1)
  with contextlib.redirect_stdout(ofile), contextlib.redirect_stderr(ofile):
    datafile = open(file, "r")
     # print(f'found the trigger {jout}')
    try:
      exec(datafile.read(), g)
    except Exception as e:
      print(e)
  ofile.close()
  efile.close()

def runwrapper_semoss_console(command, output, error,g):
  import contextlib, io, sys,os
  ofile = output
  efile = error
  with contextlib.redirect_stdout(ofile), contextlib.redirect_stderr(ofile):
    #datafile = open(file, "r")
     # print(f'found the trigger {jout}')
    try:
      exec(command, g)
    except Exception as e:
      print(e)
  ofile.close()
  efile.close()


def runwrappereval(file, output, error,g):
  import contextlib, io, sys,os
  ofile = open(output, "w", buffering=1)
  efile = open(error, "w", buffering=1)

  with contextlib.redirect_stdout(ofile), contextlib.redirect_stderr(ofile):
    datafile = open(file, "r")
    command = datafile.read()
    try:
      output_obj = eval(command, g)
      if output_obj is not None:
        print(output_obj)
    except Exception as e:
      try:
        exec(command, g)
      except Exception as e:
        print(e)
  ofile.close()
  efile.close()

def runwrappereval_semoss_console(command, output, error,g):
  import contextlib, io, sys,os
  ofile = output
  efile = error
  with contextlib.redirect_stdout(ofile), contextlib.redirect_stderr(ofile):
    #datafile = open(file, "r")
    #command = datafile.read()
    try:
      output_obj = eval(command, g)
      if output_obj is not None:
        #print(output_obj)
        return output_obj
    except Exception as e:
      try:
        exec(command, g)
      except Exception as e:
        print(e)
  ofile.close()
  efile.close()


# same as run wrapper eval but will also return the output instead of printing it and
# will not exec it
# since I need the return value
# - Updating the progress bar - https://stackoverflow.com/questions/45808140/using-tqdm-progress-bar-in-a-while-loop

def runwrappereval_return(command, output, error,g):
  import contextlib, io, sys,os
  ofile = open(output, "w", buffering=1)
  efile = open(error, "w", buffering=1)
  with contextlib.redirect_stdout(ofile), contextlib.redirect_stderr(ofile):
    from tqdm import tqdm
    pbar = tqdm(total=100)
    pbar.update(10)
    try:
      pbar.update(20)
      output_obj = eval(command, g)
      pbar.update(50)
      if output_obj is not None:
        pbar.update(10)
        print(output_obj)
        return output_obj
    except Exception as e:
      print(e)
      pbar.update(10)
      return None
  ofile.close()
  efile.close()
  pbar.close()

# used by empty py direct    
def run_empty_wrapper(file,g):
    #ofile = io.StringIO()
  #print(output)
  exec(open(file).read(), g)


#Attribution = https://github.com/bosswissam/pysize/blob/master/pysize.py
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
    if hasattr(obj, '__dict__'):
        for cls in obj.__class__.__mro__:
            if '__dict__' in cls.__dict__:
                d = cls.__dict__['__dict__']
                if inspect.isgetsetdescriptor(d) or inspect.ismemberdescriptor(d):
                    size += get_size(obj.__dict__, seen)
                break
    if isinstance(obj, dict):
        size += sum((get_size(v, seen) for v in obj.values()))
        size += sum((get_size(k, seen) for k in obj.keys()))
    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
        size += sum((get_size(i, seen) for i in obj))
        
    if hasattr(obj, '__slots__'): # can have __slots__ with __dict__
        size += sum(get_size(getattr(obj, s), seen) for s in obj.__slots__ if hasattr(obj, s))
        
    return size

def install_py(packageName):
  from pip._internal import main as pipmain
  pipmain(['install', packageName])

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
    pipe = pipeline("text2text-generation", model = model, tokenizer=tokenizer, device=0)
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
  finalList.update({"function_name":func_name.__name__})
  # first elementis the type
  # second element is the default value
  # third item is if this is optional
  # if there is a default value it is optional
  value = []
  for item in keys:
    key = dict[item].name
    param_type = dict[item].annotation
    #print(item)
    thisValue = dict[item].default
    
    # first figure out the types
    # handle the enumeration
    processed = False
    
    if(param_type.__class__ == Enum.__class__):
      dropdown = {}
      inner_dict_list = list(dict[item].annotation.__members__.copy().values())
      for i in inner_dict_list:
        dropdown.update({i.name:i.value})
      value.append('PixelDataType.Multi')
      value.append(dropdown)
      value.append('False')
      finalList.update({key:value})
      processed = True
      #print(f"handled as enum {key}: {value} {~processed}")
      value = []
      
    # handle function
    if(param_type.__class__ == types.FunctionType):
      #print("handled as function")
      value.append('PixelDataType.Function')
      value.append(get_function_signature(param_type))
      value.append('False')
      finalList.update({key:value})
      processed = True
      value = []
      
    if(param_type == dict[item].empty and not processed):  
      #print("param type is empty")
      # impute from the default value if you can
      # if it is empty - that is a string value no default
      ## if it is none it is a int value no default
      # else this is optional
      # also check to see if the type is already there
      if(thisValue == dict[item].empty):
        # need to check the pixel data type
        value.append('PixelDataType.Str')
        value.append('')
        value.append('False')
        processed = True
      else:
        if(thisValue == None): 
          value.append('PixelDataType.Int')
          value.append('')
          value.append('False')
        else:
          # check to see if this starts with quotes
          if(type(thisValue) == (bool)):
            value.append('PixelDataType.Boolean')
          else:
            if(isinstance(thisValue, (int,float))):
              value.append('PixelDataType.bool')
            else:
              if(~processed):
              # turn everything else into string
              #if(isinstance(thisValue, (str))):
                value.append('PixelDataType.Str')
          value.append(str(thisValue))
          value.append('True')
      finalList.update({key:value})
      value = []
    else:
      if(not processed):
        # use the param type to fill the data
        if(param_type == int):
          value.append('PixelDataType.Int')
          # check again to see if it is empty
          if(thisValue == dict[item].empty):
            value.append('')
            value.append('False')
          else:
            value.append(thisValue)
            value.append('True')
        else:
          if(param_type == bool):
            value.append('PixelDataType.Boolean')
            if(thisValue == dict[item].empty):
              value.append('')
              value.append('False')
            else:
              value.append(thisValue)
              value.append('True')
          else:
            #Everything else is stringif(param_type == bool):
            value.append('PixelDataType.Str')
            if(thisValue == dict[item].empty):
              value.append('')
              value.append('False')
            else:
              value.append(thisValue)
              value.append('True')
        finalList.update({key:value})
        value = []

  # last item is return
  key = 'return_value'
  returns = func_name.__annotations__
  ret_type = 'unknown'
  if('return' in returns):
    ret_type = returns['return'].__name__
  value.append(f'PixelDataType.{ret_type}')
  value.append(ret_type)
  value.append('NA')
  finalList.update({key:value})
  return finalList

def run_gpt_3(nl_query, max_tokens_value):
  #import os
  import openai
  response = openai.Completion.create(model="code-davinci-002", prompt=nl_query, temperature=0, max_tokens=max_tokens_value, top_p=1, frequency_penalty=0, presence_penalty=0,stop=["#", ";"])
  query = " SELECT " + response.choices[0].text
  print (query)
  return query
  
def chat_gpt_3(nl_query, max_tokens_value):
  #import os
  import openai
  response = openai.Completion.create(model="code-davinci-002", prompt=nl_query, temperature=0, max_tokens=max_tokens_value, top_p=1, frequency_penalty=0, presence_penalty=0,stop=["#", ";"])
  query = " SELECT " + response.choices[0].text
  print (query)
  return query

def run_alpaca(nl_query, max_tokens_value, api_base, model_name="alpaca-13b-lora-int4"):
  #import os
  import openai
  # forcing the api_key to a dummy value
  if openai.api_key is None:
    openai.api_key = "Non Existent API Key"
  openai.api_base = api_base
  #response = openai.Completion.create(model="alpaca-30b-lora", prompt=nl_query, temperature=0, max_tokens=max_tokens_value, top_p=1, frequency_penalty=0, presence_penalty=0,stop=["#", ";"])
  response = openai.Completion.create(model=model_name, prompt=nl_query, temperature=0, max_tokens=max_tokens_value, top_p=1, frequency_penalty=0, presence_penalty=0,stop=["#", ";"])
  #response = openai.Completion.create(model="alpaca-lora-7b", prompt=nl_query, temperature=0, max_tokens=max_tokens_value, top_p=1, frequency_penalty=0, presence_penalty=0,stop=["#", ";"])
  query=response.choices[0].text
  print (query)
  return query

def run_guanaco(nl_query, max_tokens_value,api_base, stop_sequences=["#", ";"], temperature_val=0.01, top_p_val=0.5, **kwargs):
  from text_generation import Client
  client = Client(api_base)
  text = ""
  #for response in client.generate_stream(compose_prompt(context, question), max_new_tokens=max_new_tokens, temperature=0.2,top_p=0.5):
  for response in client.generate_stream(compose_sql_prompt(nl_query), temperature=temperature_val,top_p=top_p_val, max_new_tokens=max_tokens_value, stop_sequences=stop_sequences, **kwargs):
    if not response.token.special:
        text += response.token.text
  text = text.replace('\r', ' ').replace('\n', ' ').replace('`', '')
  print(text)
  return text

def chat_alpaca(context, nl_query, max_tokens_value, api_base, model_name="guanaco-33b", long=False):
  #import os
  import openai
  # forcing the api_key to a dummy value
  if openai.api_key is None:
    openai.api_key = "Non Existent API Key"
  openai.api_base = api_base
  query = ""
  if context is None:
    context = ""
  if not long:
    query = f"Below is an instruction that describes a task. Write a response that appropriately completes the request.\n\n### Instruction: {nl_query}\n\n### Response:"
  else:
    query = f"A chat between a curious human and an artificial intelligence assistant. The assistant gives helpful, detailed, and polite answers to the user's questions. Based on the following paragraphs, answer the human's question:\n\n{context}.\n\n### Questions:\n\n{nl_query}\n\n### Response:"
  print(query)
  
  #response = openai.Completion.create(model="alpaca-30b-lora", prompt=nl_query, temperature=0, max_tokens=max_tokens_value, top_p=1, frequency_penalty=0, presence_penalty=0,stop=["#", ";"])
  response = openai.Completion.create(model=model_name, prompt=query, temperature=0, max_tokens=max_tokens_value, top_p=1, frequency_penalty=0, presence_penalty=0,stop=["#", ";"])
  #response = openai.Completion.create(model="alpaca-lora-7b", prompt=nl_query, temperature=0, max_tokens=max_tokens_value, top_p=1, frequency_penalty=0, presence_penalty=0,stop=["#", ";"])
  query=response.choices[0].text
  print (query)
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
  #print(query)
  return query

def compose_prompt_wizard(context=None, question=None):
  assert question is not None
  if context is None:
    query = f"Below is an instruction that describes a task. Write a response that appropriately completes the request.\n\n### Instruction: {question}\n\n### Response:"
  else:
    query = f"A chat between a curious human and an artificial intelligence assistant. The assistant gives helpful, detailed, and polite answers to the user's questions. USER: {context}. {question} ? ASSISTANT:"
  #print(query)
  return query


def compose_prompt_qa(context=None, question=None):
  prompt = f"Context information is below. \n---------------------\n{context}\n---------------------\nGiven the context information and not prior knowledge, answer the question: {question}\n"
  return prompt

def chat_guanaco(context=None, question = None, client=None, max_new_tokens=200, prev_response=None, stop_sequences=["###", "#", ";"], **kwargs):
  if context == "":
    context=None
  text = ""
  finish_reason = ""
  input_text = compose_prompt(context, question)
  final_output = {}
  if prev_response is not None:
    input_text = f"{input_text} {prev_response}"
  #for response in client.generate_stream(compose_prompt(context, question), max_new_tokens=max_new_tokens, temperature=0.2,top_p=0.5):
  for response in client.generate_stream(compose_prompt(context, question), max_new_tokens=max_new_tokens, stop_sequences=stop_sequences, **kwargs):
  #for response in client.generate_stream(compose_prompt_qa(context, question), max_new_tokens=max_new_tokens, stop_sequences=stop_sequences, **kwargs):
    if not response.token.special:
      text += response.token.text
    if response.details is not None:
      detail = response.details
      from text_generation.types import FinishReason
      # print(f"Finished with {detail.finish_reason}")
      if detail.finish_reason != "stop_sequence" and detail.finish_reason != "eos_token":
        finish_reason = f"... <Unable to complete request, please try by increasing token size from {max_new_tokens}>"
        final_output.update({"meta": finish_reason})
  #print(client.generated_stream.finish_reason)
  # print(f"{text}")
  final_output.update({"response": f"{text}"})
  return final_output

def chat_guanaco_code(context=None, question = None, client=None, prev_response=None, max_new_tokens=100, stop_sequences=["###", ";"], incremental=False,  **kwargs):
  text = ""
  # code starts with ``` and ends with ```
  if context == "":
    context=None
  input_text = compose_prompt(context, question)
  if(prev_response is not None):
    input_text = f"{input_text} {prev_response}"
  # print(input_text)
  response_available = False
  str_start = -1
  str_end = -1
  cur_new_tokens=100
  total_tokens=100
  response_so_far = ""
  final_answer = ""
  finish_reason = ""
  final_output = {}
  while not response_available and total_tokens < max_new_tokens:
    #for response in client.generate_stream(compose_prompt(context, question), max_new_tokens=max_new_tokens, temperature=0.2,top_p=0.5):
    #print("running while loop")
    for response in client.generate_stream(input_text, max_new_tokens=cur_new_tokens, stop_sequences=stop_sequences, **kwargs):
    #for response in client.generate_stream(compose_prompt_qa(context, question), max_new_tokens=max_new_tokens, stop_sequences=stop_sequences, **kwargs):
      #print("in for loop")
      if not response.token.special:
        text += response.token.text
    
    #if(len(response_so_far) == 0):
    #print(f"text is >>>> {text} <<<<<")
    #response_so_far = text

    if str_start < 0:
      str_start = text.find('```')
    
    if str_start > 0 and str_end < 0:
      str_end = text.find('```', str_start+3)

    if(str_start > 0 and str_end > 0):
      response_available = True
      #print("found both start and end")
      final_answer = text[str_start + 3:str_end]
      final_output.update({"response": f"{final_answer}"})
      #print(f" Response so far {response_so_far}")
    else:
      input_text = f"{compose_prompt(context, question)} {text}"
      total_tokens = total_tokens + 100
      if(incremental):
        print(f"{text}")
      #print(f" Response so far {response_so_far}")
      #print(f"{input_text}")

    #print(f"Start : {str_start}, End : {str_end}")    
    #print(f"Response available ? {response_available}")
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
  #print(f"{text}")
  return final_output


def convert_pdf_to_text(document_location):
  import PyPDF2
  import pathlib
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
  if(outputFile.exists() and os.path.getmtime(outputFile) > os.path.getmtime(inputFile)):
    return True
  
  pdfFileObj = open(inputFile, 'rb')
  pdfReader = PyPDF2.PdfFileReader(pdfFileObj)
  outputFile = open(outputFile, 'w')
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
    'openie.affinity_probability_cap': 2 / 3,
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
  gen_tokens = model.generate(input_ids, do_sample=True, temperature=0.1,max_length=200,)
  gen_text = tokenizer.batch_decode(gen_tokens)[0]
 
def hasTrigger(l, output, error):
  import contextlib, io, sys,os
  ofile = open(output, "w")
  efile = open(error, "w")
  with contextlib.redirect_stdout(ofile), contextlib.redirect_stderr(ofile):
    print("hello ")
    return 'trigger' in l

#https://huggingface.co/psmathur/orca_mini_3b
def compose_prompt_orca(system='You are an AI assistant that follows instruction extremely well. Help as much as you can.', instruction=None, input=None):
  prompt = ""
  if input:
      prompt = f"### System:\n{system}\n\n### User:\n{instruction}\n\n### Input:\n{input}\n\n### Response:\n"
  else:
      prompt = f"### System:\n{system}\n\n### User:\n{instruction}\n\n### Response:\n"

  return prompt

def load_module_from_file(module_name=None, file_path=None):
  import importlib.util
  import sys
  # delete the module if it exists
  prev_module = module_name
  try:
    #sys.modules.pop(module_name)
    del prev_module
  except Exception as e:
    pass
  spec = importlib.util.spec_from_file_location(module_name, file_path)
  module = importlib.util.module_from_spec(spec)
  #sys.modules[module_name] = module
  spec.loader.exec_module(module)
  return module
  #import module_name
  

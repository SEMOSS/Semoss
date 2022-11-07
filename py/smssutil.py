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
	ofile = open(output, "w")
	efile = open(error, "w")
	with contextlib.redirect_stdout(ofile):
		datafile = open(file, "r")
		try:
			exec(datafile.read(), g)
		except Exception as e:
			print(e)
	ofile.close()
	efile.close()

def runwrappereval(file, output, error,g):
	import contextlib, io, sys,os
	ofile = open(output, "w")
	efile = open(error, "w")
	with contextlib.redirect_stdout(ofile):
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



	
def runwrapper3(file, output, error,g):
	import contextlib, io
   #ofile = io.StringIO()
	ofile = open(output, "w")
	efile = open(error, "w")
	with contextlib.redirect_stdout(ofile), contextlib.redirect_stderr(efile):
		datafile = open(file, "r")
		exec(datafile.read(), g)
		ofile.close()
		efile.close()
		datafile.close()

def runwrapper2(file, output, error):
	import contextlib, io
   #ofile = io.StringIO()
	print(output)
	ofile = open(output, "w")
	efile = open(error, "w")
	with contextlib.redirect_stdout(ofile), contextlib.redirect_stderr(efile):
		exec(open(file).read())
		ofile.close()
		efile.close()

		
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
	tokenizer = AutoTokenizer.from_pretrained(modelName)
	model = AutoModelForSeq2SeqLM.from_pretrained(modelName, cache_dir=cacheFolder)
	from transformers import pipeline
	# need to check for kuda
	pipe = pipeline(typeOfModel, model=model, tokenizer=tokenizer)
	return pipe

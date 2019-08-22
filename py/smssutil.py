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
	

import numpy as np
import pandas as pd
import re as re
from array import *

def regexComparison(frameWrapper, rule):
	df = frameWrapper.cache['data']
	currCol = rule['col']
	currRule = rule['rule']
	numOptions = len(rule['options'])

	if currRule == 'Regex Input':
		regexArray = []
		description = "Check against regex input"
		for opt in rule['options']:
			regexArray.append(re.compile(opt))
	else:
		regexArray = getRegex(rule['options'], numOptions, currRule)

	tempArray = df[currCol]
	totLength = len(tempArray)
	totErrors = 0
	errorsArray = []

	for i in range(totLength):
		valid = False
		for j in range(numOptions):
			if regexArray[j].match(tempArray[i]):
				valid = True
				break
		if not valid:
			errorsArray.append(tempArray[i])
			totErrors += 1

	# remove duplicates from to paint
	toPaint = list(set(errorsArray))

	totCorrect = totLength - totErrors
	# Create DataFrame
	data = {'Columns': [currCol], 'Errors': [totErrors], 'Valid': [totCorrect], 'Total': [totLength],
			'Rules': [currRule], 'Description': [''], 'toColor': [toPaint]}
	ruleDf = pd.DataFrame(data)
	return ruleDf

def getRegex(options, numOptions, rule):
	regexArray = []
	if rule == 'Email Format':
		for opt in options:
			if opt == "xxxxx@xxxx.xxx":
				regexArray.append(re.compile("^[a-zA-Z0-9_.]+@[a-zA-Z0-9]+\.[a-zA-Z0-9]+$"))
			elif opt == "xxxxx@xxxx.xx.xx":
				regexArray.append(re.compile("^[a-zA-Z0-9_.]+@[a-zA-Z0-9]+\.[a-zA-Z0-9]+\.[a-zA-Z0-9]+$"))
			elif opt == "xxxxx@xxxx.xxx(.xx)":
				regexArray.append(re.compile("^[a-zA-Z0-9_.]+@[a-zA-Z0-9]+\.[a-zA-Z0-9]+?(\.[a-zA-Z0-9]+)$"))
	elif rule == 'Date Format':
		for opt in options:
			if opt == "mm/dd/yyyy":
				regexArray.append(re.compile("^[0-2]{,2}/[0-9]{,2}/[0-9]{4}$"))
			elif opt == "month dd, yyyy":
				regexArray.append(re.compile("^(?:J(anuary|u(ne|ly))|February|Ma(rch|y)|A(pril|ugust)|"
											 "(((Sept|Nov|Dec)em)|Octo)ber)\s[0-9]{,2},\s[0-9]{4}$"))
			elif opt == "day, month dd, yyyy":
				regexArray.append(re.compile("^(Sunday|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday),"
											 "\s(J(anuary|u(ne|ly))|February|Ma(rch|y)|A(pril|ugust)|(((Sept|"
											 "Nov|Dec)em)|Octo)ber)\s[0-9]{,2},\s[0-9]{,4}$"))
			elif opt == "mon dd, yyyy":
				regexArray.append(re.compile("^(J(an|u(n|l))|Feb|Ma(r|y)|A(pr|ug)|Sep|Nov|Dec|Oct)\s[0-"
											 "9]{,2},\s(19[0-9]{2}|[2-9][0-9]{3}|[0-9]{2})$"))
	elif rule == "Name Format":
		for opt in options:
			if opt == "last, first (m.)":
				regexArray.append(re.compile("^[a-zA-z]{1,12},\s[a-zA-z]{1,12}(\s[a-zA-z]?\.)*$"))
			elif opt == "first last":
				regexArray.append(re.compile("^[a-zA-z]*\s[a-zA-z]*$"))

	return regexArray

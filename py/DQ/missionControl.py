from .Rules import isUnique
from .Rules import isNull
from .Rules import validator
from .Rules import regexComparison
import pandas

def missionControl(frameWrapper, rule, resultsTable):
	currRule = rule['rule']
	# Check for incorrect email format
	if (currRule == "Blanks/Nulls/NAs"): 
		tempResultsTable = isNull.isNull(frameWrapper, rule)
		resultsTable = pandas.concat([resultsTable, tempResultsTable])
	# Check for incorrect email format
	elif  (currRule == "Email Format"):
		tempResultsTable = regexComparison.regexComparison(frameWrapper, rule)
		resultsTable = pandas.concat([resultsTable, tempResultsTable])
	# Check for incorrect date format
	elif  (currRule == "Date Format"):
		tempResultsTable = regexComparison.regexComparison(frameWrapper, rule)
		resultsTable = pandas.concat([resultsTable, tempResultsTable])
	# Check for duplicated entries
	elif  (currRule == "Duplicates"):
		tempResultsTable = isUnique.duplicates(frameWrapper, rule)
		resultsTable = pandas.concat([resultsTable, tempResultsTable])
	# Check for incorrect name format
	elif (currRule == "Name Format"):
		tempResultsTable = regexComparison.regexComparison(frameWrapper, rule)
		resultsTable = pandas.concat([resultsTable, tempResultsTable])
	elif (currRule == "Validate Values"):
		tempResultsTable = validator.validator(frameWrapper, rule)
		resultsTable = pandas.concat([resultsTable, tempResultsTable])
	elif (currRule == "Regex Input"):
		tempResultsTable = regexComparison.regexComparison(frameWrapper, rule)
		resultsTable = pandas.concat([resultsTable, tempResultsTable])
	return(resultsTable)
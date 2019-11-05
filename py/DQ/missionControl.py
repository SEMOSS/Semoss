from Rules import isUnique
from Rules import isNull

def missionControl(frameWrapper, rule, resultsTable):
	currRule = rule['rule']
	print(currRule)
	if (currRule == "Blanks/Nulls/NAs"): 
		tempResultsTable = isNull.isNull(frameWrapper, rule)
		resultsTable = resultsTable.append(tempResultsTable)
	# Check for incorrect email format
	elif  (currRule == "Email Format"):
		tempResultsTable = regexComparison(frameWrapper, rule)
		resultsTable = resultsTable.append(tempResultsTable)
	# Check for incorrect date format
	elif  (currRule == "Date Format"):
		tempResultsTable = regexComparison(frameWrapper, rule)
		resultsTable = resultsTable.append(tempResultsTable)
	# Check for duplicated entries
	elif  (currRule == "Duplicates"):
		tempResultsTable = isUnique.duplicates(frameWrapper, rule)
		resultsTable = resultsTable.append(tempResultsTable)
	# Check for incorrect name format
	elif (currRule == "Name Format"):
		tempResultsTable = regexComparison(frameWrapper, rule)
		resultsTable = resultsTable.append(tempResultsTable)
	elif (currRule == "Validate Values"):
		tempResultsTable = validator(frameWrapper, rule)
		resultsTable = resultsTable.append(tempResultsTable)
	elif (currRule == "Regex Input"):
		tempResultsTable = regexComparison(frameWrapper, rule)
		resultsTable = resultsTable.append(tempResultsTable)
	return(resultsTable)
    
    
    
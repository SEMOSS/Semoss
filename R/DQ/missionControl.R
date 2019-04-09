# Data Quality Checker - Function that lists the cells that break the specified rules

## Inputs:
# dt: data table
# rules: rules to be applied to the data
# colList: column name w date
# form: desired format
# resultsTable: Table with the number of cells that break the rules, comply with the rules, 
#         total number of cells and the rule it breaks
## Outputs:
# dt: data to be checked (same as in the input)
# resultsTable: Table with the number of cells that break the rules, comply with the rules, 
#         total number of cells and the rule it breaks

missionControl <- function (dt, rulesList, resultsTable){
  
  dt[dt == ""] <- NA
  
  tempResultsTable <- data.table(Columns=character(),Errors=integer(),Valid=integer(),Total=integer(),Rules=character(), Description=character(), toColor = list())
  
  numRules <- length(rulesList)
  
  for(i in 1:numRules){
    currRule = rulesList[[i]]$rule
    currCol = rulesList[[i]]$col
    
    if (currRule == "blanks") {
      tempResultsTable <- isNull(dt, rulesList[[i]])
      resultsTable <- rbind(resultsTable, tempResultsTable)
    }
    # Check for incorrect gender format
    else if (currRule == "gender"){
      tempResultsTable <- genderRule(dt, rulesList[[i]])
      resultsTable <- rbind(resultsTable, tempResultsTable)
    }
    # Check for incorrect email format
    else if (currRule == "email"){
      tempResultsTable <- emailRule(dt, rulesList[[i]])
      resultsTable <- rbind(resultsTable, tempResultsTable)
    } 
    # Check for incorrect date format
    else if (currRule == "date"){ 
      tempResultsTable <- dateRule(dt, rulesList[[i]])
      resultsTable <- rbind(resultsTable, tempResultsTable)
    }
    # Check for duplicated entries
    else if (currRule == "duplicates"){
      tempResultsTable <- duplicates(dt, rulesList[[i]])
      resultsTable <- rbind(resultsTable, tempResultsTable)
    }
    # Check for incorrect name format
    else if(currRule == "name"){
      tempResultsTable <- nameRule(dt, rulesList[[i]])
      resultsTable <- rbind(resultsTable, tempResultsTable)
    }
    else if(currRule == "validate"){
      tempResultsTable <- validator(dt, rulesList[[i]])
      resultsTable <- rbind(resultsTable, tempResultsTable)
    }
    ##### ADD NEW RULES HERE #####
    ##### UNCOMMENT BELLOW AND FILL IN REQUIRED VALUES #####
    # else if (currRule == "YOUR_RULE_IDENTIFIER"){
    #   tempResultsTable <- YOUR_RULE_FUNCTION_NAME(dt, rulesList[[i]])
    #   resultsTable <- rbind(resultsTable, tempResultsTable)
    # }
  }
 
  return (list(dt, resultsTable))
}

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

missionControl <- function (dt, rule, resultsTable){
  
  dt[dt == ""] <- NA
  
  tempResultsTable <- data.table(Columns=character(),Errors=integer(),Valid=integer(),Total=integer(),Rules=character(), Description=character(), ruleID = character(), toColor = character())
  
  currRule = rule$rule
  currCol = rule$col
  
  if (currRule == "blanks") {
    tempResultsTable <- isNull(dt, rule)
    resultsTable <- rbind(resultsTable, tempResultsTable)
  }
  # Check for incorrect gender format
  else if (currRule == "gender"){
    tempResultsTable <- genderRule(dt, rule)
    resultsTable <- rbind(resultsTable, tempResultsTable)
  }
  # Check for incorrect email format
  else if (currRule == "email"){
    tempResultsTable <- emailRule(dt, rule)
    resultsTable <- rbind(resultsTable, tempResultsTable)
  } 
  # Check for incorrect date format
  else if (currRule == "date"){ 
    tempResultsTable <- dateRule(dt, rule)
    resultsTable <- rbind(resultsTable, tempResultsTable)
  }
  # Check for duplicated entries
  else if (currRule == "duplicates"){
    tempResultsTable <- duplicates(dt, rule)
    resultsTable <- rbind(resultsTable, tempResultsTable)
  }
  # Check for incorrect name format
  else if(currRule == "name"){
    tempResultsTable <- nameRule(dt, rule)
    resultsTable <- rbind(resultsTable, tempResultsTable)
  }
  else if(currRule == "validate"){
    tempResultsTable <- validator(dt, rule)
    resultsTable <- rbind(resultsTable, tempResultsTable)
  }
  else if(currRule == "regex"){
    tempResultsTable <- regexInput(dt, rule)
    resultsTable <- rbind(resultsTable, tempResultsTable)
  }
  ##### ADD NEW RULES HERE #####
  ##### UNCOMMENT BELLOW AND FILL IN REQUIRED VALUES #####
  # else if (currRule == "YOUR_RULE_IDENTIFIER"){
  #   tempResultsTable <- YOUR_RULE_FUNCTION_NAME(dt, rulesList[[i]])
  #   resultsTable <- rbind(resultsTable, tempResultsTable)
  # }
 
  return (list(dt, resultsTable))
}

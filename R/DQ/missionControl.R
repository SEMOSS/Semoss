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
  
  tempResultsTable <- data.table(Columns=character(),Errors=integer(),Valid=integer(),Total=integer(),Rules=character(), Description=character())
  
  numRules <- length(rulesList)
  
  for(i in 1:numRules){
    currRule = rulesList[[i]]$rule
    currCol = rulesList[[i]]$col
    
    if (currRule == "blanks") {
      return <- isNull(dt, rulesList[[i]])
      tempResultsTable <- return[[1]]
      logVector <- return[[2]]
      resultsTable <- rbind(resultsTable, tempResultsTable)
      dt <- cbind(dt, logVector)
    }
    # Check for incorrect gender format
    else if (currRule == "gender"){
      return <- genderRule(dt, rulesList[[i]])
      tempResultsTable <- return[[1]]
      logVector <- return[[2]]
      resultsTable <- rbind(resultsTable, tempResultsTable)
      dt <- cbind(dt, logVector)
    }
    # Check for incorrect email format
    else if (currRule == "email"){
      return <- emailRule(dt, rulesList[[i]])
      tempResultsTable <- return[[1]]
      logVector <- return[[2]]
      resultsTable <- rbind(resultsTable, tempResultsTable)
      dt <- cbind(dt, logVector)
    } 
    # Check for incorrect date format
    else if (currRule == "date"){ 
      return <- dateRule(dt, rulesList[[i]])
      tempResultsTable <- return[[1]]
      logVector <- return[[2]]
      resultsTable <- rbind(resultsTable, tempResultsTable)
      dt <- cbind(dt, logVector)
    }
    # Check for duplicated entries
    else if (currRule == "duplicates"){
      return <- duplicates(dt, rulesList[[i]])
      tempResultsTable <- return[[1]]
      logVector <- return[[2]]
      resultsTable <- rbind(resultsTable, tempResultsTable)
      dt <- cbind(dt, logVector)
    }
    # Check for incorrect name format
    else if(currRule == "name"){
      return <- nameRule(dt, rulesList[[i]])
      tempResultsTable <- return[[1]]
      logVector <- return[[2]]
      resultsTable <- rbind(resultsTable, tempResultsTable)
      dt <- cbind(dt, logVector)
    }
    else if(currRule == "findVals"){
      tempResultsTable <- findValue(dt, rulesList[[i]])
      resultsTable <- rbind(resultsTable, tempResultsTable)
    }
    else if(currRule == "validate"){
      return <- validator(dt, rulesList[[i]])
      tempResultsTable <- return[[1]]
      logVector <- return[[2]]
      resultsTable <- rbind(resultsTable, tempResultsTable)
      dt <- cbind(dt, logVector)
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

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
  
  tempResultsTable <- data.table(Columns=character(),Errors=integer(),Valid=integer(),Total=integer(),Rules=character(), Description=character(), toColor = character())
  
  currRule = rule$rule
  currCol = rule$col
  
  if (currRule == "Blanks/Nulls/NAs") {
    tempResultsTable <- isNull(dt, rule)
    resultsTable <- rbind(resultsTable, tempResultsTable)
  }
  # Check for incorrect email format
  else if (currRule == "Email Format"){
    tempResultsTable <- regexComparison(dt, rule)
    resultsTable <- rbind(resultsTable, tempResultsTable)
  } 
  # Check for incorrect date format
  else if (currRule == "Date Format"){ 
    tempResultsTable <- regexComparison(dt, rule)
    resultsTable <- rbind(resultsTable, tempResultsTable)
  }
  # Check for duplicated entries
  else if (currRule == "Duplicates"){
    tempResultsTable <- duplicates(dt, rule)
    resultsTable <- rbind(resultsTable, tempResultsTable)
  }
  # Check for incorrect name format
  else if(currRule == "Name Format"){
    tempResultsTable <- regexComparison(dt, rule)
    resultsTable <- rbind(resultsTable, tempResultsTable)
  }
  else if(currRule == "Validate Values"){
    tempResultsTable <- validator(dt, rule)
    resultsTable <- rbind(resultsTable, tempResultsTable)
  }
  else if(currRule == "Regex Input"){
    tempResultsTable <- regexComparison(dt, rule)
    resultsTable <- rbind(resultsTable, tempResultsTable)
  }
  return(resultsTable)
}

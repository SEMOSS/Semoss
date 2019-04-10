# Data Quality Checker - Function that identifies incorrect name format

## Inputs: 
# dt: data table
# colList: column name w name entries
# form: desired name format
## Outputs:
# resultsTable: Table with the number of cells with correctly formatted names, 
#            number of cells with incorrectly formatted names, total number of cells 
#            and the rule it breaks ("Incorrect Name Format")

library(data.table)

nameRule <- function(dt, rule){
  
  ruleName <- c("Name Format")
  currCol <- rule$col
  currRule <- rule$rule
  
  regex <- whatNameRule(rule$options) #goes into options of the rule to pull out regex format desired
  
  tempArray <- dt[, get(rule$col)]
  totLength <- length(tempArray)
  tempTotErrs <- 0
  
  nameErrorArray  <- character(totLength) # list of values to paint
  
  for(i in 1:totLength){
    if(grepl(regex, tempArray[i]) == FALSE){
      nameErrorArray[i] <- tempArray[i]
      tempTotErrs = tempTotErrs + 1
    }
  }
  
  nameErrorArray <- nameErrorArray[!duplicated(nameErrorArray)]
  toPaint <- paste(nameErrorArray, collapse = "\", \"" )
  toPaint <- paste0('\"', toPaint, '\"')
  totCorrect <- totLength - tempTotErrs
  returnTable <- data.table(currCol, tempTotErrs, totCorrect, totLength, ruleName, rule$options, currRule, toPaint)
  names(returnTable) <- c('Columns','Errors', 'Valid','Total','Rules', 'Description', 'ruleID', 'toColor')
  
  return (returnTable)
}

whatNameRule <- function(form){
  if(form == "last, first (m.)"){return ("^\\w{1,12},[[:space:]]\\w{1,12}([[:space:]]\\w?.)*$")}
  else if(form == "first last"){return ("^\\w{1,12}[[:space:]]\\w{1,12}$")}
}

# Rule 1 checks for format Last, First MI.
# Rule 2 checks for format First Last
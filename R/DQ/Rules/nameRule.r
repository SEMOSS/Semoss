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
  
  ruleName <- c("Incorrect Name Format")
  currCol <- rule$col
  currRule <- rule$rule
  
  regex <- whatNameRule(rule$options) #goes into options of the rule to pull out regex format desired
  
  tempArray <- dt[, get(rule$col)]
  totLength <- length(tempArray)
  tempTotErrs <- 0
  
  logVecName <- paste(currCol,currRule,sep="_")
  
  nameErrorArray <- character(totLength) #mirror array of column with valid/invalid if the cooresponding data point is valid or not
  
  for(i in 1:totLength){
    if(grepl(regex, tempArray[i]) == FALSE){
      nameErrorArray[i] <- 'invalid'
      tempTotErrs = tempTotErrs + 1
    }
    else if(is.na(tempArray[i])){
      nameErrorArray[i] <- 'invalid'
      tempTotErrs = tempTotErrs + 1
    }
    else{
      nameErrorArray[i] <- 'valid'
    }
  }
  logVector <- data.table(nameErrorArray)
  names(logVector) <- c(logVecName)
  totCorrect <- totLength - tempTotErrs
  tempTable <- data.table(currRule, tempTotErrs, totCorrect, totLength, ruleName, rule$options)
  names(tempTable) <- c('Columns','Errors', 'Valid','Total','Rules', 'Description')
  
  return (list(tempTable, logVector))
}

whatNameRule <- function(form){
  if(form == "last, first (m.)"){return ("^\\w{1,12},[[:space:]]\\w{1,12}([[:space:]]\\w?.)*$")}
  else if(form == "first last"){return ("^\\w{1,12}[[:space:]]\\w{1,12}$")}
}

# Rule 1 checks for format Last, First MI.
# Rule 2 checks for format First Last
# Data Quality Checker - Function that identifies incorrect email format

## Inputs: 
# dt: data table
# colList: column name w emails
# form: desired email format
## Outputs:
# resultsTable: Table with the number of cells with correctly formatted emails, 
#            number of cells with incorrectly formatted emails, total number of cells 
#            and the rule it breaks ("Incorrect Email Format")

emailRule <- function(dt, rule) {

  ruleName <- c("Incorrect Email Format")
  currCol <- rule$col
  currRule <- rule$rule
  
  regex <- whatEmailRule(rule$options) #goes into options of the rule to pull out regex format desired
  
  tempArray <- dt[, get(rule$col)]
  totLength <- length(tempArray)
  tempTotErrs <- 0
  
  # logVecName <- paste(currCol,currRule,sep="_")
  
  emailErrorArray <- character(totLength)  # list of values to paint
  
  for(i in 1:totLength){
    if(grepl(regex, tempArray[i]) == FALSE){
      # emailErrorArray[i] <- 'invalid'
      emailErrorArray[i] <- tempArray[i]
      tempTotErrs = tempTotErrs + 1
    }
    # else if(is.na(tempArray[i])){
    #   # emailErrorArray[i] <- 'invalid'
    #   tempTotErrs = tempTotErrs + 1
    # }
    else{
      # emailErrorArray[i] <- 'valid'
    }
  }
  
  emailErrorArray <- emailErrorArray[!duplicated(emailErrorArray)]
  # toColor <- paste(unlist(emailErrorArray), collapse = ", ")
  
  totCorrect <- totLength - tempTotErrs
  returnTable <- data.table(currRule, tempTotErrs, totCorrect, totLength, ruleName, rule$options, list(emailErrorArray))
  names(returnTable) <- c('Columns','Errors', 'Valid','Total','Rules', 'Description', 'toColor')
  
  # return (list(tempTable, logVector))
  return (returnTable)
  
}

whatEmailRule <- function(form){
  if(form == "xxxxx@xxxx.xxx"){return ("^\\w+@[a-zA-Z_]+?\\.[a-zA-Z]{2,3}$")}
  else if(form == "xxxxx@xxxx.xx.xx"){return ("^\\w+@[a-zA-Z_]+?\\.[a-zA-Z]{2,3}\\.[a-zA-Z]{2,3}$")}
  else if(form == "xxxxx@xxxx.xxx(.xx)"){return ("^([a-zA-Z0-9_\\-\\.]+)@((\\[[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.)|(([a-zA-Z0-9\\-]+\\.)+))([a-zA-Z]{2,4}|[0-9]{1,3})(\\]?)$")}
}

# Rule 1 checks for format xxxxx@xxxx.xxx (i.e. johndoe@fpts.com)
# Rule 2 checks for format xxxxx@xxxx.xx.xx (i.e. johndoe@fpts.co.uk)
# Rule 3 checks for format xxxxx@xxxx.xxx or xxxxx@xxxx.xx.xx (i.e. johndoe@fpts.com or johndoe@fpts.co.uk)
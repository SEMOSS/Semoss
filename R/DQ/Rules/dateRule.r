# Data Quality Checker - Function that identifies incorrect date format

## Inputs: 
# dt: data table
# colList: column name w date
# form: desired date format
## Outputs:
# resultsTable: Table with the number of cells with correctly formatted dates, 
#            number of cells with incorrectly formatted dates, total number of cells 
#            and the rule it breaks ("Incorrect Date Format")

library(data.table)

dateRule <- function(dt, rule) {
  
  ruleName <- c("Date Format")  
  currCol <- rule$col
  currRule <- rule$rule
  
  numOptions = length(rule$options)
  options <- character(numOptions)
  for(i in 1:numOptions){
    options[i] <- whatDateRule(rule$options[i])
  }
  
  tempArray <- dt[, get(rule$col)]
  totLength <- length(tempArray)
  tempTotErrs <- 0
  
  dateErrorArray <- character(totLength)  # list of values to paint
  for(i in 1:totLength){
    valid = FALSE
    for(j in 1:numOptions){
      if(grepl(options[j], tempArray[i]) == TRUE){
        valid = TRUE
        break
      }
    }
    if(valid == FALSE){
      dateErrorArray[i] <- tempArray[i]
      tempTotErrs = tempTotErrs + 1
    }
  }
  
  description <- paste(rule$options, collapse = ", ")
  dateErrorArray <- dateErrorArray[!duplicated(dateErrorArray)]
  toPaint <- paste(dateErrorArray, collapse = "\", \"" )
  toPaint <- paste0('\"', toPaint, '\"')
  totCorrect <- totLength - tempTotErrs
  returnTable <- data.table(currCol, tempTotErrs, totCorrect, totLength, ruleName, description, currRule, toPaint)
  names(returnTable) <- c('Columns','Errors', 'Valid','Total','Rules', 'Description', 'ruleID', 'toColor')
  
  return (returnTable)
}

whatDateRule <- function(form){
  if(form == "mm/dd/yyyy"){return ("^[[:digit:]]{,2}\\/[[:digit:]]{,2}\\/[[:digit:]]{4}$")}
  else if(form == "month dd, yyyy"){return ("^(?:J(anuary|u(ne|ly))|February|Ma(rch|y)|A(pril|ugust)|(((Sept|Nov|Dec)em)|Octo)ber)[_]?(0?[1-9]|[1-2][0-9]|3[01]),[_](19[0-9]{2}|[2-9][0-9]{3}|[0-9]{2})$")}
  else if(form == "day, month dd, yyyy"){return ("^(?:\\s*(Sunday|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday),[_]*)(?:J(anuary|u(ne|ly))|February|Ma(rch|y)|A(pril|ugust)|(((Sept|Nov|Dec)em)|Octo)ber)[_]?(0?[1-9]|[1-2][0-9]|3[01]),[_](19[0-9]{2}|[2-9][0-9]{3}|[0-9]{2})$")}
  else if(form == "mon dd, yyyy"){return ("^(?:J(an|u(n|l))|Feb|Ma(r|y)|A(pr|ug)|Sep|Nov|Dec|Oct)[_]?(0?[1-9]|[1-2][0-9]|3[01]),[_](19[0-9]{2}|[2-9][0-9]{3}|[0-9]{2})$")}
}

# Rule 1 checks for format mm/dd/yyyy (i.e. January 12, 1995)
# Rule 2 checks for format month dd, yyyy (i.e. January 12, 1995)
# Rule 3 checks for format day, month dd, yyyy (i.e. Tuesday, January 12, 1995)
# Rule 4 checks for format mon dd, yyyy (i.e. Jan 12, 1995)
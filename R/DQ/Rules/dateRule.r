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
  
  ruleName <- c("Incorrect Date Format")  
  currCol <- rule$col
  currRule <- rule$rule
  
  regex <- whatDateRule(rule$options) #goes into options of the rule to pull out regex format desired
  
  tempArray <- dt[, get(rule$col)]
  totLength <- length(tempArray)
  tempTotErrs <- 0
  
  logVecName <- paste(currCol,currRule,sep="_")
  
  dateErrorArray <- character(totLength) #mirror array of column with valid/invalid if the cooresponding data point is valid or not
  
  for(i in 1:totLength){
    if(grepl(regex, tempArray[i]) == FALSE){
      dateErrorArray[i] <- 'invalid'
      tempTotErrs = tempTotErrs + 1
    }
    else if(is.na(tempArray[i])){
      dateErrorArray[i] <- 'invalid'
      tempTotErrs = tempTotErrs + 1
    }
    else{
      dateErrorArray[i] <- 'valid'
    }
  }
  logVector <- data.table(dateErrorArray)
  names(logVector) <- c(logVecName)
  totCorrect <- totLength - tempTotErrs
  tempTable <- data.table(currRule, tempTotErrs, totCorrect, totLength, ruleName, rule$options)
  names(tempTable) <- c('Columns','Errors', 'Valid','Total','Rules', 'Description')
  
  return (list(tempTable, logVector))
}

whatDateRule <- function(form){
  if(form == "mm/dd/yyyy"){return ("^[[:digit:]]{,2}\\/[[:digit:]]{,2}\\/[[:digit:]]{4}$")}
  else if(form == "month dd, yyyy"){return ("^(?:J(anuary|u(ne|ly))|February|Ma(rch|y)|A(pril|ugust)|(((Sept|Nov|Dec)em)|Octo)ber)[[:space:]]?(0?[1-9]|[1-2][0-9]|3[01]),[[:space:]](19[0-9]{2}|[2-9][0-9]{3}|[0-9]{2})$")}
  else if(form == "day, month dd, yyyy"){return ("^(?:\\s*(Sunday|Monday|Tueday|Wedday|Thuday|Friday|Satday),\\s*)(?:J(anuary|u(ne|ly))|February|Ma(rch|y)|A(pril|ugust)|(((Sept|Nov|Dec)em)|Octo)ber)[[:space:]]?(0?[1-9]|[1-2][0-9]|3[01]),[[:space:]](19[0-9]{2}|[2-9][0-9]{3}|[0-9]{2})$")}
  else if(form == "mon dd, yyyy"){return ("^(?:J(an|u(n|l))|Feb|Ma(r|y)|A(pr|ug)|Sep|Nov|Dec|Oct)[[:space:]]?(0?[1-9]|[1-2][0-9]|3[01]),[[:space:]](19[0-9]{2}|[2-9][0-9]{3}|[0-9]{2})$")}
}

# Rule 1 checks for format mm/dd/yyyy (i.e. January 12, 1995)
# Rule 2 checks for format month dd, yyyy (i.e. January 12, 1995)
# Rule 3 checks for format day, month dd, yyyy (i.e. Tuesday, January 12, 1995)
# Rule 4 checks for format mon dd, yyyy (i.e. Jan 12, 1995)
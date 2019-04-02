# Data Quality Checker - Function that identifies incorrect gender format

## Inputs: 
# dt: data table
# colList: column name w gender entries
# form: desired gender format
## Outputs:
# resultsTable: Table with the number of cells with correctly formatted emails, 
#            number of cells with incorrectly formatted emails, total number of cells 
#            and the rule it breaks ("Incorrect Gender Format")

library(data.table)

genderRule <- function(dt, rule){
  
  ruleName <- c("Incorrect Gender Format")
  currCol <- rule$col
  currRule <- rule$rule
  
  regex <- whatGenderRule(rule$options) #goes into options of the rule to pull out regex format desired
  
  tempArray <- dt[, get(rule$col)]
  totLength <- length(tempArray)
  tempTotErrs <- 0
  
  logVecName <- paste(currCol,currRule,sep="_")
  
  genderErrorArray <- character(totLength) #mirror array of column with valid/invalid if the cooresponding data point is valid or not
  
  for(j in 1:totLength){
    if(grepl(regex, tempArray[j]) == FALSE){
      genderErrorArray[j] <- 'invalid'
      tempTotErrs = tempTotErrs + 1
    }
    else if(is.na(tempArray[j])){
      genderErrorArray[j] <- 'invalid'
      tempTotErrs = tempTotErrs + 1
    }
    else{
      genderErrorArray[j] <- 'valid'
    }
  }
  logVector <- data.table(genderErrorArray)
  names(logVector) <- c(logVecName)
  totCorrect <- totLength - tempTotErrs
  tempTable <- data.table(currRule, tempTotErrs, totCorrect, totLength, ruleName, rule$options)
  names(tempTable) <- c('Columns','Errors', 'Valid','Total','Rules', 'Description')

  return (list(tempTable, logVector))
}

whatGenderRule <- function(form){
  if(form == "mM/fF"){return ("^(?i)(m|f)$")}
  else if(form == "m/f"){return ("^(m|f)$")}
  else if(form == "M/F"){return ("^(M|F)$")}
  else if(form == "mMale/fFemale"){return ("^(?i)(male|female)$")}
  else if(form == "male/female"){return ("^(male|female)$")}
  else if(form == "Male/Female"){return ("^(Male|Female)$")}
}

# Rule 1 checks for format "m" or "f" or "M" or "F"
# Rule 2 checks for format "male" or "female" or "Male" or "Female"
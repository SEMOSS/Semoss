





regexInput <- function(dt, rule){
  
  ruleName <- c("Regex Input")
  currCol <- rule$col
  currRule <- rule$rule
  
  numRegs <- length(rule$options)
  regex <- rule$options
  
  tempArray <- dt[, get(rule$col)]
  totLength <- length(tempArray)
  tempTotErrs <- 0
  regexErrorArray <- character(totLength)  # list of values to paint
  
  for(i in 1:totLength){
    valid = FALSE
    for(j in 1:numRegs){
      if(grepl(regex[j], tempArray[i]) == TRUE){
        valid = TRUE
        break
      }
    }
    if(valid == FALSE){
      regexErrorArray[i] <- tempArray[i]
      tempTotErrs = tempTotErrs + 1
    }
  }
  
  description <- "Check against regex input"
  regexErrorArray <- regexErrorArray[!duplicated(regexErrorArray)]
  toPaint <- paste(regexErrorArray, collapse = "\", \"" )
  toPaint <- paste0('\"', toPaint, '\"')
  totCorrect <- totLength - tempTotErrs
  returnTable <- data.table(currCol, tempTotErrs, totCorrect, totLength, ruleName, description, currRule, toPaint)
  names(returnTable) <- c('Columns','Errors', 'Valid','Total','Rules', 'Description', 'ruleID', 'toColor')
  
  return (returnTable)
  
}
library(data.table)

regexComparison <- function(dt, rule) {
  
  currCol <- rule$col
  currRule <- rule$rule
  numOptions <- length(rule$options)
  
  if(currRule == 'Regex Input') {
    regexArray <- rule$options
    description <- "Check against regex input"
  }
  else {
    regexArray <- getRegex(rule$options, numOptions, currRule)
    description <- paste(rule$options, collapse = ", ")
  }
  
  tempArray <- dt[, get(rule$col)]
  dataType <- sapply(tempArray[1], class)
  totLength <- length(tempArray)
  totErrors <- 0
  errorArray <- character(totLength)  # list of values to paint
 
  for(i in 1:totLength){
    valid = FALSE
    for(j in 1:numOptions){
      if(grepl(regexArray[j], tempArray[i]) == TRUE){
        valid = TRUE
        break
      }
    }
    if(valid == FALSE){
      errorArray[i] <- tempArray[i]
      totErrors = totErrors + 1
    }
  }
  
  errorArray <- na.omit(errorArray[!duplicated(errorArray)])
  errorArray <- errorArray[!errorArray %in% ""]
  toPaint <- c("", errorArray)
  if(dataType == "character") {
    toPaint <- paste(toPaint, collapse = "\", \"")
    toPaint <- paste0('\"', toPaint, '\"')
  }
  else if(dataType == "numeric") {
    toPaint <- paste(errorArray, collapse = ", ")
  }
  toPaint <- paste(toPaint, "null", sep = ', ')
  totCorrect <- totLength - totErrors
  returnTable <- data.table(currCol, totErrors, totCorrect, totLength, currRule, description, toPaint)
  names(returnTable) <- c('Columns', 'Errors', 'Valid', 'Total', 'Rules', 'Description', 'toColor')
  
  return (returnTable)
}
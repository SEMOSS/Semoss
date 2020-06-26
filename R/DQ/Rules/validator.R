library(data.table)

validator <- function(dt, rule){
  
  currCol <- rule$col
  currRule <- rule$rule
  
  tempArray <- dt[, get(rule$col)]
  dataType <- sapply(tempArray[1], class)
  totLength <- length(tempArray)
  
  toValidateArray <- rule$options
  toValidate <- length(toValidateArray)
  
  vec <- (tempArray %in% toValidateArray)
  totCorrect <- sum(vec, na.rm = TRUE)
  totErrors <- totLength - totCorrect
  
  errorArray <- character(totLength)
  vec <- as.integer(!vec)
  for(i in 1:totLength) {
    if(vec[i] == 1) {
      errorArray[i] <- tempArray[i]
    }
  }
  errorArray <- na.omit(errorArray[!duplicated(errorArray)])
  if(dataType == "character") {
    toPaint <- paste(errorArray, collapse = "\", \"" )
    toPaint <- paste0('\"', toPaint, '\"')
  }
  else if(dataType == "numeric") {
    toPaint <- paste(errorArray, collapse = ", ")
  }
  toPaint <- paste(toPaint, "null", sep = ', ')
  description <- paste(toValidateArray, collapse = ", ")
  returnTable <- data.table(currCol, totErrors, totCorrect, totLength, currRule, description, toPaint)
  names(returnTable) <- c('Columns', 'Errors', 'Valid', 'Total', 'Rules', 'Description', 'toColor')
  
  return (returnTable)  
}
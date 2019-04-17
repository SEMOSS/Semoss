library(data.table)

validator <- function(dt, rule){
  
  ruleName <- c("Validate Values")
  currCol <- rule$col
  currRule <- rule$rule
  
  tempArray <- dt[, get(rule$col)]
  totLength <- length(tempArray)
  
  toValidateArray <- rule$options
  toValidate <- length(toValidateArray)
  
  vec <- (tempArray %in% toValidateArray)
  totCorrect <- sum(vec, na.rm = TRUE)
  tempTotErrs <- totLength - totCorrect
  
  toPaint <- character(totLength)
  vec <- as.integer(!vec)
  for(i in 1:totLength) {
    if(vec[i] == 1) {
      toPaint[i] <- tempArray[i]
    }
  }
  toPaint <- toPaint[!duplicated(toPaint)]
  toPaint <- paste(toPaint, collapse = "\", \"" )
  toPaint <- paste0('\"', toPaint, "null", '\"')
  description <- paste(toValidateArray, collapse = ", ")
  returnTable <- data.table(currCol, tempTotErrs, totCorrect, totLength, ruleName, description, currRule, toPaint)
  names(returnTable) <- c('Columns','Errors', 'Valid','Total','Rules', 'Description', 'ruleID', 'toColor')
  
  return (returnTable)  
}
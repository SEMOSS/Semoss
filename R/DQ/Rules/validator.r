library(data.table)

validator <- function(dt, rule){
  
  ruleName <- c("Validator")
  currCol <- rule$col
  currRule <- rule$rule
  
  tempArray <- dt[, get(rule$col)]
  totLength <- length(tempArray)
  
  toValidateArray <- rule$options
  toValidate <- length(toValidateArray)
  
  vec <- (tempArray %in% toValidateArray)
  numValid <- sum(vec, na.rm = TRUE)
  numInvalid <- totLength - numValid
  
  toColor <- character(totLength)
  vec <- as.integer(!vec)
  for(i in 1:totLength) {
    if(vec[i] == 1) {
      toColor[i] <- tempArray[i]
    }
  }
  toColor <- toColor[!duplicated(toColor)]
  
  description <- paste(toValidateArray, collapse = ", ")
  returnTable <- data.table(currCol, numInvalid, numValid, totLength, ruleName, description, list(toColor))
  names(returnTable) <- c('Columns','Errors', 'Valid','Total','Rules', 'Description', 'toColor')
  
  return (returnTable)  
}
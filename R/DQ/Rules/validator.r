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
  
  vec <- as.integer(!vec)
  vec[vec == 0] <- 'valid'
  vec[vec == 1] <- 'invalid'

  logVecName <- paste(currCol,currRule,sep="_")
  logVector <- data.table(vec)
  names(logVector) <- c(logVecName)
  
  description <- paste(toValidateArray, collapse = ", ")
  tempTable <- data.table(rule$col, numInvalid, numValid, totLength, ruleName, description)
  names(tempTable) <- c('Columns', 'Errors', 'Valid', 'Total','Rules', 'Description')
  
  return (list(tempTable, logVector))
}
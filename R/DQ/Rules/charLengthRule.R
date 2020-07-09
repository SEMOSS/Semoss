charLengthRule <- function(dt, rule) {

  ruleName <- c("Character Length")
  currCol <- rule$col
  numOptions <- rule$options
  ruleName <- paste0(ruleName, " ", paste0(numOptions, collapse=", "))
  tempArray <- dt[, get(rule$col)]
  tempArray <- nchar(tempArray)
  ####### this can be altered to do <=, >=, !=, >, <
  #tempArray <- tempArray != numOptions
  tempArray <- unlist(lapply(tempArray, function(e){return (!(e %in% numOptions)) }))
  totLength <- length(tempArray)
  totCorrect <- totLength - sum(tempArray)
  totErrors <- totLength - totCorrect
  ##### grab unique values that are wrong
  toPaint <-  dt[, get(rule$col)][tempArray==TRUE]
  toPaint <- paste(unique(toPaint), collapse = "\", \"" )
  toPaint <- paste0('\"', toPaint, '\"')
  
  returnTable <- data.table(currCol, totErrors, totCorrect, totLength, ruleName, "", toPaint)
  names(returnTable) <- c('Columns', 'Errors', 'Valid', 'Total', 'Rules', 'Description', 'toColor')
  
  return (returnTable)
}
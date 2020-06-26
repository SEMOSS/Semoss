# Data Quality Checker - Function that identifies duplicated cells

## Inputs: 
# dt: data table
# colList: column names to check for repeated cells
## Outputs:
# resultsTable: Table with the number of valid cells, number of repeated cells, total number of cells 
#            and the rule it breaks ("Duplicates") 

duplicates <- function(dt, rule) {
  
  currCol <- rule$col
  currRule <- rule$rule
  
  tempArray <- dt[, get(rule$col)]
  totLength <- length(tempArray)

  
  # Identify duplicates
  idx <- as.integer(duplicated(tempArray,incomparables=NA) | duplicated(tempArray, incomparables=NA,fromLast = TRUE))
  totErrors <- sum(idx, na.rm = FALSE)
  totCorrect <- totLength - totErrors - sum(is.na(tempArray))
  tempArray <- tempArray[duplicated(tempArray)]
  tempArray <- tempArray[!duplicated(tempArray)]
  toPaint <- paste(tempArray, collapse = "\", \"" )
  toPaint <- paste0('\"', toPaint, '\"')
  
  returnTable <- data.table(currCol, totErrors, totCorrect, totLength, currRule, "", toPaint)
  names(returnTable) <- c('Columns', 'Errors', 'Valid', 'Total', 'Rules', 'Description', 'toColor')
  
  return (returnTable)
}
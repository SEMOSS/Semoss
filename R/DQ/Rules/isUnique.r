# Data Quality Checker - Function that identifies duplicated cells

## Inputs: 
# dt: data table
# colList: column names to check for repeated cells
## Outputs:
# resultsTable: Table with the number of valid cells, number of repeated cells, total number of cells 
#            and the rule it breaks ("Duplicates") 

duplicates <- function(dt, rule) {
  
  ruleName <- c("Duplicates")
  currCol <- rule$col
  currRule <- rule$rule
  
  tempArray <- dt[, get(rule$col)]
  totLength <- length(tempArray)
  
  # Identify duplicates
  idx <- as.integer(duplicated(tempArray,incomparables=NA) | duplicated(tempArray, incomparables=NA,fromLast = TRUE))

  tempArray <- tempArray[duplicated(tempArray)]
  
  # Calculate values
  totErrs <- sum(idx, na.rm = TRUE)
  totCorrect <- totLength - totErrs - sum(is.na(tempArray))
  
  returnTable <- data.table(currRule, totErrs, totCorrect, totLength, ruleName, rule$options, list(tempArray))
  names(returnTable) <- c('Columns','Errors', 'Valid','Total','Rules', 'Description', 'toColor')
  
  # return (list(tempTable, logVector))
  return (returnTable)
}
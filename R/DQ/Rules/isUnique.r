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
  
  logVecName <- paste(currCol,currRule,sep="_")
  
  # Identify duplicates
  idx <- as.integer(duplicated(tempArray,incomparables=NA) | duplicated(tempArray, incomparables=NA,fromLast = TRUE))

  # Calculate values
  totErrs <- sum(idx, na.rm = TRUE)
  totCorrect <- totLength - totErrs - sum(is.na(tempArray))
  
  # Construct table
  idx[idx == 1] <- 'invalid'
  idx[idx == 0] <- 'valid'
  logVector <- data.table(idx)
  names(logVector) <- c(logVecName)
  tempTable <- data.table(rule$col, totErrs, totCorrect, totLength, ruleName, "N/A")
  names(tempTable) <- c('Columns','Errors', 'Valid','Total','Rules', 'Description')

  return (list(tempTable, logVector))
}
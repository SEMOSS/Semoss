# Data Quality Checker - Function that identifies nulls/blanks

## Inputs: 
# dt: data table
# colList: column names to check for NA cells
## Outputs:
# resultsTable: Table with the number of valid cells, number of incomplete cells, total number of cells 
#            and the rule it breaks ("Null/Blank")

library(data.table)

## 
## allCols: boolean - true checks for NA in entire doc; false checks for NA in specified cols
## cols: array indicating which collumns to check for NA vals if allCols is false
## dt: data table read in passed to function
isNull <- function(dt, rule){

  currCol <- rule$col
  currRule <- rule$rule
  
  tempArray <- dt[, get(currCol)]
  totLength <- length(tempArray)
  
  totErrors <- sum(is.na(tempArray))
  totCorrect <- totLength - totErrors
  toPaint <- c("\"\", null")
  returnTable <- data.table(currCol, totErrors, totCorrect, totLength, currRule, "", toPaint)
  names(returnTable) <- c('Columns', 'Errors', 'Valid', 'Total', 'Rules', 'Description', 'toColor')

  return (returnTable)
}
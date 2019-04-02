# Data Quality Checker - Function that returns index of specific value

## Inputs:
# allData: data to be checked
# ValList: list of values to check for in data
# colList: list of colums to check for values in valList
# 
## Outputs:
# FoundValue: Returns an array with the found value index. If none, vector is blank

findValue <- function(dt, rule){
  
  ruleName <- c("Is In Column")

  # numCols <- length(rule$cols)
  description = character()

  tempValFactor <- factor(rule$options)
  tempValArray <- sort(rule$options)
  
  tempColFactor <- factor(dt[, get(rule$col)])
  totLength <- length(tempColFactor)
  matchesTable <- (table(tempValFactor[match(tempColFactor, tempValFactor)]))

  for(i in 1:length(matchesTable)){
    tempDescription <- paste(tempValArray[i], matchesTable[i][1], sep = ":")
    description <- paste(description, tempDescription, sep = " | ")
  }
  
  tempTable <- data.table(rule$col, "N/A", "N/A", totLength, ruleName, description)
  names(tempTable) <- c('Columns','Errors', 'Valid','Total','Rules','Description')

  return(tempTable)
}
rule1 <- list(name = "isBlank", description = "Identifies blank and null cells", format = c())
rule2 <- list(name = "isUnique", description = "Identifies duplicated cells", format = c())
rule3 <- list(name = "dateRule", description = "Identifies incorrectly formatted date entries", format = c("mm/dd/yyyy", "month dd, yyyy", "day, month dd, yyyy", "mon dd, yyyy"))
rule4 <- list(name = "emailRule", description = "Identifies incorrectly formatted email entries", format = c("xxxxx@xxxx.xxx", "xxxxx@xxxx.xx.xx", "xxxxx@xxxx.xxx or xxxxx@xxxx.xx.xx"))
rule5 <- list(name = "genderRule", description = "Identifies incorrectly formatted gender entries", format = c("M or F", "Male or Female"))
rule6 <- list(name = "nameRule", description = "Identifies incorrectly formatted name entries", format = c("Last, First MI", "First Last"))
rule7 <- list(name = "findValues", description = "Can take in a list of values and check how many times each value is in a specified column or columns", format = "na")

availableRules <- list(rule1, rule2, rule3, rule4, rule5, rule6, rule7)

getRules <- function(){
  ruleNames = c()
  for (items in availableRules) {
    ruleNames = c(ruleNames, items$name)
  }
  return (ruleNames)
}

getDescription <- function(rule){
  for (items in availableRules) {
    if (rule == items$name){
      ruleDescription = items$description
    }
  }
  return (ruleDescription)
}

getFormat <- function(rule){
  for (items in availableRules) {
    if (rule == items$name){
      ruleFormat = items$format
    }
  }
  return (ruleFormat)
}
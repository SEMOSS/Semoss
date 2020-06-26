
sourceFiles <- function(base){
  source(paste(base, "missionControl.R", sep = ""))

  source(paste(base, "Rules/isNull.R", sep = ""))
  source(paste(base, "Rules/isUnique.R", sep = ""))
  source(paste(base, "Rules/findValue.R", sep = ""))
  source(paste(base, "Rules/validator.R", sep = ""))
  source(paste(base, "Rules/charLengthRule.R", sep = ""))
  source(paste(base, "Rules/regexComparison.R", sep = ""))
  source(paste(base, "Rules/whatRegexValue.R", sep = ""))
}
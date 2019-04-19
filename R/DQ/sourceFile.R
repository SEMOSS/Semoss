
sourceFiles <- function(base){
  source(paste(base, "missionControl.R", sep = ""))

  source(paste(base, "Rules/isNull.r", sep = ""))
  source(paste(base, "Rules/isUnique.r", sep = ""))
  source(paste(base, "Rules/findValue.r", sep = ""))
  source(paste(base, "Rules/validator.r", sep = ""))
  
  source(paste(base, "Rules/regexComparison.R", sep = ""))
  source(paste(base, "Rules/whatRegexValue.R", sep = ""))
}

sourceFiles <- function(base){
  source(paste(base, "missionControl.R", sep = ""))

  source(paste(base, "Rules/isNull.r", sep = ""))
  source(paste(base, "Rules/isUnique.r", sep = ""))
  source(paste(base, "Rules/dateRule.r", sep = ""))
  source(paste(base, "Rules/emailRule.r", sep = ""))
  source(paste(base, "Rules/findValue.r", sep = ""))
  source(paste(base, "Rules/genderRule.r", sep = ""))
  source(paste(base, "Rules/nameRule.r", sep = ""))
  source(paste(base, "Rules/validator.r", sep = ""))
  source(paste(base, "Rules/regexInput.R", sep = ""))
}
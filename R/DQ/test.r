# Data Quality Checker Function Tester

library(data.table)

# Set up
cat("\014")
setwd("C:/workspace/Semoss_Dev/R/DQ")


dt <- fread("patientdq.csv", header = TRUE, sep = ",", fill = TRUE)
dt[dt == ""] <- NA # makes empty cells NA
dt <- data.table(dt, name = paste(dt$last_name, dt$first_name, sep = ", "))

resultsTable <- data.table(Columns=character(),Errors=integer(),Valid=integer(),Total=integer(),Rules=character(), Description=character())

rule <- list(rule = "duplicates", col = "gender", options = "")
# nullRuleMap2 <- list(rule = "blanks", col = "prescription_processed_date", options = "")
# nullRuleMap3 <- list(rule = "blanks", col = "gender", options = "")
# nullRuleMap4 <- list(rule = "blanks", col = "drug_manufacturer", options = "")
rulesList <- list(rule) #, nullRuleMap2, nullRuleMap3, nullRuleMap4)
return <- missionControl(dt, rulesList, resultsTable)
resultsTable <- return[[2]]
dt <- return[[1]]
# # 
genderRuleMap <- list(rule = "gender", col = "gender", options = "mM/fF")
# genderRuleMap2 <- list(rule = "gender", col = "gender", options = "mMale/fFemale")
# rulesList <- list(genderRuleMap), genderRuleMap2)
# resultsTable <- missionControl(dt, rulesList, data.table(Columns=character(),Errors=integer(),Valid=integer(),Total=integer(),Rules=character(), Description=character()))[[2]]
return <- missionControl(dt, rulesList, resultsTable)
resultsTable <- return[[2]]
dt <- return[[1]]

dateRuleMap <- list(rule = "date", col = "prescription_processed_date", options = "mm/dd/yyyy")
# dateRuleMap2 <- list(rule = "date", col = "prescription_processed_date", options = "day, month dd, yyyy")
rulesList <- list(dateRuleMap)#, dateRuleMap2)
return <- missionControl(dt, rulesList, resultsTable)
resultsTable <- return[[2]]
dt <- return[[1]]
# 
# emailRuleMap <- list(rule = "email", col = "email", options = "xxxxx@xxxx.xxx")
# emailRuleMap2 <- list(rule = "email", col = "email", options = "xxxxx@xxxx.xxx(.xx)")
# rulesList <- list(emailRuleMap, emailRuleMap2)
# resultsTable <- missionControl(dt, rulesList, resultsTable)[[2]]
# 
# nameRuleMap <- list(rule = "name", col = "name", options = "last, first (m.)")
# nameRuleMap2 <- list(rule = "name", col = "name", options = "first last")
# rulesList <- list(nameRuleMap, nameRuleMap2)
# resultsTable <- missionControl(dt, rulesList, resultsTable)[[2]]
# 
# uniqueRuleMap <- list(rule = "duplicates", col = "prescription_processed_date", options = "")
# uniqueRuleMap2 <- list(rule = "duplicates", col = "Pharmacist", options = "")
# uniqueRuleMap3 <- list(rule = "duplicates", col = "drug_manufacturer", options = "")
# rulesList <- list(uniqueRuleMap, uniqueRuleMap2, uniqueRuleMap3)
# resultsTable <- missionControl(dt, rulesList, resultsTable)[[2]]
# 
# 
# findValueRuleMap <- list(rule = "findVals", col = "drug_manufacturer", options = c("Sanofi","AstraZeneca", "GlaxoSmithKline", "Abbott Laboratories", "ABC Pharamacy", "Oasis Drug Manufacturers"))
# findValueRuleMap2 <- list(rule = "findVals", col = "drug", options = c("Crestor", "Nexium"))
# rulesList <- list(findValueRuleMap, findValueRuleMap2)
# resultsTable <- missionControl(dt, rulesList, resultsTable)[[2]]

rule <- list(rule = "validate", col = "drug_manufacturer", options = c("Sanofi", "GlaxoSmithKline", "Abbott Laboratories", "ABC Pharamacy", "Oasis Drug Manufacturers"))
rulesList <- list(rule)
return <- missionControl(dt, rulesList, resultsTable)
resultsTable <- return[[2]]
dt <- return[[1]]


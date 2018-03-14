getDqFrame <- function(df, issueFrame) {
	library(validate)
	originalRules <- issueFrame$rule
	issueFrame$translatedRules <- ""

	# figure out translated rule from E object
	for(i in 1:nrow(issueFrame)) {
	  rule <- issueFrame$rule[i]
	  ruleEScript <- paste("V<- validator(",rule,")", sep="") 
	  print(ruleEScript)
	  tryCatch({
	    eval(parse(text=ruleEScript))
		if(exists('V')) {
			editFrame <- as.data.frame(V)
			translatedRule <- editFrame$rule
			issueFrame$translatedRules[i] <- translatedRule
	    }
	  }, error = function(e) {
	  
	  })
	
	  rm(V, editFrame, translatedRule, ruleEScript)
	}

	# now create V with valid rules 
	originalRules <- issueFrame[which(issueFrame$translatedRules != ""),]
	testDF <- data.frame(rule = originalRules$rule)
	v <- validator(.data=testDF)

	# check violations
	cf <- confront(df, v)
	veFrame <- summary(cf)
	
	# join issueFrame name description columns with originalRules
	joinDf <- merge (veFrame, issueFrame, by.x = c("expression") , by.y = c("translatedRules"), all.y = TRUE)
	joinDf <- subset(joinDf, select=-c(name.x, expression, warning))
	colnames(joinDf) <- c("Count", "Valid_Count", "Invalid_Count", "Number_of_NAs", "Rule_Error", "Rule_Name", "Description", "Rule")
	return(joinDf)
}
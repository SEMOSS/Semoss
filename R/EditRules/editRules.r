getDqFrame <- function(df, issueFrame) {
	library(editrules)
	originalRules <- issueFrame$rule
	ruleDF <- data.frame(originalRules)
	ruleDF$translatedRules <- ""

	# figure out translated rule from E object
	for(i in 1:nrow(ruleDF)) {
	  rule <- ruleDF$originalRules[i]
	  ruleEScript <- paste("E<- editset(expression(", rule, "))", sep="") 

	  tryCatch({
	    eval(parse(text=ruleEScript))
		  
		if(exists('E')) {
			editFrame <- as.data.frame(E)
			translatedRule <- editFrame$edit
			ruleDF$translatedRules[i] <- translatedRule
	    }
	  }, error = function(e) {
	  })
	
	  rm(E, editFrame, translatedRule, ruleEScript)
	}

	# now create E with valid rules 
	ruleDF <- ruleDF[which(ruleDF$translatedRules != ""),]
	originalRules <- as.vector(ruleDF$originalRules)
	E <- editset(originalRules)

	# get constraints in frame
	editFrame <- as.data.frame(E)
	editNames <- editFrame$name

	# check violations
	ve <- violatedEdits(E, df)
	veFrame <- as.data.frame(ve)

	# add results to data quality frame
	dqFrame <- data.frame(originalRules)
	dqFrame$validCount <- ""
	dqFrame$invalidCount <- ""
	dqFrame$totalCount <- ""
	for(i in 1:nrow(editFrame)) {
		col <- editNames[i]
		# get valid count syntax looks like this: validCount <- sum(veFrame$col == FALSE )
		validCountScript <- paste("validCount <- sum(veFrame$", col, "== FALSE)", sep="") 
		eval(parse(text=validCountScript))
		dqFrame$validCount[i] <- validCount
		
		# get invalid count syntax looks like this: invalidCount <- sum(veFrame$col == TRUE )
		invalidCountScript <- paste("invalidCount <- sum(veFrame$", col, "== TRUE)", sep="") 
		eval(parse(text=invalidCountScript))
		dqFrame$invalidCount[i] <- invalidCount

		# get Total count
		totalCount <- validCount + invalidCount
		dqFrame$totalCount <- totalCount
	}
	
	# join issueFrame name description columns with originalRules
	joinDf <- merge (dqFrame, issueFrame, by.x = c("originalRules") , by.y = c("rule"), all.y = TRUE)
	return(joinDf)
}
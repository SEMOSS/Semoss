createCF <- function (df, issueFrame) {
	library(validate)
	originalRules <- issueFrame$rule
	issueFrame$translatedRules <- ""
	# figure out translated rule from V object
	for(i in 1:nrow(issueFrame)) {
	  rule <- issueFrame$rule[i]
	  ruleEScript <- paste("V<- validator(",rule,")", sep="") 
	  tryCatch({
	    eval(parse(text=ruleEScript))
		if(exists('V')) {
			editFrame <- as.data.frame(V)
			translatedRule <- editFrame$rule
			issueFrame$translatedRules[i] <- translatedRule
	    }
	  }, error = function(e) {})
	  rm(V, editFrame, translatedRule, ruleEScript)
	}

	# now create V with valid rules 
	originalRules <- issueFrame[which(issueFrame$translatedRules != ""),]
	testDF <- data.frame(rule = originalRules$rule)
	v <- validator(.data=testDF)

	# check violations
	cf <- confront(df, v)
	
	# add rule alias to issueFrame
	vFrame <- as.data.frame(v)
	vFrame <- subset(vFrame, select=-c(label, origin, description, created))
	colnames(vFrame) <- c("translatedRules", "ruleAlias")
	issueFrame <- merge (vFrame, issueFrame, by.x = c("translatedRules") , by.y = c("translatedRules"), all.y = TRUE)

	return (list(cf=cf, issueFrame=issueFrame))
}

getDqFrame <- function(cf, issueFrame) {
	# check violations
	summaryFrame <- summary(cf)
	summaryFrame <- subset(summaryFrame, select=-c(name, warning))
	# join issueFrame name description columns with originalRules
	joinDf <- merge (summaryFrame, issueFrame, by.x = c("expression") , by.y = c("translatedRules"), all.y = TRUE)
	joinDf <- subset(joinDf, select=-c(expression, ruleAlias))
	colnames(joinDf) <- c("Total_Count", "Valid_Count", "Invalid_Count", "Number_of_NAs", "Rule_Error", "Rule_Name", "Description", "Rule", "InputColumns")
	return(joinDf)
}

getErrorIndex <- function(cf, issueFrame) {
	values <- values(cf)
	vdat <- as.data.frame(values)
	size <- nrow(vdat)
	errorFrameColSize <- ncol(vdat)
	errorFrame <- data.frame(matrix(NA, nrow=nrow(vdat), ncol=errorFrameColSize))
	for(i in colnames(vdat)) {
		errorIndexScript <- paste("indicesPerRule <- which(vdat$",i,"== FALSE)", sep="")
		eval(parse(text=errorIndexScript))
		#indicesPerRule <- which(vdat$V2 == FALSE)
		ruleRow <- issueFrame[which(issueFrame$ruleAlias == i),]
		ruleName <- as.character(ruleRow$name)

		#adding error indicies for rule name
		indexCol <- as.vector(indicesPerRule)
		length(indexCol) <- size
		errorFrame <- cbind(errorFrame, indexCol)
		# rename col to rule name
		colnames(errorFrame)[colnames(errorFrame)=="indexCol"] <- ruleName
	}
	
	# retrieves indices where all validations = false 
	# only for more than 1 rule
	if(errorFrameColSize > 1) {
		indicesAllRules <- which(apply(values, 1, function(x) all(x == FALSE))) 
		length(indicesAllRules) <- size

		errorFrame$all <- indicesAllRules
		colnames(errorFrame)[colnames(errorFrame)=="all"] <- "All_Rules"

	}
	errorFrame <- subset(errorFrame, select=-c(1:errorFrameColSize))	

	return (errorFrame)
}

getErrorFrame <- function(cf, issueFrame) {
	values <- values(cf)
	vdat <- as.data.frame(values)
	errorFrameColSize <- ncol(vdat)
	for(i in colnames(vdat)) {
		ruleRow <- issueFrame[which(issueFrame$ruleAlias == i),]
		ruleName <- as.character(ruleRow$name)
		colnames(vdat)[colnames(vdat)==i] <- ruleName
	}
	
	# retrieves indices where all validations = false only for more than 1 rule
	if(errorFrameColSize > 1) {
		allRules <- apply(values, 1, function(x) all(x == FALSE))
		vdat$all <- allRules
		colnames(vdat)[colnames(vdat)=="all"] <- "All_Rules"
	}

	return (vdat)
}

###############################################
# Add rule columns to dataframe
###############################################
getDF <- function (df, errorFrame) {
	df <- cbind(df,errorFrame)
	return (df)
}


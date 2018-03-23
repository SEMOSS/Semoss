run.seq <- function(x) as.numeric(ave(paste(x), x, FUN = seq_along))

escapeRegexR <- function(greplExpr){
	splitRule <- strsplit(as.character(greplExpr), '"')[[1]]
	lengthMinusOne <- length(splitRule) - 1
	regex <- paste(splitRule[2:as.double(lengthMinusOne)], sep="")
	regex <- stri_escape_unicode(regex)
	greplExpr <- paste(splitRule[1],'"',regex,'"', splitRule[length(splitRule)], sep="")
}

createCF <- function (df, issueFrame) {
	lapply(list('validate', 'stringi'), require, character.only = TRUE)
	originalRules <- issueFrame$rule
	issueFrame$translatedRules <- ""
	# figure out translated rule from V object
	for(i in 1:nrow(issueFrame)) {
	  rule <- issueFrame$rule[i]
	  
	  if(grepl("^(grepl).*", rule)) rule <- escapeRegexR(rule)
	  
	  ruleEScript <- paste("V<- validator(",rule,")", sep="") 
	  tryCatch({
	    eval(parse(text=ruleEScript))
		if(exists('V')) {
			editFrame <- as.data.frame(V)
			translatedRule <- editFrame$rule
			issueFrame$translatedRules[i] <- translatedRule
	    }
	  }, error = function(e) {})
	  rm(V, editFrame, translatedRule, ruleEScript, splitRule, lengthMinusOne, regex)
	}

	# now create V with valid rules 
	validRules <- issueFrame[which(issueFrame$translatedRules != ""),]
	testDF <- data.frame(rule = validRules$translatedRules)
	v <- validator(.data=testDF)

	# check violations
	cf <- confront(df, v)
	
	# add rule alias to issueFrame
	vFrame <- as.data.frame(v)
	vFrame <- subset(vFrame, select=-c(label, origin, description, created))
	colnames(vFrame) <- c("translatedRules", "ruleAlias")
	
	L <- list(issueFrame,vFrame)
	L2 <- lapply(L, function(x) cbind(x, run.seq = run.seq(x$translatedRules)))
	issueFrame <- Reduce(function(...) merge(..., all = TRUE), L2)[-2]

	return (list(cf=cf, issueFrame=issueFrame))
}

getDqFrame <- function(cf, issueFrame) {
	# check violations
	summaryFrame <- summary(cf)
	summaryFrame <- subset(summaryFrame, select=-c(name, warning))
	# rename columns
	colnames(summaryFrame) <- c("Total_Count", "Valid_Count", "Invalid_Count", "Number_of_NAs", "Rule_Error", "translatedRules")
	colnames(issueFrame) <- c("translatedRules", "Name", "Description", "Rule", "InputColumns", "ruleAlias")
	
	# merge issueFrame with summaryFrame by translatedRules
	L <- list(issueFrame,summaryFrame)
	L2 <- lapply(L, function(x) cbind(x, run.seq = run.seq(x$translatedRules)))
	joinDf <- Reduce(function(...) merge(..., all = TRUE), L2)[-2]
	
	# drop translatedRules, ruleAlias
	joinDf <- subset(joinDf, select=-c(translatedRules, ruleAlias))
	return (joinDf)
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



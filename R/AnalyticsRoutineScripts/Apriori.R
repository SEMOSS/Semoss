runApriori <- function(dt, attrList, transactionIdList = NULL, support = NULL, confidence = NULL, maxlen = NULL, sortBy = "lift", lhsSpecified = NULL, rhsSpecified = NULL) 
{
	lapply(list('data.table', 'arules', 'dpylr'), require, character.only = TRUE)
	
	set.seed(123)
		
	#data prep / create transaction object
	txn <- 
		#if transactionIdList==null, each row could be a unique transaction; else group unique transaction by id
		if(!is.null(transactionIdList)){
			tempDt <- dt[, c(attrList, transactionIdList), with=FALSE]			
			aggData <- 
				if (length(transactionIdList) > 1) {
					tempDt[, transactionId_ := do.call(paste, c(.SD, sep = "_")), .SDcols = transactionIdList]
					split(tempDt[[attrList]], tempDt$transactionId_)
				} else {
					split(tempDt[[attrList]], tempDt[[transactionIdList]])
				}
			aggData <- lapply(1:length(aggData), function(i) paste0(unique(aggData[[i]])))
			as(aggData, "transactions")
		} else {
			tempDt <- dt[, attrList, with=FALSE] 
			nonFactorCols <- names(tempDt)[sapply(tempDt, function(x) !is.factor(x))]
			if (length(nonFactorCols) > 0 ) {tempDt[,(nonFactorCols):= lapply(.SD, as.factor), .SDcols = nonFactorCols]}
			as(tempDt, "transactions")
		}
	
	#create parameter list 
	parameter <- list(minlen=2)
	if (!is.null(support)) {parameter$supp <- support}
	if (!is.null(confidence)) {parameter$conf <- confidence}
	if (!is.null(maxlen)) {parameter$maxlen <- maxlen}
	
	#create appearance list if needed
	appearance = list()
	if (!is.null(lhsSpecified)){
		lhsList <- lapply(lhsSpecified, function(x) paste(x, levels(tempDt[[x]]), sep='=')) 
		lhsVector <- as.vector(unlist(lhsList))
		appearance$lhs <- lhsVector
	}
	if (!is.null(rhsSpecified)){
		appearance$rhs <- paste(rhsSpecified, levels(tempDt[[rhsSpecified]]), sep='=')
	}
	if (!is.null(lhsSpecified) && is.null(rhsSpecified)) {
		appearance$default <- "rhs"
	} else if (is.null(lhsSpecified) && !is.null(rhsSpecified)) {
		appearance$default <- "lhs"
	} else if (!is.null(lhsSpecified) && !is.null(rhsSpecified)) {
		appearance$default <- "none"
	}
	
	#get rules
	rules <- apriori(txn, parameter, appearance, control = list(verbose=F))
	rulesLength <- length(rules)
	
	if (rulesLength == 0) {
		return (list=(rulesLength=rulesLength))
	} else {
		#find and remove redundant rules
		rules <- sort(rules, by = sortBy)
		uniqueRules <- rules[!is.redundant(rules)]
		
		#convert rules object to data table for export
		str <- paste0("data.table( lhs = labels( lhs(uniqueRules) ), rhs = labels( rhs(uniqueRules) ), quality(uniqueRules) )[ order(-", sortBy, "),]")
		rulesDt <- eval(parse(text=str)) %>% .[, c("lhs", "rhs") := lapply(.SD, function(x) gsub("[{}]", "", x)), .SDcols=c("lhs", "rhs")]
		return (list(rulesLength=length(uniqueRules), rulesDt=rulesDt))
	}
}


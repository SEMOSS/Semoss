###OOB ERROR ESTIMATE EXPLAINED
#In random forests, there is no need for cross-validation or a separate test set to get an unbiased estimate of the test set error. It is #estimated internally, during the run, as follows:
#
#Each tree is constructed using a different bootstrap sample from the original data. About one-third of the cases are left out of the #bootstrap sample and not used in the construction of the kth tree.
#
#Put each case left out in the construction of the kth tree down the kth tree to get a classification. In this way, a test set classification #is obtained for each case in about one-third of the trees. At the end of the run, take j to be the class that got most of the votes every #time case n was oob. The proportion of times that j is not equal to the true class of n averaged over all cases is the oob error estimate. #This has proven to be unbiased in many tests.


getRF <- function(dt, instanceCol, attrColList, options=NULL){	
	#randomForest package is not compatible with data table
	
	lapply(list('data.table', 'randomForest', 'dplyr'), require, character.only = TRUE)
	set.seed(123)
	
	tempDt <- dt[complete.cases(dt), c(instanceCol, attrColList), with=FALSE]

	#convert columns of character class to factor class
	charCols <- colnames(tempDt)[which(as.vector(tempDt[,lapply(.SD, class)]) == "character")]
	if (length(charCols) > 0 ) {tempDt[,(charCols):= lapply(.SD, as.factor), .SDcols = charCols]}
	
	#build formula & get tree
	formula <- paste0(instanceCol, " ~ .")
	rf <- 
		if (is.null(options) || nchar(options) == 0) {
			randomForest(as.formula(formula), data = tempDt, keep.inbag=TRUE, importance=TRUE, na.action=na.omit)
		} else {
			dedupOptions <- {
				defaultOptions <- c("keep.inbag=TRUE", "importance=TRUE", "na.action=na.omit")
				optionsSplit <- strsplit(options,",")[[1]] %>% gsub("[[:space:]]", "", .)
				optionsSplit[!optionsSplit %in% defaultOptions] %>% paste( . , collapse=',')
			}
			eval(parse(text=paste0("randomForest(as.formula(formula), data = tempDt,", dedupOptions ,", keep.inbag=TRUE, importance=TRUE, na.action=na.omit)")))
		}
	
	if (is.null(rf))
		stop("RandomForest errored out")
	
	return (rf)
}

getRFResults <- function(rf, method, sortBy="1") {
	#method should be either "confmatrix" or "varimp"
	#randomForest package is not compatible with data table
	
	lapply(list('data.table', 'randomForest', 'dplyr'), require, character.only = TRUE)
	set.seed(123)	
	
	if (is.null(rf))
		stop("RandomForest errored out")
	
	print.rf <- capture.output(rf) 
	rfType <- rf$type
	sortBy <- 
		paste0(rfType, sortBy) %>% switch(.,
			classification1 = "MeanDecreaseAccuracy",
			classification2 = "MeanDecreaseGini",
			regression1 = "%IncMSE",
			regression2 = "IncNodePurity"
		)
	
	method <- paste0(rfType, method)
	#alignmentInfo is summarized in order specified: label,x,y,z (series will be == label)
	switch(method,
		classificationvarimp = {
			varImpDt <- as.data.table(round(rf$importance,2), keep.rownames = TRUE) %>% setnames(., "rn", "AttributeNames") %>% 
				.[, c("AttributeNames", "MeanDecreaseAccuracy", "MeanDecreaseGini"), with=FALSE] %>% setorderv(., sortBy, order=-1L)
			return (list(alignmentInfo=c("AttributeNames", "MeanDecreaseAccuracy", "MeanDecreaseGini", sortBy), returnObject=varImpDt))
		},
		regressionvarimp = {
			varImpDt <- as.data.table(round(rf$importance,2), keep.rownames = TRUE) %>% setnames(., "rn", "AttributeNames") %>% 
				.[, c("%IncMSE", "IncNodePurity"), with=FALSE] %>% setorderv(., sortBy, order=-1L)
			return (list(alignmentInfo=c("AttributeNames", "%IncMSE", "IncNodePurity", sortBy), returnObject=varImpDt))
		},
		classificationconfmatrix = {
			confmatrix <- as.data.table(round(rf$confusion,4), keep.rownames = TRUE) %>% setnames(., c("rn", "class.error"), c("_", "Error(%)"))
			confmatrix[, "Error(%)"] <- confmatrix[, "Error(%)"] * 100
			return (confmatrix)
		}
	)
}
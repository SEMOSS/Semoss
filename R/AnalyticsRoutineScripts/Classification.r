getUsefulPredictors <- function(x) {
#variables actually used in tree construction:
  varid <- nodeapply(x, ids = nodeids(x),
    FUN = function(n) split_node(n)$varid)
  varid <- unique(unlist(varid))
  names(data_party(x))[varid]
}

getCTree <- function(dt, instanceCol, attrColList, subsetPercent=0.8){	
	lapply(list('data.table', 'partykit', 'dplyr'), require, character.only = TRUE)
	set.seed(123)	

	tempDt <- setDT(dt)
	tempDt <- tempDt[, c(instanceCol, attrColList), with=FALSE]

	#convert columns of character class to factor class
	charCols <- colnames(tempDt)[which(as.vector(tempDt[,lapply(.SD, class)]) == "character")]
	if (length(charCols) > 0 ) {tempDt[,(charCols):= lapply(.SD, as.factor), .SDcols = charCols]}
	
	#split into training and test sets
	tempDt[,"inTrain"]   <- ifelse(runif(nrow(tempDt))<subsetPercent,1,0)
	trainset <- tempDt[inTrain==1][, inTrain:=NULL]
	testset  <- tempDt[inTrain==0][, inTrain:=NULL]	
	
	#build formula & get tree
	formula <- paste0(instanceCol, " ~ .")
	tree <- ctree(as.formula(formula), data = trainset, na.action=na.pass)
	treeMap <- capture.output(print(tree[1]))
	
	#get predicted probabilites for each terminal node if method = classification else not applicable
	predictedProbDt <- NULL
	predictedProbDt <- 
		if (!is.numeric(trainset[[instanceCol]])) {
			nTerminalNodeTable <- table(predict(tree, type = "node"), trainset[[instanceCol]])	
			as.data.frame(format(round(prop.table(nTerminalNodeTable, 1) * 100, digits=2))) %>% mutate(nodes = rownames(.)) %>% setDT(.)
		}

	#accuracy (if method = classification then as % else if method = regression then as RMSE, root mean square error)
	predict <- predict(tree,testset,type="response")
	accuracy <- 
		if (is.numeric(trainset[[instanceCol]])) {
			rmse <- sqrt(mean((predict-testset[[instanceCol]])^2))
			paste0(round(rmse, digits = 2))
	} else {
			acc <- 100 * mean(predict==testset[[instanceCol]])
			paste0(round(acc,digits = 2), "%")
		}
	
	#predictors 
	predictors <- getUsefulPredictors(tree) %>% as.vector(.)
	
	return (list(accuracy=accuracy, predictors=predictors, predictedProbDt=predictedProbDt, tree=treeMap))
}

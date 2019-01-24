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

	tempDt <- dt[, c(instanceCol, attrColList), with=FALSE]
	tempDt[tempDt==''|tempDt==' '] <- NA 
	tempDt <- tempDt[complete.cases(tempDt),]
	
	#split into training and test sets
	tempDt[,"inTrain"]   <- ifelse(runif(nrow(tempDt))<subsetPercent,1,0)
	trainset <- tempDt[inTrain==1][, inTrain:=NULL]
	testset  <- tempDt[inTrain==0][, inTrain:=NULL]	
	
	#convert columns of character class to factor class in both the training and test sets
	charCols <- colnames(tempDt)[which(as.vector(tempDt[,lapply(.SD, class)]) == "character")]
	if (length(charCols) > 0 ) {
		trainset[,(charCols):= lapply(.SD, as.factor), .SDcols = charCols]
		testset[,(charCols):= lapply(.SD, as.factor), .SDcols = charCols]
	}
	
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
		if (is.numeric((trainset[[instanceCol]]))) {
			# need to normalize to keep values between 0 and 1
			# will normalize based on the range and the actual range
		  min_norm <- min(testset[[instanceCol]])
		  max_norm <- max(testset[[instanceCol]])
		  predict_acc <- (as.numeric(predict) - min_norm) / ( max_norm - min_norm)
		  test_acc <- ( testset[[instanceCol]] - min_norm) / ( max_norm - min_norm) 
		  rmse <- (1 - mean(sqrt( (predict_acc - test_acc )^2 ) )) * 100;
			paste0(round(rmse, digits = 2))
		} else {
			acc <- 100 * mean(as.character(predict)==as.character(testset[[instanceCol]]))
			paste0(round(acc,digits = 2), "%")
		}
	
	#predictors 
	predictors <- getUsefulPredictors(tree) %>% as.vector(.)
	
	return (list(accuracy=accuracy, predictors=predictors, predictedProbDt=predictedProbDt, tree=treeMap))
}

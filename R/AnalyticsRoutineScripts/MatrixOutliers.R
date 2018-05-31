

#instanceCol <- "Genre"
#attrColList <- c("Nominated")
#originalFrameName <- fread("C:/Users/micstone/Desktop/Movies.csv")
#newColName <- "myOutliers"
######################################################################################################
######################################################################################################
#resultsdf <- GetMatrixOutliers(originalFrameName, instanceCol, attrColList, newColName)
#orderedResults <- resultsdf[order(resultsdf$myOutliers),]

####################################################################################################
###########MAIN FUNCTION FOR GETTING MATRIX OUTLIERS################################################
####################################################################################################
GetMatrixOutliers <- function(originalFrameName, instanceCol, attrColList, newColName) {
library(data.table)
library(dplyr)
frameName <- as.data.table(originalFrameName)
data <- ScaleUniqueData(frameName, instanceCol, attrColList)
scaledFrame <- data[[1]]
dtSubset <- data[[2]]
numAttrList <- data[[3]]
catAttrList <- data[[4]]
noBlanks <- data[[5]]

weights <- NULL  
weightsTable <- GenerateWeightsTable(attrColList, weights)

# define the ratios of numeric and categorical columns 
ratioFrame <- DefineRatios(catAttrList, numAttrList, dtSubset, instanceCol)
numRatio <- ratioFrame[1]
catRatio <- ratioFrame[2]

if (length(numAttrList) > 0) {
# if we only have numerical attributes then the scaling constant is zero
if (length(catAttrList) > 0) {
sumCategoricalWeights <- sum(weightsTable[weightsTable$attribute %in% c(catAttrList), "weight"])
numNumericWeights <- length(numAttrList)
scalingConstant <- sumCategoricalWeights/numNumericWeights
} else {
scalingConstant <- 0
}
} 

#####################################################################################################
#################CALCULATE SIMILARITY################################################################
#####################################################################################################

# create a table or matrix to store the final outlier value for each row 
resultsFrame <- data.table(instanceCol = dtSubset[[instanceCol]], similarityVal = double(nrow(dtSubset)))
names(resultsFrame) <- c(instanceCol, newColName)

############IF WE HAVE BOTH CATEGORICAL AND NUMERIC ATTRIBUTES########################################
if (length(catAttrList) > 0 & length(numAttrList > 0)) {
	t <- GenerateCountTable(noBlanks, instanceCol, catAttrList)[[1]]

	# divide t table values by instance size 
	for (i in 1:nrow(t)) {
		denominator <- t[i][["INSTANCE__SIZE"]]
		calcCols <- names(t[,-c(instanceCol, "INSTANCE__SIZE"), with = FALSE])
		for (j in 1: length(calcCols)) {
		t[i, calcCols[j]] <- t[[i, calcCols[j]]]/denominator
		}
	}

	#order t
	t <- t[order(t[[instanceCol]]),]

#GET THE CATEGORICAL SIMILARITY VALUES FOR EACH ROW #####################
	m <- as.matrix(t[, -c(instanceCol, "INSTANCE__SIZE"), with = FALSE])
	#rownames(m) <- t[[instanceCol]]
	m_colnames <- colnames(m)

	n <- as.matrix(dtSubset[,-c(catAttrList, instanceCol), with = FALSE])
	#rownames(n) <- dtSubset[[instanceCol]]
	n_colnames <- colnames(n)

	for (k in 1:nrow(m)) {
		mDiffToSum <- rowSums(sapply(m_colnames, function(x) abs(m[-c(k), x] - m[k,x]) * weightsTable[attribute==(strsplit(x, "__")[[1]][[1]]),weight]))
		# diff to sum first looks like a matrix where for each column (e.g., Studio_Sony) we have a row with the absolute value of the difference between that row and the current row; then we sum across these rows 

		# then for each of these row sums we want to take catRatio * (1-(diffToSum/length(m_colnames)))
		catSims <- catRatio * (1-(mDiffToSum/length(m_colnames)))

		########WHILE WE ARE LOOPING THROUGH EACH ROW WE WANT TO GET THE NUMERIC SIMILARITY VALUE FOR EACH ROW ################
		nDiffToSum <- rowSums(sapply(n_colnames, function(x) abs(n[-c(k), x] - n[k,x]) *  (1/(weightsTable[attribute==x, weight] + scalingConstant))))
		numSims <- numRatio * (1 - (sqrt(nDiffToSum)/length(n_colnames)))

		#then for each row we want to sum numSims and catSims
		similarities <- numSims + catSims

		#store 1 - meanSimilarity for each row 
		resultsFrame[k,2] <- 1 - mean(similarities)
	}

} else {

    ##CATEGORICAL ATTRIBUTES ONLY########################################
	if (length(catAttrList) > 0) {
		t <- GenerateCountTable(noBlanks, instanceCol, catAttrList)[[1]]
		
		for (i in 1:nrow(t)) {
			denominator <- t[i][["INSTANCE__SIZE"]]
			calcCols <- names(t[,-c(instanceCol, "INSTANCE__SIZE"), with = FALSE])
				
				for (j in 1: length(calcCols)) {
					t[i, calcCols[j]] <- t[[i, calcCols[j]]]/denominator
				}
		}

   #order t
   t <- t[order(t[[instanceCol]]),]
   m <- as.matrix(t[, -c(instanceCol, "INSTANCE__SIZE"), with = FALSE])
   #rownames(m) <- t[[instanceCol]]
   m_colnames <- colnames(m)

   for (k in 1:nrow(m)) {
		mDiffToSum <- rowSums(sapply(m_colnames, function(x) abs(m[-c(k), x] - m[k,x]) * weightsTable[attribute==(strsplit(x, "__")[[1]][[1]]),weight]))
		catSims <- catRatio * (1-(mDiffToSum/length(m_colnames)))
		resultsFrame[k,2] <- 1 - mean(catSims)
	}
	
	} else {
	##NUMERIC RESULTS ONLY#################################################
    n <- as.matrix(dtSubset[,-c(catAttrList, instanceCol), with = FALSE])
    #rownames(n) <- dtSubset[[instanceCol]]
    n_colnames <- colnames(n)

	for (k in 1:nrow(n)) {
		nDiffToSum <- rowSums(sapply(n_colnames, function(x) abs(n[-c(k), x] - n[k,x]) *  (1/(weightsTable[attribute==x, weight] + scalingConstant))))
		# diff to sum first looks like a matrix where for each column (e.g., MovieBudget) we have a row with the absolute value of the difference between that row and the current row; then we sum across these rows 

		# then for each of these row sums we want to take numRatio * (1 - (sqrt(diffToSum)/length(numAttrList)))
		numSims <- numRatio * (1 - (sqrt(nDiffToSum)/length(n_colnames)))
		resultsFrame[k,2] <- 1 - mean(numSims)
	}
	
  }

}
resultsdf <- merge(x = originalFrameName, y = resultsFrame, by = instanceCol, all.x = TRUE)
return (resultsdf)
}

#####################################GET SCALED AND COLLAPSED FRAME########################################################
ScaleUniqueData <- function(dt, instanceCol, attrColList){

attr_numeric <- names(select_if(dt[,attrColList,with=FALSE], is.numeric))
attr_nonNumeric <- c(names(select_if(dt[,attrColList,with=FALSE], is.factor)), names(select_if(dt[,attrColList,with=FALSE], is.character)))
dtSubset <- dt[,c(instanceCol,attrColList), with=FALSE] 
dtSubset[dtSubset==''|dtSubset==' '] <- NA 

dtSubset <- unique(dtSubset[complete.cases(dtSubset),])
noBlanks <- copy(dtSubset)
                             
#for numerical attribute columns, scale each value then take average grouped by the instance col
if (length(attr_numeric) > 0) {
max_numCols =  sapply(dt[,attr_numeric,with=FALSE], max, na.rm=TRUE)
min_numCols =  sapply(dt[,attr_numeric,with=FALSE], min, na.rm=TRUE)

scaledAttrCols <- lapply(attr_numeric, function(x) if((max_numCols[x] - min_numCols[x]) > 0) (dtSubset[[x]] - min_numCols[x])/(max_numCols[x] - min_numCols[x]) else 0) 

dtSubset <- dtSubset[, (attr_numeric) := lapply(1:length(attr_numeric), function(i) unlist(scaledAttrCols[[i]]))]
scaledFrame <- dtSubset                                   
temp <- dtSubset[complete.cases(dtSubset[[instanceCol]]), lapply(.SD, mean, na.rm=TRUE), by = instanceCol, .SDcols = attr_numeric]
dtSubset <- merge(x=dtSubset[complete.cases(dtSubset[[instanceCol]]), c(instanceCol,attr_nonNumeric), with=FALSE], y=temp, by=instanceCol, all.x=TRUE) %>% unique(.)
}
                              
#for nonnumerical attribute columns, convert any factor types to char types. For each col, order alphabetically then concatenate values, comma separated, grouped by instance col
if (length(attr_nonNumeric) > 0) {
scaledFrame <- dtSubset
factorCols <- names(select_if(dt[,attr_nonNumeric,with=FALSE], is.factor))
if (length(factorCols) > 0 ) {
dtSubset[,(factorCols):= lapply(.SD, as.character), .SDcols = factorCols]
}
for (j in names(dtSubset)) set(dtSubset, j = j, value = dtSubset[[trimws(j)]])
                                             
for (i in attr_nonNumeric){
temp.y <- dtSubset[order(dtSubset[, instanceCol, with=FALSE], dtSubset[,i, with=FALSE]), c(instanceCol,i), with=FALSE] %>% .[complete.cases(.),] %>% .[, lapply(.SD, paste0, collapse=","), by=instanceCol, .SDcols=i]
dtSubset <- merge(x=dtSubset[,-i,with=FALSE], y=temp.y, by=instanceCol, all.x=TRUE)
}
}
                              
#finally, dedup rows
dtSubset <- unique(dtSubset)
return (list(scaledFrame=scaledFrame, dtSubset=dtSubset, attr_numeric = attr_numeric, attr_nonNumeric = attr_nonNumeric, noBlanks=noBlanks))
}

GenerateCountTable <- function(frameName, instanceCol, attrList) {

t <- unique(frameName[complete.cases(frameName[[instanceCol]]), instanceCol, with=FALSE])
listedColVals <- lapply(1:length(attrList), function(i) unlist(unique(frameName[complete.cases(frameName[[attrList[i]]]), attrList[i], with=FALSE])))
tempCols <- c(unlist(lapply(1:length(listedColVals), function(i) paste0(attrList[i], "__", listedColVals[[i]]))), "INSTANCE__SIZE")
t[, (tempCols) := 0]

multiplierTable <- data.table(colTitle = c(unlist(lapply(1:length(listedColVals), function(i) paste0(attrList[i], "__", listedColVals[[i]])))), multiplier = double(length(listedColVals)))


# iterate through each COLUMN, exclude our studio column
for (i in 2: ncol(t)) {

split <- strsplit(colnames(t)[i], "__")
# get the category (e.g., Genre)
category <- split[[1]][[1]]
# get the value (e.g., Drama)
value <- split[[1]][[2]]

# get the multiplier to pass into the daisy command
# this will even out the weights so that the attributes with more values (i.e., genre has 6 values) do not carry more weight 
if (value != "SIZE") {
multiplierTable[colTitle == colnames(t)[i]]$multiplier <- 1/length(unique(frameName[[category]]))
}

# evaluate the original data table for the category; and sum how many match the given value
	for (j in 1:nrow(t)) {
	if (value == "SIZE") {
	  s <- length(which(frameName[[instanceCol]] == t[[j,1]]))
	} else {
	  #where does the attribute column have the right value and the studio matches
	  s <- length(which(frameName[[category]] == value & frameName[[instanceCol]] == t[[j,1]]))
	}
	  t[[j,i]] <- s
	}
}
	return (list(t = t, multiplierTable = multiplierTable))
}

DefineRatios <- function(catAttrList, numAttrList, dtSubset, instanceCol) {

# determine the number of numerical and categorical attributes we are looking attrColList
catRatio <- length(catAttrList)
numRatio <- length(numAttrList)
totalLength <- catRatio + numRatio # + 1

	numRatio <- numRatio / (totalLength)
	catRatio <- catRatio / (totalLength)
	ratioFrame <- c(numRatio,catRatio)
	return (ratioFrame)
}

################CAPABILITY TO WEIGHT ATTRIBUTES DIFFERENTLY; THIS BUILDS THE TABLE WE NEED TO DO THIS#################
GenerateWeightsTable <- function(attrColList, weights) {
if (length(weights) > 0) {
return (data.table(attribute = attrColList, weight = weights))
} else {
return (data.table(attribute = attrColList, weight = rep(1/length(attrColList))))
}
}

CalculateSimilarity <- function(instanceName1, instanceName2, instanceCol, attrColList, noBlanks, dtSubset, weightsTable, scalingConstant, catRatio, numRatio, catAttrList, numAttrList) {

if (instanceName1 != instanceName2) {

#reduce the frame to just the instances that we need
instancesOnly <- noBlanks[noBlanks[[instanceCol]] %in% c(instanceName1, instanceName2),]

#CATEGORICAL SIMILARITY
if (length(catAttrList) > 0) {
t <- GenerateCountTable(instancesOnly, instanceCol, catAttrList)

t_colnames <- names(t)[-c(grep(instanceCol, names(t)), grep("__SIZE", names(t)))]

###APPLY FUNCTION
diffToSum <- sum(sapply(t_colnames, function(x) abs(t[[1,x]]/t[[1,"INSTANCE__SIZE"]] - t[[2,x]]/t[[2,"INSTANCE__SIZE"]]) * weightsTable[attribute==(strsplit(x, "__")[[1]][[1]]),weight]))

catSimilarity <- catRatio * (1-(diffToSum/length(t_colnames)))
} else {
t <- NULL
catSimilarity <- 0
}
#NUMERICAL SIMILARITY
if (length(numAttrList) > 0) {
dtSubsetInstancesOnly <- dtSubset[dtSubset[[instanceCol]] %in% c(instanceName1, instanceName2),]

diffToSum <- sum(sapply(numAttrList, function(x) ((abs(dtSubsetInstancesOnly[[1,x]] - dtSubsetInstancesOnly[[2,x]]))^2) * (1/(weightsTable[attribute==x, weight] + scalingConstant)))) #538 micro

numSimilarity <- numRatio * (1 - (sqrt(diffToSum)/length(numAttrList)))

} else {
numSimilarity <- 0
}

similarity <- numSimilarity + catSimilarity

} else {

similarity <- 0
}
return (similarity)


}




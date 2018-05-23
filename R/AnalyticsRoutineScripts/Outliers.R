GetOutliers <- function(originalFrameName, numInSubset, numLoops, instanceCol, attrColList, newColName) {
set.seed(123)
frameName <- (originalFrameName)
# Collapsing the frame 
data <- ScaleUniqueData(frameName, instanceCol, attrColList)
# just the scaled frame
scaledFrame <- data[[1]]
# the scaled and collapsed frame
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

#########numeric vs categorical weights for scaling##################
# get a constant that is equal to (sum of categorical weights) / (number of numeric weights)
# we only need a scaling constant if we have numerical attributes
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

# create a new column to hold the outlier values
dtSubset[[newColName]] <- double(nrow(dtSubset))

# first take one row of data at a time
for (i in 1:nrow(dtSubset)) {
    curRow <- dtSubset[i]
	# for each of the loops
	#we will keep track of the maxSim for each loop
	loopSims <- vector()
	for (j in 1:numLoops) {
		# generate a random set of data points based on the size specified
		sampleFrame <- dtSubset[sample(nrow(dtSubset), numInSubset), ]
	    # reset the keep similarity for each row
		maxSimilarity <- 0
			#for each item in the sample
			for (k in 1:nrow(sampleFrame)) {
				compareRow <- sampleFrame[k]
				
				instanceName1 <- curRow[[instanceCol]]
				instanceName2 <- compareRow[[instanceCol]]
				
				# compare the row of data to each item in the random subset (INDIVIDUALLY)
				# we can do this by comparing the row index to see if it matches 
				if (instanceName1 != instanceName2) {
				# if the rows are not the same, then we calculate the similarity between them 
				
				# calculate single similarity value between the two instances 
				similarity <- CalculateSimilarity(instanceName1, instanceName2, instanceCol, attrColList, noBlanks, dtSubset, weightsTable, scalingConstant, catRatio, numRatio, catAttrList, numAttrList)
				
				# of these comparisons, what is the maximum similarity
				# keep track of max similarity
				maxSimilarity <- max(similarity, maxSimilarity)
				
				} else {
				# do nothing if the row is the same - go ahead and move on to the next row 
				}
				#loopSims <- apply(dtSubset, 1, function(y) mean(replicate(numLoops, 1 - max(apply(dtSubset[sample(nrow(dtSubset), numInSubset), ], 1, function(x) CalculateSimilarity(instanceName1 = y[[instanceCol]], instanceName2= x[[instanceCol]], instanceCol, attrColList, noBlanks, dtSubset, weightsTable, scalingConstant, catRatio, numRatio, catAttrList, numAttrList) )))))
		}
		
		# once we have finished iterating through each item in our random set, we want to store 1 - maxSimilarity for this loop
		loopSims <- c(loopSims, 1 - maxSimilarity)
	}
    # at the end of all loops, what is the average of 1 - maxSim values that we have stored 
	dtSubset[i, newColName] <- mean(loopSims)
}

# join the results to the original frame 
results <- dtSubset[, c(instanceCol, newColName), with = FALSE]
resultsdf <- merge(x = originalFrameName, y = results , by = instanceCol, all.x = TRUE)
return (resultsdf)
}

###########DETERMINE RATIONS OF CATEGORICAL AND NUMERICAL COLUMNS (INCLUDING INSTANCE COLUMN)########################################
DefineRatios <- function(catAttrList, numAttrList, dtSubset, instanceCol) {

# determine the number of numerical and categorical attributes we are looking attrColList
catRatio <- length(catAttrList)
numRatio <- length(numAttrList)
totalLength <- catRatio + numRatio # + 1

# determine the type of the instance column to include this in the ratio	
#if (is.numeric(dtSubset[[instanceCol]])) {
#   numRatio = numRatio + 1
#} else {
#   catRatio = catRatio + 1 
#}
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

diffToSum <- 0
for (i in 1:length(t_colnames)) {
val1 <- t[[1,t_colnames[i]]]/t[[1,"INSTANCE__SIZE"]]
val2 <- t[[2,t_colnames[i]]]/t[[2,"INSTANCE__SIZE"]]
difference <- abs(val1 - val2)
diffToSum <- diffToSum + (difference * weightsTable[attribute==(strsplit(t_colnames[i], "__")[[1]][[1]]
),weight])
}

catSimilarity <- catRatio * (1-(diffToSum/length(t_colnames)))
} else {
t <- NULL
catSimilarity <- 0
}
#NUMERICAL SIMILARITY
if (length(numAttrList) > 0) {
diffToSum <- 0
dtSubsetInstancesOnly <- dtSubset[dtSubset[[instanceCol]] %in% c(instanceName1, instanceName2),]
for (i in 1: length(numAttrList)) {
weight <- weightsTable[attribute==numAttrList[i], weight]
val1 <- dtSubsetInstancesOnly[[1,numAttrList[i]]]
val2 <- dtSubsetInstancesOnly[[2,numAttrList[i]]]
difference <- abs(val1 - val2)
diffToSum <- diffToSum + (difference ^ 2) * (1/(weight + scalingConstant))
}

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

##################SCALE AND NORMALIZE THE DATA FRAME###############################
ScaleUniqueData <- function(dt, instanceCol, attrColList){
lapply(list('data.table', 'dplyr'), require, character.only = TRUE)
               
attr_numeric <- names(select_if(dt[,attrColList,with=FALSE], is.numeric))
attr_nonNumeric <- c(names(select_if(dt[,attrColList,with=FALSE], is.factor)), names(select_if(dt[,attrColList,with=FALSE], is.character)))
dtSubset <- dt[,c(instanceCol,attrColList), with=FALSE] 
dtSubset[dtSubset==''|dtSubset==' '] <- NA 

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
                              
#finally, dedup rows then remove any rows with NAs
dtSubset <- unique(dtSubset[complete.cases(dtSubset),])
return (list(scaledFrame=scaledFrame, dtSubset=dtSubset, attr_numeric = attr_numeric, attr_nonNumeric = attr_nonNumeric, noBlanks=noBlanks))
}

GenerateCountTable <- function(frameName, instanceCol, attrList) {

t <- unique(frameName[complete.cases(frameName[[instanceCol]]), instanceCol, with=FALSE])
listedColVals <- lapply(1:length(attrList), function(i) unlist(unique(frameName[complete.cases(frameName[[attrList[i]]]), attrList[i], with=FALSE])))
tempCols <- c(unlist(lapply(1:length(listedColVals), function(i) paste0(attrList[i], "__", listedColVals[[i]]))), "INSTANCE__SIZE")
t[, (tempCols) := 0]

# iterate through each COLUMN, exclude our studio column
for (i in 2: ncol(t)) {

split <- strsplit(colnames(t)[i], "__")
# get the category (e.g., Genre)
category <- split[[1]][[1]]
# get the value (e.g., Drama)
value <- split[[1]][[2]]

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
	return (t)
}


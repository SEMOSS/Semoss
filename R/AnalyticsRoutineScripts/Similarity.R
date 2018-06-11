

####################FIND THE CENTROIDS/MEANS FOR OUR NUMERICAL DATA###########################################################	
###this method will return a table that contains column names for each column in the colVector as well as the column number, the centroid value, and the column type in the original frame
FindCentroids <- function(scaledFrame, colVector) {

# create a dataframe to hold the centroids
# only hold the relevant columns
N <- length(colVector)
centroids <- data.frame(columnName = character(N), centroidVal = numeric(N), colType = character(N), stringsAsFactors = FALSE)

for (i in 1:length(colVector)) {
centroids$columnName[i] <- colVector[i]
vals <- scaledFrame[[which(colnames(scaledFrame) == colVector[i])]]
centroids$centroidVal[i] <- mean(vals, na.rm = TRUE)
centroids$colType[i] <- sapply(scaledFrame[, colVector[i], with=FALSE], class)
}
#rm(vals, N)
return (centroids) 
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


#################METHOD USED TO CALCULATE THE CATEGORICAL SIMILARITY FOR EACH ROW#####################################
CSimilarity <- function(x, t, lookupDt, len, colnames, catRatio, weightsTable){             
denominator <- x[len + 1]

catRatio * (1-(sum(unlist(lapply(1:len, function(i) if (x[i] > 0)  weightsTable[attribute==(strsplit(colnames[i], "__")[[1]][[1]]
),weight] * (abs(x[i]/denominator - lookupDt[keyCol==colnames[i], valueCol])))))))

}

###############CREATE A TABLE TO HOLD A CONSTANT FOR EACH CATEGORICAL VALUE (I.E., GENRE__DRAMA) (# TIMES THAT VALUE OCCURS / TOTAL ROWS OF DATA)#######
GenerateLookupDT <- function(t_colnames, frameName) {
t_colnames <- t_colnames
lookupDT <- data.table(keyCol = t_colnames, valueCol = double(length(t_colnames)))
denominator <- nrow(frameName)
for (i in 1:length(t_colnames)) {
split <- strsplit(t_colnames[i], "__")
columnLookup <- split[[1]][[1]]
valueLookup <- split[[1]][[2]]
# how many times do we see dramas (or other value) in our Genre column (or other category)
numerator <- length(which(frameName[[columnLookup]] == valueLookup))
tableValue <- numerator/denominator
lookupDT[which(lookupDT[[1]] == t_colnames[i]),2] <- tableValue
}
#rm(numerator, denominator, columnLookup, valueLookup, tableValue)
return (lookupDT)
}

##########CALCULATE THE SIMILARITY AND POPULATE THESE VALUES INTO A TABLE##########################################################
CalculateSimilarity <- function(frameName, numAttrList, catAttrList, dtSubset, instanceCol, numRatio, catRatio, centroids, t, weightsTable) {
#if the weight changes from 1, we also need to incorporate into the categorical similarity
N <- nrow(dtSubset)
# we will append the categorical similarity to this table later on; for now create columns for numerical and overall similarity
simDf <- data.table(instanceName = dtSubset[[1]], numSimilarity = double(N), similarity = double(N), stringsAsFactors = FALSE)
names(simDf)[names(simDf) == 'instanceName'] <- instanceCol

###############categorical################################
if(length(catAttrList) > 0) {
t_colnames <- names(t)[-c(grep(instanceCol, names(t)), grep("__SIZE", names(t)))]
# generate a lookup table to hold the ratio of each categorical value to total values in the frame (the original data frame)
lookupDT <- GenerateLookupDT(t_colnames, frameName)
# add a column to t to hold the categorical similarities
t$catSimilarity <- apply(t[,-1], 1 , CSimilarity, t=t, lookupDt = lookupDT, len=length(t_colnames), colnames=t_colnames, catRatio = catRatio, weightsTable = weightsTable)
# now join the tables based on the instance column
#setindexv(t, instanceCol)
#setindexv(simDf, instanceCol)
#simDf<- merge(x = simDf, y = t[,c(instanceCol, "catSimilarity"), with = FALSE], all.x = TRUE)
simDf<- merge(x = simDf, y = t[,c(instanceCol, "catSimilarity"), with = FALSE], by = instanceCol, all.x = TRUE)
} else {
simDf$catSimilarity <- rep(0) 
}

##########numeric vs categorical weights for scaling##################
# get a constant that is equal to (sum of categorical weights) / (number of numeric weights)
# we only need a scaling constant if we have numerical atttributes
if (length(numAttrList > 0)) {
# if we only have numerical attributes then the scaling constant is zero
if (length(catAttrList > 0)) {
sumCategoricalWeights <- sum(weightsTable[which(weightsTable$attribute %in% c(catAttrList)), "weight"])
numNumericWeights <- length(numAttrList)
scalingConstant <- sumCategoricalWeights/numNumericWeights
} else {
scalingConstant <- 0
}
}

#############numeric##################################################
for (j in 1:nrow(dtSubset)) {
if (length(numAttrList > 0)) {
# reset for each row
rowNumericSim <- 0.0
# for the row, add up all of the numerical similarities
# while the row has more numeric similarities, keep summing the calculated similarity 
for (i in 1: length(numAttrList)) {
    # determine if numeric or categorical
	value <- dtSubset[[j, numAttrList[i]]]
	#value <- dtSubset[[j, columns[i]]]
	centroidForInstance <- centroids$centroidVal[which(centroids$columnName == numAttrList[i])]
	weight <- weightsTable[attribute==numAttrList[i], weight]
	rowNumericSim <- rowNumericSim + (((value - centroidForInstance)^2)* (1/(weight + scalingConstant)))	
}
rowNumericSim <- 1 - (sqrt(rowNumericSim)/length(numAttrList))
rowNumericSim <- numRatio * rowNumericSim
simDf[j,"numSimilarity"] <- rowNumericSim
}
simDf[j,"similarity"] <- simDf[j,"numSimilarity"] + simDf[j,"catSimilarity"]
}
#rm(N, j, i, value, rowNumericSim, centroidForInstance, t_colnames, lookupDT, weight, sumCategoricalWeights, numNumericWeights, scalingConstant)
return (simDf[,-c("numSimilarity","catSimilarity")])
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

###############GET COUNTS FOR CATEGORICAL ATTRIBUTES#######################################
# the table t created will create a row for each instance value (e.g., there will be 40 rows if the instance column is studio)
# the table will contain counts (per each instance value of how many items are in each category for each categorical column)
# for example, if Genre is our attribute column, then t will have Genre__Drama, Genre__Documentary etc. 
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

################CAPABILITY TO WEIGHT ATTRIBUTES DIFFERENTLY; THIS BUILDS THE TABLE WE NEED TO DO THIS#################
GenerateWeightsTable <- function(attrColList, weights) {
if (length(weights) > 0) {
return (data.table(attribute = attrColList, weight = weights))
} else {
return (data.table(attribute = attrColList, weight = rep(1/length(attrColList))))
}
}

###################################################################################
###################################################################################
# method used in Java reactor for generating the similarity table

# originalFrameName <- the full data frame
# instanceCol <- the column we are running the algorithm on (e.g., "Title")
# attrColList <- the attribute columns (do not include the instance column)
# newColName <- what to we want to name the new column holding our similiarity values
GenerateSimilarityTable <- function(originalFrameName, instanceCol, attrColList, newColName) {
# originalFrameName <- fread("C:/Users/micstone/Desktop/diabetes.csv")
frameName <- as.data.table(originalFrameName)
# instanceCol <- "gender"
# attrColList <- c("frame")
weights <- NULL  
weightsTable <- GenerateWeightsTable(attrColList, weights)

# scale/normalize the data frame and collapse based on categorical attributes
data <- ScaleUniqueData(frameName, instanceCol, attrColList)
# just the scaled frame
scaledFrame <- data[[1]]
# the scaled and collapsed frame
dtSubset <- data[[2]]
numAttrList <- data[[3]]
catAttrList <- data[[4]]
noBlanks <- data[[5]]

# find the centroids, which is needed for the numerical data
# find the mean based on the scaled frame, but not the collapsed frame
centroids <- FindCentroids(scaledFrame, attrColList)

# define the ratios of numeric and categorical columns 
ratioFrame <- DefineRatios(catAttrList, numAttrList, dtSubset, instanceCol)
numRatio <- ratioFrame[1]
catRatio <- ratioFrame[2]

# if we have categorical attributes, generate a table that keeps track of counts of each categorical value for a given instance value
# use the original data frame (frameName) to get the counts; we don't want the collapsed frame here
if (length(catAttrList) > 0) {
t <- GenerateCountTable(noBlanks, instanceCol, catAttrList)
} else {
t <- NULL
}

simFrame <- CalculateSimilarity(noBlanks, numAttrList, catAttrList, dtSubset, instanceCol, numRatio, catRatio, centroids, t, weightsTable)

#setindexv(originalFrameName, instanceCol)
#setindexv(simFrame, instanceCol)
#resultsdf <- merge(x = originalFrameName, y = simFrame, all.x = TRUE)
resultsdf <- merge(x = originalFrameName, y = simFrame, by = instanceCol, all.x = TRUE)

names(resultsdf)[names(resultsdf) == 'similarity'] <- newColName
rm(frameName, data, scaledFrame, dtSubset, numAttrList, catAttrList, ratioFrame, numRatio, catRatio, simFrame, originalFrameName, weightsTable)
return (resultsdf)
#resultsdf[order(resultsdf$similarity),]
}



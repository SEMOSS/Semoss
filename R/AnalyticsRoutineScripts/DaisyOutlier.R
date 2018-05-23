

instanceCol <- "Genre"
attrColList <- c("Studio", "Nominated", "MovieBudget")
originalFrameName <- fread("C:/Users/micstone/Desktop/Movies.csv")
newColName <- "myOutliers"
######################################################################################################
######################################################################################################
resultsdf <- GetDaisyOutliers(originalFrameName, instanceCol, attrColList, newColName)
daisyR <- resultsdf[order(resultsdf$OUTLIERS),]



GetDaisyOutliers <- function(originalFrameName, instanceCol, attrColList, newColName) {
library(cluster)
frameName <- as.data.table(originalFrameName)
############CLEANING THE DATA##############################################
data <- ScaleUniqueData(frameName, instanceCol, attrColList)
# just the scaled frame
scaledFrame <- data[[1]]
# the scaled and collapsed frame
dtSubset <- data[[2]]
numAttrList <- data[[3]]
catAttrList <- data[[4]]
noBlanks <- data[[5]]
#############GET A T TABLE FOR ANY CATEGORICAL ATTRIBUTES##################
if (length(catAttrList) > 0) {
t <- GenerateCountTable(noBlanks, instanceCol, catAttrList)[[1]]
multiplierTable <- GenerateCountTable(noBlanks, instanceCol, catAttrList)[[2]]
temp <- dtSubset[,-c(catAttrList), with = FALSE]

for (i in 1:nrow(t)) {
denominator <- t[i][["INSTANCE__SIZE"]]
calcCols <- names(t[,-c(instanceCol, "INSTANCE__SIZE"), with = FALSE])
for (j in 1: length(calcCols))

t[i, calcCols[j]] <- t[[i, calcCols[j]]]/denominator


}

t <- t[, -c("INSTANCE__SIZE")]

appendedTable <- merge(x = temp, y = t, by = instanceCol, all.x = TRUE, all.y = TRUE)

# get the similarity values
columnNames <- colnames(appendedTable[, -c(instanceCol), with = FALSE])

weightsVector <- c()

for (i in 1:length(columnNames)) {

if (columnNames[i] %in% multiplierTable$colTitle) {
weightsVector <- c(weightsVector, multiplierTable[colTitle == columnNames[i], multiplier])

} else {
weightsVector <- c(weightsVector, 1)
}

}

y <- data.table(as.matrix(daisy(appendedTable[,c(columnNames),with=FALSE], metric = "gower", stand =TRUE, weights = weightsVector)))

#take the average of each column in the matrix 
appendedTable$OUTLIERS <- apply(y, 2, function(i) mean(i))
appendedTable <- appendedTable[,c(instanceCol, "OUTLIERS"), with = FALSE]

#appendedTable[order(appendedTable$OUTLIERS),]

} else {
 # if there are no categorical attributes, we don't even need a t table; instead we can just go forward with the algorithm 
 y <- data.table(as.matrix(daisy(dtSubset[,c(attrColList),with=FALSE], metric = "gower", stand =TRUE)))
 #take the average of each column in the matrix 
 dtSubset$OUTLIERS <- apply(y, 2, function(i) mean(i))
 appendedTable <- dtSubset[,c(instanceCol, "OUTLIERS"), with = FALSE]
}
resultsdf <- merge(x = originalFrameName, y = appendedTable, by = instanceCol, all.x = TRUE)
return (resultsdf)
}



#####################################GET SCALED AND COLLAPSED FRAME########################################################
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



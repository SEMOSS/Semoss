
########MOVIES###############################################
#instanceCol <- "Director"
#attrColList <- c("RottenTomatoesAudience")
#originalFrameName <- fread("C:/Users/micstone/Desktop/Movies.csv")
#newColName <- "myOutliers"
#unique <- FALSE
#############################################################

#######DIABETES##############################################
#instanceCol <- "location"
#attrColList <- c("chol", "bp.2s")
#originalFrameName <- fread("C:/Users/micstone/Desktop/diabetes.csv")
#newColName <- "myOutliers"
#################################################################

##############################################################
#alpha <- 0.05
#alpha <- 0.15

#unique <- TRUE 
#uniqiue <- FALSE 
##########################################################
#test <- DetermineHDoutliers(originalFrameName, instanceCol, attrColList, alpha, newColName, unique)
#test[which(test$myOutliers == "TRUE"),]

# the instance col is only needed if we are not treating as unique
DetermineHDoutliers <- function(originalFrameName, instanceCol, attrColList, alpha, newColName, unique) {

library(HDoutliers)
dt <- as.data.table(originalFrameName)


if (unique == TRUE) {
# append a ROW_ID because we will treat each row as unique even if it is not
dt$ROW_ID <- seq.int(nrow(dt))
dtSubset <- dt[, c(attrColList, "ROW_ID"), with = FALSE]
# clean data to remove any missing values
dtSubset[dtSubset==''|dtSubset==' '] <- NA 
dtSubset <- dtSubset[complete.cases(dtSubset),]

# determine indices of the outliers in dtSubset
out <- HDoutliers(dtSubset[, -c("ROW_ID")], alpha = alpha)

if (length(out) < 1) {
# no outliers returned
# every value in our extra column will be false 
dtSubset[[newColName]] <- rep("FALSE")
} else {
# set outlier values to TRUE and other values to FALSE 
dtSubset[[newColName]] <- rep("FALSE")
dtSubset[c(out), newColName] <- "TRUE"
}
# now merge back with the original frame 
results <- merge(x = dtSubset[, -c(attrColList), with = FALSE], y = dt, by = "ROW_ID", all.x = TRUE, all.y = TRUE)
results <- results[, -c("ROW_ID")]
}

else {
dtSubset <- dt[, c(instanceCol, attrColList), with = FALSE]
# clean data to remove any missing values
dtSubset[dtSubset==''|dtSubset==' '] <- NA 
dtSubset <- dtSubset[complete.cases(dtSubset),]
# here we create the row id for dt subset; the merge of dt subset to the original frame will be done on the instance column
dtSubset$ROW_ID <- seq.int(nrow(dtSubset))
# the temp frame is our transformed frame so that all categorical instances are converted to numeric
temp <- as.data.table(dataTrans(dtSubset[, -c(instanceCol, "ROW_ID"), with = FALSE]))
# the ids on temp will match the ids in dtsubset
temp$ROW_ID <- seq.int(nrow(temp)) 
# merge back with the instance column for grouping
mergedFrame <- merge(x = dtSubset[, -c(attrColList), with = FALSE], y = temp, by = "ROW_ID", all.x = TRUE)

# now transform and group based on the instance column

data <- mergedFrame[complete.cases(mergedFrame[[instanceCol]]), lapply(.SD, mean, na.rm=TRUE), by = instanceCol, .SDcols = attrColList]

# get outliers on the collapsed frame
out <- HDoutliers(data[, -c(instanceCol), with = FALSE], alpha = alpha)

if (length(out) < 1) {
# no outliers returned
# every value in our extra column will be false 
data[[newColName]] <- rep("FALSE")
} else {
# assign true to the outlier row ids
data[[newColName]] <- rep("FALSE")
data[c(out), newColName] <- "TRUE"
}

# now merge back with the original frame 
results <- merge(x = data[, -c(attrColList), with = FALSE], y = dt, by = instanceCol, all.x = TRUE, all.y = TRUE)
}
return (results)
}











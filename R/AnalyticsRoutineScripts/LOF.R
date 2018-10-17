runLOF <- function(dts, instCol, attrList, k, uniqInstPerRow, fullColNameList){
	invisible(lapply(list('Rlof', 'data.table', 'dplyr', 'VGAM'), require, character.only = TRUE))
	
	set.seed(123)	
	
	dt <- dts$dt
	tempDt <- dts$dtSubset
	
	lofDt <- 
		lofDt <- as.data.table(lof(tempDt[, attrList, with=FALSE], k))
		if (length(k) == 1) {
			setnames(lofDt, "V1", getNewColumnName(as.character(k), instCol=instCol, allColNames=fullColNameList))
		} else {
			setnames(lofDt, as.character(k), unlist(lapply(as.character(k), getNewColumnName, instCol=instCol, allColNames=fullColNameList)))
		}
	
	lofNames <- names(lofDt)
	tempDt[,c(lofNames) := lofDt]
	
	lopNames <- gsub("LOF", "LOP", lofNames)
	lopCols <- lapply(lofNames, getLOP, tempDt=tempDt)
	tempDt[, (lopNames) := lapply(1:length(lopNames), function(i) unlist(lopCols[[i]]))]

	if (uniqInstPerRow==F) {
		tempDt <- tempDt[, c(instCol, gsub("LOF", "LOP", lofNames)), with=FALSE] %>% setnames(., names(.), gsub("LOP", paste0(instCol, "_LOF"), names(.)))
		dt <- merge(dt, tempDt, by=instCol, all.x=TRUE)
	} else {
		tempDt <- tempDt[, c("generated_uuid99", gsub("LOF", "LOP", lofNames)), with=FALSE] %>% setnames(., names(.), gsub("LOP", paste0(instCol, "_LOF"), names(.)))
		dt <- merge(dt, tempDt, by="generated_uuid99", all.x=TRUE) %>% .[,generated_uuid99 := NULL]
	}
	
	#convert any integer data type columns to numeric type
	integerCols <- names(select_if(dt[,c(grep(paste0(instCol, "_LOF_"), names(dt))),with=FALSE], is.integer))
	if (length(integerCols) > 0 ) {
		dt[,(integerCols):= lapply(.SD, as.numeric), .SDcols = integerCols]
	}
	
	return (dt)
}

getLOP <- function(lofCol, tempDt){
	lof <- tempDt[[lofCol]]
	#if true then valid else if false then invalid (na, nan, inf)
	lof.valid <- lapply(lof, function(x) any(!is.na(x) && !is.infinite(x) && !is.nan(x))) %>% unlist(.) %>% as.character(.)
	lofTempDt <- as.data.table(lof)[, lof.valid := lof.valid]
	
	lof.size <- min(length(lof.valid[lof.valid == "TRUE"]), length(lof))
	lof <- 
		if (all(lof.valid) == "FALSE") lof[grep("TRUE", lof.valid)] else lof
	lof.mean <- mean(lof)
	sstdv <- lapply(lof, function(x) (x - lof.mean)^2) %>% unlist(.) %>% sum(.)

	if (sstdv == 0) {
		lofTempDt$lop[lofTempDt$lof.valid=="FALSE"] <- ifelse(is.na(lofTempDt$lof), NA, 1)
		lofTempDt$lop[lofTempDt$lof.valid == TRUE] <- 0
	} else {
		lofTempDt$lop[lofTempDt$lof.valid=="FALSE"] <- ifelse(is.na(lofTempDt$lof), NA, 0)
		lofTempDt$lop[lofTempDt$lof.valid=="TRUE"] <- lapply(lof, function(x) max(0, erf((x-1)/(sstdv*sqrt(2))))) %>% unlist(.)
	}
	lopCol <- lofTempDt$lop
	rm(lofTempDt)
	return (lopCol)
}

getNewColumnName <- function(requestedColName, instCol, allColNames, constant= "LOF_"){
	proposedColName <- paste0(constant, requestedColName)
	proposedCompleteColName <- paste0(instCol, "_", proposedColName)
	nameExistIndex <- grep(proposedCompleteColName, allColNames)
	if (length(nameExistIndex) > 0) {
		#the proposed name of column already exists in the original frame
		suffix <- substr(allColNames[nameExistIndex], nchar(proposedCompleteColName) + 2, nchar(allColNames[nameExistIndex]))
		if (length(suffix) == 1 && suffix == "") { 
			return (paste0(proposedColName, "_1"))
		} else{
			return (paste0(proposedColName, "_", max(as.integer(suffix), na.rm=T) + 1))
		} 
	} else {
		return (proposedColName)
	}
}

scaleUniqueData <- function(dt, instanceCol, attrColList, uniqInstPerRow){
	lapply(list('data.table', 'dplyr'), require, character.only = TRUE)
	
	if (uniqInstPerRow==F) {
		attr_numeric <- names(select_if(dt[,attrColList,with=FALSE], is.numeric))
		attr_nonNumeric <- c(names(select_if(dt[,attrColList,with=FALSE], is.factor)), names(select_if(dt[,attrColList,with=FALSE], is.character)))
		dtSubset <- dt[,c(instanceCol,attrColList), with=FALSE]
		dtSubset[dtSubset==''|dtSubset==' '] <- NA 
		
		#for numerical attribute columns, scale each value then take average grouped by the instance col
		if (length(attr_numeric) > 0) {
			max_numCols =  sapply(dt[,attr_numeric,with=FALSE], max, na.rm=TRUE)
			min_numCols =  sapply(dt[,attr_numeric,with=FALSE], min, na.rm=TRUE)

			scaledAttrCols <- lapply(attr_numeric, function(x) if((max_numCols[x] - min_numCols[x]) > 0) (dtSubset[[x]] - min_numCols[x])/(max_numCols[x] - min_numCols[x]) else 0)
			dtSubset <- dtSubset[, (attr_numeric) := lapply(1:length(attr_numeric), function(i) unlist(scaledAttrCols[[i]]))]
			
			temp <- dtSubset[complete.cases(dtSubset[[instanceCol]]), lapply(.SD, mean, na.rm=TRUE), by = instanceCol, .SDcols = attr_numeric]
			dtSubset <- merge(x=dtSubset[complete.cases(dtSubset[[instanceCol]]), c(instanceCol,attr_nonNumeric), with=FALSE], y=temp, by=instanceCol, all.x=TRUE) %>% unique(.)
		}
		
		#for nonnumerical attribute columns, convert any factor types to char types. For each col, order alphabetically then concatenate values, comma separated, grouped by instance col
		if (length(attr_nonNumeric) > 0) {
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
		return (list(dt=dt, dtSubset=dtSubset))
	} else {
		dt$generated_uuid99 <- seq.int(nrow(dt))
		tempKeyCol <- names(dt)[which( grepl("^tempGenUUID99SM_", names(dt), ignore.case=FALSE))] %>% .[which(unlist(lapply(., nchar)) == 23)]
		subsetCol <- c(tempKeyCol,"generated_uuid99", instanceCol, attrColList)
		dtSubset <- dt[, subsetCol, with=FALSE] %>% .[complete.cases(.),]
		return (list(dt=dt, dtSubset=dtSubset))
	}
}

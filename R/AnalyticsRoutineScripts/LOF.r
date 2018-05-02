runLOF <- function(dt, tempDt, instCol, attrList, k){
	lapply(list('Rlof', 'data.table', 'dplyr', 'VGAM'), require, character.only = TRUE)
	
	set.seed(123)	
	
	scaledAttrCols <- as.data.table(sapply(tempDt[,attrList,with=FALSE], normalizeCol, na.rm=TRUE)) %>% setnames(., names(.), paste0(names(.), "_SCALED"))
	tempDt <- cbind(tempDt, scaledAttrCols)
	attrListScaled <- paste0(attrList, "_SCALED")
	
	lofDt <- 
		lofDt <- as.data.table(lof(tempDt[, attrListScaled, with=FALSE], k))
		if (length(k) == 1) {
			setnames(lofDt, "V1", getNewColumnName(as.character(k), names(dt)))
		} else {
			setnames(lofDt, as.character(k), unlist(lapply(as.character(k), getNewColumnName, allColNames=names(dt))))
		}
	
	lofNames <- names(lofDt)
	tempDt[,c(lofNames) := lofDt]

	for (i in lofNames){
		tempDt <- getLOP(i, tempDt)
	}
	
	tempDt <- tempDt[, c(instCol, gsub("LOF", "LOP", lofNames)), with=FALSE] %>% setnames(., names(.), gsub("LOP", paste0(instCol, "_LOF"), names(.)))
	
	setindexv(tempDt, instCol)
	setindexv(dt, instCol)
	dt <- merge(dt, tempDt, all.x=TRUE)
	return (dt)
}

getLOP <- function(lofColName, dt){
	lopColName <- gsub("LOF", "LOP", lofColName)
	lof <- dt[[lofColName]]
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
	dt[, lopColName] <- lofTempDt$lop
	rm(lofTempDt)
	dt
}

getNewColumnName <- function(requestedColName, allColNames, colPrefix= "LOF_"){
	proposedColName <- paste0(colPrefix, requestedColName)
	nameGrepVec <- grep(proposedColName, allColNames)
	if (length(nameGrepVec) == 0) {
		proposedColName
	} else {
		existingColNames <- allColNames[nameGrepVec]
		if (length(existingColNames) == 1) {
			paste0(proposedColName, "_1")
		} else {
			largestIndex <- strsplit(existingColNames, paste0(proposedColName, "_")) %>% unlist(lapply(1:length(existingColNames), function(i) paste0(.[[i]][2]))) %>% as.integer(.) %>% max(., na.rm=TRUE) 
			paste0(proposedColName, "_", largestIndex+1)
		}
	}
}

normalizeCol <- function(column, ...) {
	(column - min(column, ...)) / (max(column, ...) - min(column, ...))
}
discretizeColumnsDt <- function(dt, inputList=NULL){
	#inputList = list(colName=list(breaks=..., labels=...), colName2=list(breaks2=..., labels2=...))
	lapply(list('dplyr', 'data.table'), require, character.only = TRUE)
	if("arules" %in% (.packages()))  detach("package:arules", unload=TRUE) 
	
	#vector of indexes of requested-columns containing NAs
	naColVector<- sapply(dt[,names(inputList), with=FALSE], function(x) any(is.na(x) | is.nan(x) |is.infinite(x))) 
	naColVector <- names(subset(naColVector, naColVector == TRUE))
	if (length(naColVector) > 0) {
		dt$generated_uuid99 <- seq.int(nrow(dt))
	}
	
	#get max value from each requested-columns
	maxValues <- sapply(dt[,names(inputList), with=FALSE], function(x) ceiling(max(x, na.rm = TRUE)))

	#validate requested breaks
	for (requestedColName in names(inputList)) {
		str <- ""
		
		#if breaks is not specificed, then need to run hist function to determine binning number (Sturges algorithm under the hood of hist)
		breaksItem <- 
			if(!is.null(getElement(inputList, requestedColName)$breaks)) {
				unique(getElement(inputList, requestedColName)$breaks)
			} else {
				bin <- length(hist(dt[[requestedColName]], plot=FALSE)$counts)
				inputList[[requestedColName]]["breaks"] <- bin
			}
		
		if (is.numeric(breaksItem) && is.vector(breaksItem) && (
		(length(breaksItem) == 1 && (breaksItem > 1L && eval(parse(text=paste0("is.integer(", breaksItem ,"L)"))))) ||
		(length(breaksItem) > 1))) {
			labels <- inputList[[requestedColName]][["labels"]]
			if (is.null(labels) || (!is.null(labels) && ( (length(breaksItem) > 1 && length(labels) == length(breaksItem) - 1) || 
			(length(breaksItem) == 1 && length(labels) == breaksItem) ))) {
				if (length(breaksItem) > 1) inputList[[requestedColName]][["method"]] <- "'fixed'"
				
				if (is.null(inputList[[requestedColName]][["dig.lab"]])){
					inputList[[requestedColName]][["dig.lab"]] <- nchar(maxValues[[requestedColName]]) + 3
				}
				
				for (name in names(inputList[[requestedColName]])) {
					valuesToSTring <- {
						substring <- ifelse(name != "labels", paste0(inputList[[requestedColName]][[name]], collapse=","), 
							toString(sprintf("'%s'", unlist(inputList[[requestedColName]][[name]]))))
						ifelse(length(inputList[[requestedColName]][[name]]) > 1, paste0("c(", substring, ")", collapse=''),substring)
					}
					str <- paste0(str, name, " = ", valuesToSTring, sep = ", ")
				}
				str <- substr(str,1,nchar(str)-2)
			
				newColName <- getNewColumnName(requestedColName, names(dt))
				
				if (length(grep(requestedColName, naColVector)) == 0) { 
					dt[, (newColName):=eval(parse(text = paste0("discretize(dt[['", requestedColName, "']],", str, ",include.lowest = TRUE, right=TRUE,ordered_result=TRUE)")))]
				} else { 
					#requestedCol has NAs
					subsetDt <- dt[complete.cases(dt[[requestedColName]]),c("generated_uuid99", requestedColName), with=FALSE]
					subsetDt[, (newColName):=eval(parse(text = paste0("discretize(subsetDt[['", requestedColName, "']],", 	str, ",include.lowest = TRUE, right=TRUE,ordered_result=TRUE)")))][, eval(requestedColName):=NULL]
					setindex(dt, generated_uuid99)
					setindex(subsetDt, generated_uuid99)
					dt <- merge(x = dt, y = subsetDt, all.x = TRUE)
				}
			}
		}
	}
	dt[, generated_uuid99 := NULL]

	return (dt)
}


getNewColumnName <- function(requestedColName, allColNames, colSuffix="_Discretized"){
	proposedColName <- paste0(requestedColName, colSuffix)
	nameGrepVec <- grep(proposedColName, allColNames)
	if (length(nameGrepVec) == 0) {
		proposedColName
	} else {
		existingColNames <- allColNames[nameGrepVec]
		if (length(existingColNames) == 1) {
			paste0(requestedColName, colSuffix, "_1")
		} else {
			largestIndex <- strsplit(existingColNames, paste0(colSuffix,'_')) %>% unlist(lapply(1:length(existingColNames), function(i) paste0(.[[i]][2]))) %>% as.integer(.) %>% max(., na.rm=TRUE) 
			paste0(requestedColName, colSuffix, "_", largestIndex+1)
		}
	}
}
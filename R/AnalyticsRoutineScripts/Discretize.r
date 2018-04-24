discretizeColumnsDt <- function(dt, inputList=NULL){
	#inputList = list(colName=list(breaks=..., labels=...), colName2=list(breaks2=..., labels2=...))
	lapply(list('arules', 'dplyr', 'data.table'), require, character.only = TRUE)

	#vector of indexes of requested-columns containing NAs
	naColVector<- apply(dt[,names(inputList), with=FALSE], 2, function(x) any(is.na(x) | is.nan(x) |is.infinite(x))) 
	naColVector <- names(subset(naColVector, naColVector == TRUE))
	if (length(naColVector) > 0) {
		dt$generated_uuid99 <- seq.int(nrow(dt))
	}

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
			if (is.null(labels) || (!is.null(labels) && length(labels) == length(breaksItem) - 1)) {
				if (length(breaksItem) > 1) inputList[[requestedColName]][["method"]] <- "'fixed'"
				
				for (name in names(inputList[[requestedColName]])) {
					valuesToSTring <- {
						substring <- ifelse(name != "labels", paste0(inputList[[requestedColName]][[name]], collapse=","), 
							toString(sprintf("'%s'", unlist(inputList[[requestedColName]][[name]]))))
						ifelse(length(inputList[[requestedColName]][[name]]) > 1, paste0("c(", substring, ")", collapse=''),substring)
					}
					str <- paste0(str, name, " = ", valuesToSTring, sep = ", ")
				}
				str <- substr(str,1,nchar(str)-2)
			
				newColName <- {
					proposedColName <- paste0(requestedColName, "_Discretized")
					nameGrepVec <- grep(proposedColName, names(dt))
					if (length(nameGrepVec) == 0) {
						proposedColName
					} else {
						existingColNames <- names(dt)[nameGrepVec]
						if (length(existingColNames) == 1) {
							paste0(requestedColName, "_Discretized_1")
						} else {
							largestIndex <- strsplit(existingColNames, '_Discretized_') %>% unlist(lapply(1:length(existingColNames), function(i) paste0(.[[i]][2]))) %>% as.integer(.) %>% max(., na.rm=TRUE) 
							paste0(requestedColName, "_Discretized_", largestIndex+1)
						}
					}
				}
				
				if (length(grep(requestedColName, naColVector)) == 0) { #requestedCol has no NAs
					dt[, (newColName):=eval(parse(text = paste0("discretize(dt[['", requestedColName, "']],", str, ",include.lowest = TRUE, right=TRUE,ordered_result=TRUE)")))]
				} else { #requestedCol has NAs
					subsetDt <- dt[complete.cases(dt[[requestedColName]]),c("generated_uuid99", requestedColName), with=FALSE]
					subsetDt[, (newColName):=eval(parse(text = paste0("discretize(subsetDt[['", requestedColName, "']],", 	str, ",include.lowest = TRUE, right=TRUE,ordered_result=TRUE)")))][, eval(requestedColName):=NULL]
					dt <- merge(x = dt, y = subsetDt, by = "generated_uuid99", all.x = TRUE)
				}
			}
		}
	}
	dt[, generated_uuid99 := NULL]

	return (dt)
}
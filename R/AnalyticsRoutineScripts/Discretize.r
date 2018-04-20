discretizeColumnsDt <- function(dt, inputList=NULL){
	#inputList = list(colName=list(breaks=..., labels=...), colName2=list(breaks2=..., labels2=...))
	lapply(list('arules', 'dplyr', 'data.table'), require, character.only = TRUE)
	
	dt <- data.frame(dt)
	
	#vector of indexes of requested-columns containing NAs
	naColVector<- sapply(dt[,names(inputList)], function(x) any(is.na(x) | is.nan(x) |is.infinite(x)))
	naColList.index <- as.vector(grep("TRUE", naColVector))
	if (length(naColList.index) > 0) {
		dt$generated_uuid99 <- seq.int(nrow(dt))
		naColVector <- names(inputList)[naColList.index]
	}

	#validate requested breaks
	for (requestedColName in names(inputList)) {
		str <- ""
		
		#if breaks is not specificed, then need to run hist function to determine binning number (Sturges algorithm under the hood of hist)
		breaksItem <- 
			if(!is.null(getElement(inputList, requestedColName)$breaks)) {
				unique(getElement(inputList, requestedColName)$breaks)
			} else {
				bin <- length(hist(dt[, requestedColName], plot=FALSE)$counts)
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
			
				newColName <- paste0(requestedColName, "_Discretized")
				if (length(grep(requestedColName, naColVector)) == 0) { #requestedCol has no NAs
					dt[, newColName] <- eval(parse(text = paste0("discretize(dt[,'", requestedColName, "'],", str, ",include.lowest = TRUE, right=TRUE)")))
				} else { #requestedCol has NAs
					subsetDt <- dt[complete.cases(dt[, requestedColName]), c("generated_uuid99", requestedColName)]
					subsetDt[, newColName] <- eval(parse(text = paste0("discretize(subsetDt[,'", requestedColName, "'],", str, ",include.lowest = TRUE, right=TRUE)")))
					subsetDt[, requestedColName] <- NULL
					dt <- merge(x = dt, y = subsetDt, by = "generated_uuid99", all.x = TRUE)
				}
				
			}
		}
	}
	#dt <- dt %>% mutate_if(is.factor, as.character) %>% select(-one_of("generated_uuid99"))
	dt <- dt %>% select(-one_of("generated_uuid99"))

	return (setDT(dt))
}
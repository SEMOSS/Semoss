getDtClusterTable <- function(method, dts, instanceCol, attrColList, numClusters=NULL, 
minNumCluster=NULL, maxNumCluster=NULL, uniqInstPerRow, fullColNameList){
	#supported method options : (1) pamGower (categorical and numberical data), (2) kmeans (numerical data only; sensitive to outliers),
	#(3) pam (numerical data only; less sensitive to outliers)
	
	invisible(lapply(list('data.table', 'cluster', 'stats', 'dplyr'), require, character.only = TRUE))
	
	dt <- dts$dt
	dtSubset <- dts$dtSubset
	
	dtScaled <- {
		#scale and daisy both handle NAs
		if (method == "pamGower") {
			charCols <- colnames(dtSubset)[which(as.vector(dtSubset[,lapply(.SD, class)]) == "character")]
			if (length(charCols) > 0 ) {dtSubset[,(charCols):= lapply(.SD, as.factor), .SDcols = charCols]}
			typeList <- list(logratio=(ifelse(length(attrColList) < 3, length(attrColList), 3)))
			daisy(dtSubset[,attrColList, with=FALSE], metric = "gower", type = typeList)
		} else if (method == "pam" || method == "kmeans") {
			scale(dtSubset[,attrColList, with=FALSE])
		}
	}
	
	#attemping multiclustering if min/max number of clusters defined
	if (!is.null(minNumCluster) && !is.null(maxNumCluster)) {
		#if multiclustering reactor is called, then determine optimal cluster count first
		tempOptimalIndex <- -1
		sil_width <- c(NA)		
		for(i in minNumCluster:maxNumCluster){
			clustObj_i <- 
				if (grepl("pam", method)) {
					if (method == "pamGower") {
						pam(dtScaled, diss = TRUE, k = i)
					} else if (method == "pam") {
						pam(dtScaled, k = i)
					}
				} else if (method == "kmeans") {
					kmeans(dtScaled, centers = i, nstart = 50)
				}
			
			sil_width[i] <-
				if (grepl("pam", method)) {
					clustObj_i$silinfo$avg.width
				} else if (method == "kmeans") {
					ss <- silhouette(clustObj_i$cluster, dist(dtScaled))
					mean(ss[, 3])
				}
			
			if (!is.na(sil_width[i])) {
				#if tempOptimalIndex has not yet been set OR if current sil_width_i is >= sil_width @ tempOptimalIndex 
				#then reset tempOptimalIndex and tempCluster
				if (tempOptimalIndex == -1 || sil_width[i] >= sil_width[tempOptimalIndex]) {
					tempOptimalIndex <- i
					if (grepl("pam", method)) { 
						dtSubset[, temp:=clustObj_i$clustering]
					} else if (method == "kmeans") {
						dtSubset[, temp:=clustObj_i$cluster]
					}
				}
			}
			rm(clustObj_i);gc()
			
		}
	}
	
	#attemping single clustering if number of clusters defined
	if (!is.null(numClusters)) {
		if (grepl("pam", method)) {
			pamClustering <- 
				if (method == "pamGower") {
					pam(dtScaled, diss = TRUE, k = as.numeric(numClusters), cluster.only=TRUE)
				} else if (method == "pam") {
					pam(dtScaled, k = as.numeric(numClusters), cluster.only=TRUE)
				}
			dtSubset[, temp:=pamClustering]
		} else {
			km <- kmeans(dtScaled, as.numeric(numClusters), nstart = 50)
			dtSubset[, temp:=km$cluster]
		}
		rm(pamClustering, km);gc()
	}
	
	rm(dtScaled);gc()
	
	clusterColName <- getNewColumnName(instanceCol, fullColNameList)
	
	if (uniqInstPerRow==F) {
		dt <- merge(x=dt, y=dtSubset[, c(instanceCol, "temp"),with=FALSE], by=instanceCol, all.x=TRUE)
	} else {
		dt <- merge(x=dt, y=dtSubset[, c("generated_uuid99","temp"), with=FALSE], by="generated_uuid99", all.x=TRUE) %>% .[,generated_uuid99 := NULL]
	}
	
	#replace NA values in temp column with -1 value
	dt[is.na(temp), temp := -1]
	
	#convert the data type of the cluster column from integer to numeric
	dt$temp <- as.numeric(dt$temp)
	
	#rename temp column to {instance column}_cluster
	setnames(dt, "temp", clusterColName)

	return (dt)
}

getNewColumnName <- function(requestedColName, allColNames, constant= "_Cluster"){
	proposedColName <- paste0(requestedColName, constant)
	nameExistIndex <- grep(proposedColName, allColNames)
	if (length(nameExistIndex) > 0) {
		#the proposed name of column already exists in the original frame
		suffix <- substr(allColNames[nameExistIndex], nchar(proposedColName) + 2, nchar(allColNames[nameExistIndex]))
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

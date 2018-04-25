getDtClusterTable <- function(method, dt, instanceCol, attrColList, dateAttrColList=NULL, 
																		numClusters=NULL, minNumCluster=NULL, maxNumCluster=NULL)
{
	#supported method options : (1) pamGower (categorical and numberical data), (2) kmeans (numerical data only; sensitive to outliers),
	#(3) pam (numerical data only; less sensitive to outliers)
	
	lapply(list('data.table', 'cluster', 'stats'), require, character.only = TRUE)
	
	if(!is.null(dateAttrColList)){
		#convert date in yyy-MM-dd format into epoch time so kmeans can operate on attribute(s)
		for (i in 1:length(dateAttrColList)) {
			col <- dateAttrColList[i]
			datecol <- dt[,col]
			epoch <- as.data.frame(unlist(lapply(datecol, function(x) as.numeric(as.POSIXlt(x, format = "%Y-%m-%d")))))
			names(epoch)[1] <- "epoch"
			dt[[paste(col)]] <- epoch$epoch
			rm(col, datecol, epoch)
		}
		attrColList <- c(attrColList, dateAttrColList)
	}
		
	set.seed(123)	
	dt$generated_uuid99 <- seq.int(nrow(dt))
	subsetCol <- c("generated_uuid99", attrColList)
	dtSubset <- dt[,subsetCol, with=FALSE]
	dtSubset <- dtSubset[complete.cases(dtSubset), ]
	
	dtScaled <- {
		#scale and daisy both handle NAs
		if (method == "pamGower") {
			charCols <- colnames(dtSubset)[which(as.vector(dtSubset[,lapply(.SD, class)]) == "character")]
			if (length(charCols) > 0 ) {dtSubset[,(charCols):= lapply(.SD, as.factor), .SDcols = charCols]}
			daisy(dtSubset[,attrColList, with=FALSE], metric = "gower", type = list(logratio = 3))
		} else if (method == "pam" || method == "kmeans") {
			scale(dtSubset[,attrColList, with=FALSE])
		}
	}
	nrows <- nrow(dtSubset)
	tempClust <- data.table(temp=seq.int(nrows))
	
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
						tempClust[, temp:=clustObj_i$clustering]
					} else if (method == "kmeans") {
						tempClust[, temp:=clustObj_i$cluster]
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
			tempClust[, temp:=pamClustering]
		} else {
			km <- kmeans(dtScaled, as.numeric(numClusters), nstart = 50)
			tempClust[, temp:=km$cluster]
		}
		rm(pamClustering, km);gc()
	}
	
	rm(dtScaled);gc()
	
	if (exists("tempClust")) {
		clusterColumnName <- getNewColumnName(instanceCol, names(dt))
		
		#find index from dtSubset
		tempClust[, ':=' (generated_uuid99=dtSubset[,generated_uuid99])]
		
		#convert any factor column classes to character classes
		factorCols <- colnames(dt)[which(as.vector(dt[,lapply(.SD, class)]) == "factor")]
		if (length(factorCols) > 0 ) {dt[,(factorCols):= lapply(.SD, as.character), .SDcols = factorCols]}
		
		#left join dt and tempClust via generated_uuid99 column
		dt <- merge(x = dt, y = tempClust, by = "generated_uuid99", all.x = TRUE)
		
		#replace NA values in temp column with -1 value; drop generated_uuid99 column
		dt[is.na(temp), temp := -1][, generated_uuid99 := NULL]
		
		#rename temp column to {instance column}_cluster
		setnames(dt, "temp", clusterColumnName)
	}
	
	return (dt)
}

getNewColumnName <- function(requestedColName, allColNames, colSuffix="_Cluster"){
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
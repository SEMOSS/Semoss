###################################################################
# Creates JSON of suggested new values 
# with the similar instance matches
###################################################################
collision_resolver <- function(df1, col1, mode, max_dist, method, q, p){
	library(RJSONIO)
	# join frame to itself with the same column name
	results <- fuzzy_join(df1, df1, col1, col1, mode, max_dist, method, q, p)
	
	# rename columns to manipulate by variables
	replacementValue <- "replacementValue"
	count <- "count"
	matchColumn <- "matchColumn"
	matchCount <- "matchCount"
	colnames(results) <- c(replacementValue, count,  matchColumn, matchCount)
	
	#########################################################################
	# results df looks like this we need to denormalize!!!!!
	# replacementValue | count | matchColumn | matchCount
	# ----------------------------------------------------
	# "new value"      | "5"   | "old value" | "2"
    # "new value"      | "5"   | "old VALUE" | "1" 
	# "new value"      | "5"   | "oLD value" | "5"
	#########################################################################
	
	# to denormalize matchColumn is aggregated with a delimiter
	
	delimiter <- '2342234COLLISION_DELIMITER5728934'
	# remove silimar row values where instances match itself
	results <- results[results$replacementValue != results$matchColumn,]
	
	# aggregate match values
	matchAgg <- aggregate(results$matchColumn, list(results$replacementValue), paste, collapse=delimiter)
	colnames(matchAgg) <- c(replacementValue,  matchColumn)
	
	#########################################################################
	# results of matchAgg looks like this
	# replacementValue | matchColumn
	# ----------------------------------------------------
	# "new value"      | "old value+++old VALUE+++oLD value"
	#########################################################################
	
	# aggregate source count
	countAgg <- unique(results[,c(1,2)])
	
	#########################################################################
	# results of countAgg looks like this
	# replacementValue | count
	# ----------------------------------------------------
	# "new value"      | "5"
	#########################################################################
	
	# aggregate match count
	matchCountAgg <- aggregate(results$matchCount, list(results$replacementValue), paste, collapse=delimiter)
	colnames(matchCountAgg) <- c(replacementValue,  matchCount)
	
	#########################################################################
	# results of matchCountAgg looks like this
	# replacementValue | matchCount
	# ----------------------------------------------------
	# "new value"      | "2+++1+++5"
	#########################################################################

	# join aggregated dfs
	joinedDf <- merge(matchAgg, countAgg, by = replacementValue)
	joinedDf <- merge(joinedDf, matchCountAgg, by = replacementValue)
	
	# sort by count
	joinedDf <- joinedDf[order(-rank(joinedDf$count)),] 
	
	#########################################################################
	# results denormalized looks like this now
	# replacementValue | count | matchColumn | matchCount
	# ----------------------------------------------------
	# "new value"      | "5"   | "old value" | "2"
    # "new value"      | "5"   | "old VALUE" | "1" 
	# "new value"      | "5"   | "oLD value" | "5"
	#########################################################################
	
	ldf = lapply(as.list(1:dim(joinedDf)[1]), function(x) joinedDf[x[1],])
	
	#########################################################################
	# ldf is a list that looks like this
	# [[1]]
	# replacementValue | matchColumn                                     | count       | matchCount
	# ------------------------------------------------------------------------------------------------------
	# "new value"      | "old value+++old VALUE+++oLD value" | 5+++5 |2SPLITHIS1+++5
	
	#### split match concat string into list
	#the length of ldf is the number of data results, so each i is one row of data
	for(i in 1:length(ldf)) {
		#matchString is the list of matches for each i
		#it is located at index 2 in ldf[[i]]
		matchString <-ldf[[i]][[2]]
		# matchString looks like "old value+++old VALUE+++oLD value"
		ldf[[i]][[2]] <- as.list(strsplit(matchString, delimiter))
		# ldf[[1]][[2]] looks like "old value" "old VALUE" "oLD value"

		#matchCountString is the number of times each match string appears in the data
		#it is located at index 4 in ldf[[i]]
		matchCountString <-ldf[[i]][[4]]
		ldf[[i]][[4]] <- as.list(strsplit(matchCountString, delimiter))

		#match keeps track of the match values and counts for each i
		#the largest value of j will be the number of matches for that i
		#four each match, we have a value and a count - put this at location j for each match
		ldf[[i]]$match[[1]] <- vector(mode="list", length=length(ldf[[i]][[4]][[1]]))
		for (j in 1:length(ldf[[i]][[4]][[1]])) {
		 ldf[[i]]$match[[1]][[j]]$value<- ldf[[i]][[2]][1][[1]][j]
		 ldf[[i]]$match[[1]][[j]]$count<- ldf[[i]][[4]][1][[1]][j]
		 #ldf will have a column called match that looks like "old value, 2"
		}
		
		#delete columns 
		ldf[[i]] <- ldf[[i]][ , c(1,3,5)]
	}
	json <- toJSON(ldf)
	# clean up values
	rm(ldf)
	rm(delimiter)
	rm(matchString)
	rm(joinedDf)
	rm(matchCountAgg)
	rm(countAgg)
	rm(matchAgg)
	rm(results)
	rm(replacementValue)
	rm(count)
	rm(matchColumn)
	rm(matchCount)
	gc()
	return(json)
}

fuzzy_join <- function(df1,df2,col1, col2, mode, max_dist, method, q, p){
# df1 the dataframe of the first concept
# df2 the dataframe of the first concept
# col1 column name from df1 to join
# col2 column name from df12 to join
# mode of the join: inner, left, right, full, semi, anti
# max_dist maximum distance(lv,dl,qgram,osa,lcs,soundex) or minimum similarity(jw,cosine,jaccard) required to join the concept records
# method for fuzzy matching
# q the size of grams for q-gram matching
# p - penalty for some methods for fuzzy matching
# 
library(fuzzyjoin)
a<-paste(col1,"=","\"",col2,"\")",sep="")
b<-paste(",mode=\"",mode,"\",max_dist=",max_dist,",method=\"",method,"\",q=",q,",p=",p,")",sep="")
c<-paste("r<-","stringdist_join(df1,df2,by=c(",a,b,sep="")
eval(parse(text=c))
r<-as.data.frame(r)
if(ncol(df1) == 1){
r<-as.data.frame(r)
names(r)[1]<-col1
}

return(r)
}
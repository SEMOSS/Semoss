


getCorrelationTable <- function(frameName, colVector) {
	require(data.table)
 
	#results <- eval(parse(text = colSelect))
	results<-cor(frameName[,colVector, with=FALSE], use = "all.obs")

	# format the results as a table where we have x,y,corVal
	columns <- colnames(results)
	rows <- rownames(results)

	counter <- 1
	l <- vector("list", length = nrow(results) * 2)

	for (i in 1:nrow(results)) {

		for (j in 1:nrow(results)) {
		v <- c(columns[i], rows[j], results[columns[i],rows[j]])
		l[[counter]] <- v
		counter <- counter + 1
		rm(v)
		}
	}

	retTable <- as.data.table(matrix(unlist(l), ncol = 3, byrow=T),stringsAsFactors=FALSE)
	names(retTable) <- c("Column_Header_X", "Column_Header_Y", "Correlation")
	return (retTable)
}



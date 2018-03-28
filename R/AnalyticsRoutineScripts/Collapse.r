run.seq <- function(x) as.numeric(ave(paste(x), x, FUN = seq_along));

collapse <- function (df, groupByCol, valueCol, delimiter) {
	# aggregate frame by inputs
	aggFrameScript <- paste("aggFrame <- aggregate(df$",valueCol,", list(df$",groupByCol,"), paste, collapse='",delimiter,"')", sep="") 
	eval(parse(text=aggFrameScript))
	# rename columns 
	colnames(aggFrame) <- c("groupByCol", "valueCol_Collapse");
	renameColScript <- paste("colnames(aggFrame) <- c('",groupByCol,"', '",valueCol,"_Collapse')", sep="") 
	eval(parse(text=renameColScript))
	# merge results to dt
	L <- list(aggFrame,df);
	L2 <- lapply(L, function(x) cbind(x, run.seq = run.seq(aggFrame[1])));
	joinDf <- Reduce(function(...) merge(..., all = TRUE), L2)[-2];
	# return new dt with joined column
	df <- joinDf
	df <- as.data.table(df)
	return(df)
}
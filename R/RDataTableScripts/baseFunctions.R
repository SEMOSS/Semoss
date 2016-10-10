#Making sure these methods are unqiue by appending the random numbers after a period

createEmptyDataTable.123456<-function(elems) {
  return (as.data.table(t(elems)))
}

appendToDataTable.123456<-function(dt, elems) {
	elems = t(elems);
	n<-attr(dt, 'rowcount')
	if (is.null(n))
		n<-nrow(dt)
	if (n==nrow(dt))
	{
		tmp<-data.frame(matrix(nrow=(2*n),ncol=ncol(dt)))
		dt<-rbindlist(list(dt, tmp), fill=FALSE, use.names=FALSE)
		setattr(dt,'rowcount', n)
	}
	pos<-0:(ncol(dt)-1)
	for (j in seq_along(pos))
	{
		set(dt, as.integer(n+1), j, elems[j])
	}
	setattr(dt,'rowcount',n+1)
	return(dt)
}

removeEmptyRows.123456<-function(dt) {
	dt <- dt[rowSums(is.na(dt)) != ncol(dt), ]
}
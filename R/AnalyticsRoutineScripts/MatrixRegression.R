fit_lm<-function(df,dep_var,ind_vars){
	df<-df[,c(dep_var,ind_vars)]
	df[df=="null" | df==""]<-NA
	df<-na.omit(df)
	X<-as.matrix(df[,c(ind_vars)])
	y<-as.numeric(df[[dep_var]])
	fit<-.lm.fit(X,y)
	out<-list()
	out[[1]]<-data.frame(ColumnName=ind_vars,Coefficient=fit$coefficients,stringsAsFactors=F)
	out[[2]]<-data.frame(Actual=y,Predicted=y+fit$residuals,stringsAsFactors=F)
	gc()
	return(out)
}
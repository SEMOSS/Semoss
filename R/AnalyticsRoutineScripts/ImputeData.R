impute_data<-function(in_df,attrs=colnames(df),candidates=3,max_iter=5,nbr_trees=100){
# Imputes missing values
# Args
# df - dataframe with missing values (NA)
# attrs - an array of column names to impute missing values
# Outout
# dataframe with missing values imputed
	library(missRanger)
	if(nrow(in_df)>0){
		cols<-which(colnames(in_df) %in% attrs)
		cmd<-paste0('df<-in_df[,',paste0('c(',paste(cols,collapse=','),')'),']')
		eval(parse(text=cmd))
		df[df=="null" | df=="NULL"]<-NA
		out = tryCatch({
			missRanger(df,.~1,pmm.k = candidates,maxiter=max_iter,num.trees=nbr_trees)
		}, error = function(e) {
			df[0,]
		})
		result<-replace_columns(in_df,out)
		if(nrow(out)==0){
			result<-"No non-missing elements to sample from"
		}
	}else{
		result<-"Input dataframe is empty"
	}
	gc()
	return(result)
}

replace_columns<-function(in_df,out_df,row_id=""){
	attrs<-colnames(out_df)
	cols<-which(colnames(in_df) %in% attrs)
	if(row_id==""){
		cmd<-paste0('in_df[,',paste0('c(',paste(cols,collapse=','),')'),']<-out_df')
	}else{
		ind<-which(in_df[[row_id]] %in% out_df[[row_id]])
		cmd<-paste0('in_df[ind,',paste0('c(',paste(cols,collapse=','),')'),']<-out_df')
	}
	eval(parse(text=cmd))
	
	return(in_df)
}

find_outliers<-function(input_df,attrs,threshold=0.05,mis_data="as_is",outlier_col="Outlier") {
# Find outliers procedure
# Args
# input_df - an input table/dataframe to use for outliers search
# attrs - a list of column names of df to be used for the outliers search
# threshold - a threshold for determine the cutof for outliers
# mis_data - an action specifying how to handle missing data (options: "impute","drop","as_is")
	ACTIONS<-c("impute","drop","as_is")
	row_id<-paste(sample(c(1:9,letters,LETTERS),15),collapse="")
	
	library(HDoutliers)
	input_df[[row_id]]<-seq(nrow(input_df))
	attrs<-append(attrs,row_id)

	result<-list()
	# clean up the input frame
	cols<-which(colnames(input_df) %in% attrs)
	cmd<-paste0('df<-input_df[,',paste0('c(',paste(cols,collapse=','),')'),']')
	eval(parse(text=cmd))
	if(tolower(mis_data)==ACTIONS[1]){
		df<-impute_data(df,attrs)
	}else if(tolower(mis_data)==ACTIONS[2]){
		df[df=="null" | df=="NULL"]<-NA
		df<-df[complete.cases(df),]
	}else if(tolower(mis_data)!=ACTIONS[3]){
		result[[1]]<-nrow(df)
		result[[2]]<-"Possible missing data actions are: impute, drop and as is"
		return(result)
	}
	
	rows<-nrow(df)
	if(rows>0){
		# determine indices of the outliers in dtSubset
		outliers = tryCatch({
			HDoutliers(df, alpha = threshold)
		}, error = function(e) {
			"An unknown error occurred"
		})
		result[[1]]<-rows
		if(class(outliers)=="character"){
			result[[2]]<-outliers
		}else {
			dropped_rows<-which(!(input_df[[row_id]] %in% df[[row_id]]))
			df<-replace_columns(input_df,df,row_id)
			ind<-which(colnames(df)==row_id)
			cmd<-paste0("df<-df[,-",ind,"]")
			eval(parse(text=cmd))
			
			if(is.null(outliers)){
				df[[outlier_col]]<-'no'
			}else{
				df[[outlier_col]]<-'no'
				if(length(outliers)>0){
					df[c(outliers),outlier_col]<-'yes'
				}
			}
			if(length(dropped_rows)>0){
				df[dropped_rows,outlier_col]<-'unknown'
			}
			n<-ncol(df)
			cmd<-paste0('x<-df[,c(',n,',1:',n-1,')]')
			eval(parse(text=cmd))
			result[[2]]<-x
		}
	}else{
		result[[1]]<-0
		result[[2]]<-df
	}
	gc()
	return(result)
}




get_dataset_outliers<-function(df,nbr_int=10,method="mean"){
# Identifies column insights
# Args
# df - dataframe/table to search
# nbr_int - number of interval to discretize data (default 10)
# ext - extention coefficient (default 1.5)
# Output
# list with each element is a list itself 
# with first element discretized data, second anomalies of the data 
	library(data.table)
	library(arules)
	n<-ncol(df)
	cols<-colnames(df)
	out<-data.frame(Column=character(),Outlier=character(),Frequency=integer(),stringsAsFactors=FALSE)
	if(n>0){
		for(i in 1:n){
			item<-list()
			if(class(df[[i]]) %in% c("integer","numeric")){
				item[[1]]<-table(discretize(df[[i]],method="interval",breaks=nbr_int))
				item[[2]]<-get_univarible_outliers(item[[1]],method)
				m<-length(item[[2]])
				if(m>0){
					out<-rbind(out,data.frame(Column=rep(names(df)[i],m),Outlier=names(item[[2]]),Frequency=unname(as.integer(item[[2]]))))
				}
			}else if(class(df[[i]]) %in% c("character","factor")){
				item[[1]]<-table(as.numeric(as.factor(df[[i]])))
				names(item[[1]])<-levels(as.factor(df[[i]]))
				item[[2]]<-get_univarible_outliers(item[[1]],method)
				m<-length(item[[2]])
				if(m>0){
					out<-rbind(out,data.frame(Column=rep(names(df)[i],m),Outlier=names(item[[2]]),Frequency=unname(as.integer(item[[2]]))))
				}
			}else if(class(df[[i]])=="logical"){
				item[[1]]<-table(as.numeric(df[[i]]))
				names(item[[1]])<-unname(sapply(names(out[[i]]),function(x) if(x=="0") "FALSE" else "TRUE"))
				item[[2]]<-get_univarible_outliers(item[[1]],method)
				m<-length(item[[2]])
				if(m>0){
					out<-rbind(out,data.frame(Column=rep(names(df)[i],m),Outlier=names(item[[2]]),Frequency=unname(as.integer(item[[2]]))))
				}
			}
		}
	}
	gc()
	return(out)
}


get_univarible_outliers<-function (x,method="mean"){
	if (method=="mean") {
		avrg <- mean(x)
		stdev <-sd(x)
		dtf <- data.frame(ID=seq.int(length(x)), obs=x, outlier=abs(x-avrg)>2*stdev)
		midp <- avrg
		lower <- avrg-2*stdev
		upper <- avrg+2*stdev
		ind <- which(dtf$outlier)
	} else {}
	if (method=="median") {
		med <- median(x)
		MAD <-median(abs(med-x))
		dtf <- data.frame(ID=seq.int(length(x)), obs=x, outlier=abs(x-med)>2*(MAD/0.6745))
		midp <- med
		lower <- med-2*(MAD/0.6745)
		upper <- med+2*(MAD/0.6745)
		ind <- which(dtf$outlier)
	} else {}
	if (method=="boxplot") {
		Q1 <- quantile(x, 0.25)
		Q3 <- quantile(x, 0.75)
		IntQ <-Q3-Q1
		dtf <- data.frame(ID=seq.int(length(x)), obs=x, outlier=x<Q1-1.5*IntQ | x>Q3+1.5*IntQ)
		midp <- median(x)
		lower <- Q1-1.5*IntQ
		upper <- Q3+1.5*IntQ
		ind <- which(dtf$outlier)
	} 
	if(length(ind)>0){
		outliers<-x[ind]
	}else{
		outliers<-vector()
	}
	return(outliers)
}

get_subset<-function(df,z,col_name){
	b<-paste0('c("',paste(as.character(z[z$Column==col_name,]$Outlier),collapse='","'),'")')
	cmd<-paste0("df1<-df[df$",col_name," %in% ",b,",]")
	eval(parse(text=cmd))
	return(df1)
}


get_dep_ranking<-function(dtf,dep_vars,mthd = "estevez",desc_mthd="sturges",schm="mid"){
	library(varrank)
	out <- varrank(data.df = dtf,method = mthd,variable.important = dep_vars,discretization.method = desc_mthd,algorithm = "forward", scheme = schm,verbose=FALSE)
	plot(out)
	r<-diag(out$distance.m)
	p<-data.frame(Column=names(r),RankScore=unname(r))
	gc()
	return(p)
}

get_frequent_itemsets<-function(df,min_cnt=2,sprt=0.1){
	MIN_BREAKS=3
	library(arules)
	
	y<-sapply(df,class)
	z<-rapply(df,function(x)length(unique(x)))
	ind<-which(y!='numeric' | z > MIN_BREAKS)
	if(length(ind)>0){
		df<-df[,ind]
		max_cnt<-ncol(df)
		trans<-as(df,"transactions")
		itemsets <- apriori(trans, parameter = list(target = "frequent",  supp=sprt, minlen = min_cnt, maxlen=max_cnt))
		out<-interestMeasure(itemsets, transactions = trans)
		out$items<-labels(itemsets)
		out<-out[out$lift>1,]
		out<-out[order(-out$support,-out$lift),]
		out<-out[,c(6,1,2,3,4,5)]
	}else{
		out<-data.frame()
	}
	gc()
	return(out)	
}

select_features<-function(df,dep_var){
	library(Boruta)
	library(lubridate)
	
	# Drop Date columns
	types<-sapply(df,class)
	col_ind<-which(types == 'Date')
	if(length(col_ind)>0){
		# convert date to seconds (integer)
		for(i in 1:length(col_ind)){
			df[,col_ind[i]]<-time_length(interval(ymd("1970-01-01"), df[,col_ind[i]]), "second")
		}
	}
	df<-impute_data(df,colnames(df))
	
	if(class(df[[dep_var]])=="character"){
		df[[dep_var]]<-as.factor(df[[dep_var]])
	}
	cmd<-paste0("x<-Boruta(",dep_var,"~.,data=df,doTrace=0)")
	eval(parse(text=cmd))
	y<-attStats(x)
	out<-data.frame(Feature=rownames(y),Importance=y$meanImp,Selection=as.character(y$decision))
	out<-out[order(-out$Importance),]
	#plot(x)
	gc()
	return(out)
}

get_df_scan<-function(df){
	char_names<-c('Column','Missing','Min_Chars','Max_Chars','Empty','Unique','Whitespace')
	num_names<-c('Column','Missing','Mean','SD','P0','P25','P50','P75','P100')
	date_names<-c('Column','Missing','Min','Max','Median','Unique')
	factor_names<-c('Column','Missing','Unique','Top_Counts')
	library(data.table)
	library(skimr)
	library(plyr)
	if(nrow(df)>0){
		out<-list()
		types<-sapply(df,class)	
		ind<-which(types=='factor')
		if(length(ind)>0){
			for(i in 1:length(ind)){
				cmd<-paste0('df$',colnames(df)[ind[i]],'<-as.character(df$',colnames(df)[ind[i]],')')
				eval(parse(text=cmd))
			}
		}
		ind<-which(types=='integer')
		if(length(ind)>0){
			for(i in 1:length(ind)){
				cmd<-paste0('df$',colnames(df)[ind[i]],'<-as.numeric(df$',colnames(df)[ind[i]],')')
				eval(parse(text=cmd))
			}
		}
		types<-sapply(df,class)
		types_freq<-count(types)
		for(i in 1:nrow(types_freq)){
			type<-as.character(types_freq$x[i])
			ind<-which(types %in% type)
			s<-paste0('c(',paste(ind,collapse=','),')')
			cmd<-paste0('r<-as.data.frame(skim_without_charts(df[,',s,']))')
			eval(parse(text=cmd))
			r[,2]<-colnames(df)[ind]
			if(type=='character'){
				r<-r[,c(2,3,5,6,7,8,9)]
				colnames(r)<-char_names
			}else if(type=='numeric'){
				type<-'number'
				r<-r[,c(2,3,5,6,7,8,9,10,11)]
				colnames(r)<-num_names
				r$Mean<-round(r$Mean,4)
				r$SD<-round(r$SD,4)
				r$P0<-round(r$P0,4)
				r$P25<-round(r$P25,4)
				r$P50<-round(r$P50,4)
				r$P75<-round(r$P75,4)
				r$P100<-round(r$P100,4)
			}else if(type=='Date'){
				r<-r[,c(2,3,5,6,7,8)]
				colnames(r)<-date_names
				r$Min<-as.character(r$Min)
				r$Max<-as.character(r$Max)
				r$Median<-as.character(r$Median)
			}else if(type=='factor'){
				r<-r[,c(2,3,6,7)]
				colnames(r)<-factor_names
			}
			out[[type]]<-r
		}
	}
	gc()
	return(out)
}

detect_outliers<-function(input_df){
	library(HDoutliers)
	library(lubridate)
	
	df<-input_df
	# Drop Date columns
	types<-sapply(df,class)
	col_ind<-which(types == 'Date')
	if(length(col_ind)>0){
		# convert date to seconds (integer)
		for(i in 1:length(col_ind)){
			df[,col_ind[i]]<-time_length(interval(ymd("1970-01-01"), df[,col_ind[i]]), "second")
		}
	}
	# impute missing data
	df<-impute_data(df,colnames(df))
	# detect outliers
	ind<-HDoutliers(df)
	if(length(ind)>0){
		out<-input_df[ind,]	
	}else{
		out<-data.frame()
	}
	gc()
	return(out)
}

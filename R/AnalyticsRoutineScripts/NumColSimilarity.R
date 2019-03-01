exec_kstest<-function(x,y,alpha=0.05){
# Checks whether two numerical samples belong to the identical cdf
# Arguments
# x - first numerical samples
# y - second numerical samples
# alpha - significance level
# Output
# Two item (yes/no) array: itentical cdf, close cdf

	options(warn=-1)
	if(!exists("ks_table")){
		ks_table <<- read.csv("ks_table.csv")
		colnames(ks_table)<-substring(colnames(ks_table),2)
	}
	z<-ks.test(x,y)
	D<-z$statistic
	p.value<-z$p.value
	n<-length(x)
	if(n>40){
		myrow<-ks_table[41,]/sqrt(n)
	}else{
		myrow<-ks_table[n,]
	}
	ind<-which(colnames(ks_table)==alpha)
	if(length(ind) >0){
		if(ind==1){
			ind<-3
		}
	}else{
		ind=3
	}
	out<-vector()
	cvalue<-myrow[ind]
	if(p.value>=alpha){
		out["Identical_CDF"]<-1
	}else{
		out["Identical_CDF"]<-0
	}
	if(D<=cvalue){
		out["Close_CDF"]<-1
	}else{
		out["Close_CDF"]<-0
	}
	options(warn=0)
	gc()
	return(out)
}

find_match<-function(df_new,df_exist,alpha=0.05, show_all=TRUE){
# Discovers when numeric sets have identical cumulative probability distribution
# Arguments
# df_new - dataframe of new columns of numerical values to match
# df_exist - dataframe consisting of existing numerical columns values (column names reflect db, tbl and col of their origin)
# alpha - significance level
# Output
# Dataframe of column names and status whether they have identical or close cdf to the new column
	library(data.table)
	matches<-data.table(New=character(),Existing=character(),Identical_CDF=numeric(),Close_CDF=numeric())
	n<-ncol(df_exist)
	if(n>0){
		m<-ncol(df_new)
		if(m>0){
			for(k in 1:m){
				x<-df_new[,k]
				for(i in 1:n){
					y<-df_exist[,i]
					status<-exec_kstest(x,y,alpha)
					if(show_all | status["Identical_CDF"]==1 | status["Close_CDF"]==1){
						matches<-rbindlist(list(matches,list(colnames(df_new)[k],colnames(df_exist)[i],status["Identical_CDF"],status["Close_CDF"])))
					}
				}
			}
		}
	}
	gc()
	return(matches)
}

plot_ecdf<-function(df){
	b<-sort(df[,1])
	c<-sort(df[,2])
	n<-nrow(df)
	z<-b
	ecdf.fun<-ecdf(z)
	x<-seq(1,n,length=n)
	
	plot(x, ecdf.fun(b), lwd=2, col="red", type="l", ylab="Probability", main="ECDF")
	lines(x, ecdf.fun(c), lwd=2)
	cmd<-paste0("legend(\"topleft\", legend=c(\"",colnames(df)[1],"\",\"",colnames(df)[2],"\"), lwd=2,col=c(\"red\",\"black\"))")
	eval(parse(text=cmd))
}

ecdf_values<-function(v,n){
# Extracts coordinates from constructed cumulative probability distribution
# Arguments
# v - vector of numerical values
# n - number partitions of probability interval
# Output
# List of x and y coordinates for cdf

	f<-ecdf(v)
	x<-v
	y<-f(x)
	inv_ecdf <- function(f){
		x <- environment(f)$x
		y <- environment(f)$y
		approxfun(y, x)
	}
	g <- inv_ecdf(f)
	library(data.table)
	coords<-data.table(x=numeric(),y=numeric())
	for(i in 1:n){
		coords<-rbindlist(list(coords,list(g(i/n),i/n)))
	}
	gc()
	return(coords)
}


master_col_data<-function(col,margin=0.1,ignoreCase=TRUE,method="jw",p=0.1){
	library(plyr)
	library(stringdist)
	# compute term freq
	paste2 <- function(x, y, sep = " ") paste(x, y, sep = sep)
	mydoc<-Reduce(paste2,col)
	# termFreq replacement
	a<-count(col)
	r<-a$freq
	names(r)<-tolower(as.character(a$x))
	
	r<-mean(r)/r
	# identify unique values
	c<-unique(col)
	# compute string dist matrix
	if(ignoreCase){
		d<-stringdistmatrix(tolower(c),tolower(c),method=method,p=p)
	} else{
		d<-stringdistmatrix(c,c,method=method,p=p)
	}
	
	r<-r[order(match(names(r),tolower(c)))]
	
	d<-diag(r)%*%d
	d<-t(diag(r)%*%t(d))

	d<-round(d,4)
	out<-find_hubs(d,margin)
	if(!is.null(out)){
		hubs<-c[out$medoids]
		r<-hubs[out$clustering]
		f<-function(x) r[which(c == x)]
		r<-unlist(lapply(col,f))
	}else{
		r<-col
	}
	gc()
	return(r)
}

bisection<-function(d, threshold, prec = 1e-4,  n = 50){
	x1<-nrow(d)
	x0<-1
	for (i in 1:n) {
		x2=(x1+x0)/2	
		r<-pam(d, x2, diss = TRUE,stand=TRUE)
		max_diss<-max(r$clusinfo[,2])
		if(max_diss < threshold){
			x0<-x0
			x1<-x2
		} else{
			x0<-x2
			x1<-x1
		}
		if(x1 - x0 <= prec){
			return(round(x2,0))
		}
	}
}

find_hubs<-function(d,reach){
	library(cluster)
	n<-bisection(d,reach)
	if(n != nrow(d)){
		r<-pam(d, n, diss = TRUE)
	}else{
		r<-NULL
	}
}

best_match_zero<-function(col1,col2,method="jw",p=0.1){
	z<-best_match(col1,col2,method,p)
	z<-z[z$dist == 0,]
	gc()
	return(z)
}

best_match_nonzero<-function(col1,col2,method="jw",p=0.1){
	z<-best_match(col1,col2,method,p)
	z<-z[z$dist != 0,]
	gc()
	return(z)
}

best_match<-function(col1,col2,ignoreCase=TRUE,method="jw",p=0.1){
	library(stringdist)
	c1<-unique(col1)
	c2<-unique(col2)
	if(ignoreCase){
		d<-stringdistmatrix(tolower(c1),tolower(c2),method=method,p=p)
	}else{
		d<-stringdistmatrix(c1,c2,method=method,p=p)
	}
	n<-dim(d)
	x<-as.vector(t(d))
	x<-round(x,4)
	y<-rep(c1,each=n[2])
	z<-rep(c2,n[1])
	r<-as.data.frame(cbind(y,z,x))
	r[,3] <- as.numeric(as.character(r[,3]))
	names(r)<-c("col1","col2","dist")
	gc()
	return(r)
}

blend<-function(src,srccol,trg,trgcol,link,jointype="inner"){
	# Blending two dataframes
	# Arguments
	# src - source dataframe
	# srccol - column of the source dataframe to be blended on
	# trg - dataframed to be blended
	# trgcol - column of to be blended dataframe to be blended on
	# link - two column dataframe defining the blend selection previously established
	# jointype - type of join requested: left, right, full. Anything else interpreted as inner
	
	link$i.and.d<-seq(1:nrow(link))
	m<-max(nrow(src)+nrow(trg),nrow(src)*nrow(trg))
	
	cmd<-paste0("r1<-merge(src,link,by.x=\"",srccol,"\",by.y=\"",colnames(link)[1],"\",all.x=TRUE,allow.cartesian=TRUE)")
	eval(parse( text=cmd ))
	r1[is.na(r1$i.and.d),"i.and.d"]<-m+1
	
	cmd<-paste0("r2<-merge(trg,link,by.x=\"",trgcol,"\",by.y=\"",colnames(link)[2],"\",all.x=TRUE,allow.cartesian=TRUE)")
	eval(parse( text=cmd ))
	r2[is.na(r2$i.and.d),"i.and.d"]<-m+2
	
	if(jointype=="left"){
		r<-merge(r1,r2,by="i.and.d",all.x=TRUE,allow.cartesian=TRUE)
	}else if(jointype=="right"){
		r<-merge(r1,r2,by="i.and.d",all.y=TRUE,allow.cartesian=TRUE)
	}else if(jointype=="full"){
		r<-merge(r1,r2,by="i.and.d",all=TRUE,allow.cartesian=TRUE)
	}else{
		r<-merge(r1,r2,by="i.and.d",allow.cartesian=TRUE)
	}
	gc()
	return(r)
	
}

curate<-function(col,link){
	# link dataframe contains col1,col2 where col1 contains instances that needs to be changed 
	# and col2 to which values
	
	# identify instance to be fixed
	df<-as.data.frame(col)
	names(df)<-"col"
	df1<-merge(df,link,by.x="col",by.y="col1",all.x=TRUE)
	# fix it
	df1<-df1[order(match(df1[,1],col)),]
	df1[!(is.na(df1$col2)),1]<-df1[!(is.na(df1$col2)),2]
	gc()
	return(as.character(df1[,1]))
}

self_match<-function(col,ignoreCase=TRUE,method="jw",p=0.1){
	library(stringdist)
	c<-unique(col)
	if(ignoreCase){
		d<-stringdistmatrix(tolower(c),tolower(c),method=method,p=p)
	} else{
		d<-stringdistmatrix(c,c,method=method,p=p)
	}
	n<-nrow(d)
	x<-d[lower.tri(d)]
	x<-round(x,4)
	y<-matrix(c,nrow=nrow(d),ncol=ncol(d))
	y<-y[lower.tri(y)]
	z<-t(matrix(c,nrow=nrow(d),ncol=ncol(d)))
	z<-z[lower.tri(z)]
	r<-as.data.frame(cbind(y,z,x))
	r[,3] <- as.numeric(as.character(r[,3]))
	names(r)<-c("col1","col2","dist")
	gc()
	return(r)
}	

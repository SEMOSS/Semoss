best_match_zero<-function(col1,col2){
	z<-best_match(col1,col2)
	z<-z[z$dist == 0,]
	gc()
	return(z)
}

best_match_nonzero<-function(col1,col2){
	z<-best_match(col1,col2)
	z<-z[z$dist != 0,]
	gc()
	return(z)
}

best_match<-function(col1,col2,method="jw",p=0.1){
	# Returns the best match for each instance and the respected distance
	# Arguments
	# col1 - an array of instances of the column to be matched
	# col1 - an array of instances of the matching column
	# minimum distance allowed
	
	library(stringdist)
	dist<-stringdistmatrix(col1,col2,method=method,p=p)
	mins<-apply(dist,1,function(x)return(array(which.min(x))))
	z<-data.frame(col1);
	z$col2<-col2[mins]
	z$dist<-apply(dist,1,function(x)return(array(min(x))))
	z$dist<-round(z$dist,4)
	gc()
	return(z)
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
	
	link$id<-seq(1:nrow(link))
	m<-nrow(src)+nrow(trg)
	
	cmd<-paste0("r1<-merge(src,link,by.x=\"",srccol,"\",by.y=\"",colnames(link)[1],"\",all.x=TRUE)")
	eval(parse( text=cmd ))
	r1[is.na(r1$id),"id"]<-m+1
	
	cmd<-paste0("r2<-merge(trg,link,by.x=\"",trgcol,"\",by.y=\"",colnames(link)[2],"\",all.x=TRUE)")
	eval(parse( text=cmd ))
	r2[is.na(r2$id),"id"]<-m+2
	
	if(jointype=="left"){
		r<-merge(r1,r2,by="id",all.x=TRUE)
	}else if(jointype=="right"){
		r<-merge(r1,r2,by="id",all.y=TRUE)
	}else if(jointype=="full"){
		r<-merge(r1,r2,by="id",all=TRUE)
	}else{
		r<-merge(r1,r2,by="id")
	}
	#r<-r[,ifelse(!(colnames(r) %in% c(colnames(link)[1],colnames(link)[2],"id")))]
	#list <- c("id","col1","col2","dist.y","dist.x")
	#r<-r[,colnames(r) %in% list]
	#	r<-r[,colnames(r) %in% c("id","col1","col2","dist.y","dist.x")]
# 
	gc()
	return(r)
	
}

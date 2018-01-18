viz_history<-function(df){
	viz<-df[df$dimension1 == "viz",]
	n<-nrow(viz)
	z<-data.table(unit=integer(),element=integer(),dbname=character(),tblname=character(),colname=character(),component=character(),chart=character(),user=character());
	if(n > 0){
		for(i in 1:n){
			data<-fromJSON(viz[i,2])
			user<-viz[i,5]
			names(data[1])
			chart<-names(data[[1]][1])
			if(chart != "false" & chart != "collision-resolver"){
				m<-length(data[[1]][[1]])
				if(m > 0){
					for(j in 1:m){
						row<-data[[1]][[1]][[j]]
						if(length(row) > 0){
							db<-row[1,1]
							tbl<-row[1,2]
							col<-row[1,3]
							comp<-row[1,5]
							# store in the table chart plus db, etc.
							z<-rbindlist(list(z,list(i,j,db,tbl,col,comp,chart,user)))
						}
					}
				}
			}
		}
	}
	rm(n,user)
	return(z)
}

viz_recom<-function(df,df1,chartToExclude=NULL,top=3){
	df$id<-paste0(df$dbname,"$",df$tblname,"$",df$colname,"$",df$chart)
	df0<-count(df,names(df)[c(3,4,5,7)])

	library(dplyr)
	df2<-semi_join(df0,df1)
	df3<-df2[,4:5]
	detach("package:dplyr", unload=TRUE)
	library(plyr)
	o<-count(df3,names(df3)[1])
	o<-o[order(-o$freq),]
	if(!is.null(chartToExclude)){
		o<-o[tolower(o$chart) != tolower(chartToExclude),]
	}
	o$weight<-round(o$freq/max(o$freq),4)
	o<-o[,-2]
	o<-head(o,top)
	q<-df2[tolower(df2$chart) %in% tolower(o[,1]),]
	q1<-do.call(rbind, lapply(split(q,q$chart), function(x) {return(x[which.max(x$freq),])}))
	q1$id<-paste0(q1$dbname,"$",q1$tblname,"$",q1$colname,"$",q1$chart)
	
	p<-df[df$id %in% q1$id,]
	units<-aggregate(p$unit,by=list(chart=p$id),FUN=min)[,2]
	
	p1<-df[df$unit %in% units,][,3:7]
	r<-merge(p1,o,by="chart")[,c(2,3,4,1,5,6)]
	
	rm(df0,df2,df3,p,p1,q,q1,o)
	return(r[order(-r$weight,r$chart,r$component,r$dbname,r$tblname,r$colname),])
}


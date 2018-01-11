viz_history<-function(df){
	viz<-df[df$dimension1 == "viz",]
	n<-nrow(viz)
	z<-data.table(dbname=character(),tblname=character(),colname=character(),chart=character());
	if(n > 0){
		for(i in 1:n){
			data<-fromJSON(viz[i,2])
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
							# store in the table chart plus db, etc.
							z<-rbindlist(list(z,list(db,tbl,col,chart)))
						}
					}
				}
			}
		}
	}
	# count for db, table, col, chart to find frequencies
	o<-count(z,names(z))
	return(o)
}

viz_recom<-function(df,df1,grid=NULL,top=3){
	library(dplyr)
	df2<-semi_join(df,df1)[,4:5]

	detach("package:dplyr", unload=TRUE)
	library(plyr)
	o<-count(df2,names(df2)[1])
	o<-o[order(-o$freq),]
	if(!is.null(grid)){
		o<-o[tolower(o$chart) != tolower(grid),]
	}
	o$weight<-round(o$freq/max(o$freq),4)
	o<-o[,-2]
	return(head(o,top))
}



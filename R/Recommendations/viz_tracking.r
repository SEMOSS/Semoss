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
	# df - is the history frame
	# df1 - is the current frame
	df$dbname<-as.character(df$dbname)
	df$tblname<-as.character(df$tblname)
	df$colname<-as.character(df$colname)
	df1$dbname<-as.character(df1$dbname)
	df1$tblname<-as.character(df1$tblname)
	df1$colname<-as.character(df1$colname)

	# filter the history based on the current frame
	library(dplyr)
	dft<-semi_join(df,df1)
	detach("package:dplyr", unload=TRUE)
	# count number of records in each viz both in original history and filtered
	a<-count(df,"unit")
	b<-count(dft,"unit")
	# remove those viz where at least one record does not belong the current frame
	library(dplyr)
	c<-semi_join(a,b)
	detach("package:dplyr", unload=TRUE)
	dff<-df[df$unit %in% c$unit,]
	
	# dff is this is the data frame we will be working with
	# it contains only columns of the current frame
	# count the chart/column frequency
	dff0<-count(dff,names(dff)[c(3,4,5,7)])
	dff1<-dff0[,4:5]
	
	o<-count(dff1,names(dff1)[1])
	o<-o[order(-o$freq),]
	if(!is.null(chartToExclude)){
		o<-o[tolower(o$chart) != tolower(chartToExclude),]
	}
	o$weight<-round(o$freq/max(o$freq),4)
	o<-o[,-2]
	# the is the list most frequent charts
	o<-head(o,top)
	
	# Filter out history alteready filtered by our frame columns
	# by the column frequency charts
	q<-dff0[tolower(dff0$chart) %in% tolower(o[,1]),]
	q1<-do.call(rbind, lapply(split(q,q$chart), function(x) {return(x[which.max(x$freq),])}))
	
	# determine the visualization number to get the details
	dff$id<-paste0(dff$dbname,"$",dff$tblname,"$",dff$colname,"$",dff$chart)
	q1$id<-paste0(q1$dbname,"$",q1$tblname,"$",q1$colname,"$",q1$chart)
	p<-dff[dff$id %in% q1$id,]
	units<-aggregate(p$unit,by=list(chart=p$id),FUN=min)[,2]
	# get the details
	p1<-dff[dff$unit %in% units,][,1:7]
	# merge with the respected chart weights
	r<-merge(p1,o,by="chart")[,c(2,3,4,5,6,7,1,8)]
	
	# clean up and return
	rm(dff,dff0,dff1,a,b,c,p,p1,q,q1,o,units)
	return(r[order(-r$weight,r$chart,r$unit,r$element),])
}


get_userdata<-function(startDate,endDate,tokenPath){
# Description
# Retrieve the event data from GA project site then parse 
# and assemble them with the requested level of details
# Arguments
# startDate - starting date for analysis
# endDate - en date for analysis

set_config(config(ssl_verifypeer = 0L))

load(tokenPath)
# Validate the token
ValidateToken(token)

viewID="ga:151491080"
dim=c("1","2","3","4","5")
query.list<-Init(
            start.date=startDate,
            end.date=endDate, 
            dimension=toString(paste("ga:dimension", dim, sep="")),
            metrics="ga:totalEvents",
            sort=toString(paste("-ga:dimension", dim, sep="")),
            max.results=10000,
            table.id=viewID  
        )
ga.query<-QueryBuilder(query.list)
#ga.df <- GetReportData(ga.query, token)
ga.df <- tryCatch({
    GetReportData(ga.query, token)
  }, error = function(e) {
    return(data.frame(dimension1 = character(), dimension2 = character(), dimension3 = character(), dimension4 = character(),dimension5 = character(),date = character(), totalEvents=numeric(1)))
  })
gc()
return(ga.df)
}

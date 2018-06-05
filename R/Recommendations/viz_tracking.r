viz_history<-function(df){
	LM<-6
	MH<-15
	library(data.table)
	library(jsonlite)
	viz<-df[df$dimension1 == "viz",]
	n<-nrow(viz)
	z<-data.table(unit=integer(),element=integer(),dbname=character(),tblname=character(),colname=character(),datatype=character(),reference=character(),component=character(),uniquevalues=integer(),chart=character(),user=character());
	if(n > 0){
		for(i in 1:n){
			tryCatch({
				data<-fromJSON(viz[i,2])
				user<-viz[i,5]
				chart<-names(data[[1]][1])
				if(chart != "false" & chart != "collision-resolver"){
					m<-length(data[[1]][[1]])
					if(m > 0){
						for(j in 1:m){
							row<-data[[1]][[1]][[j]]
							k<-length(row)
							if(k > 0){
								if(k >= 11){
									# new format
									db<-row[1,1]
									tbl<-row[1,2]
									col<-row[1,3]
									type<-row[1,4]
									comp<-row[1,5]
									message(type)
									if("uniqueValues" %in% colnames(row)){
										uniqueValues<-row[1,6]
										message(uniqueValues)
										if(uniqueValues <= LM){
											message("again")
											type<-paste0(type,"?L")
										}else if(uniqueValues <= MH){
											type<-paste0(type,"?M")
										}else{
											type<-paste0(type,"?H")
										}
									}else{
										uniqueValues<-0
									}
									for(l in 7:k){
										# need a check that alias is not null!!!
										semantic<-row[1,l]
										if(semantic != ""){
											z<-rbindlist(list(z,list(i,j,db,tbl,col,type,semantic,comp,uniqueValues,chart,user)))
											# message(z)
										}
									}
								} else{
									# old format
									uniqueValues=0
									db<-row[1,1]
									tbl<-row[1,2]
									col<-row[1,3]
									comp<-row[1,5]
									semantic<-paste0(db,"$",tbl,"$",col)
									# store in the table chart plus db, etc.
									z<-rbindlist(list(z,list(i,j,db,tbl,col,type,semantic,comp,uniqueValues,chart,user)))
								}
							}
						}
					}
				}
			}, warning = function(w) {
				#warning-handler-code
				message("warning")
			}, error = function(e) {
				#error-handler-code
				message("error")
			}, finally = {
				#cleanup-code
				#message("clean")
			})
		}
	}
	message(nrow(z))
	
	#r<-z[z$uniquevalues == 0,]
	#u<-unique(r$unit)
	#z<-z[!(z$unit %in% u),]
	rm(n,user,viz)
	gc()
	return(z)
}

restore_datatype<-function(df){
	df[grepl("\\?",df$reference),"reference"]<-substring(df[grepl("\\?",df$reference),"reference"],1,nchar(as.character(df[grepl("\\?",df$reference),"reference"]))-2)
	return(df)
}

viz_recom_mgr<-function(df,df1,chartToExclude=NULL,top=5){
	df<-unique(df[,-7])
	colnames(df)[6]<-"reference"
	df<-unique(df[,-8])

	df$dbname<-as.character(df$dbname)
	df$tblname<-as.character(df$tblname)
	df$colname<-as.character(df$colname)
	df$reference<-as.character(df$reference)
	# process df1
	df1<-get_reference(df1)
	df1<-df1[!grepl("\\$",df1$reference),]
	r<-viz_recom_offline(df,df1,chartToExclude,top)
	
	if(nrow(r) == 0){
		df<-restore_datatype(df)
		df1<-restore_datatype(df1)
		r<-viz_recom_offline(df,df1,chartToExclude,top)
	}else{
		n<-length(unique(r$chart))
		if(n < top){
			df<-restore_datatype(df)
			df1<-restore_datatype(df1)
			chartToExclude<-c("Grid",as.character(unique(r$chart)))
			top<-top - n
			r<-rbind(r,viz_recom_offline(df,df1,chartToExclude,top))
		}
	}	
	return(r)
}

viz_recom_offline<-function(df,df1,chartToExclude=NULL,top=5){
	# df - is the history frame
	# df1 - is the current frame
	
	
	# process df1
	
	#df1<-get_reference(df1)
	# remove duplicates
	#df1<-df1[!duplicated(df1$reference),]
	# keep only data types
	#df1<-df1[!grepl("\\$",df1$reference),]
	
	library(plyr)
	
	# filter the history based on the current frame
	dft<-merge(df,df1,by="reference")
	#dft<-unique(dft[,-1])
	# count number of records in each viz both in original history and filtered
	a<-count(df,c("unit","element"))
	
	a<-a[,1:2]
	a<-count(a,"unit")
	b<-count(dft,c("unit","element"))
	b<-b[,1:2]
	b<-count(b,"unit")
	# remove those viz where at least one record does not belong the current frame
	c<-merge(a,b,by=c("unit","freq"))
	dff<-df[df$unit %in% c$unit,]
	
	#Remove viz with entries more than the size of the current dataset
	z<-count(dff,"unit")
	z<-z[z$freq <= nrow(df1),]
	dff<-dff[dff$unit %in% z$unit,]
	rm(z)
	
	# dff is this is the data frame we will be working with
	# it contains only columns of the current frame
	# count the chart/column frequency
	dff0<-count(dff,names(dff)[c(3,4,5,6,8)])
	dff1<-dff0[,5:6]
	
	o<-count(dff1,names(dff1)[1])
	o<-o[order(-o$freq),]

	if(!is.null(chartToExclude)){
		o<-o[!(tolower(o$chart) %in% tolower(chartToExclude)),]
		#o<-o[tolower(o$chart) != tolower(chartToExclude),]
	}
	o$weight<-round(o$freq/max(o$freq),4)
	o<-o[,-2]
	# the is the list most frequent charts
	######o<-head(o,top)

#######################################################
	q<-dff[tolower(dff$chart) %in% tolower(o[,1]),]
	q$chart<-as.character(q$chart)
	
	# identify the most popular number of components in the respected charts
	m<-nrow(o)
	r<-q[0,]
	for(i in 1:m){
		q1<-q[q$chart==as.character(o[i,"chart"]),]
		x<-count(q1,"unit")
		y<-count(x,"freq")
		y<-y[order(-y$freq.1),]
		z<-x[x$freq==y[1,"freq"],]
		r<-rbind(r,q1[q1$unit %in% z$unit,])
	}
	q<-r
	rm(q1,x,y,z,r)
	
	
	s<-count(q,c("unit","reference"))
	s$reference<-as.character(s$reference)
	dt<-count(df1,"reference")
	dt$reference<-as.character(dt$reference)
	n<-nrow(dt)
	units<-vector()
	for(i in 1:n){
		s1<-s[s$reference == dt[i,"reference"],] 
		s2<-s1[s1$freq <= dt[i,"freq"],"unit"]
		units<-c(units,s2)
	}
	units<-unique(units)
	r<-q[q$unit %in% units,]
	if(nrow(r) > 0){
		s<-count(r,"unit")
		r<-merge(r,s,by="unit")
		r<-r[order(-r$freq,r$unit,r$element),]
		
		# get one representative of each chart
		q1<-r[!duplicated(r$chart),]
		units<-q1$unit
		
		p<-dft[dft$unit %in% units,]
		p<-p[order(p$unit,p$element),]
		
		n<-length(units)
		r<-p[0,]
		for(i in 1:n){
			p1<-p[p$unit == units[i],]
			elements<-unique(p1$element)
			m<-length(elements)
			dt$id<-0
			for(j in 1:m){
				p2<-p1[p1$element==j,]
				type<-p2[1,"reference"]
				k<-dt[dt$reference==type,"id"]+1
				dt[dt$reference==type,"id"]<-k
				r<-rbind(r,p2[k,])
			}
		}
		m<-nrow(r)
		for(i in 1:m){
			x<-unlist(strsplit(r[i,"item"], "\\$"))
			r[i,"dbname"]<-x[1]
			r[i,"tblname"]<-x[2]
			r[i,"colname"]<-x[3]
		}
		
		r<-r[,2:8]	
		
		# merge with the respected chart weights
		r<-merge(r,o,by="chart")[,c(2,3,4,5,6,7,1,8)]
		r<-unique(r[order(-r$weight,r$chart,r$unit,r$element),])
		top_charts<-as.character(head(unique(r$chart),top))
		r<-r[r$chart %in% top_charts,]
		# clean up and return
		rm(dff,dff0,dff1,dft,a,b,c,p,p1,p2,q,q1,s1,s2,s,k,o,dt,top_charts)
	}else{
		rm(dff,dff0,dff1,dft,a,b,c,q,o,s,dt)
	}
	return(r)
	
}	
	
get_reference<-function(df){
	LM<-6
	MH<-15
	library(data.table)
	n<-nrow(df)
	z<-data.table(item=character(),reference=character());
	for(i in 1:n){	
		row<-df[i,]
		m<-length(row)
		if(m == 2){
			z<-rbindlist(list(z,list(row[1,1],row[1,2])))
		} else if(m == 3){
			uniqueValues<-as.integer(row[1,3])
			if(!is.na(uniqueValues)){
				if(uniqueValues == 0){
					type<-row[1,2]
				} else if(uniqueValues <= LM){
					type<-paste0(row[1,2],"?L")
				} else if(uniqueValues <= MH){
					type<-paste0(row[1,2],"?M")
				} else{
					type<-paste0(row[1,2],"?H")
				}
				z<-rbindlist(list(z,list(row[1,1],type)))
			}else{
				z<-rbindlist(list(z,list(row[1,1],row[i,2])))
			}
		}
	}
	z<-as.data.frame(unique(z))
	rm(n,m,row)
	return(z)
}

get_userdata<-function(startDate,endDate,tokenPath){
# Description
# Retrieve the event data from GA project site then parse 
# and assemble them with the requested level of details
# Arguments
# startDate - starting date for analysis
# endDate - en date for analysis

library(RGoogleAnalytics);
library(httr);
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

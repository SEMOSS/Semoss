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
						process=TRUE
						for(j in 1:m){
							row<-data[[1]][[1]][[j]]
							k<-length(row)
							if(k < 11){
								process<-FALSE
								break
							}
						}
						if(process){
							for(j in 1:m){
								row<-data[[1]][[1]][[j]]
								k<-length(row)
								# new format
								db<-row[1,1]
								tbl<-row[1,2]
								col<-row[1,3]
								type<-row[1,4]
								comp<-row[1,5]
								if("uniqueValues" %in% colnames(row)){
									uniqueValues<-row[1,6]
									if(uniqueValues <= LM){
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
									}
								}
							}
						}
					}
				}
			}, warning = function(w) {
				#warning-handler-code
			}, error = function(e) {
				#error-handler-code
			}, finally = {
				#cleanup-code
			})
		}
	}
	
	r<-z[z$uniquevalues == 0 & tolower(substr(z$datatype,1,6)) == "string",]
	u<-unique(r$unit)
	z<-z[!(z$unit %in% u),]
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
	r<-viz_recom(df,df1,chartToExclude,top)
	
	if(nrow(r) == 0){
		df<-restore_datatype(df)
		df1<-restore_datatype(df1)
		r<-viz_recom(df,df1,chartToExclude,top)
	}else{
		n<-length(unique(r$chart))
		if(n < top){
			df<-restore_datatype(df)
			df1<-restore_datatype(df1)
			chartToExclude<-c("Grid",as.character(unique(r$chart)))
			top<-top - n
			r<-rbind(r,viz_recom(df,df1,chartToExclude,top))
		}
	}	
	return(r)
}

viz_recom<-function(df,df1,chartToExclude=NULL,top=5){
	library(plyr)
	library(data.table)
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
	# need to find units that all they elements in df1
	units<-unique(dff$unit)
	n<-length(units)
	dt<-df1
	dt$done<-0
	m<-nrow(dt)
	df<-df[order(df$unit,df$element),]
	chosen<-vector()
	# find fitting units
	for(i in 1:n){
		p<-df[df$unit==units[i],]
		n1<-nrow(p)
		dt$done<-0
		found<-0
		for(j in 1:n1){
			p1<-p[j,]
			type<-p1[1,"reference"]
			for(k in 1:m){
				if(dt[k,"reference"]==type & dt[k,"done"]==0){
						dt[k,"done"]<-1
						found<-found+1
						break
				}
			}
		}
		if(found==n1){
			chosen[length(chosen)+1]<-p[1,"unit"]
		}
	}
	# this is a valid subset
	dff<-df[df$unit %in% chosen,]
	# get the most frequent charts
	dff0<-count(dff,names(dff)[c(3,4,5,6,8)])
	dff1<-dff0[,5:6]
	o<-count(dff1,names(dff1)[1])
	o<-o[order(-o$freq),]
	if(!is.null(chartToExclude)){
		o<-o[!(tolower(o$chart) %in% tolower(chartToExclude)),]
	}
	o$weight<-round(o$freq/max(o$freq),4)
	o<-o[,-2]

	# identify the most popular from subset
	q<-dff[tolower(dff$chart) %in% tolower(o[,1]),]
	q$chart<-as.character(q$chart)
	
	n<-length(chosen)
	tbl<-data.table(unit=integer(),chart=character(),types=character());
	paste2 <- function(x, y, sep = "-") paste(x, y, sep = sep)
	for(i in 1:n){
		p<-q[q$unit == chosen[i],]
		t<-p$reference
		if(length(t) > 1){
			types<-Reduce(paste2,t)
			tbl<-rbindlist(list(tbl,list(chosen[i],p[1,"chart"],types)))
		}
	
	}
	# determine the most popular chart within most frequent
	o<-count(tbl,names(tbl)[c(2,3)])
	o<-o[order(-o$freq),]
	o<-o[!duplicated(o$chart,o$types),]
	o<-head(o,top)
	tbl1<-merge(tbl,o)
	tbl1<-tbl1[!duplicated(tbl1$chart,tbl1$types),]
	tbl1$weight<-round(tbl1$freq/sum(tbl1$freq),4)
	tbl2<-tbl1[,c("chart","weight")]
	units<-tbl1$unit
	q1<-q[q$unit %in% units,]
	q1$weight = tbl2[match(q1$chart, tbl2$chart), "weight"]$weight
	# add items from dt then parse it
	n<-length(units)
	m<-nrow(dt)
	q1$item<-""
	for(i in 1:n){
		items<-vector()
		p<-q1[q1$unit==units[i],]
		n1<-nrow(p)
		dt$done<-0
		for(j in 1:n1){
			p1<-p[j,]
			type<-p1[1,"reference"]
			for(k in 1:m){
				if(dt[k,"reference"]==type & dt[k,"done"]==0){
						dt[k,"done"]<-1
						items[j]<-dt[k,"item"]
						break
				}
			}
		}
		q1[q1$unit==units[i],"item"]<-items
	}
	m<-nrow(q1)
	for(i in 1:m){
		x<-unlist(strsplit(q1[i,"item"], "\\$"))
		q1[i,"dbname"]<-x[1]
		q1[i,"tblname"]<-x[2]
		q1[i,"colname"]<-x[3]
	}
	q1<-q1[,c(1:5,7:8,10)]
	gc()
	return(q1[order(-q1$weight,q1$unit,q1$element),])
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

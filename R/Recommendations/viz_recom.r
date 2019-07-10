# new tracking table contains all history is needed except data type plus unique value
# also it is not clear what to do with INPUT_VALUE column - mostly it is the same as column
# however it can have grouping operations like sum/aver etc.

viz_history<-function(fileroot){
	LM<-6
	MH<-15
	library(data.table)
	
	z<-data.table(unit=character(),element=integer(),dbid=character(),dbname=character(),tblname=character(),colname=character(),datatype=character(),component=character(),complabel=character(),
	uniquevalues=integer(),chart=character(),user=character());
	filename<-paste0(fileroot,"-visualization.tsv")
	if(file.exists(filename)){
		# read new semantic file
		viz<-read.table(file = filename, sep = '\t', header = TRUE)
		ids<-unique(as.character(viz$ID))
		n<-length(ids)
		if(n > 0){
			for(i in 1:n){
				unit<-viz[viz$ID %in% ids[i],]
				m<-nrow(unit)
				for(j in 1:m){
					type<-as.character(unit[j,"DATA_TYPE"])
					uniqueValues<-as.integer(unit[j,"UNIQUE_COUNT"])
					if(uniqueValues <= LM){
						type<-paste0(type,"?L")
					}else if(uniqueValues <= MH){
						type<-paste0(type,"?M")
					}else{
						type<-paste0(type,"?H")
					}
					z<-rbindlist(list(z,list(as.character(unit[j,"ID"]),j,as.character(unit[j,"ENGINE_ID"]),as.character(unit[j,"ENGINE_NAME"]),as.character(unit[j,"TABLE_NAME"]),
					as.character(unit[j,"COLUMN_NAME"]),type,as.character(unit[j,"INPUT_NAME"]),as.character(unit[j,"INPUT_VALUE"]),uniqueValues,as.character(unit[j,"INPUT_SUBTYPE"]),
					as.character(unit[j,"USER_ID"]))))
				}
			}
			r<-z[z$uniquevalues == 0,]
			u<-unique(r$unit)
			z<-z[!(z$unit %in% u),]
			saveRDS(z,paste0(fileroot,"-user-history.rds"))
		}		
	}
	gc()
}

restore_datatype<-function(df){
	df[grepl("\\?",df$reference),"reference"]<-substring(df[grepl("\\?",df$reference),"reference"],1,nchar(as.character(df[grepl("\\?",df$reference),"reference"]))-2)
	return(df)
}

viz_recom_mgr<-function(fileroot,df1,chartToExclude=NULL,top=5){
	# datatype becomes reference
	# original reference and user removed
	
	# read history
	filename<-paste0(fileroot,"-user-history.rds")
	if(file.exists(filename)){
		df<-readRDS(filename)
		colnames(df)[7]<-"reference"
		df<-within(df, rm("uniquevalues","user"))
		
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
	}else{
		r<-data.frame()
	}
	return(r)
}

viz_recom<-function(df,df1,chartToExclude=NULL,top=5){
	library(plyr)
	library(data.table)
	top_in<-top
	if(top < 5){	
		top<-5
	}

	# filter the history based on the current frame
	dft<-merge(df,df1,by="reference",allow.cartesian=TRUE)
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
	if(n>0){
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
		####### get the most frequent charts  ### exclude complabel
		dff0<-count(dff,names(dff)[c(3,4,5,6,7,10)])
		dff1<-dff0[,c("chart","freq")]
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
		if(n>0){
			for(i in 1:n){
				p<-q[q$unit == chosen[i],]
				t<-p$reference
				if(length(t) > 1){
					types<-Reduce(paste2,t)
					tbl<-rbindlist(list(tbl,list(chosen[i],as.character(p[1,"chart"]),types)))
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
			if(n>0){
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
					x<-unlist(strsplit(as.character(q1[i,"item"]), "\\$"))
					q1[i,"dbid"]<-x[1]
					q1[i,"dbname"]<-x[2]
					q1[i,"tblname"]<-x[3]
					q1[i,"colname"]<-x[4]
				}
				q1<-q1[,c(1:6,8,10:11)]
				q1<-q1[order(-q1$weight,q1$unit,q1$element),]
				x<-count(q1,c("unit"))
				q1$freq<-x$freq[match(unlist(q1$unit), x$unit)]
				q1<-q1[order(-q1$freq,-q1$weight,q1$unit,q1$element),]
				top_unit<-head(unique(q1$unit),top_in)
				q1<-q1[q1$unit %in% top_unit,]
				q1<-q1[,-10]
			}else{
				q1<-data.frame()
			}
		}else{
			q1<-data.frame()
		}
			
	}else{
		q1<-data.frame()
	}
	gc()
	return(q1)
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

sync_numeric<-function(sourceroot,targetroot){
	x<-readRDS(paste0(sourceroot,"-user-history.rds"))
	source_number<-x$datatype
	temp_number<-gsub("INT","NUMBER",source_number)
	target_number<-gsub("DOUBLE","NUMBER",temp_number)
	x$datatype<-target_number
	saveRDS(x,paste0(targetroot,"-user-history.rds"))
}
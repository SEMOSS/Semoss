build_data_landmarks<-function(fileroot){
	TOP<-0.667
	SEP="$"
	x<-readRDS(paste0(fileroot,"-semantic-history.rds"))
	z<-build_tdm(x,"description","score",vocabulary=NULL,weighted=TRUE)
	a<-z[[1]]
	b<-z[[2]]
	d<-z[[3]]
	topN<-5
	library(data.table)
	tbl<-data.table(col=character(),concept=character(),score=numeric());
	for(i in 1:ncol(b)){
		for(j in 1:topN){
			y<-head(b[order(b[,i],decreasing=T),i],j)
			tbl<-rbindlist(list(tbl,list(d[i,1],names(y)[j],y[j])))
		}
	}
	# column topics
	#tbl$dbid<-sapply(strsplit(tbl$col, "[$]"), "[", 1)
	tbl$dbid<-paste0(sapply(strsplit(tbl$col, "[$]"), "[", 1),SEP,sapply(strsplit(tbl$col, "[$]"), "[", 2),SEP)
	tbl<-tbl[,c(4,1,2,3)]
	saveRDS(tbl,paste0(fileroot,"-column-topic.rds"))
	library(plyr)
	# db topics
	ds_topics<-aggregate(tbl$score,list(tbl$dbid,tbl$concept),sum)
	colnames(ds_topics)<-c("dbid","concept","score")
	ds_topics<-ds_topics[order(ds_topics$dbid,-ds_topics$score),]
	#ds_topics<-setDT(ds_topics)[order(dbid,-score), .SD[1:topN], by=dbid]
	# cut off instead of topN
	lookup<-aggregate(TOP*ds_topics$score, by = list(ds_topics$dbid), max)
	colnames(lookup)[1]<-"dbid"
	d<-merge(ds_topics,lookup,all.x=TRUE)
	ds_topics<-d[d$score>d$x,-4]
	saveRDS(ds_topics,paste0(fileroot,"-database-topic.rds"))
	all_topics<-aggregate(ds_topics$score,list(ds_topics$concept),sum)
	all_topics<-all_topics[order(-all_topics$x),]
	colnames(all_topics)<-c("concept","score")
	saveRDS(ds_topics,paste0(fileroot,"-all-topic.rds"))
	myList<-list()
	myList[[1]]<-tbl
	myList[[2]]<-ds_topics
	myList[[3]]<-all_topics
	gc()
	return(myList)
}

build_dbid_domain<-function(fileroot){
	SEP<-"$"
	x<-readRDS(paste0(fileroot,"-semantic-history.rds"))
	#x$Original_Column<-sapply(strsplit(x$Original_Column, "[$]"), "[", 1)
	x$Original_Column<-paste0(sapply(strsplit(x$Original_Column, "[$]"), "[", 1),SEP,sapply(strsplit(x$Original_Column, "[$]"), "[", 2),SEP)
	x<-x[order(x$Original_Column),]
	dbids<-unique(x$Original_Column)
	library(data.table)
	tbl<-data.table(Original_Column=character(),description=character(),score=numeric(),user=character());
	n<-length(dbids)
	if(n>0){
		for(i in 1:n){
			y<-x[x$Original_Column==dbids[i],]
			if(nrow(y)>0){
				z<-Reduce(paste,y$description)
				tbl<-rbindlist(list(tbl,list(dbids[i],z,1,as.character(y[1,"user"]))))
			}
		}
		saveRDS(tbl,paste0(fileroot,"-dbid-desc.rds"))
	}
	lsa_filename<-paste0(fileroot,"-dbid-desc-lsa")
	lsi_mgr(tbl,"description","score",filename_lsa=lsa_filename)
	gc()
}

find_db<-function(fileroot,query_string,margin=0.1,low_limit=0.5){
	# retrieve lsa space info
	query_tbl<-data.table(description=character(),score=numeric());
	query_tbl<-rbindlist(list(query_tbl,list(query_string,1)))
	myLSAspace=readRDS(paste0(fileroot,"-dbid-desc-lsa.rds"))
	myVocabulary=readRDS(paste0(fileroot,"-dbid-desc-lsa-vocab.rds"))
	
	# build query document
	q_doc<-build_query_tdm(query_tbl,myLSAspace,myVocabulary,desc_col="description",freq_col="score",weighted=TRUE)
	lookup_tbl<-readRDS(paste0(fileroot,"-dbid-desc.rds"))
	
	# find the best matches
	dbids<-get_similar_doc("description",lookup_tbl,q_doc,myLSAspace,margin,low_limit,orig_col="Original_Column")
	dbids$dbid<-sapply(strsplit(dbids$item_id, "[$]"), "[", 1)
	dbids$dbname<-sapply(strsplit(dbids$item_id, "[$]"), "[", 2)
	dbids<-dbids<-dbids[,c(3,4,2)]
	gc()
	return(dbids)
}
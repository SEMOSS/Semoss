datasemantic_history<-function(df,fileroot){
# Description
# Extract from GA tracking data queries data items history
# Arguments
# df - data queries info from the tracked period
# filename - name of the file the data items history will be saved to
	sep="$"
	z<-df[df$dimension1=="datasemantic",c("dimension2","dimension5","date")]
	# remove duplicates
	z<-z[!duplicated(z$dimension2),]
	library(data.table)
	library(jsonlite)
	x<-data.table(Original_Column=character(),description=character(),score=integer(),user=character())
	n<-nrow(z)
	for(i in 1:n){
		tryCatch({
			#recdate<-as.Date(paste0(substr(z[i,3],1,4),"-",substr(z[i,3],5,6),"-",substr(z[i,3],7,8)))
			data<-fromJSON(z[i,1])
			user<-z[i,2]
			db_rec<-data[[1]]
			dbname<-db_rec$dbName
			dbid<-db_rec$dbId
			tbls_rec<-db_rec$tables[[1]]
			m<-ncol(tbls_rec)
			for(j in 1:m){
				tblname<-colnames(tbls_rec)[j]
				tbl_rec<-tbls_rec[1,j][[1]]
				k<-nrow(tbl_rec)
				for(l in 1:k){
					colname<-tbl_rec[l,"columnName"]
					original_column<-paste0(dbid,sep,dbname,sep,tblname,sep,colname,sep)
					desc<-tbl_rec[l,"description"]
					x<-rbindlist(list(x,list(original_column,desc,1,user)))
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
	if(nrow(x) > 0){
		found<-TRUE
		filename<-paste0(fileroot,"-semantic-history.rds")
		if(file.exists(filename)){
			exist_df<-readRDS(filename)
			exist_df<-exist_df[!(exist_df$Original_Column %in% x$Original_Column),]
			exist_df<-rbind(exist_df,x)
			saveRDS(exist_df,filename)
		}else{
			saveRDS(x,filename)
		}
	}else{
		found<-FALSE
	}
	gc()
	return(found)
}

column_lsi_mgr<-function(fileroot,desc_col,freq_col,share,weighted=TRUE){
	filename<-paste0(fileroot,"-semantic-history.rds")
	if(file.exists(filename)){
		r<-readRDS(filename)
		filename_lsa<-paste0(fileroot,"-lsa")
		lsi_mgr(r,desc_col,freq_col,share,filename_lsa,weighted)
	}
}

compute_column_desc_sim<-function(fileroot){
	library(lsa)
	filename<-paste0(fileroot,"-semantic-history.rds")
	if(file.exists(filename)){
		exist_df<-readRDS(filename)
		lsa<-readRDS(paste0(fileroot,"-lsa.rds"))
		dk<-lsa$dk
		sim<-cosine(t(dk))
		sim[which(is.nan(sim))]=0
		columns<-unique(as.character(exist_df$Original_Column))
		rownames(sim)<-columns
		colnames(sim)<-columns
		saveRDS(sim,paste0(fileroot,"-sim.rds"))
	}
	gc()
}

compute_entity_sim<-function(fileroot,type="database",sep="$"){
	filename<-paste0(fileroot,"-sim.rds")
	if(file.exists(filename)){
		r<-readRDS(filename)
		cols<-rownames(r)
		if(tolower(type) == "table"){
			tbls<-unique(col2tbl(cols,sep))
		}else if(tolower(type) == "database"){
			tbls<-unique(col2db(cols,sep))
		}else{
			return("Please provide correct entity type: table or database")
		}
		n<-length(tbls)
		sim<-diag(rep(1,n))
		rownames(sim)<-tbls
		colnames(sim)<-tbls
		if(n > 1){
			for(i in 1:n){
				tbl1<-tbls[i]
				for(j in 1:n){
					if(i !=j){
						tbl2<-tbls[j]
						n1<-nchar(tbl1)
						n2<-nchar(tbl2)
						ind1<-append(which(cols %in% cols[nchar(cols)==n1]),which(cols %in% cols[substr(cols,(n1+1),(n1+1))==sep]))
						dim1<-intersect(which(substr(cols,1,nchar(tbl1)) == tbl1),ind1)
						ind2<-append(which(cols %in% cols[nchar(cols)==n2]),which(cols %in% cols[substr(cols,(n2+1),(n2+1))==sep]))
						dim2<-intersect(which(substr(cols,1,nchar(tbl2)) == tbl2),ind2)
						if(length(dim1) == 1){
							value=max(r[dim1,dim2])
						}else{
							m<-as.matrix(r[dim1,][,dim2])
							value<-mean(apply(m,1,max))
						}
						sim[i,j]<-value
					}
				}
			}
		}
		saveRDS(sim,paste0(fileroot,"-",type,"-sim.rds"))
		gc()
	}
}

col2tbl<-function(col,sep="$"){
	items<-strsplit(col,paste0("[",sep,"]"))
	n<-length(items)
	tbls<-vector()
	for(i in 1:n){
		if(length(items[[i]]) > 1){
			tbls[i]<-paste0(items[[i]][1],sep,items[[i]][2])
		}else{
			tbls[i]<-items[[i]][1]
		}
	}
	gc()
	return(tbls)
}

col2db<-function(col,sep="$"){
	items<-strsplit(col,paste0("[",sep,"]"))
	n<-length(items)
	dbs<-vector()
	for(i in 1:n){
		dbs[i]<-items[[i]][1]
	}
	gc()
	return(dbs)
}
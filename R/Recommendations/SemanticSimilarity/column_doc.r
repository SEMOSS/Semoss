column_doc_mgr<-function(df,fileroot,googleSearch=TRUE){
	library(XML)
	library(RCurl)
	library(stringr)
	n<-ncol(df)
	for(i in 1:n){
		colname<-colnames(df)[i]
		values<-as.character(df[,colname])
		if(googleSearch){
			t<-construct_column_doc(colname,values)
		}else{
			t<-create_column_doc(colname,values)
		}
		if(i == 1){
			r<-t
		}else{
			if(nrow(t) >0){
				r<-rbind(r,t)
			}
		}
	}
	filename<-paste0(fileroot,".rds")
	if(file.exists(filename)){
		exist_df<-readRDS(filename)
		exist_df<-exist_df[!(exist_df$Original_Column %in% r$Original_Column),]
		exist_df<-rbind(exist_df,r)
		saveRDS(exist_df,filename)
	}else{
		saveRDS(r,filename)
	}
	gc()
}

getSearchURL <- function(searchTerm, domain = '.com', quotes=TRUE) {
	searchTerm <- gsub(' ', '%20', searchTerm)
	if(quotes) search.term <- paste('%22', searchTerm, '%22', sep='') 
		searchURL <- paste('http://www.google', domain, '/search?q=',searchTerm, sep='')
	return(searchURL)
}

get_column_desc<-function(searchTerm){
	EXCLUDE<-"Advanced searchSearch Help Send feedback"
	EXCLUDE1<-"In order to show you the most relevant results"
	searchURL<-getSearchURL(searchTerm=searchTerm)
	doc.html<-htmlTreeParse(getURL(searchURL),useInternal = TRUE)
	doc.text<-unlist(xpathApply(doc.html, '//p', xmlValue))
	doc.text<-str_trim(gsub('\\n', ' ', doc.text),"both")
	doc.text<-doc.text[!(doc.text==EXCLUDE)]
	doc.text<-doc.text[!(substr(doc.text,1,nchar(EXCLUDE1))==EXCLUDE1)]
	return(doc.text)
}

construct_column_doc<-function(colname,values){
	SAMPLE_SIZE=5
	d<-data.frame()
	n<-length(values)
	for(i in 1:n){
		a<-get_column_desc(values[i])
		m<-min(length(a),SAMPLE_SIZE)
		a<-a[sample(length(a),m)]
		if(length(a) > 0){
			b<-as.data.frame(a)
			names(b)<-"X4"
			b$score=nrow(b)-row(b)+1
			b$score<-b$score/sum(row(b))
			b$Original_Column<-colname
			b<-b[,c(3,1,2)]
			d<-rbind(d,b)
		}
	}
	return(d)
}


find_exist_columns_bydesc<-function(column,fileroot="column-desc-set",margin=0.2,low_limit=0.5){
	exist_df<-readRDS(paste0(fileroot,".rds"))
	columns<-unique(as.character(exist_df$Original_Column))
	cur_row<-which(columns==column)
	existLSA<-readRDS(paste0(fileroot,"-lsa.rds"))
	dk<-existLSA$dk
	q_doc<-dk[cur_row,]
	lookup_tbl<-as.data.frame(columns)
	colnames(lookup_tbl)[1]<-"Original_Column"
	r<-get_similar_doc(column,lookup_tbl,q_doc,existLSA,margin,low_limit)
	r<-r[r$Joined_Column != column,]
	gc()
	return(r)
}

compute_column_desc_sim<-function(fileroot){
	library(lsa)
	exist_df<-readRDS(paste0(fileroot,".rds"))
	lsa<-readRDS(paste0(fileroot,"-lsa.rds"))
	dk<-lsa$dk
	sim<-cosine(t(dk))
	sim[which(is.nan(sim))]=0
	columns<-unique(as.character(exist_df$Original_Column))
	rownames(sim)<-columns
	colnames(sim)<-columns
	saveRDS(sim,paste0(fileroot,"-sim.rds"))
	gc()
}

get_sim_query<-function(column,fileroot,margin=0.2,low_limit=0.8){
	library(data.table)
	sim<-readRDS(paste0(fileroot,"-sim.rds"))
	q_sim<-sim[rownames(sim) == column,]
	q_sim<-q_sim[q_sim>max(q_sim)-margin & q_sim>low_limit & names(q_sim) != column]
	z<-data.table(Selected_Column=character(),Joined_Column=character(),Similarity=numeric());
	n<-length(q_sim)
	if(n>0){
		for(i in 1:n){
			z<-rbindlist(list(z,list(column,names(q_sim)[i],q_sim[i])))
		}
	}
	gc()
	return(z)
}

table_lsi_mgr<-function(filename,entity_filename,desc_col,freq_col,share,weighted=TRUE,sep="$"){
	r<-readRDS(paste0(filename,".rds"))
	items<-strsplit(r$Original_Column,paste0("[",sep,"]"))
	n<-length(items)
	tbls<-vector()
	for(i in 1:n){
		tbls[i]<-paste0(items[[i]][1],".",items[[i]][2])
	}
	r$Original_Column<-tbls
	saveRDS(r,paste0(entity_filename,".rds"))
	filename_lsa<-paste0(entity_filename,"-lsa")
	lsi_mgr(r,desc_col,freq_col,share,filename_lsa,weighted)
}

column_lsi_mgr<-function(filename,desc_col,freq_col,share,weighted=TRUE){
	r<-readRDS(paste0(filename,".rds"))
	filename_lsa<-paste0(filename,"-lsa")
	lsi_mgr(r,desc_col,freq_col,share,filename_lsa,weighted)
}

create_column_doc<-function(colname,values){
	library(WikidataR)
	d<-data.frame()
	n<-length(values)
	for(i in 1:n){
		a<-find_item(values[i])
		if(length(a) > 0){
			b<-data.frame(Reduce("rbind",lapply(a,function(x) cbind(x$title,x$label,x$pageid,ifelse(length(x$description)==0,NA,x$description)))))
			if(nrow(b) > 0){
				b<-b[!(is.na(b$X4)),]
				if(nrow(b) > 0){
					if(nrow(d) > 0){
						d<-rbind(d,b[1,])
					}else{
						d<-b[1,]
					}
				}
			}
		}
	}
	if(nrow(d) > 0){
		d$X3<-as.numeric(as.character(d$X3))
		if(nrow(d) > 1){
			if(sd(d$X3) == 0){
				d$score<-abs(mean(d$X3))
			}else{
				d$score<-abs(mean(d$X3))/(abs(d$X3-mean(d$X3))/sd(d$X3)+1)
			}
		}
		d$score<-d$score/sum(d$score)
		d<-d[order(-d$score),c("X4","score")]
		d$Original_Column<-colname
		d<-d[,c("Original_Column","X4","score")]
	}
	return(d)
}

find_columns_bydesc<-function(filename,query_desc,colname,margin,low_limit){
	# Identify the candidates based on similarity documents when concept descriptions are available
	# Arguments
	# query_desc - a dataframe containing concepts descriptions
	# colname - a column name in the query_df for which we are searching for similar existing columns
	# lsa_filename - the file name for the LSa space and its vocabulary
	# margin - the range of similarity to keep documents compare with the best match
	# low_limit - the lowest level for the document similarity
	concept_desc<-readRDS(paste0(filename,".rds"))
	orig_tbl<-concept_desc[!duplicated(concept_desc$Original_Column),]
	conceptLSAspace<-readRDS(paste0(filename,"-lsa.rds"))
	conceptVocabulary<-readRDS(paste0(filename,"-lsa-vocab.rds"))
	q_doc<-build_query_tdm(query_desc,conceptLSAspace,conceptVocabulary,"X4","score")
	r<-get_similar_doc(colname,orig_tbl,q_doc,conceptLSAspace,margin,low_limit)
	gc()
	return(r)
}

compute_entity_sim<-function(filename,entity_filename,type="table",sep="$"){
	r<-readRDS(paste0(filename,"-sim.rds"))
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
	saveRDS(sim,paste0(entity_filename,"-sim.rds"))
	gc()
	return(sim)	
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
	return(tbls)
}

col2db<-function(col,sep="$"){
	items<-strsplit(col,paste0("[",sep,"]"))
	n<-length(items)
	dbs<-vector()
	for(i in 1:n){
		dbs[i]<-items[[i]][1]
	}
	return(dbs)
}
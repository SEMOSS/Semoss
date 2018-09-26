semantic_tracking_mgr<-function(df,fileroot){
# Description
# Manages semantic data generation and upload to semantic tracking table
# arguments
# df - dataframe with the instances of database being uploaded and the column names as ENGINE_ID$ENGINE_NAME$TABLE$COLUMN
# fileroot - name of the file root for the data items history will be saved to
	column_doc_mgr_dopar(df,fileroot)
	datasemantic_history(fileroot)
}


datasemantic_history<-function(fileroot){
# Description
# Extract from GA tracking data queries data items history
# Arguments
# df - structure of db being uploaded
# fileroot - name of the file root for the data items history will be saved to
	sep<-"$"
	paste2 <- function(x, y, sep = " ") paste(x, y, sep = sep)

	df_desc<-readRDS(paste0(fileroot,"-description.rds"))
	n<-nrow(df_desc)
	if(n>0){
		# Create a dataframe that will contain the semantic info
		df<-as.data.frame(sapply(strsplit(df_desc$Original_Column, "[$]"), "[", 1))
		colnames(df)[1]<-"ENGINE_ID"
		df$ENGINE_ID<-sapply(strsplit(df_desc$Original_Column, "[$]"), "[", 1)
		df$ENGINE_NAME<-sapply(strsplit(df_desc$Original_Column, "[$]"), "[", 2)
		df$TABLE<-sapply(strsplit(df_desc$Original_Column, "[$]"), "[", 3)
		df$COLUMN<-sapply(strsplit(df_desc$Original_Column, "[$]"), "[", 4)
		df$Original_Column<-df_desc$Original_Column
		df<-unique(df)
		m<-nrow(df)
		for(i in 1:m){
			col<-as.character(df[i,"Original_Column"])
			col_desc<-df_desc[df_desc$Original_Column == col,2]
			if(length(col_desc)>0){
				desc<-Reduce(paste2,col_desc)
				df[i,"DESCRIPTION"]<-desc
			}
		}
	}
	if(nrow(df) > 0){
		found<-TRUE
		filename<-paste0(fileroot,"-semantic-history.rds")
		if(file.exists(filename)){
			# update to remove all existing tables of there are any updates to  them
			df$Original_Column<-paste(df$ENGINE_ID,df$ENGINE_NAME,df$TABLE,sep="$")
			exist_df<-readRDS(filename)
			exist_df$Original_Column<-paste(exist_df$ENGINE_ID,exist_df$ENGINE_NAME,exist_df$TABLE,sep="$")
			exist_df<-exist_df[!(exist_df$Original_Column %in% df$Original_Column),]
			exist_df<-rbind(exist_df,df)
			exist_df<-exist_df[,!(colnames(exist_df) == "Original_Column")]
			saveRDS(exist_df,filename)
		}else{
			df<-df[,!(colnames(df) == "Original_Column")]
			saveRDS(df,filename)
		}
	}else{
		found<-FALSE
	}
	gc()
	return(found)
}

column_lsi_mgr<-function(fileroot,desc_col,freq_col,share,weighted=TRUE){
# Description
# Builds LSA space for the semantic history column info
# Arguments
# fileroot - the root of family of feles supporting the data recommendation
# desc_col - the name of the column with description data
# freq_col - the name of the column with description frequency data
# share - the approximation used in matrix factorization
# weighted - whether to use term frequency inverse document frequency transformation
	filename<-paste0(fileroot,"-semantic-history.rds")
	if(file.exists(filename)){
		r<-readRDS(filename)
		if(nrow(r)>0){
			filename_lsa<-paste0(fileroot,"-lsa")
			cmd<-paste0("r$",freq_col,"<-1")
			eval(parse( text=cmd ))
			r$Original_Column<-paste(r[,1],r[,2],r[,3],r[,4],sep="$")
			lsi_mgr(r,desc_col,freq_col,share,filename_lsa,weighted)
		}else{
			print("Semantic history data not found")
		}
	}
}

compute_column_desc_sim<-function(fileroot){
# Description
# Constructs similarity matrix based on column LSA info
# Arguments
# fileroot - the root of family of files supporting the data recommendation
	library(lsa)
	filename<-paste0(fileroot,"-semantic-history.rds")
	if(file.exists(filename)){
		exist_df<-readRDS(filename)
		exist_df$Original_Column<-paste(exist_df[,1],exist_df[,2],exist_df[,3],exist_df[,4],sep="$")
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
# Description
# Constructs entity similarity matrix based on column similarity matrix
# Arguments
# fileroot - the root of family of feles supporting the data recommendation
# type - entity type
# sep - separator used for parsing
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
# Description
# Extract from the column label table label
# Arguments
# col - column name to parse
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
# Description
# Extract from the column label database label
# Arguments
# col - column name to parse
	items<-strsplit(col,paste0("[",sep,"]"))
	n<-length(items)
	dbs<-vector()
	for(i in 1:n){
		dbs[i]<-items[[i]][1]
	}
	gc()
	return(dbs)
}

column_doc_mgr_dopar<-function(df,fileroot,choice="both"){
# Description
# Generates wikidata/google descriptions based on provided instances using parallel processing
# Arguments
# df - dataframe containing available instances
# fileroot - the root of the family of file providing data recommendations
# choice - specifies whether use wiki, google r both to generate the descriptions
	library(doParallel)
	library(XML)
	library(RCurl)
	library(stringr)
	
	clusterDesc<-function(colname,values,choice){
		if(tolower(choice) == "both"){
			t<-create_column_doc(colname,values)
			if(nrow(t) == 0){
				t<-construct_column_doc(colname,values)
			}
		}else if(tolower(choice) == "wiki"){
			t<-create_column_doc(colname,values)
		}else{
			t<-construct_column_doc(colname,values)
		}
		return(t)
	}
	
	n<-ncol(df)
	# determine the number of physical cores
	cores <- detectCores(logical = FALSE)
	cl <- makeCluster(cores)
	registerDoParallel(cl, cores=cores)
	
	r<-foreach(i=1:n, .init=data.frame(),.combine=rbind, .export=c("create_column_doc","construct_column_doc","discover_column_desc","getSearchURL")) %dopar% 
	{ 
		values<-as.character(df[[i]])
		values<-gsub("_"," ",values)
		colname<-colnames(df)[i]
		clusterDesc(colname,values,choice)
	}
	stopImplicitCluster()
	stopCluster(cl)
	
	filename<-paste0(fileroot,"-description.rds")
	if(file.exists(filename)){
		exist_df<-readRDS(filename)
		exist_df<-exist_df[!(exist_df$Original_Column %in% r$Original_Column),]
		exist_df<-rbind(exist_df,r)
		saveRDS(exist_df,filename)
	}else{
		saveRDS(r,filename)
	}
	gc()
	return(r)
}

column_doc_mgr_do<-function(df,fileroot,choice="both"){
# Description
# Generates wikidata/google descriptions based on provided instances using
# Arguments
# df - dataframe containing available instances
# fileroot - the root of the family of file providing data recommendations
# choice - specifies whether use wiki, google r both to generate the descriptions
	library(XML)
	library(RCurl)
	library(stringr)
	n<-ncol(df)
	r<-data.frame()
	#tempName<-Reduce("paste0",sample(letters,26))
	tempName<-constructName()
	for(i in 1:n){
		# save original column name in case it has special characters
		colname<-colnames(df)[i]
		# Operate with temporary column name
		colnames(df)[i]<-tempName
		cmd<-paste0("values<-as.character(df$",tempName,")")
		eval(parse( text=cmd ))
		values<-gsub("_"," ",values)
		colnames(df)[i]<-colname
		if(tolower(choice) == "both"){
			t<-create_column_doc(colname,values)
			if(nrow(t) == 0){
				t<-construct_column_doc(colname,values)
			}
		}else if(tolower(choice) == "wiki"){
			t<-create_column_doc(colname,values)
		}else{
			t<-construct_column_doc(colname,values)
		}
		if(nrow(t) > 0){
			if(i == 1){
				r<-t
			}else{
				if(nrow(t) >0){
					r<-rbind(r,t)
				}
			}
		}	
	}
	filename<-paste0(fileroot,"-description.rds")
	if(file.exists(filename)){
		exist_df<-readRDS(filename)
		exist_df<-exist_df[!(exist_df$Original_Column %in% r$Original_Column),]
		exist_df<-rbind(exist_df,r)
		saveRDS(exist_df,filename)
	}else{
		saveRDS(r,filename)
	}
	gc()
	return(r)
}

constructName<-function(n=1,size=26){
# Description
# Generates temporary column name to be used when needed
# Arguments

	name <- c(1:n) 
	for (i in 1:n){
		header<-sample(c(letters,LETTERS),1,replace=TRUE)
		body<-paste(sample(c(0:9,letters,LETTERS),size,replace=TRUE),collapse="")
		name[i]<-paste0(header,body)
	}
	return(name)
}

construct_column_doc<-function(colname,values){
# Description
# Generates google based description for the column instances
# Arguments
# colname - the name of the column with instances
# values - san array of the column instances
	SAMPLE_SIZE=1
	d<-data.frame()
	n<-length(values)
	for(i in 1:n){
		a<-tryCatch({
			discover_column_desc(values[i])
		},error=function(e){
			NULL
		})
		#a<-discover_column_desc(values[i])
		if(!is.null(a)){
			m<-min(length(a),SAMPLE_SIZE)
			a<-a[sample(length(a),m)]
			if(length(a) > 0){
				b<-as.data.frame(a)
				names(b)<-"X4"
				b$score=nrow(b)-row(b)+1
				if(nrow(b) > 1){
					b$score<-b$score/sum(row(b))
				}
				b$Original_Column<-colname
				b<-b[,c(3,1,2)]
				d<-rbind(d,b)
			}
		}
	}
	return(d)
}

create_column_doc<-function(colname,values){
# Description
# Generates wiki based description for the column instances
# Arguments
# colname - the name of the column with instances
# values - san array of the column instances
	library(WikidataR)
	d<-data.frame()
	n<-length(values)
	for(i in 1:n){
		a<-find_item(values[i])
		if(length(a) > 0){
			b<-data.frame(Reduce("rbind",lapply(a,function(x) cbind(x$title,x$label,x$pageid,ifelse(length(x$description)==0,NA,x$description)))))
			if(nrow(b) > 0){
				b$X4<-as.character(b$X4)
				b[is.na(b$X4),"X4"]<-as.character(b[is.na(b$X4),"X2"])
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
		}else{
			d$score<-d$X3
		}
		d$score<-d$score/sum(d$score)
		d<-d[order(-d$score),c("X4","score")]
		d$Original_Column<-colname
		d<-d[,c("Original_Column","X4","score")]
	}
	return(d)
}


discover_column_desc<-function(searchTerm){
# Description
# Constructs google search string
# Arguments
# searchTerm - a given entry to use for search

	EXCLUDE<-"Advanced searchSearch Help Send feedback"
	EXCLUDE1<-"In order to show you the most relevant results"
	searchURL<-getSearchURL(searchTerm=searchTerm)
	doc.html <- getURL(searchURL, httpheader = c("User-Agent" = "R(2.10.0)"))
	doc.html<-htmlTreeParse(doc.html,useInternal = TRUE)
	doc.nodes <- getNodeSet(doc.html, "//h3[@class='r']//a")
	col.desc<-NULL
	if(length(doc.nodes) > 0){
		doc.desc<-unlist(xpathApply(doc.nodes[[1]], '//p', xmlValue))
		doc.desc<-str_trim(gsub('\\n', ' ', doc.desc),"both")
		n<-length(doc.desc)
		doc.desc<-doc.desc[!(doc.desc==EXCLUDE)]
		doc.desc<-doc.desc[!(substr(doc.desc,1,nchar(EXCLUDE1))==EXCLUDE1)]
		if(length(doc.desc) > 0){
			col.desc<-doc.desc[1]
		}	
	}
	return(col.desc)
}

getSearchURL <- function(searchTerm, domain = '.com', quotes=TRUE) {
# Description
# Google search string helper
# Arguments
# searchTerm - a given entry to use for search
	searchTerm <- gsub(' ', '%20', searchTerm)
	if(quotes) search.term <- paste('%22', searchTerm, '%22', sep='') 
		searchURL <- paste('http://www.google', domain, '/search?q=',searchTerm, sep='')
	return(searchURL)
}

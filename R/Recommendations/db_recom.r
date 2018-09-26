dataitem_history_do<-function(fileroot){
# Description
# Extract from GA tracking data queries data items history
# Arguments
# fileroot - the root of family of file containing local data recommendation info
	
	library(data.table)
	df<-read.table(file = paste0(fileroot,"-dataquery.tsv"), sep = '\t', header = TRUE)
	ids<-unique(as.character(df$ID))
	n<-length(ids)
	if(n>0){
		x<-data.table(dbid=character(),database=character(),table=character(),column=character(),user=character(),daysago=integer())
		for(i in 1:n){
			unit<-df[df$ID %in% ids[i],]
			m<-nrow(unit)
			for(j in 1:m){
				daysago<-as.integer(Sys.Date()-as.Date(unit[j,"TIME"]))
				x<-rbindlist(list(x,list(as.character(unit[j,"ENGINE_ID"]),as.character(unit[j,"ENGINE_NAME"]),
				as.character(unit[j,"TABLE_NAME"]),as.character(unit[j,"COLUMN_NAME"]),as.character(unit[j,"USER_ID"]),
				daysago)))
			}
		}
	}
	saveRDS(x,paste0(fileroot,"-history.rds"))
	gc()
}


dataitem_history_dopar<-function(fileroot){
# Description
# Extract from GA tracking data queries data items history
# Arguments
# df - data queries info from the tracked period
# filename - name of the file the data items history will be saved to
	
	library(doParallel)
	library(data.table)

	df<-read.table(file = paste0(fileroot,"-dataquery.tsv"), sep = '\t', header = TRUE)
	parseRows<-function(unit){	
		x<-data.table(dbid=character(),database=character(),table=character(),column=character(),user=character(),daysago=integer())
		
		m<-nrow(unit)
		for(j in 1:m){
				daysago<-as.integer(Sys.Date()-as.Date(unit[j,"TIME"]))
				x<-rbindlist(list(x,list(as.character(unit[j,"ENGINE_ID"]),as.character(unit[j,"ENGINE_NAME"]),
				as.character(unit[j,"TABLE_NAME"]),as.character(unit[j,"COLUMN_NAME"]),as.character(unit[j,"USER_ID"]),
				daysago)))
			}
		return(x)
	}		

	ids<-unique(as.character(df$ID))
	n<-length(ids)
	if(n>0){
		cores <- detectCores(logical = FALSE)
		cl <- makeCluster(cores)
		registerDoParallel(cl, cores=cores)
		r<-foreach(i=1:n, .init=data.frame(),.combine=rbind, .packages=c("data.table" )) %dopar% 
		{
			unit<-df[df$ID %in% ids[i],]
			parseRows(unit)
		}
		stopImplicitCluster()
		stopCluster(cl)
	}
	saveRDS(r,paste0(fileroot,"-history.rds"))
	gc()
}


get_dataitem_rating<-function(fileroot,type,tfidf){
# Description
# Aggregate data based type and tfidf
# Arguments
# fileroot - file root name for data items history
# destfile - file name for user item matrix
# type - data item types: database, table or column
# tfidf - boolean reflecting whether to use tfidf
	library(plyr)
	TYPES<-c("database","table","column")
	sep="$"
	if(tolower(type) %in% TYPES){
		x<-readRDS(paste0(fileroot,"-history.rds"))
		if(type==TYPES[1]){
			x$dataitem<-paste0(x$dbid,sep,x$database,sep)
			lookup<-x[,c("dbid","dataitem")]
			lookup<-lookup[!duplicated(lookup$dbid),]
			x<-x[,c(1,5,6)]
			y<-count(x,c("dbid","user","daysago"))
			y$dataitem<-as.character(lapply(y$dbid, function(x) lookup$dataitem[match(x, lookup$dbid)]))
			y<-y[,c(5,2,3,4)]
		}else if(type==TYPES[2]){
			x$dataitem<-paste0(x$dbid,sep,x$database,sep,x$table)
			x<-x[,c(7,5,6)]
			y<-count(x,c("dataitem","user","daysago"))
		}else{
			x$dataitem<-paste0(x$dbid,sep,x$database,sep,x$table,sep,x$column)
			x<-x[,c(7,5,6)]
			y<-count(x,c("dataitem","user","daysago"))
		}
		colnames(y)[1]<-"dataitem"
		y<-y[order(y$daysago,-y$freq,y$dataitem,y$user),]
	}else{
		return("Incorrect type argument, allowed: \"database\",\"table\",\"column\"")
	}
	z<-populate_ratings(y)
	if(tfidf){
		z<-exec_tfidf(z)
	}
	gc()
	saveRDS(z,paste0(fileroot,"-matrix.rds"))
}

assign_unique_concepts<-function(df){
	x<-data.table(dataitem=character(),concept=integer())
	ind<-0
	while(nrow(df) > 0){
		concept<-as.character(df[1,"concept"])
		dataitem<-as.character(df[1,"dataitem"])
		rows<-df[df$concept==concept & df$dataitem!=dataitem,]
		n<-nrow(rows)
		if(n > 0){
			ind<-ind+1
			x<-rbindlist(list(x,list(dataitem,ind)))
			for(i in 1:n){
				x<-rbindlist(list(x,list(rows[i,"dataitem"],ind)))
			}
		}else{
			if(length(which(x$dataitem == dataitem)) == 0){
				ind<-ind+1
				x<-rbindlist(list(x,list(dataitem,ind)))
			}
		}
		df<-df[!(df$concept %in% concept),]
	}
	gc()
	return(x)
}

populate_ratings<-function(df){
# Description
# Populate user item matrix based on aggregated data frame history
# Arguments
# df - aggregated data frame history
	items<-as.character(unique(df$dataitem))
	users<-as.character(unique(df$user))
	n<-length(users)
	m<-length(items)
	out<-matrix(0,nrow=n,ncol=m)
	rownames(out)<-users
	colnames(out)<-items
	N<-nrow(df)
	for(i in 1:N){
		rec<-df[i,]
		currow<-which(rec$user == users)
		curcol<-which(rec$dataitem == items)
		out[currow,curcol]<-out[currow,curcol]+round(rec$freq/log10(10+rec$daysago),4)
	}
	gc()
	return(out)
}

read_datamatrix<-function(fileroot){
	filename<-paste0(fileroot,"-blended-matrix.rds")
	if(file.exists(filename)){
		r<-readRDS(filename)
	}else{
		r<-readRDS(paste0(fileroot,"-matrix.rds"))
	}
	return(r)
}

build_sim<-function(fileroot){
	r<-read_datamatrix(fileroot)
	sim<-cosine_jaccard_sim(r)
	diag(sim)<-1
	saveRDS(sim,paste0(fileroot,"-itemsim.rds"))
	sim<-cosine_jaccard_sim(t(r))
	diag(sim)<-1
	saveRDS(sim,paste0(fileroot,"-usersim.rds"))
}

cosine_jaccard_sim<-function(m){
# Description
# Compute cosine jacqard similarity between cells of a matrix
# m - rating/frequency user item  matrix
	library(lsa)
	out<-cosine(m)*jaccard_sim(m)
	return(out)
}

jaccard_sim<-function(m){
	z<-m
	z[z>0]<-1
	myFun<-function(i,j,M){
		sums<-rowSums(M[,c(i, j)])
		similarity<-length(sums[sums==2])
		total<-length(sums[sums==1]) + similarity
		return(similarity/total)
	}
	myVecFun <- Vectorize(myFun,vectorize.args = c('i','j'))
	
	out<-outer(1:ncol(z),1:ncol(z),myVecFun,z)
	rownames(out)<-colnames(m)
	colnames(out)<-colnames(m)
	return(out)
}



exec_tfidf<-function(r){
# Description
# Apply tfidf for a given rating matrix
# r - rating/frequency user item matrix
	# r is user item rating matrix
	m<-r
	m[m>0]<-1
	idf<-log10(nrow(m)/colSums(m))
	z<-matrix(idf,nrow=nrow(r),ncol=length(idf),byrow=T)
	out<-r*z
	return(out)
}


dataitem_recom_mgr<-function(users,fileroot,topN=25){
# Description
# Produce data items recommendations for a given user
# Arguments
# user - user id
# filename - file containing the latest data items matrix
# topN - threshold for a cutoff for top N recommendations
	# read user data item ratings matrix
	r<-read_datamatrix(fileroot)
	
	userList<-get_user_recom(fileroot,users,r)
	user_recom<-userList[[2]]
	item_recom<-get_item_recom(fileroot,users,r)

	# mixed
	out<-user_recom
	out<-rbind(out,item_recom)
	out<-out[,-4]
	colnames(out)[2]<-"score"
	# weighted
	x<-merge(item_recom,user_recom,by.x="item",by.y="item")
	x$score<-0.5*(x$freq.x+x$freq.y)
	x<-x[,c("item","score")]
	x$cat<-"weighted"
	out<-rbind(out,x)
	out<-out[order(-out$score,out$item,out$cat),]
	out<-out[!duplicated(out$item),]
	out<-head(out,topN)
	
	myList<-list()
	myList[[1]]<-userList[[1]]
	myList[[2]]<-out
	gc()
	return(myList)
}

get_item_recom<-function(fileroot,users,r){
# Description
# Perform content based filtering
# Arguments
# users - an array of user ids
# r - user item rating/frequency matrix
	# construct item similarity matrix
	sim<-readRDS(paste0(fileroot,"-itemsim.rds"))
	ind<-which(tolower(rownames(r)) %in% tolower(users))
	if(length(users)>1){
		user_items<-r[ind,]
		user_items<-colSums(user_items)
	}else{
		user_items<-r[ind,]
	}
	items_ind<-which(user_items>0)
	simitems_rec<-sim[items_ind,]
	if(length(items_ind)>1){
		items_score<-colSums(user_items[items_ind]*simitems_rec)
		#items_score<-colSums(simitems_rec)
	}else{
		items_score<-simitems_rec
	}
	size<-length(items_score[items_score>0])
	if(size>0){
		out<-as.data.frame(names(items_score))
		out$freq<-items_score
		colnames(out)[1]<-"item"
		out<-out[out$freq>0,]
		out<-out[order(-out$freq),]
		out$cat<-"item"
		out$size<-size
	}else{
		out <- data.frame(item=character(),freq=numeric(),cat=character(),size=integer(),stringsAsFactors=FALSE)
	}
	return(out)
}


get_user_recom<-function(fileroot,users,r){
# Description
# Perform collaborative filtering
# Arguments
# users - array of user ids
# r - user item rating/frequency matrix
	# construct user similarity matrix
	sim<-readRDS(paste0(fileroot,"-usersim.rds"))
	# get the current user record)
	ind<-which(tolower(rownames(sim)) %in% tolower(users))
	if(length(ind) > 1){
		user_sim_rec<-sim[ind,]
		user_sim_rec<-colSums(user_sim_rec)
	}else{
		user_sim_rec<-sim[ind,]
	}
	
	# get current user index
	user_ind<-which(rownames(sim) %in% tolower(users))
	# get similar users indexes and ids
	simusers_ind<-which(user_sim_rec > 0)
	size<-length(simusers_ind)
	simusers<-colnames(sim)[simusers_ind]
	users_sim_score<-user_sim_rec[simusers_ind]
	# get similar users data items
	simusers_rec<-r[simusers_ind,]
	
	# aggregate similar users items
	if(size>1){
		items_score<-colSums(simusers_rec*users_sim_score)
	}else{
		items_score<-simusers_rec
	}
	items_score<-items_score[items_score>0]
	items_score<-items_score[order(-items_score)]
	if(length(items_score)>0){
		out<-as.data.frame(names(items_score))
		out$freq<-items_score
		colnames(out)[1]<-"item"
		out$cat<-"user"
		out$size<-size
	}else{
		out <- data.frame(item=character(),freq=numeric(),cat=character(),size=integer(),stringsAsFactors=FALSE)
	}
	
	myList<-list()
	myList[[1]]<-simusers[order(simusers)]
	myList[[2]]<-out
	return(myList)
}

hop_away_recom_mgr<-function(users,fileroot,topN=25){
# Description
# Compute data items for hop away users based on hop away users goupd
# Arguments
# users - an array of user ids
# fileroot - file containing the latest data items matrix
	# get neighborhood users
	r<-read_datamatrix(fileroot)
	sim<-readRDS(paste0(fileroot,"-usersim.rds"))
	if(length(users) > 1){
		user_sim_rec<-sim[rownames(sim) %in% tolower(users),]
		user_sim_rec<-colSums(user_sim_rec)
		user_items<-r[rownames(r) %in% users,]
		user_items<-colSums(user_items)
	}else{
		user_sim_rec<-sim[rownames(sim) %in% tolower(users)]
		user_items<-r[rownames(r) %in% users]
	}

	user_ind<-which(rownames(sim) %in% tolower(users))
	simusers_ind<-which(user_sim_rec > 0)
	size<-length(simusers_ind)
	simusers<-colnames(sim)[simusers_ind]
	users_sim_score<-user_sim_rec[simusers_ind]
	
	# get neighborhood items
	simusers_rec<-r[simusers_ind,]
	
	
	items_ind<-which(user_items>0)
	if(size>1){
		items_score<-colSums(simusers_rec*users_sim_score)
	}else{
		items_score<-simusers_rec
	}
	
	# get hop away items
	neighborhood_items_ind<-which(items_score>0)
	hopaway_items_ind<-neighborhood_items_ind[!(neighborhood_items_ind %in% items_ind)]
	
	# get hop away users
	z<-rowSums(r[,hopaway_items_ind])
	z<-z[z>0]
	hop_away_users<-names(z[!(names(z) %in% simusers)])
	hop_away_users_ind<-which(rownames(r) %in% hop_away_users)
	
	out<-dataitem_recom_mgr(hop_away_users,fileroot,topN)
	myList<-list()
	myList[[1]]<-hop_away_users[order(hop_away_users)]
	myList[[2]]<-out[[1]]
	myList[[3]]<-out[[2]]
	
	gc()
	return(myList)	 
}

hop_away_mgr<-function(users,fileroot){
# Description
# Compute data items for hop away users based on neighborhood of hop away users
# Arguments
# users - an array of user ids
# fileroot - file root containing the latest data items matrix

	# get neighborhood users
	r<-read_datamatrix(fileroot)
	sim<-readRDS(paste0(fileroot,"-usersim.rds"))
	
	if(length(users) > 1){
		user_sim_rec<-sim[rownames(sim) %in% tolower(users),]
		user_sim_rec<-colSums(user_sim_rec)
		user_items<-r[rownames(r) %in% users,]
		user_items<-colSums(user_items)
	}else{
		user_sim_rec<-sim[rownames(sim) %in% tolower(users)]
		user_items<-r[rownames(r) %in% users]
	}

	user_ind<-which(tolower(rownames(sim)) %in% tolower(users))
	simusers_ind<-which(user_sim_rec > 0)
	size<-length(simusers_ind)
	simusers<-colnames(sim)[simusers_ind]
	users_sim_score<-user_sim_rec[simusers_ind]
	
	# get neighborhood items
	simusers_rec<-r[simusers_ind,]
	
	
	items_ind<-which(user_items>0)
	if(size>1){
		items_score<-colSums(simusers_rec*users_sim_score)
	}else{
		items_score<-simusers_rec
	}
	
	# get hop away items
	neighborhood_items_ind<-which(items_score>0)
	hopaway_items_ind<-neighborhood_items_ind[!(neighborhood_items_ind %in% items_ind)]
	
	# get hop away users
	z<-rowSums(r[,hopaway_items_ind])
	z<-z[z>0]
	hop_away_users<-names(z[!(names(z) %in% simusers)])
	hop_away_users_ind<-which(rownames(r) %in% hop_away_users)
	
	# get hop away data items
	hop_away_rec<-r[hop_away_users_ind,hopaway_items_ind]
	z<-colSums(hop_away_rec)
	z<-z[z>0]
	z<-z[order(-z)]
	if(length(z) >0){
		out<-as.data.frame(z)
		out$item<-names(z)
		out$item<-names(z)
		rownames(out)<-NULL
		colnames(out)[1]<-"freq"
		out<-out[,c(2,1)]
	}else{
		out <- data.frame(item=character(),freq=integer(),stringsAsFactors=FALSE)
	}
	myList<-list()
	myList[[1]]<-hop_away_users[order(hop_away_users)]
	myList[[2]]<-out
	gc()
	return(myList) 
}

locate_user_communities<-function(fileroot){
# Description
# Detecting communities based on users data item similarity
# Arguments
# fileroot - file root containing the latest data items matrix
	library(igraph)
	#r<-read_datamatrix(fileroot)
	sim<-readRDS(paste0(fileroot,"-usersim.rds"))
	
	diag(sim)=0
	g <- graph.adjacency(sim, mode="undirected", weighted=TRUE)
	fg<-cluster_fast_greedy(g)
	com<-communities(fg)
	myList<-list()
	myList[[1]]<-g
	myList[[2]]<-fg
	myList[[3]]<-com
	gc()
	return(myList)
}

drilldown_communities<-function(comList,ind){
# Description
# Detect communities within communities
# Arguments
# comList - initial communities data
# ind - community number to use for detection smaller communities
	g<-comList[[1]]
	fg<-comList[[2]]
	com<-comList[[3]]
	
	g.ind<-induced.subgraph(g,which(fg$membership %in% ind))
	fg.ind<-cluster_fast_greedy(g.ind)
	com.ind<-communities(fg.ind)
	myList<-list()
	myList[[1]]<-g.ind
	myList[[2]]<-fg.ind
	myList[[3]]<-com.ind
	gc()
	return(myList)
}

locate_data_district<-function(fileroot,users=vector()){
	if(length(users) > 0){
		neighbors<-unlist(locate_data_communities(fileroot,users)[[4]])
		neighbors<-neighbors[neighbors !="null"]
		out<-locate_data_communities(fileroot,neighbors)
		saveRDS(unlist(out[[3]]),paste0(fileroot,"-datadistrict.rds"))
		gc()
	}
}

locate_data_communities<-function(fileroot,users=vector(),items=vector()){
# Description
# Detecting communities based on users data item similarity
# Arguments
# filename - file containing the latest data items matrix
# users - an array of users for which data communities should be lodated
# type - type of data item to compute recommendations for (database, table, column)
	library(igraph)
	r<-read_datamatrix(fileroot)
	sim<-readRDS(paste0(fileroot,"-itemsim.rds"))
	# get community items
	diag(sim)=0
	g <- graph.adjacency(sim, mode="undirected", weighted=TRUE)
	fg<-cluster_fast_greedy(g)
	com<-communities(fg)
	if(length(items)>0){
		item_membership<-unique(membership(fg)[items])
		com<-com[item_membership]
	}
	# Identify community membership
	if(length(users) > 0){
		if(length(users)>1){
			user_items<-r[rownames(r) %in% users,]
			user_items<-colSums(user_items)
		}else{
			user_items<-r[rownames(r) %in% users]
		}
		items_ind<-which(user_items>0)
		com_membership<-unique(fg$membership[items_ind])
	}else{
		com_membership<-unique(fg$membership)
	}
	if(length(items)>0){
		com_membership<-intersect(com_membership,item_membership)
		com_membership<-com_membership[order(com_membership)]
	}else{
		com_membership<-com_membership[order(com_membership)]
		com<-com[com_membership]
	}
	
	# Identify users for each data community
	teams<-list()
	n<-length(com_membership)
	if(n > 0){
		for(i in 1:n){
			com_items<-com[[i]]
			if(length(com_items) > 1){
				com_data<-r[,colnames(r) %in% com_items]
				com_data<-rowSums(com_data)
			}else{
				com_data<-r[,colnames(r) %in% com_items]
			}
			com_data<-com_data[com_data>0]
			
			com_users<-names(com_data)
			if(length(com_data)>0){
				com_users<-com_users[order(com_users)]
			}else{
				com_users<-""
			}
			teams[[i]]=com_users
		}
		teams<-setNames(teams,com_membership)
	}
	myList<-list()
	myList[[1]]<-g
	myList[[2]]<-fg
	myList[[3]]<-com
	myList[[4]]<-teams
	gc()
	return(myList)
}

get_items_users<-function(fileroot,items){
# Description
# Identify users for a given set of data items
# Arguments
# fileroot - file root of the data domain files
# items - arrays of data items
	r<-read_datamatrix(fileroot)
	if(length(items)>1){
		items_data<-r[,colnames(r) %in% items]
		items_data<-rowSums(items_data)
	}else{
		items_data<-r[,colnames(r) %in% items]
	}
	items_data<-items_data[items_data>0]
	items_users<-names(items_data)
	items_users<-items_users[order(items_users)]
	gc()
	return(items_users)
}

refresh_base<-function(fileroot){
# Description
# Manage data domain construction
# arguments
# df - dataframe with data tracking info
# fileroot - file root of the data domain files
	dataitem_history_dopar(fileroot)
	get_dataitem_rating(fileroot,type="database",tfidf=T)
	#build_sim(fileroot)
}

blend_tracking_semantic<-function(r,s){
# Description
# Blending user item matrix with items semantic blending
# Arguments
# r - user item matrix
# s - square databases semantic similarity matrix
	K<-0.5
	p<-matrix(0,nrow=nrow(r),ncol=ncol(r))
	# extract db id
	q<-sapply(strsplit(colnames(r), "[$]"), "[", 1)
	backup_colnames<-colnames(r)
	colnames(r)<-q
	q_ind<-which(q %in% colnames(s))
	n<-nrow(r)
	m<-length(q_ind)
	if(m > 0){
		for(i in 1:n){
			ratings<-r[i,q_ind]
			
			for(j in 1:m){		
				if(r[i,q_ind[j]] == 0){
					sim<-s[names(ratings)[j],q[q_ind]]
					sim[j]<-0
					p[i,q_ind[j]]<-K*crossprod(ratings,sim)/sum(sim)	
				}
			}
		}
	}
	r<-r+p
	colnames(r)<-backup_colnames
	return(r)
}

blend_mgr<-function(fileroot){
	if(file.exists(paste0(fileroot,"-matrix.rds")) & file.exists(paste0(fileroot,"-database-sim.rds"))){
		r<-readRDS(paste0(fileroot,"-matrix.rds"))
		s<-readRDS(paste0(fileroot,"-database-sim.rds"))
		x<-blend_tracking_semantic(r,s)
		saveRDS(x,paste0(fileroot,"-blended-matrix.rds"))
		gc()
	}else{
		print("Database user matrix or database similarity matrix is not found")
	}
	
}

remove_files<-function(fileroot){
	# Remove files with fileroot
	filenames <- Sys.glob(paste0(fileroot,"*.rds"))
	file.remove(filenames)
}

data_domain_mgr<-function(fileroot){
# Description
# Construct data domain to provide  a user data recommendations
# Arguments
# startDate - starting date for the data retrieval tracking span
# endDate - ending date for the data retrieval tracking span
# fileroot - file root of the data domain files
	# retrieve tracking data
	remove_files(fileroot)

	# construct user data rating matrixsemantic_tracking_mgr
	refresh_base(fileroot)

	# update data semantic history based on the csv retrieve from the semantic tracking table
	filename<-paste0(fileroot,"-datasemantic.tsv")
	if(file.exists(filename)){
		df<-read.table(file = filename, sep = '\t', header = TRUE)
		saveRDS(df,paste0(fileroot,"-semantic-history.rds"))
		# New addition - calculate databases domain (documents, term document matrix, lsi space)
		build_dbid_domain(fileroot)
		# construct data semantic similarity
		column_lsi_mgr(fileroot,"DESCRIPTION","SCORE",0.8)
		compute_column_desc_sim(fileroot)
		compute_entity_sim(fileroot)
		# blend data semantic into user item rating matrix
		blend_mgr(fileroot)
	}else{
		print("Semantic data update not found!")
	}

	# construct user similarity and data similarity matrices
	build_sim(fileroot)
	gc()
}

refresh_data_mgr<-function(fileroot,users=vector()){
# Description
# Construct data domain to provide  a user data recommendations with the existing local semantic file
# Arguments
# startDate - starting date for the data retrieval tracking span
# endDate - ending date for the data retrieval tracking span
# fileroot - file root of the data domain files

	# construct user data rating matrix
	refresh_base(fileroot)

	filename<-paste0(fileroot,"-semantic-history.rds")
	if(file.exists(filename)){
		# New addition - calculate databases domain (documents, term document matrix, lsi space)
		build_dbid_domain(fileroot)
		# construct data semantic similarity
		column_lsi_mgr(fileroot,"DESCRIPTION","SCORE",0.8)
		compute_column_desc_sim(fileroot)
		compute_entity_sim(fileroot)
		# blend data semantic into user item rating matrix
		blend_mgr(fileroot)
	}else{
		print("Semantic history data not found")
	}
	# construct user similarity and data similarity matrices
	build_sim(fileroot)
	# construct neighborhood data communities
	locate_data_district(fileroot,users)
	gc()
}

refresh_semantic_mgr<-function(fileroot){
# Description
# Construct data domain to provide  a user data recommendations
# Arguments
# fileroot - file root of the data domain files
# df_sample - the sample of instances from the database we are uploading

	
	# update data semantic history based on the tsv retrieve from the semantic tracking table
	filename<-paste0(fileroot,"-datasemantic.tsv")
	if(file.exists(filename)){
		# read new semantic file
		df<-read.table(file = filename, sep = '\t', header = TRUE)
		
		# if the semantic history file exists update it with new data, otherwise save new as existing
		filename<-paste0(fileroot,"-semantic-history.rds")
		if(file.exists(filename)){
			df$Original_Column<-paste(df$ENGINE_ID,df$ENGINE_NAME,df$TABLE_NAME,sep="$")
			exist_df<-readRDS(filename)
			exist_df$Original_Column<-paste(exist_df$ENGINE_ID,exist_df$ENGINE_NAME,exist_df$TABLE_NAME,sep="$")
			exist_df<-exist_df[!(exist_df$Original_Column %in% df$Original_Column),]
			exist_df<-rbind(exist_df,df)
			exist_df<-exist_df[,!(colnames(exist_df) == "Original_Column")]
			saveRDS(exist_df,filename)
		}else{
			saveRDS(df,filename)
		}
		
		# New addition - calculate databases domain (documents, term document matrix, lsi space)
		build_dbid_domain(fileroot)
		# construct data semantic similarity
		column_lsi_mgr(fileroot,"DESCRIPTION","SCORE",0.8)
		compute_column_desc_sim(fileroot)
		compute_entity_sim(fileroot)
		# blend data semantic into user item rating matrix
		blend_mgr(fileroot)
	}else{
		print("Semantic data update not found!")
	}

	# construct user similarity and data similarity matrices
	build_sim(fileroot)
	gc()
}

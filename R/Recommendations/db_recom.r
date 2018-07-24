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
            dimensions=paste0(toString(paste("ga:dimension", dim, sep="")),", ga:date"),
            metrics="ga:totalEvents",
			sort="-ga:date",
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


dataitem_history<-function(df,fileroot){
# Description
# Extract from GA tracking data queries data items history
# Arguments
# df - data queries info from the tracked period
# filename - name of the file the data items history will be saved to
	sep="$"
	z<-df[df$dimension1=="dataquery",c("dimension2","dimension5","date")]
	library(data.table)
	library(jsonlite)
	x<-data.table(dbid=character(),database=character(),table=character(),column=character(),user=character(),daysago=integer())
	n<-nrow(z)
	for(i in 1:n){
		tryCatch({
			recdate<-as.Date(paste0(substr(z[i,3],1,4),"-",substr(z[i,3],5,6),"-",substr(z[i,3],7,8)))
			data<-fromJSON(z[i,1])
			user<-z[i,2]
			rows<-data[[1]]
			cols<-ncol(rows)
			m<-nrow(rows)
			if(m > 0){
				for(j in 1:m){
					daysago<-as.integer(Sys.Date()-recdate)
					if(cols==6){
						x<-rbindlist(list(x,list(rows[j,1],"",rows[j,2],rows[j,3],user,daysago)))
					}else if(cols==7){
						x<-rbindlist(list(x,list(rows[j,2],rows[j,1],rows[j,3],rows[j,4],user,daysago)))
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
	gc()
	saveRDS(x,paste0(fileroot,"-history.rds"))
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
			x<-x[,c(7,5,6)]
			y<-count(x,c("dataitem","user","daysago"))
			#y<-count(x,c("dbid","database","user","daysago"))
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
		z<-apply_tfidf(z)
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

build_sim<-function(fileroot){
	r<-readRDS(paste0(fileroot,"-matrix.rds"))
	sim<-cosine_jaccard_sim(r)
	saveRDS(sim,paste0(fileroot,"-usersim.rds"))
	sim<-cosine_jaccard_sim(t(r))
	saveRDS(sim,paste0(fileroot,"-itemsim.rds"))
}

cosine_jaccard_sim<-function(m){
# Description
# Compute cosine jacqard similarity between cells of a matrix
# m - rating/frequency user item  matrix
	library(philentropy)
	out<-cosine_sim(m)*jaccard_sim(m)
	return(out)
}

cosine_sim<-function(m){
# Description
# Compute cosine jacqard similarity between cells of a matrix
# m - rating/frequency matrix
	library(philentropy)
	tryCatch({
		out<-distance(m,"cosine")
	})
	rownames(out)<-rownames(m)
	colnames(out)<-rownames(m)
	return(out)
}

jaccard_sim<-function(m){
# Description
# Compute jacqard similarity between cells of a matrix
# m - rating/frequency user item  matrix
	# m is a matrix where each row is a relevant vector
	n<-m
	n[n>0]<-1
	library(philentropy)
	tryCatch({
		out<-distance(n,"jaccard")
	})
	rownames(out)<-rownames(m)
	colnames(out)<-rownames(m)
	return(out)
}


apply_tfidf<-function(r){
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

compute_weight<-function(n,N){
# Description
# Compute weight of a set 
	return(1/log10(N/n))
}

dataitem_recom_mgr<-function(users,fileroot,cutoff=0.05){
# Description
# Produce data items recommendations for a given user
# Arguments
# user - user id
# filename - file containing the latest data items matrix
# cutoff - threshold for a cutoff for recommendations
	# read user data item ratings matrix
	r<-readRDS(paste0(fileroot,"-matrix.rds"))
	userList<-get_user_recom(fileroot,users,r)
	user_recom<-userList[[2]]
	user_group<-max(1,unique(user_recom$size))
	item_recom<-get_item_recom(fileroot,users,r)
	item_group<-max(1,unique(item_recom$size))
	user_weight<-compute_weight(user_group,user_group+item_group)
	item_weight<-compute_weight(item_group,user_group+item_group)
	user_recom$freq<-user_recom$freq*user_weight/(user_weight+item_weight)
	out<-user_recom
	item_recom$freq<-item_recom$freq*item_weight/(user_weight+item_weight)
	out<-rbind(out,item_recom)
	out<-out[,-ncol(out)]
	out<-out[order(-out$freq,out$item,out$cat),]
	out<-out[!duplicated(out$item),]
	if(nrow(out)>0){
		out<-out[out$freq > max(out$freq)*cutoff,]
	}
	
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
	if(nrow(simitems_rec)>1){
		items_score<-colSums(simitems_rec)
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

hop_away_recom_mgr<-function(users,fileroot,cutoff=0.05){
# Description
# Compute data items for hop away users based on hop away users goupd
# Arguments
# users - an array of user ids
# fileroot - file containing the latest data items matrix
	# get neighborhood users
	r<-readRDS(paste0(fileroot,"-matrix.rds"))
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
	
	out<-dataitem_recom_mgr(hop_away_users,fileroot,cutoff)
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
	r<-readRDS(paste0(fileroot,"-matrix.rds"))
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
	r<-readRDS(paste0(fileroot,"-matrix.rds"))
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

locate_data_communities<-function(fileroot,users=vector(),items=vector()){
# Description
# Detecting communities based on users data item similarity
# Arguments
# filename - file containing the latest data items matrix
# users - an array of users for which data communities should be lodated
# type - type of data item to compute recommendations for (database, table, column)
# tfidf - boolean whether to use tfidf calculations
	library(igraph)
	r<-readRDS(paste0(fileroot,"-matrix.rds"))
	sim<-readRDS(paste0(fileroot,"-itemsim.rds"))
	# if items defined remove the respected ratings from the matrix
	if(length(items) > 0){
		ind<-which(colnames(r) %in% items)
		r<-r[,ind]
		sim<-sim[ind,ind]
	}
	diag(sim)=0
	g <- graph.adjacency(sim, mode="undirected", weighted=TRUE)
	fg<-cluster_fast_greedy(g)
	com<-communities(fg)
	
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
	com_membership<-com_membership[order(com_membership)]
	
	# Identify users for each data community
	teams<-list()
	n<-length(com_membership)
	if(n > 0){
		for(i in 1:n){
			com_items<-com[[com_membership[i]]]
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
			teams[[i]]<-com_users				
		}
		teams<-setNames(teams,com_membership)
	}
	myList<-list()
	myList[[1]]<-g
	myList[[2]]<-fg
	myList[[3]]<-com[com_membership]
	myList[[4]]<-teams
	gc()
	return(myList)
}

get_items_users<-function(fileroot,items){
# Description
# Identify users for a given set of data items
# Arguments
# filename - file name with user item matrix
# items - arrays of data items
	r<-readRDS(paste0(fileroot,"-matrix.rds"))
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

refresh_base<-function(df,fileroot){
	dataitem_history(df,fileroot)
	get_dataitem_rating(fileroot,type="database",tfidf=T)
	build_sim(fileroot)
}

blend_tracking_semantic<-function(r,s){
# Description
# Blending user item matrix with items semantic blending
# Arguments
# r - user item matrix
# s - square databases semantic similarity matrix
	K<-0.5
	p<-matrix(0,nrow=nrow(r),ncol=ncol(r))
	r_ind<-which(colnames(r) %in% colnames(s))
	n<-nrow(r)
	m<-ncol(r)
	for(i in 1:n){
		for(j in r_ind){
			if(r[i,j]==0){
				items<-r[i,r_ind]
				s_ind<-which(colnames(s) %in% colnames(r)[r_ind])
				sim<-s[j,s_ind]
				p[i,j]<-value<-K*crossprod(items,sim)
			}
		}
	}
	r<-r+p
	return(r)
}
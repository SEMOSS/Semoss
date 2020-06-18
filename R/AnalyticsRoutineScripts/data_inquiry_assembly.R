build_query<-function(db,joins,text,root_fn="nldr"){
	KEYWORDS<-c("sum","average","count","min","max",'where','group','by','having','order')
	library(igraph)
	library(data.table)
	library(SteinerNet)

	p<-data.table(query=integer(),appid=character(),appid2=character(),part=character(),item1=character(),item2=character(),
	item3=character(),item4=character(),item5=character(),item6=character(),item7=character())
	words<-unlist(strsplit(text," "))
	cols<-unique(db[tolower(db$Column) %in% tolower(words),"Column"])
	# remove keywords from column names
	cols<-cols[!(tolower(cols) %in% KEYWORDS)]

	# make sure the column names has the propper capitalization
	if(length(cols)>0){
		for(i in 1:length(cols)){
			ind<-which(tolower(words) %in% tolower(cols[i]))
			if(length(ind)>0){
				words[ind]<-cols[i]
			}
		}
	}
	# Retrieve cluster info
	partition<-readRDS(paste0(root_fn,"_membership.rds"))
	clusters<-unique(unname(partition))
	if(length(clusters)>0){
		k<-1
		for(i in 1:length(clusters)){
			# make sure that all columns present in the cluster
			tbls<-names(partition[partition==clusters[i]])
			tbls_cols<-unique(db[db$Table %in% tbls,"Column"])
			if(all(cols %in% tbls_cols)){
				cur_db<-db[db$Table %in% tbls,]
				cur_joins<-joins[joins$tbl1 %in% tbls & joins$tbl2 %in% tbls,]
				cluster_joins<-build_joins(cols,cur_joins,cur_db)
				
				# process statement parts and accumulate them
				p<-process_stmnt(cur_db,cur_joins,cluster_joins,words,cols,p,k)
				k<-k+1
			}
		}
	}
	gc()
	return(p)
}

process_stmnt<-function(db,joins,cluster_joins,words,cols,p,k){
	stmnt_parts<-partition_request(words)
	part_names<-names(stmnt_parts)
	out<-list()
	if(length(part_names)>0){
		for(j in 1:length(part_names)){
			part<-stmnt_parts[[part_names[j]]]
			if(part_names[j]=='select'){
				p<-process_select(db,part,cols,p,k)
				p<-process_from(cluster_joins,p,k)
			}else if(part_names[j]=='where'){
				p<-process_where(db,part,cols,p,k)
			}else if(part_names[j]=='group'){
				p<-process_group(db,part,cols,p,k)
			}else if(part_names[j]=='having'){
				p<-process_having(db,part,cols,p,k)
			}else if(part_names[j]=='order'){
				p<-process_order(db,part,cols,p,k)
			}
		}
	}
	gc()
	return(p)
}

process_select<-function(db,part,cols,p,k){
	AGGR<-c("sum","average","count","min","max")

	ind<-which(tolower(part) %in% tolower(cols))
	if(length(ind)>0){
		for(i in 1:length(ind)){
			cur<-part[ind[i]]
			if(ind[i]>1){
				prev<-intersect(part[ind[i]-1],AGGR)
				if(length(prev)>0){
					appid_tbl<-unlist(strsplit(db[tolower(db$Column)==tolower(cur),"Table"][1],"._.",fixed=TRUE))
					p<-rbindlist(list(p,list(k,appid_tbl[1],'','select',appid_tbl[2],cur,prev,'','','','')))
				}else{
					appid_tbl<-unlist(strsplit(db[tolower(db$Column)==tolower(cur),"Table"][1],"._.",fixed=TRUE))
					p<-rbindlist(list(p,list(k,appid_tbl[1],'','select',appid_tbl[2],cur,'','','','','')))
				}
			}else{
				appid_tbl<-unlist(strsplit(db[tolower(db$Column)==tolower(cur),"Table"][1],"._.",fixed=TRUE))
				p<-rbindlist(list(p,list(k,appid_tbl[1],'','select',appid_tbl[2],cur,'','','','','')))
			}
		}
	}
	return(p)
}

process_from<-function(cluster_joins,p,k){
	n<-nrow(cluster_joins)
	if(n>0){
		for(i in 1:n){
			if(cluster_joins$AppID[i] == cluster_joins$AppID2[i]){
				appid<-cluster_joins$AppID[i]
				appid2=''
				tbl1<-unlist(strsplit(cluster_joins$tbl1[i],"._.",fixed=TRUE))[2]
				tbl2<-unlist(strsplit(cluster_joins$tbl2[i],"._.",fixed=TRUE))[2]
				col1<-cluster_joins$joinby1[i]
				if(tbl1==tbl2){
					tbl2<-''
					col2<-''
				}else{
					col2<-cluster_joins$joinby2[i]
				}
			}else{
				appid<-cluster_joins$AppID[i]
				tbl1<-unlist(strsplit(cluster_joins$tbl1[i],"._.",fixed=TRUE))[2]
				appid2<-cluster_joins$AppID2[i]
				tbl2<-unlist(strsplit(cluster_joins$tbl2[i],"._.",fixed=TRUE))[2]
				col1<-cluster_joins$joinby1[i]
				col2<-cluster_joins$joinby2[i]
			}
			p<-rbindlist(list(p,list(k,appid,appid2,'from',tbl1,col1,tbl2,col2,'','','')))
		}
	}
	return(p)
}

process_where<-function(db,part,cols,p,k){
	OPS<-c("<","<=",">",">=","<>","=")
	
	ind<-which(tolower(part) %in% tolower(cols))
	if(length(ind)>0){
		for(i in 1:length(ind)){
			cur<-part[ind[i]]
			if(length(part) >= ind[i]+2){
				op<-intersect(part[ind[i]+1],OPS)
				if(length(op)>0){
					value<-part[ind[i]+2]
					if(!(value %in% cols)){
						appid_tbl<-unlist(strsplit(db[tolower(db$Column)==tolower(cur),"Table"][1],"._.",fixed=TRUE))
						p<-rbindlist(list(p,list(k,appid_tbl[1],'','where',appid_tbl[2],cur,op,value,'','','')))
					}
				}
			}
		}
	}
	return(p)
}

process_group<-function(db,part,cols,p,k){

	ind<-which(tolower(part) %in% tolower(cols))
	if(length(ind)>0){
		for(i in 1:length(ind)){
			cur<-part[ind[i]]
			appid_tbl<-unlist(strsplit(db[tolower(db$Column)==tolower(cur),"Table"][1],"._.",fixed=TRUE))
			p<-rbindlist(list(p,list(k,appid_tbl[1],'','group',appid_tbl[2],cur,'','','','','')))
		}
	}
	return(p)
}

process_having<-function(db,part,cols,p,k){
	AGGR<-c("sum","average","count","min","max")
	OPS<-c("<","<=",">",">=","<>","=")
	
	ind<-which(tolower(part) %in% tolower(cols))
	if(length(ind)>0){
		for(i in 1:length(ind)){
			cur<-part[ind[i]]
			if(ind[i]>1){
				prev<-intersect(part[ind[i]-1],AGGR)
				if(length(prev)>0 & length(part) >= ind[i]+2){
					op<-intersect(part[ind[i]+1],OPS)
					if(length(op)>0){
						value<-part[ind[i]+2]
						if(!(value %in% cols)){
							appid_tbl<-unlist(strsplit(db[tolower(db$Column)==tolower(cur),"Table"][1],"._.",fixed=TRUE))
							p<-rbindlist(list(p,list(k,appid_tbl[1],'',appid_tbl[2],'having',cur,prev,op,value,'','')))
						}
					}
				}
			}
		}
	}
	return(p)
}

process_order<-function(db,part,cols,p,k){
	ITEMS<-c('ascending','descending')

	ind<-which(tolower(part) %in% tolower(cols))
	if(length(ind)>0){
		for(i in 1:length(ind)){
			cur<-part[ind[i]]
			if(length(part)>=ind[i]+1){
				item<-intersect(part[ind[i]+1],ITEMS)
				appid_tbl<-unlist(strsplit(db[tolower(db$Column)==tolower(cur),"Table"][1],"._.",fixed=TRUE))
				if(length(item)>0){
					p<-rbindlist(list(p,list(k,appid_tbl[1],'',appid_tbl[2],'order',cur,item,'','','','')))
				}else{
					item<-ITEMS[1]
					p<-rbindlist(list(p,list(k,appid_tbl[1],'',appid_tbl[2],'order',cur,item,'','','','')))
				}
				item<-ITEMS[1]
			}
		}
	}
	return(p)
}

partition_request<-function(words){
	PARTS<-c('select','where','group','having','order')	
	
	if('select' %in% words){
		ind<-which(PARTS %in% tolower(words))
	}else{
		ind<-c(1,which(PARTS %in% tolower(words)))
	}
	
	stmnt_parts<-list()
	strt<-0
	if(length(ind)==0){
		stmnt_parts[["select"]]<-words
	}else{
		for(i in 1:length(ind)){
			r<-get_next_part(words,ind[i],strt)
			stmnt_parts[[PARTS[ind[i]]]]<-r[[2]]
			if(length(r[[2]])>0){
				strt<-r[[1]]+1
				# process select
			}
		}
	}
	return(stmnt_parts)
}

get_next_part<-function(words,keyword_ind,strt){
	KEYWORDS<-c('','where','group','by','','having','order','by')
	
	# if no keywords in the remaining section then special pocessing
	ind<-which(tolower(words) %in% c(KEYWORDS[2],KEYWORDS[4],KEYWORDS[6]))
	ind<-ind[ind > strt]
	out<-list()
	if(length(ind)==0){
		if(strt<=length(words)){
			out[[1]]<-length(words)
			out[[2]]<-words[strt:length(words)]
		}else{
			out[[1]]<-strt
			out[[2]]<-vector()
		}
	}else{
		keyword_ind_max<-length(KEYWORDS)/2
		k<-0
		for(i in keyword_ind:keyword_ind_max){
			ind<-which(tolower(words)==KEYWORDS[2*i])
			ind<-ind[ind > strt]
			
			if(length(ind)>0){
				if(KEYWORDS[2*i-1] != ''){
					if(words[ind[1]-1] == KEYWORDS[2*i-1]){
						k<-i
						break
					}
				}else{
					k<-i
					break
				}
			}
		}
		if(k==0){
			out[[1]]<-strt
			out[[2]]<-vector()
		}else{
			if(length(ind)>0){
				if(KEYWORDS[2*k-1] != ''){
					if(words[ind[1]-1] == KEYWORDS[2*k-1]){
						part<-words[strt:(ind[1]-2)]
						out[[1]]<-ind[1]
					}else{
						part<-vector()
						out[1]<-0
					}
				}else{
					part<-words[strt:(ind[1]-1)]
					out[[1]]<-ind[1]
				}
			}else{
				part<-vector()
				out[1]<-0
			}
			
			out[[2]]<-part
		}
	}
	return(out)
}

build_joins<-function(cols,joins,cur_db){
	# if joins is not empty assemble links required for optinization
	if(nrow(joins)>0){
		# if combined databases drop extra links if possible
		g<-graph_from_edgelist(as.matrix(joins[,1:2]),directed=FALSE)
		nodes_df<-as.data.frame(unique(names(V(g))),stringsAsFactors=FALSE)
		names(nodes_df)<-"node"
		nodes_df$label<-unlist(strsplit(nodes_df$node,"._.",fixed=TRUE))[c(FALSE,TRUE)]
		edges_df<-joins[,1:2]
	
		# add terminal column nodes
		join_cols<-vector()
		for(i in 1:length(cols)){
			# identify tables with the current column
			col_tbls<-cur_db[cur_db$Table %in% nodes_df$node & cur_db$Column==cols[i],"Table"]
			if(length(col_tbls)>0){
				# get tables and labels from node_df
				col_df<-nodes_df[nodes_df$node %in% col_tbls,]
				names(col_df)<-c("tbl1","tbl2")
				edges_df<-rbind(edges_df,col_df)
				join_cols<-unique(append(join_cols,col_df$tbl2))
			}
		}
		# remove circular links
		edges_df<-edges_df[edges_df$tbl1 != edges_df$tbl2,]
	}else{
		join_cols<-cols
		tbl<-unique(cur_db[cur_db$Column %in% cols,"Table"])
		edges_df<-data.frame(tbl1=tbl,tbl2=tbl,stringsAsFactors=FALSE)
	}
	if(length(join_cols)==1 | nrow(joins)==0){
		tbls<-edges_df[1,"tbl1"]
		items<-unlist(strsplit(tbls,"._.",fixed=TRUE))
		joins<-joins[0,]
		joins$AppID2<-character()
		joins<-rbindlist(list(joins,list(tbls,tbls,"","",items[1],items[1])))
	}else{
		g1<-graph_from_edgelist(as.matrix(edges_df),directed=FALSE)
		cols_id<-which(V(g1)$name %in% join_cols)	
		stree<-steinertree(type = "RSP",repeattimes=30, optimize = FALSE,terminals = cols_id,graph = g1,color = FALSE, merge = FALSE) 
		if(length(stree)==1){
			new_joins<-as.data.frame(as_edgelist(stree[[1]],names=TRUE),stringsAsFactors=FALSE)
			names(new_joins)<-c("tbl1","tbl2")
			new_joins<- new_joins[!(new_joins$tbl1 %in% join_cols) & !(new_joins$tbl2 %in% join_cols),]
			# run mst to make sure there are no extra edges!
			g1<-graph_from_edgelist(as.matrix(new_joins),directed=FALSE)
			g2<-mst(g1)
			new_joins<-as.data.frame(as_edgelist(g2,names=TRUE),stringsAsFactors=FALSE)
			names(new_joins)<-c("tbl1","tbl2")
			
			arranged_joins<-joins[0,]
			for(j in 1:nrow(new_joins)){
				x<-joins[joins$tbl1==new_joins$tbl1[j] & joins$tbl2==new_joins$tbl2[j],]
				if(nrow(x)>0){
					arranged_joins<-rbind(arranged_joins,x)
				}else{
					x<-joins[joins$tbl2==new_joins$tbl1[j] & joins$tbl1==new_joins$tbl2[j],]
					if(nrow(x)>0){
						arranged_joins<-rbind(arranged_joins,x)
					}
				}
			}
			new_joins<-arranged_joins
			# add other columns from original joins!!!
			tbl1<-unlist(strsplit(new_joins$tbl1,"._.",fixed=TRUE))
			tbl2<-unlist(strsplit(new_joins$tbl2,"._.",fixed=TRUE))
			new_joins$joinby1<-tbl1[c(FALSE,TRUE)]
			new_joins$joinby2<-tbl2[c(FALSE,TRUE)]
			new_joins$AppID<-tbl1[c(TRUE,FALSE)]
			new_joins$AppID2<-tbl2[c(TRUE,FALSE)]
			joins<-new_joins
		}else{
			joins<-vector()
		}
	}
	gc()
	return(joins)
}


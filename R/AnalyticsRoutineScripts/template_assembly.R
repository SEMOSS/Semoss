process_select<-function(db,part,cols,p,k){
	library(tools)
	AGGR<-c("fsum","faverage","fcount","fmin","fmax","fstdev","fdayname","fweek","fmonthname","fquarter","fyear")

	ind<-which(tolower(part) %in% tolower(cols))
	if(length(ind)>0){
		for(i in 1:length(ind)){
			cur<-part[ind[i]]
			if(ind[i]>1){
				prev<-intersect(part[ind[i]-1],AGGR)
				prev<-substring(prev,2)
				if(length(prev)>0){
					unique_count<-FALSE
					if(ind[i]>2){
						if(part[ind[i]-2]=='unique'){
							unique_count<-TRUE
						}
					}
					appid_tbl<-unlist(strsplit(db[tolower(db$Column)==tolower(cur),"Table"][1],"._.",fixed=TRUE))
					if(unique_count){
						aggr_alias<-paste0('UniqueCount','_',cur)
						p<-rbindlist(list(p,list(k,appid_tbl[1],'','select',appid_tbl[2],cur,'UniqueCount',aggr_alias,'','','')))
					}else{
						aggr_alias<-paste0(tools::toTitleCase(prev),'_',cur)
						p<-rbindlist(list(p,list(k,appid_tbl[1],'','select',appid_tbl[2],cur,prev,aggr_alias,'','','')))
					}
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
	OPS<-c('=','<','<=','>','>=','!=','between','after','before')

	ind<-which(tolower(part) %in% tolower(cols))
	if(length(ind)>0){
		for(i in 1:length(ind)){
			cur<-part[ind[i]]
			if(length(part) >= ind[i]+2){
				op<-intersect(part[ind[i]+1],OPS)
				if(length(op)>0){
					value<-part[ind[i]+2]
					if(op==OPS[7]){
						value2<-part[ind[i]+4]
						appid_tbl<-unlist(strsplit(db[tolower(db$Column)==tolower(cur),"Table"][1],"._.",fixed=TRUE))
						p<-rbindlist(list(p,list(k,appid_tbl[1],'','where',appid_tbl[2],cur,OPS[5],value,'','','')))
						p<-rbindlist(list(p,list(k,appid_tbl[1],'','where',appid_tbl[2],cur,OPS[3],value2,'','','')))	
					}else{
						if(!(value %in% cols)){
							appid_tbl<-unlist(strsplit(db[tolower(db$Column)==tolower(cur),"Table"][1],"._.",fixed=TRUE))
							if(op==OPS[8]){
								op<-OPS[4]
							}else if(op==OPS[9]){
								op<-OPS[2]
							}
							p<-rbindlist(list(p,list(k,appid_tbl[1],'','where',appid_tbl[2],cur,op,value,'','','')))
						}
					}
				}else{
					if(length(part) - ind[i] == 2){
						if(part[ind[i]+1]=='contains'){
							value<-part[ind[i]+2]
							appid_tbl<-unlist(strsplit(db[tolower(db$Column)==tolower(cur),"Table"][1],"._.",fixed=TRUE))
							p<-rbindlist(list(p,list(k,appid_tbl[1],'','where',appid_tbl[2],cur,'?like',value,'','','')))
						}
					}else if(length(part) - ind[i] == 3){
						if(part[ind[i]+1]=='begins' & part[ind[i]+2]=='with'){
							value<-part[ind[i]+3]
							appid_tbl<-unlist(strsplit(db[tolower(db$Column)==tolower(cur),"Table"][1],"._.",fixed=TRUE))
							p<-rbindlist(list(p,list(k,appid_tbl[1],'','where',appid_tbl[2],cur,'?begins',value,'','','')))
						}else if(part[ind[i]+1]=='ends' & part[ind[i]+2]=='with'){
							value<-part[ind[i]+3]
							appid_tbl<-unlist(strsplit(db[tolower(db$Column)==tolower(cur),"Table"][1],"._.",fixed=TRUE))
							p<-rbindlist(list(p,list(k,appid_tbl[1],'','where',appid_tbl[2],cur,'?ends',value,'','','')))
						}else if(part[ind[i]+1]=='not' & part[ind[i]+2]=='contains'){
							value<-part[ind[i]+3]
							appid_tbl<-unlist(strsplit(db[tolower(db$Column)==tolower(cur),"Table"][1],"._.",fixed=TRUE))
							p<-rbindlist(list(p,list(k,appid_tbl[1],'','where',appid_tbl[2],cur,'?nlike',value,'','','')))
						}
					}else{
						if(part[ind[i]+1]=='not' & part[ind[i]+2]=='begins' & part[ind[i]+3]=='with'){
							value<-part[ind[i]+4]
							appid_tbl<-unlist(strsplit(db[tolower(db$Column)==tolower(cur),"Table"][1],"._.",fixed=TRUE))
							p<-rbindlist(list(p,list(k,appid_tbl[1],'','where',appid_tbl[2],cur,'?nbegins',value,'','','')))
						}else if(part[ind[i]+1]=='not' & part[ind[i]+2]=='ends' & part[ind[i]+3]=='with'){
							value<-part[ind[i]+4]
							appid_tbl<-unlist(strsplit(db[tolower(db$Column)==tolower(cur),"Table"][1],"._.",fixed=TRUE))
							p<-rbindlist(list(p,list(k,appid_tbl[1],'','where',appid_tbl[2],cur,'?nends',value,'','','')))
						}
					}
				}
			}
		}
	}
	return(p)
}

process_group<-function(db,part,cols,p,k){
	# will be updated for the respected SEMOSS function!!!
	DATE_GROUPPING<-c('fdayname','fweek','fmonthname','fquarter','fyear')
	
	ind<-which(tolower(part) %in% tolower(cols))
	if(length(ind)>0){
		for(i in 1:length(ind)){
			cur<-part[ind[i]]
			if(ind[i]>1){
				op<-intersect(part[ind[i]-1],DATE_GROUPPING)
				op<-substring(op,2)
				aggr_alias<-get_aggr_alias(part[(ind[i]-1):ind[i]])
			}else{
				op<-vector()
			}
			appid_tbl<-unlist(strsplit(db[tolower(db$Column)==tolower(cur),"Table"][1],"._.",fixed=TRUE))
			if(length(op)>0){				
				p<-rbindlist(list(p,list(k,appid_tbl[1],'','group',appid_tbl[2],aggr_alias,'','','','','')))
			}else{
				p<-rbindlist(list(p,list(k,appid_tbl[1],'','group',appid_tbl[2],cur,'','','','','')))
			}
		}
	}
	return(p)
}

process_having<-function(db,part,cols,p,k){
	AGGR<-c("fsum","faverage","fcount","fmin","fmax")
	OPS<-c("<","<=",">",">=","<>","=")
	
	ind<-which(tolower(part) %in% tolower(cols))
	if(length(ind)>0){
		for(i in 1:length(ind)){
			cur<-part[ind[i]]
			if(ind[i]>1){
				prev<-intersect(part[ind[i]-1],AGGR)
				prev<-substring(prev,2)
				if(length(prev)>0 & length(part) >= ind[i]+2){
					op<-intersect(part[ind[i]+1],OPS)
					if(length(op)>0){
						value<-part[ind[i]+2]
						if(!(value %in% cols)){
							appid_tbl<-unlist(strsplit(db[tolower(db$Column)==tolower(cur),"Table"][1],"._.",fixed=TRUE))
							p<-rbindlist(list(p,list(k,appid_tbl[1],'','having',appid_tbl[2],cur,prev,op,value,'','')))
						}
					}
				}
			}
		}
	}
	return(p)
}

process_sort<-function(db,part,all_cols,p,k){
	ITEMS<-c('ascending','descending')

	#ind<-which(tolower(part) %in% tolower(cols))
	# columns in select
	cols<-all_cols[seq(1,by=2, len=length(all_cols)/2)]
	ind<-which(tolower(part) %in% tolower(cols))
	
	if(length(ind)>0){
		for(i in 1:length(ind)){
			cur<-part[ind[i]]
			cur_ind<-which(all_cols==cur)
			appid_tbl<-unlist(strsplit(all_cols[cur_ind[1]+1],"._.",fixed=TRUE))

			item<-part[ind[i]+1]
			if(item==ITEMS[1]){
				item<-'ASC'
			}else{
				item<-'DESC'
			}
			if(length(item)>0){
				p<-rbindlist(list(p,list(k,appid_tbl[1],'','sort',appid_tbl[2],cur,item,'','','','')))
			}
		}
	}
	return(p)
}

process_rank<-function(db,part,all_cols,p,k){

	# columns in select
	cols<-all_cols[seq(1,by=2, len=length(all_cols)/2)]
	ind<-which(tolower(part) %in% tolower(cols))
	
	if(length(ind)>0){
		for(i in 1:length(ind)){
			cur<-part[ind[i]]
			location<-part[ind[i]+1]
			if(location=='top'){
				item<-'DESC'
			}else{
				item<-'ASC'
			}
			if(i==length(ind)){
				if(length(part)==(ind[i]+1)){
					n<-1
				}else{
					n<-part[ind[i]+2]
				}
			}else{
				if((ind[i+1]-ind[i])==2){
					n<-1
				}else{
					n<-part[ind[i]+2]
				}
			}
			cur_ind<-which(all_cols==cur)
			if(length(cur_ind)>0){
				appid_tbl<-unlist(strsplit(all_cols[cur_ind[1]+1],"._.",fixed=TRUE))
				p<-rbindlist(list(p,list(k,appid_tbl[1],'','rank',appid_tbl[2],cur,item,n,'','','')))
			}
		}
	}
	return(p)
}

build_joins<-function(cols,joins,cur_db){
	library(igraph)
	library(SteinerNet)
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
		if(length(join_cols)==1 & ncol(edges_df) !=2){
			edges_df<-edges_df[edges_df$tbl2==join_cols,]
		}
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


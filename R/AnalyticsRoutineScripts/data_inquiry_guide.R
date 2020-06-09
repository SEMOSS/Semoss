
cluster_tables<-function(db,joins,root_fn="nldr"){
# Partitions tables into connected clusters
# Arguments
# db - a table of the columns, tables and apps
# joins - a table of existing joins
# root_fn - a root of files with the required info 
# Output
# A frame with 2 columns: list of tables and the respected cluster number
	library(igraph)

	if(nrow(joins)>0){
		g<-graph_from_edgelist(as.matrix(joins[,1:2]),directed=FALSE)
		
		tables<-unique(append(joins[,1],joins[,2]))
		standalone<-unique(db[!(db$Table %in% tables),]$Table)
		if(length(standalone)>0){
			g<-g+vertices(standalone)
		}
		membership<-components(g)$membership
	}else{
		tables<-unique(db$Table)
		membership<-seq(length(tables))
		names(membership)<-tables
	}
	saveRDS(membership,paste0(root_fn,"_membership.rds"))
	gc()
}

get_feasible_tables<-function(db,cols,root_fn="nldr"){
# Identifies clusters and their tables that contain all required columns
# Arguments
# db - a table of the columns, tables and apps
# cols - a list of columns in the select part
# root_fn - a root of file names with the required info
# Output
# a list of feasible tables or an error message is there is 
# not a single cluster with all required ones
	# read partition
	partition<-readRDS(paste0(root_fn,"_membership.rds"))
	if(length(cols)>0){
		# get tables for the existing columns
		tables<-unique(db[tolower(db$Column) %in% tolower(cols),]$Table)
		# all clusters and their tables that contain the required columns
		my_partition<-partition[names(partition) %in% tables]
		my_clusters<-unique(unname(my_partition))
		
		clusters<-vector()
		for(i in 1:length(my_clusters)){
			# tables for a candidate cluster
			tbls<-names(my_partition[unname(my_partition)==my_clusters[i]])
			# if the current cluster has all required columns add it to the feasible list
			if(all(cols %in% db[db$Table %in% tbls,]$Column)){
				clusters<-append(clusters,my_clusters[i])
			}
		}
		# all feasible clusters tables are feasible tables (names array)
		if(length(clusters)>0){
			feasible_tables<-partition[unname(partition) %in% unique(clusters)]
		}else{
			feasible_tables<-vector()
		}
	}else{
		feasible_tables<-partition
	}
	gc()
	return(feasible_tables)
}

get_next_keyword<-function(db,text,root_fn="nldr"){
# Recommend users next word while entering data request
# Arguments
# rxr - already entered text of the request
# root_fn - root for the file names with the required info
# Output
# an array of options for the next word
	# initial elements, should be extended 
	ITEMS<-c("where","group","by","having","order")
	
	words<-tolower(unlist(strsplit(text," ")))
	text_cols<-unique(db[tolower(db$Column) %in% words,"Column"])
	
	feasible_tables<-get_feasible_tables(db,text_cols,root_fn)
	if(length(feasible_tables)>0){
		if(length(words)>0){
			# get columns in select component
			r<-get_selected_columns(db,words)
			cols<-r[[1]]
			group_required<-r[[2]]
			aggr_cols<-r[[3]]
			
			last_word<-words[length(words)]
			input<-words[words %in% ITEMS]
			
			last_item<-input[length(input)]
			# check the last word	
			if(length(last_item)==0){
				next_keyword<-next_keyword_select(db,cols,last_word,group_required,feasible_tables)
			}else if(last_item=="where"){
				if(length(words)>0){
					prev_word<-words[length(words)-1]
					where_part_col<-db[tolower(db$Column)==prev_word,'Column']
				}else{
					prev_word<-''
					where_part_col<-''
				}
				next_keyword<-next_keyword_where(db,cols,last_word,prev_word,where_part_col,group_required,feasible_tables)
			}else if(last_item=="by"){
				if(length(input)>1){
					before_last_item<-input[length(input)-1]
					if(before_last_item=="group"){
						ind<-which(words %in% last_item)
						if(ind < length(words)){
							group_part<-words[(ind+1):length(words)]
						}else{
							group_part<-vector()
						}
						group_cols<-unique(db[tolower(db$Column) %in% group_part,"Column"])
						next_keyword<-next_keyword_group(cols,last_word,group_cols)
					}else if(before_last_item=="order"){
						ind<-max(which(words %in% last_item))
						if(ind < length(words)){
							order_part<-words[(ind+1):length(words)]
						}else{
							order_part<-vector()
						}
						order_cols<-db[tolower(db$Column) %in% order_part,'Column']
						next_keyword<-next_keyword_order(cols,last_word,order_cols)
					}
				}
			}else if(last_item=="having"){
				ind<-which(words %in% last_item)
				if(ind < length(words)){
					having_part<-words[(ind+1):length(words)]
					having_cols<-db[tolower(db$Column) %in% having_part,'Column']
				}else{
					having_cols<-vector()
				}	
				next_keyword<-next_keyword_having(last_word,aggr_cols,having_cols)
			}
		}else{
			# if nothing entered -> only columns or aggrcolumn
			next_keyword<-next_keyword_select(db,vector(),"",FALSE,feasible_tables)
		}
	}else{
		next_keyword<-"Selected data could not be joined together"
	}
	next_keyword<-next_keyword[tolower(substr(next_keyword, nchar(next_keyword)-2, nchar(next_keyword)))!='_fk']
	gc()
	return(next_keyword)
}

get_next_column<-function(db,cols,feasible_tables){
	# get columns for the existing clusters tables
	next_column<-unique(db[db$Table %in% names(feasible_tables),]$Column)
	gc()
	return(next_column)
}

get_selected_columns<-function(db,words){
# Determines the list of columns and aggregated columns in the select part of the request
# Arguments
# db - a table of the columns, tables and apps
# words - words of the already entered text of the request
# Output
# An array of columns in the request
# Boleean whether groupping is required
# An array of aggregated columns in the request
	ITEMS<-c("where","group","by","having","order","ascending","descending")
	AGGR<-c("total","average","count","min","max")
	ind<-which(words %in% ITEMS)
	if(length(ind)>0){
		if(min(ind)>1){
			selected_part<-words[1:(min(ind)-1)]
			group_required<-TRUE
		}else{
			selected_part<-words
			group_required<-FALSE
		}
	}else{
		nbr_cols<-length(db[tolower(db$Column) %in% words,'Column'])
		nbr_aggr<-length(intersect(words,AGGR))
		if(nbr_aggr > 0 & nbr_cols > nbr_aggr){
			group_required<-TRUE
		}else{
			group_required<-FALSE
		}
		selected_part<-words
	}
	aggr_ind<-which(selected_part %in% AGGR)
	if(length(aggr_ind)>0){
		aggr_ind_ext<-unique(append(aggr_ind,aggr_ind+1))
		aggr_part<-selected_part[aggr_ind_ext]
		aggr_cols<-unique(db[tolower(db$Column) %in% aggr_part,"Column"])
		selected<-selected_part[-aggr_ind_ext]
		cols<-unique(db[tolower(db$Column) %in% selected,"Column"])
	}else{
		cols<-unique(db[tolower(db$Column) %in% selected_part,"Column"])
		aggr_cols<-vector()
	}
	out<-list()
	out[[1]]<-cols
	out[[2]]<-group_required
	out[[3]]<-aggr_cols
	return(out)
}

next_keyword_select<-function(db,cols,last_word,group_required,feasible_tables){
# Determines the next word in the select part of the request
# Arguments
# db - database - a dataframe with columns, table and applications
# cols - a list of columns in the select part
# last_word - last word of the request
# group_required - boolean whether the groupping is required
# feasible_tables - a list of tables that can be joined together
# Output
# An array of possible next words
	AGGR<-c("total","average","count","min","max")
	if(last_word==""){
		# feasible columns or aggr
		next_word<-append(db$Column,AGGR)
	}else if(length(intersect(last_word,AGGR))>0){
		# only columns from feasible tables 
		next_word<-get_next_column(db,cols,feasible_tables)
		if(last_word=='count'){
			# when aggregate count only character columns used
			char_cols<-db[db$Table %in% names(feasible_tables) & db$Column %in% next_word & db$Datatype=='STRING','Column']
			next_word<-intersect(next_word,char_cols)
		}else{
			# when aggregate sum, average, min, max only numerical columns used
			char_cols<-db[db$Table %in% names(feasible_tables) & db$Column %in% next_word & db$Datatype=='NUMBER','Column']
			next_word<-intersect(next_word,char_cols)
		}
	}else{
		if(group_required){
			# only column from feasible tables or 'where','order'
			next_word<-append(get_next_column(db,cols,feasible_tables),c(AGGR,"where","group by","order by"))
		}else{
			# only column from feasible tables or 'where','order'
			next_word<-append(get_next_column(db,cols,feasible_tables),c(AGGR,"where","order by"))
		}
	}
	return(next_word)
}

next_keyword_where<-function(db,cols,last_word,prev_word,where_part_col,group_required,feasible_tables){
# Determines the next word in the where part of the request
# Arguments
# db - database - a dataframe with columns, table and applications
# cols - a list of columns in the select part
# last_word - last word of the request
# group_required - boolean whether the groupping is required
# feasible_tables - a list of tables that can be joined together
# Output
# An array of possible next words
	OPS<-c("<","<=",">",">=","<>","=")
	if(length(db[tolower(db$Column)==last_word,'Column'])>0 & !(prev_word %in% c('min','max'))){
		# next word operations
		next_word<-OPS
	}else if(length(intersect(OPS,last_word))>0){
		#next word value
		next_word<-'...'
		if(last_word == '=' & where_part_col != ''){
			if(db[db$Column==where_part_col,'Datatype']=='NUMBER'){
				next_word<-c(next_word,paste0("min ",where_part_col),paste0("max ",where_part_col))
			}
		}
	}else{
		if(group_required){
			# only column from feasible tables or 'where','order'
			next_word<-append(get_next_column(db,cols,feasible_tables),c("group by","order by"))
		}else{
			# only column from feasible tables or 'order'
			next_word<-append(get_next_column(db,cols,feasible_tables),c("order by"))
		}
	}
	return(next_word)
}

next_keyword_group<-function(cols,last_word,group_cols){
# Determines the next word in the group part of the request
# Arguments
# db - database - a dataframe with columns, table and applications
# cols - a list of columns in the select part
# last_word - last word of the request
# group_required - boolean whether the groupping is required
# feasible_tables - a list of tables that can be joined together
# Output
# An array of possible next words
	# if non aggregated column were in select they must be in group by
	remaining_cols<-cols[!(cols %in% group_cols)]
	if(length(remaining_cols)>0){
		next_word<-remaining_cols
	}else{
		next_word<-c("having","order by")
	}
	return(next_word)
}

next_keyword_having<-function(last_word,aggr_cols,having_cols){
# Determines the next word in the having part of the request
# Arguments
# last_word - last word of the request
# group_required - boolean whether the groupping is required
# aggr_cols - a list of aggregated columns in the select part
# having_cols - a list of columns in the having part 
# Output
# An array of possible next words
	AGGR<-c("total","average","count","min","max")
	OPS<-c("<","<=",">",">=","<>","=")
	
	remaining_cols<-aggr_cols[!(aggr_cols %in% having_cols)]
	if(length(intersect(last_word,AGGR))>0){
		next_word<-remaining_cols
		if(last_word=='count'){
			# when aggregate count only character columns used
			char_cols<-db[db$Table %in% names(feasible_tables) & db$Column %in% next_word & db$Datatype=='STRING','Column']
			next_word<-intersect(next_word,char_cols)
		}else{
			# when aggregate sum, average, min, max only numerical columns used
			char_cols<-db[db$Table %in% names(feasible_tables) & db$Column %in% next_word & db$Datatype=='NUMBER','Column']
			next_word<-intersect(next_word,char_cols)
		}
	}else if(length(intersect(last_word,tolower(aggr_cols)))>0){
		next_word<-OPS
	}else if(length(intersect(last_word,OPS))>0){
		next_word<-'...'
	}else{
		if(length(remaining_cols)>0){	
			next_word<-c(AGGR,'order by')
		}else{
			next_word<-'order by'
		}
	}
	return(next_word)
}

next_keyword_order<-function(cols,last_word,order_cols){
# Determines the next word in the order part of the request
# Arguments
# db - database - a dataframe with columns, table and applications
# cols - a list of columns in the select part
# last_word - last word of the request
# order_oart - a list of columns in the order part 
# Output
# An array of possible next words
	remaining_cols<-cols[!(cols %in% order_cols)]
	
	if(length(intersect(last_word,tolower(cols)))>0){
		next_word<-c("ascending","descending")
	}else{
		if(length(remaining_cols)>0){
			next_word<-remaining_cols
		}else{
			next_word<-vector()
		}
	}
	return(next_word)
}


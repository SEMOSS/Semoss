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

get_feasible_columns<-function(db,cols,root_fn="nldr"){
# Identifies clusters and their columns that contain all required columns
# Arguments
# cols - a list of columns in the select part
# root_fn - a root of file names with the required info
# Output
# a list of feasible columns or an error message is there is 
# not a single cluster with all required ones
	# read partition
	# Retrieve cluster info
	partition<-readRDS(paste0(root_fn,"_membership.rds"))
	clusters<-unique(unname(partition))
	feas_cols<-vector()
	if(length(clusters)>0){
		for(i in 1:length(clusters)){
			# make sure that all columns present in the cluster
			tbls<-names(partition[partition==clusters[i]])
			tbls_db<-db[db$Table %in% tbls,]
			tbls_cols<-tbls_db$Column
			names(tbls_cols)<-tbls_db$Datatype
			if(all(cols %in% unname(tbls_cols))){
				feas_cols<-append(feas_cols,tbls_cols)
			}
		}
	}
	feas_cols<-feas_cols[!duplicated(feas_cols)]
	feas_cols<-feas_cols[order(feas_cols)]
	gc()
	return(feas_cols)
}

analyze_request<-function(db,df,root_fn="nldr"){
	MISSING_VALUE<-'?'
	ind<-which(df$Value==MISSING_VALUE)
	if(length(ind)>0){
		nbr<-ind[1]
	}else{
		nbr<-0
	}
	if(nbr>0){
		# element alternatives
		choices<-get_element_alternatves(db,df,nbr,root_fn)
	}else{
		choices<-get_component_alternatives(df)
	}
	gc()
	return(choices)
}

get_element_alternatves<-function(db,df,nbr,root_fn){
	OPS<-c('=','<','<=','>','>=','!=','between value and value')
	OPS_STRING<-c('begins with','contains','ends with','not begins with','not contains','not ends with')
	OPS_DATE<-c('after','before')
	DATE_GROUPPING<-c('daily','weekly','monthly','yearly')
	
	MISSING_VALUE<-'?'
	
	selected_cols<-df[df$Element=='column' & df$Value != MISSING_VALUE,]$Value
	# cols is now a named array
	if(length(selected_cols)>0){
		cols<-get_feasible_columns(db,selected_cols,root_fn=root_fn)
	}else{
		cols<-db$Column
		names(cols)<-db$Datatype
		cols<-cols[!duplicated(cols)]
	}
	# element alternatives
	component<-df$Component[nbr]
	element<-df$Element[nbr]
	if(component %in% c('select','based on')){
		if(nbr==1){
			# single
			out<-unname(cols)
		}else if(df$Element[nbr-1]=='aggregate'){
			# aggregate column
			if(df$Value[nbr-1] %in% c('count','unique count')){
				out<-unname(cols[names(cols)=='STRING']) 
			}else if(df$Value[nbr-1]!='sum'){
				out<-unname(cols[names(cols) %in% c('DATE','NUMBER')])
			}else{
				out<-unname(cols[names(cols) %in% c('NUMBER')])
			}
		}else{
			# single column
			out<-unname(cols)
		}
	}else if(component=='where'){
		if(element=='column'){
			out<-cols
		}else if(element=='is'){
			column<-df$Value[nbr-1]
			type<-db[tolower(cols)==tolower(column),'Datatype']
			if(type=='STRING'){
				out<-c(OPS[1],OPS_STRING)
			}else if(type=='DATE'){
				out<-c(OPS[1],OPS[7],OPS_DATE)
			}else{
				out<-OPS[2:length(OPS)]
			}
		}else{
			# value
			out<-MISSING_VALUE
		}
	}else if(component=='group'){
		# get all string columns
		out<-unname(cols[names(cols) %in% c('STRING','DATE')])
		# get all date columns and append date groupping
		#date_cols<-unname(cols[names(cols)=='DATE'])
		#if(length(date_cols)>0){
		#	out<-append(out,c(date_cols,as.vector(sapply(date_cols,function(x) paste(x,DATE_GROUPPING,sep=' ')))))
		#}
		aggr_cols<-get_aliases(df)
		out<-append(out,aggr_cols)
	}else if(component=='having'){
		if(element=='column'){
			if(df$Value[nbr-1] %in% c('count','unique count')){
				out<-unname(cols[names(cols)=='STRING']) 
			}else{
				out<-unname(cols[names(cols) %in% c('DATE','NUMBER')])
			}
		}else if(element=='is'){
			out<-OPS
		}else{
			out<-MISSING_VALUE
		}
	}else if(component=='position'){
		if(element=='column'){
			aggr_cols<-get_aliases(df)
			out<-append(unname(cols),aggr_cols)
		}else{
			out<-MISSING_VALUE
		}
	}else if(component=='distribution'){
		if(element=='column'){
			# get all string columns
			out<-unname(cols[names(cols) %in% c('STRING','DATE')])
		}
	}else if(component=='sort'){
		single_cols<-get_single_cols(df)
		aggr_cols<-get_aliases(df)
		out<-append(single_cols,aggr_cols)
	}
	return(out)
}

get_single_cols<-function(df){
	ind<-which(df$Element=='aggregate')
	if(length(ind)>0){
		df<-df[-c(ind,ind+1),]
	}
	single_cols<-df[df$Component %in% c('select','position') & df$Element=='column','Value']
	return(single_cols)
}

get_component_alternatives<-function(df){
	COMPONENTS<-c('aggregate column','where column is value','top n column','bottom n column','- top n column','- bottom n column',
	'sort column direction','based on aggregate column','group column','having column is value')	
	REQUEST_COMPONENTS<-list('1'='select column','2'=c('select column','where column is value'),
	'3'=c('select column','aggregate column','group column'),'4'=c('top n column','based on aggregate column'),'5'=c('bottom n column','based on aggregate column'),
	'7'=c('- top n column','based on aggregate column'),'7'=c('- bottom n column','based on aggregate column'),'8'=c('distribution column','based on aggregate column'))
	if(nrow(df)==0){
		# for the first run only select components available
		out<-REQUEST_COMPONENTS
	}else{
		# where is always available
		out<-COMPONENTS[1:7]
		
		ind<-which(df$Component=='position')
		ind2<-which(df$Component=='based on')
		if(length(ind)>0 & length(ind2)==0){
			out<-append(out,COMPONENTS[8])
		}
		# if aggregate element present and there is no group or based on components
		# we can add group component
		ind<-which(df$Element=='aggregate')
		# if there are aggregates
		if(length(ind)>0){
			group_ind<-which(df$Component %in% c('group','based on'))
			if(length(group_ind)==0){
				out<-append(out,COMPONENTS[9])
			}
		}
		# if group or based on components present we can add having component
		group_ind<-which(df$Component %in% c('group','based on'))
		if(length(group_ind)>0){
			out<-append(out,COMPONENTS[10])
		}
	}
	return(out)
}
	
exec_componentized_query<-function(db,joins,request,root_fn="nldr"){
	COMPONENTS<-c('select','where','group','having','rank','sort')
	KEYWORDS<-c('sum','average','unique','count','min','max','stdev','where','group','by','having','sort','position','based','on')
	
	library(data.table)
	words<-unlist(strsplit(unname(request),' '))
	cols<-db[db$Column %in% words,"Column"]
	cols<-cols[!(tolower(cols) %in% KEYWORDS)]
	request<-preprocess_request(request,cols)
	if(!is.null(names(request))){
		p<-data.table(query=integer(),appid=character(),appid2=character(),part=character(),item1=character(),item2=character(),
		item3=character(),item4=character(),item5=character(),item6=character(),item7=character())
		# Identify relevant clusters for join
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
					
					components<-names(request)
					unique_components<-intersect(COMPONENTS,components)
					n<-length(unique_components)
					if(n > 0){
						for(i in 1:n){
							ind<-which(components==unique_components[i])
							for(j in 1:length(ind)){
								part<-unlist(strsplit(unname(sapply(request[ind[j]],function(x) paste(unlist(strsplit(x,' '))[-1],collapse=' '))),' '))
								if(unique_components[i]=='select'){
									p<-process_select(cur_db,part,cols,p,k)
									if(j==length(ind)){
										p<-process_from(cluster_joins,p,k)
									}
								}else if(unique_components[i]=='where'){
									p<-process_where(cur_db,part,cols,p,k)
								}else if(unique_components[i]=='group'){
									p<-process_group(cur_db,part,cols,p,k)
								}else if(unique_components[i]=='having'){
									p<-process_having(cur_db,part,cols,p,k)
								}else if(unique_components[i]=='rank'){
									# ger only current part of p
									all_cols<-get_all_cols(p[p$query==k,])
									p<-process_rank(cur_db,part,all_cols,p,k)
								}else if(unique_components[i]=='sort'){
									# ger only current part of p
									all_cols<-get_all_cols(p[p$query==k,])
									p<-process_sort(cur_db,part,all_cols,p,k)
								}
							}
						}
					}
					k<-k+1
				}
			}
		}
	}else{
		p<-request
	}
	gc()
	return(p)
}

preprocess_request<-function(request,cols){
	out<-vector()
	components<-names(request)
	n<-length(components)
	if(n>0){
		if('distribution' %in% components){
			ind<-which(components=='distribution')
			items<-unlist(strsplit(unname(request[ind]),' '))
			group_items<-paste(items[2:length(items)],collapse=' ')
			group<-paste0(c('group',group_items),collapse=' ')
			
			items<-unlist(strsplit(unname(request[ind+1]),' '))
			select_items<-paste(append(group_items,items[3:length(items)]),collapse=' ')
			select<-paste0('select ',select_items)
			
			request[ind]<-select
			request[ind+1]<-group
			names(request)[ind:(ind+1)]<-c('select','group')
			components<-names(request)
			n<-length(components)
		}
		if('position' %in% components | 'based on' %in% components){
			if('based on' %in% components){
				based_on<-TRUE
			}else{
				based_on<-FALSE
			}
			pos_ind<-0
			for(i in 1:n){
				if(components[i]=='position'){
					pos_ind<-i
					comp_words<-unlist(strsplit(unname(request[i]),' '))
					if(comp_words[2]=='-'){
						comp_words<-c(comp_words[1],comp_words[3],paste0('-',comp_words[4]),comp_words[5])
					}
					comp_cols<-intersect(comp_words,cols)
					if(based_on){
						position<-paste(comp_words[2:3],collapse=' ')
						if(length(comp_cols)>0){
							out<-append(out,setNames(c(paste0('select ',paste(comp_cols,collapse=' ')),paste0('group ',paste(comp_cols,collapse=' '))),c('select','group')))
						}
					}else{
						out<-append(out,setNames(c(paste0('select ',paste(comp_cols,collapse=' ')),paste0('rank ',comp_words[4],' ',paste(comp_words[2:3],collapse=' '))),c('select','rank')))
					}	
					
				}else if(components[i]=='based on'){
					if(pos_ind>0){
						comp_words<-unlist(strsplit(unname(request[i]),' '))
						comp_cols<-intersect(comp_words,cols)
						aggr_select<-paste0('select ',paste(comp_words[3:length(comp_words)],collapse=' '))
						comp_rank<-paste0('rank ',get_aggr_alias(comp_words[3:length(comp_words)]),' ',position)
						out<-append(out,setNames(c(aggr_select,comp_rank),c('select','rank')))
					}else{
						out<-'request format is incorrect'
						break
					}
				}else{
					out<-append(out,request[i])
				}
				
			}
		}else{
			out<-request
		}
	}
	gc()
	return(out)
}

get_all_cols<-function(p){
	cols<-p[p$part=='select' & p$item3=='',]$item2
	if(length(cols)>0){
		tbls<-paste0(p[p$part=='select' & p$item3=='',]$appid,'._.',p[p$part=='select' & p$item3=='',]$item1)
		out<-c(rbind(cols, tbls))
	}else{
		out<-vector()
	}

	cols<-p[p$part=='select' & p$item3!='',]$item4
	if(length(cols)>0){
		tbls<-paste0(p[p$part=='select' & p$item3!='',]$appid,'._.',p[p$part=='select' & p$item3!='',]$item1)
		out<-append(out,c(rbind(cols, tbls)))
	}
	return(out)
}

get_aliases<-function(df){
	library(tools)
	aliases<-vector()
	ind<-which(df$Component %in% c('select','based on') & df$Element=='aggregate')
	if(length(ind)>0){
		for(i in 1:length(ind)){
			if(df$Value[ind[i]] == 'unique count'){
				aliases<-append(aliases,paste0('UniqueCount_',df$Value[ind[i]+1]))
			}else{
				aliases<-append(aliases,paste0(tools::toTitleCase(df$Value[ind[i]]),'_',df$Value[ind[i]+1]))
			}
		}
	}
	return(aliases)
}

get_aggr_alias<-function(comp_text){
	library(tools)
	if(comp_text[1]=='unique' & comp_text[2]=='count'){
		aggr_alias<-paste0('UniqueCount_',comp_text[3])
	}else{
		aggr_alias<-paste0(tools::toTitleCase(comp_text[1]),'_',comp_text[2])
	}
	return(aggr_alias)
}















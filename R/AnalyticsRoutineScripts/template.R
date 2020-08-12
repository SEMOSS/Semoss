analyze_request<-function(db,df){
	MISSING_VALUE<-'?'
	ind<-which(df$Value==MISSING_VALUE)
	if(length(ind)>0){
		nbr<-ind[1]
	}else{
		nbr<-0
	}
	if(nbr>0){
		# element alternatives
		choices<-get_element_alternatves(db,df,nbr)
	}else{
		#if(nrow(df)>0){
		#	df<-adjust_request(df)
		#}
		# component alternatives
		choices<-get_component_alternatives(df)
	}
	#out<-list()
	#out[[1]]<-df
	#out[[2]]<-choices
	gc()
	return(choices)
}

adjust_request<-function(df){
	# if aggregate column is present in select then all single columns must be in group
	select_part<-df[df$Component=='select',]
	single_part<-df[0,]
	if(nrow(select_part)>0){
		ind<-which(df$Element=='aggregate')
		# check whether aggregate column present
		if(length(ind)>0){
			# check whether a single column present
			if(ind[1]>1){
				single_part<-select_part[1:(ind[1]-1),]
				group_candidate_part<-single_part
				group_candidate_part$Component<-'group'
				group_part<-df[df$Component=='group',]
				group_part<-unique(rbind(group_part,group_candidate_part))
				where_part<-df[df$Component=='where',]
				df<-do.call("rbind", list(select_part,where_part,group_part,df[df$Component=='having',]))
			}
		}else{
			# if aggregate not present remove groups and having
			df<-df[df$Component %in% c('select','where'),]
		}
	}
	# if column inserted in the group -> add it to the singles!!!
	group_part<-df[df$Component=='group',]
	if(nrow(group_part)>0){
		group_part$Component<-'select'
		df<-unique(rbind(group_part,df))
	}
	return(df)
}

get_element_alternatves<-function(db,df,nbr){
	OPS<-c('=','<','<=','>','>=','!=','between value and value')
	OPS_STRING<-c('begins with','contains','ends with','not begins with','not contains','not ends with')
	OPS_DATE<-c('after','before')
	DATE_GROUPPING<-c('daily','weekly','monthly','yearly')
	
	MISSING_VALUE<-'?'
	# element alternatives
	component<-df$Component[nbr]
	element<-df$Element[nbr]
	if(component=='select'){
		if(nbr==1){
			# single
			out<-unique(db$Column)
		}else if(df$Element[nbr-1]=='aggregate'){
			# aggregate column
			if(df$Value[nbr-1] %in% c('count','unique count')){
				out<-unique(db[db$Datatype=='STRING','Column'])
			}else if(df$Value[nbr-1]!='sum'){
				out<-unique(db[db$Datatype %in% c('DATE','NUMBER'),'Column'])
			}else{
				out<-unique(db[db$Datatype %in% c('NUMBER'),'Column'])
			}
		}else{
			# single column
			out<-unique(db$Column)
		}
	}else if(component=='where'){
		if(element=='column'){
			out<-unique(db$Column)
		}else if(element=='is'){
			column<-df$Value[nbr-1]
			type<-db[tolower(db$Column)==tolower(column),'Datatype']
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
		out<-unique(db[db$Datatype=='STRING','Column'])
		# get all date columns and append date groupping
		date_cols<-unique(db[db$Datatype=='DATE','Column'])
		if(length(date_cols)>0){
			out<-append(out,c(date_cols,as.vector(sapply(date_cols,function(x) paste(x,DATE_GROUPPING,sep=' ')))))
		}
	}else if(component=='having'){
		if(element=='column'){
			if(df$Value[nbr-1] %in% c('count','unique count')){
				out<-unique(db[db$Datatype=='STRING','Column'])
			}else{
				out<-unique(db[db$Datatype %in% c('DATE','NUMBER'),'Column'])
			}
		}else if(element=='is'){
			out<-OPS
		}else{
			out<-MISSING_VALUE
		}
	}else if(component=='rank'){
		if(element=='column'){
			single_cols<-get_single_cols(df)
			aggr_cols<-get_aliases(df)
			out<-append(single_cols,aggr_cols)
		}else{
			out<-MISSING_VALUE
		}
	}
	else if(component=='sort'){
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
	single_cols<-df[df$Component=='select' & df$Element=='column','Value']
	return(single_cols)
}

get_component_alternatives<-function(df){
	COMPONENTS<-c('select column','sum column','average column','min column','max column','stdev column','count column','unique column count','where column is value',
	'group column','based on column','having min column is value','having max column is value','having sum column is value','having average column is value',
	'having count column is value','having unique count column is value','rank column top n','rank column bottom n','sort column ascending','sort column descending')	
	REQUEST_COMPONENTS<-list('1'='select column','2'=c('select column','rank column position n'),'3'=c('select column','where column is value'),'4'=c('select column','where column is value',
	'rank column position n'),'5'=c('select column', 'aggregate column','based on column'),'6'=c('select column','aggregate column','based on column','rank column position n'),'7'=c('select column',
	'aggregate column','based on column','having column operation value'),'8'=c('select column','aggregate column','based on column','having column operation value','rank column position n'))
	if(nrow(df)==0){
		# for the first run only select components available
		out<-REQUEST_COMPONENTS
	}else{
		# where is always available
		out<-COMPONENTS[2:9]
		
		ind<-which(df$Element=='aggregate')
		# if there are aggregates
		if(length(ind)>0){
			group_ind<-which(df$Component=='group')
			if(length(group_ind)>0){
				# when aggregate element is present in select component then having is avaible
				# if group component already present it is not included for the next component list
				out<-append(out,COMPONENTS[12:21])
			}else{
				out<-append(out,COMPONENTS[10:21])
			}
		}else{
			out<-append(out,COMPONENTS[18:21])
		}
	}
	return(out)
}


exec_componentized_query<-function(db,joins,request){
	COMPONENTS<-c('select','where','group','having','rank','sort')
	KEYWORDS<-c('sum','average','unique','count','min','max','stdev','where','group','by','having','sort','rank')
	
	library(data.table)
	p<-data.table(query=integer(),appid=character(),appid2=character(),part=character(),item1=character(),item2=character(),
	item3=character(),item4=character(),item5=character(),item6=character(),item7=character())
	words<-unlist(strsplit(unname(request),' '))
	cols<-db[db$Column %in% words,"Column"]
	cols<-cols[!(tolower(cols) %in% KEYWORDS)]
	cluster_joins<-build_joins(cols,joins,db)
	components<-names(request)
	unique_components<-intersect(COMPONENTS,components)
	n<-length(unique_components)
	if(n > 0){
		for(i in 1:n){
			ind<-which(components==unique_components[i])
			part<-unlist(strsplit(unname(sapply(request[ind],function(x) paste(unlist(strsplit(x,' '))[-1],collapse=' '))),' '))
			if(unique_components[i]=='select'){
				p<-process_select(db,part,cols,p,1)
				p<-process_from(cluster_joins,p,1)
			}else if(unique_components[i]=='where'){
				p<-process_where(db,part,cols,p,1)
			}else if(unique_components[i]=='group'){
				p<-process_group(db,part,cols,p,1)
			}else if(unique_components[i]=='having'){
				p<-process_having(db,part,cols,p,1)
			}else if(unique_components[i]=='rank'){
				all_cols<-get_all_cols(p)
				p<-process_rank(db,part,all_cols,p,1)
			}else if(unique_components[i]=='sort'){
				all_cols<-get_all_cols(p)
				p<-process_sort(db,part,all_cols,p,1)
			}
		}
	}
	gc()
	return(p)
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
	ind<-which(df$Component=='select' & df$Element=='aggregate')
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






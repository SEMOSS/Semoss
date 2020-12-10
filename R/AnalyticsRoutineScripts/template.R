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
		choices<-get_component_alternatives(df)
	}
	gc()
	return(choices)
}

get_element_alternatves<-function(db,df,nbr){
	OPS<-c('=','<','<=','>','>=','!=','between value and value')
	OPS_STRING<-c('begins with','contains','ends with','not begins with','not contains','not ends with')
	OPS_DATE<-c('after','before')
	DATE_GROUPPING<-c('dayname','week','monthname','quarter','year')
	
	MISSING_VALUE<-'?'
	
	# element alternatives
	component<-df$Component[nbr]
	element<-df$Element[nbr]
	if(component %in% c('select','based on')){
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
				out<-c(OPS[1],OPS[6],OPS_STRING)
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
		out<-unique(db[db$Datatype == 'STRING','Column'])
		# date columns
		date_cols<-unique(db[db$Datatype == 'DATE','Column'])
		if(length(date_cols)>0){
			out<-append(out,c(date_cols,as.vector(sapply(date_cols,function(x) paste(DATE_GROUPPING,x,sep=' ')))))
		}
		aggr_cols<-get_aliases(df)
		out<-append(out,aggr_cols)
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
	}else if(component=='position'){
		if(element=='column'){
			aggr_cols<-get_aliases(df)
			out<-append(db$Column,aggr_cols)
		}else{
			out<-MISSING_VALUE
		}
	}else if(component=='distribution'){
		if(element=='column'){
			# get all string & date columns
			out<-unique(db[db$Datatype %in% c('STRING','DATE'),'Column'])
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
	REQUEST_COMPONENTS<-list('1'='select column','2'=c('select column','where column is value'),'3'=c('aggregate column','group column'),
	'4'=c('select column','aggregate column','group column'),'5'=c('top n column','based on aggregate column'),'6'=c('bottom n column','based on aggregate column'),
	'7'=c('- top n column','based on aggregate column'),'8'=c('- bottom n column','based on aggregate column'),'9'=c('distribution column','based on aggregate column'))
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


# updates to the function below
# what is the the group components should be in the select
# in the rank should be aggregate column

exec_componentized_query<-function(db,joins,request){
	COMPONENTS<-c('select','where','group','having','rank','sort')
	KEYWORDS<-c('fsum','faverage','fcount','fmin','fmax','fstdev','where','fgroup','by','having','sort','position','based','on')
	
	library(data.table)
	words<-unlist(strsplit(unname(request),' '))
	cols<-db[db$Column %in% words,"Column"]
	cols<-cols[!(tolower(cols) %in% KEYWORDS)]
	request<-preprocess_request(request,cols)
	if(!is.null(names(request))){
		p<-data.table(query=integer(),appid=character(),appid2=character(),part=character(),item1=character(),item2=character(),
		item3=character(),item4=character(),item5=character(),item6=character(),item7=character())
		cluster_joins<-build_joins(cols,joins,db)
		components<-names(request)
		unique_components<-intersect(COMPONENTS,components)
		n<-length(unique_components)
		if(n > 0){
			for(i in 1:n){
				ind<-which(components==unique_components[i])
				for(j in 1:length(ind)){
					part<-unlist(strsplit(unname(sapply(request[ind[j]],function(x) paste(unlist(strsplit(x,' '))[-1],collapse=' '))),' '))
					if(unique_components[i]=='select'){
						p<-process_select(db,part,cols,p,1)
						if(j==length(ind)){
							p<-process_from(cluster_joins,p,1)
						}
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
		}
	}else{
		p<-request
	}
	gc()
	return(p)
}

preprocess_request<-function(request,cols){
	DATE_GROUPPING<-c('fdayname','fweek','fmonthname','fquarter','fyear')
	
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
	comp_text[1]<-substring(comp_text[1],2)
	if(comp_text[1]=='unique' & comp_text[2]=='count'){
		aggr_alias<-paste0('UniqueCount_',comp_text[3])
	}else{
		aggr_alias<-paste0(tools::toTitleCase(comp_text[1]),'_',comp_text[2])
	}
	return(aggr_alias)
}















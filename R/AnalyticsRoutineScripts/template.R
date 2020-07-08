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
	OPS<-c('=','<','<=','>','>=','<>')
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
			if(df$Value[nbr-1]=='count'){
				out<-unique(db[db$Datatype=='STRING','Column'])
			}else{
				out<-unique(db[db$Datatype %in% c('DATE','NUMBER'),'Column'])
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
				out<-OPS[1]
			}else{
				out<-OPS[2:length(OPS)]
			}
		}else{
			# value
			out<-MISSING_VALUE
		}
	}else if(component=='group'){
		out<-unique(db[db$Datatype=='STRING','Column'])
	}else if(component=='having'){
		if(element=='column'){
			if(df$Value[nbr-1]=='count'){
				out<-unique(db[db$Datatype=='STRING','Column'])
			}else{
				out<-unique(db[db$Datatype %in% c('DATE','NUMBER'),'Column'])
			}
		}else if(element=='is'){
			out<-OPS
		}else{
			out<-MISSING_VALUE
		}
	}
	return(out)
}

get_component_alternatives<-function(df){
	COMPONENTS<-c('select column','sum column','average column','min column','max column','count column','where column is value',
	'group column','having min column is value','having max column is value','having sum column is value',
	'having average column is value','having count column is value')
	if(nrow(df)==0){
		# for the first run only select components available
		out<-COMPONENTS[1:6]
	}else{
		# where is always available
		out<-COMPONENTS[2:7]
		
		ind<-which(df$Element=='aggregate')
		if(length(ind)>0){
			# when aggregate element is present in select group and having avaible
			out<-append(out,COMPONENTS[8:13])
		}
	}
	return(out)
}

exec_componentized_query<-function(db,joins,request){
	COMPONENTS<-c('select','where','group','having')
	KEYWORDS<-c("sum","average","count","min","max",'where','group','by','having','order')
	
	library(data.table)
	p<-data.table(query=integer(),appid=character(),appid2=character(),part=character(),item1=character(),item2=character(),
	item3=character(),item4=character(),item5=character(),item6=character(),item7=character())
	words<-unlist(strsplit(unname(request),' '))
	cols<-db[db$Column %in% words,"Column"]
	cols<-cols[!(tolower(cols) %in% KEYWORDS)]
	cluster_joins<-build_joins(cols,joins,db)
	for(i in 1:length(COMPONENTS)){
		ind<-which(names(request)==COMPONENTS[i])
		if(length(ind)>0){
			part<-unlist(strsplit(unname(sapply(request[ind],function(x) paste(unlist(strsplit(x,' '))[-1],collapse=' '))),' '))
			if(COMPONENTS[i]=='select'){
				p<-process_select(db,part,cols,p,1)
				p<-process_from(cluster_joins,p,1)
			}else if(COMPONENTS[i]=='where'){
				p<-process_where(db,part,cols,p,1)
			}else if(COMPONENTS[i]=='group'){
				p<-process_group(db,part,cols,p,1)
			}else if(COMPONENTS[i]=='having'){
				p<-process_having(db,part,cols,p,1)
			}
		}
	}
	gc()
	return(p)
}








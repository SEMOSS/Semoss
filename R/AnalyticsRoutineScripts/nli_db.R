nliapp_mgr<-function(txt,db,joins=data.frame(),filename="unique-values-table.rds",refine=TRUE){
	library(data.table)
	library(plyr)
	library(udpipe)
	library(stringdist)
	library(igraph)
	
	r<-data.table(Response=character(),AppID=character(),Statement=character())
	p<-data.table(appid=character(),part=character(),item1=character(),item2=character(),item3=character(),item4=character(),item5=character(),item6=character(),item7=character())
	# identify multip words column names
	txt<-db_match_extension(txt,db$Column)
	df_in<-parse_question_mgr(txt)
	# get all appids used in the request
	# refinement is needed if it is desired and there are more than 1 app
	refine<-refine & (length(unique(db$AppID))>1)
	if(refine){
		refined_df<-filter_apps(df_in,db)
		apps<-refined_df[[1]]
		db<-refined_df[[2]]
	}else{
		apps<-unique(db$AppID)
	}
	apps<-unique(db$AppID)
	N<-length(apps)
	if(N>0){
		if(file.exists(filename)){
			db_values<-readRDS(filename)
		}else{
			db_values<-data.frame(AppID=character(),Table=character(),Column=character(),Values=character(),stringsAsFactors=FALSE)
		}
		for(i in 1:N){	
			cur_db<-db[db$AppID==apps[i],]
			cur_values<-db_values[db_values$AppID==apps[i],]
			if(nrow(joins)!=0){
				cur_joins<-joins[joins$AppID==apps[i],1:4]
			}else{
				cur_joins<-joins
			}
			
			#####################################################################################################
			# Prepare request for current db
			df<-parse_request(df_in,cur_db,refine)
			
			# get columns used in the request
			cols<-as.character(df[df$itemtype == "column","item"])
			# If no columns present skip this app
			if(length(cols)>0){
			
				# get from clause (joins)
				# If required tables not found or not connected move to the next app
				out<-join_clause_mgr(cols,cur_db,cur_joins)
				if(out[[1]]!=""){
					sql<-out[[1]]
					response<-"Error"
					next
				}else{
					from_clause<-out[[2]]
					from_joins<-unique(out[[3]])
					request_tbls<-unique(c(from_joins$tbl1,from_joins$tbl2))
					request_tbls<-request_tbls[request_tbls!=""]
				}
			
				# Start constructing the query object
				df$processed<-"no"
				out<-get_start(df)
				if(!is.null(out[[1]]) & !is.null(out[[2]])){
					select_part<-get_select(out[[1]])
					mypart<-get_where(out[[2]],request_tbls,cur_joins,cur_values)
					if(length(mypart[[5]])>0){
						cols<-append(cols,mypart[[5]])
						out<-join_clause_mgr(cols,cur_db,cur_joins)
						if(out[[1]]==""){
							from_clause<-out[[2]]
							from_joins<-unique(out[[3]])
							pixel_from<-build_pixel_from(from_joins)
							request_tbls<-unique(c(from_joins$tbl1,from_joins$tbl2))
							request_tbls<-request_tbls[request_tbls!=""]
							response<-"SQL"
						}else{
							sql<-out[[1]]
							response<-"Error"
							next
						}
					}else{
						pixel_from<-build_pixel_from(from_joins)
						request_tbls<-unique(c(from_joins$tbl1,from_joins$tbl2))
						request_tbls<-request_tbls[request_tbls!=""]
						response<-"SQL"
					}
					
					
					where_part<-mypart[[1]]
					having_part<-mypart[[3]]
					mypart1<-validate_select(select_part,mypart[[2]])
					# append misfits from having clause
					select_aggr<-append(mypart1[[1]],mypart[[4]])
					select_part<-mypart1[[2]]
					group_part<-mypart1[[3]]
					# if groupping present then all non aggregate select should be in groups
					# aggregates from having clause place in the select to make the results easier to understand
					if(length(having_part)>0){
						group_part<-unique(append(group_part,select_part))
						select_aggr<-select_having(having_part)
					}
					# add to select section all group columns if they are not there
					if(length(group_part)>0){
						select_part<-unique(append(group_part,select_part))
					}			
					
					# add where columns into select section
					if(length(where_part)>0){
						# if we do not have aggregate columns we can add columns from where to the select section
						if(length(select_aggr)==0){
							select_part<-unique(append(select_part,select_where(where_part,request_tbls,cur_db)))
						}
					}
					pixel_where<-build_pixel_where(where_part,request_tbls,cur_db)
					pixel_group<-build_pixel_group(group_part,request_tbls,cur_db)
					
					select_aggr<-get_alias(select_aggr)
					pixel_aggr_select<-build_pixel_aggr_select(select_aggr,request_tbls,cur_db)
					
					pixel_single_select<-build_pixel_single_select(select_part,request_tbls,cur_db)
					# Initially it is an empty clause
					pixel_having<-build_pixel_having(having_part,request_tbls,cur_db)
					
					# complete building sql nad pixel objects
					if(response == "SQL"){
						sql<-"Correct sql"
						pixel<-build_pixel(apps[i],pixel_aggr_select,pixel_single_select,pixel_where,pixel_group,pixel_having,pixel_from)
						p<-rbind(p,pixel)
					}
					r<-rbindlist(list(r,list(response,as.character(apps[i]),sql)))
				}
			}
		}
	}else{
		r<-rbindlist(list(r,list("Error","","Rephrase the request: no applicable databases found")))
	}
	# keep only query object if at least one of the results is proper
	if(nrow(r[r$Response == "SQL",])>0){
		#r<-r[r$Response == "SQL",]
		myapps<-r$AppID
		p<-validate_pixel(p,myapps)
	}else{
		p<-data.table(appid=character(),part=character(),item1=character(),item2=character(),item3=character(),item4=character(),item5=character(),item6=character(),item7=character())
		n<-nrow(r)
		if(n>0){
			for(i in 1:n){
				p<-rbindlist(list(p,list(r$AppID[i],r$Response[i],r$Statement[i],"","","","","","")))
			}
		}
	}
	gc()
	# for debugging purposes only!!!
	write.csv(p,file="query_object.csv")
	return(p)
}

get_max_seq<-function(idx){
	mydif<-diff(idx)
	if(length(mydif)==1){
		if(mydif[1]==1){
			myidx=idx
		}else{
			myidx<-0
		}
	}else{
		mydif[mydif>1]<-0
		x<-which(mydif==0)
		y<-0
		y<-append(y,x)
		y[length(y)+1]<-length(mydif)+1
		z<-diff(y)
		p<-which(z==max(z))[1]
		myidx<-idx[y[p]+1]:idx[y[p+1]]
	}
	return(myidx)
}

db_match_extension<-function(txt,cols){
	words<-unlist(strsplit(txt," "))
	out<-words
	n<-length(words)
	library(plyr)
	if(n>0){
		df<-data.frame(word=character(),idx=numeric(),stringsAsFactors=FALSE)
		for(i in 1:n){
			# skip words in single quotes
			if(substr(words[i],1,1)!="'" & substr(words[i],nchar(words[i]),nchar(words[i]))!="'"){
				word_idx<-grep(tolower(words[i]),tolower(cols))
				if(length(word_idx>0)){
					mydf<-data.frame(idx=word_idx,stringsAsFactors=FALSE)
					mydf$word<-words[i]
					df<-rbind(df,mydf)
				}
			}
		}
		x<-count(df,"idx")
		x<-x[x$freq>1,]
		if(nrow(x)>0){
			candidates<-cols[x$idx]
			n<-length(candidates)
			for(i in 1:n){
				singles<-unique(df[df$idx %in% x$idx[i],]$word)
				seq_idx<-which(words %in% singles)
				seq_idx<-seq_idx[order(seq_idx)]
				seq_idx<-get_max_seq(seq_idx)
				if(length(seq_idx)>1){
					m<-length(seq_idx)
					candidate<-tolower(candidates[i])
					for(j in seq_idx){	
						candidate<-gsub(tolower(words[j]),"",candidate)
					}
					candidate<-gsub("[[:punct:]]","",candidate)
					if(nchar(candidate)==0){
						# Found multiple words column
						out[seq_idx]<-""
						out[seq_idx[1]]<-candidates[i]
					}
				}
			}
		}
	}
	out<-out[out!=""]
	mytxt<-paste(out,collapse=" ")
	gc()
	return(mytxt)
}

validate_pixel<-function(p,myapps){
	n<-length(myapps)
	err<-data.table(appid=character(),part=character(),item1=character(),item2=character(),item3=character(),item4=character(),item5=character(),item6=character(),item7=character())
	for(i in 1:n){
		app<-myapps[i]
		app_p<-p[p$appid == app,]
		if(nrow(app_p[app_p$part == "select",])==0 | nrow(app_p[app_p$part == "from",])==0){
			err<-rbindlist(list(err,list(app,"Error","Rephrase the request: no applicable databases found","","","","","","")))
			p<-p[p$appid!=app,]
		}
	}
	if(nrow(p)==0){
		p<-err
	}
	return(p)
}

get_alias<-function(items){
	n<-length(items)
	repl<-vector()
	if(n>0){
		for(i in 1:n){
			pos1<-unlist(gregexpr(pattern="[(]",items[i]))
			if(length(pos1)>0){
				pos2<-unlist(gregexpr(pattern="[)]",items[i]))
				if(length(pos2)>0){
					repl[i]<-paste0(items[i]," as ",substr(items[i],1,pos1-1),"_of_",substr(items[i],pos1+1,pos2-1))
				}
			}
		}
	}
	return(repl)
}

select_having<-function(having_part){
	items<-unlist(strsplit(having_part," "))
	ind<-which(tolower(substr(items,1,3)) %in% c("uni","sum","max","min","avg"))
	return(items[ind])
}

select_where<-function(where_part,request_tbls,cur_db){
	items<-vector()
	n<-length(where_part)
	for(i in 1:n){
		x<-trim(unlist(strsplit(where_part[i]," ")))
		if(length(x)==3){
			items<-append(items,select_where_helper(x[1],request_tbls,cur_db))
			items<-append(items,select_where_helper(x[3],request_tbls,cur_db))		
		}else if(length(x)==5){
			items<-append(items,select_where_helper(x[1],request_tbls,cur_db))
			items<-append(items,select_where_helper(x[3],request_tbls,cur_db))		
			items<-append(items,select_where_helper(x[5],request_tbls,cur_db))	
		}
	}
	gc()
	return(items)
}

select_where_helper<-function(item,request_tbls,cur_db){
	selected<-vector()
	tbls<-cur_db[tolower(cur_db$Column) == tolower(item) & tolower(cur_db$Table) %in% tolower(request_tbls),"Table"]
	if(length(tbls)>0){
		selected<-item
	}
	return(selected)
}

validate_select<-function(select_part,group_part){
	ind<-which(tolower(substr(select_part,1,3)) %in% c("uni","sum","max","min","avg"))
	if(length(ind)>0){
		select_aggr<-select_part[select_part %in% select_part[ind]]
		select_group<-select_part[!(select_part %in% select_part[ind])]
		select_part<-select_group
		if(length(select_group)>0){
			group_part<-unique(append(group_part,select_group))
		}
	}else{
		select_aggr<-vector()
		if(length(group_part)>0){
			select_part<-unique(append(select_part,group_part))
			group_part<-vector()
		}
	}
	myList<-list()
	myList[[1]]<-select_aggr
	myList[[2]]<-select_part
	myList[[3]]<-group_part
	gc()
	return(myList)
}


get_conj<-function(parsed_df,kids){
	conj<-vector()
	kids2<-kids[kids$dep_rel=="conj" & kids$itemtype=="column",]
	if(nrow(kids2)>0){
		for(i in 1:nrow(kids2)){
			if(nrow(parsed_df[parsed_df$head_token_id==kids2$token_id[i] & tolower(parsed_df$token)=="and",])>0){
				if(i==1){
					conj<-kids2$token_id[i]
				}else{
					conj<-append(conj,kids2$token_id[i])
				}	
			}
		}
	}
	return(as.integer(conj))
}

get_start<-function(parsed_df){
	out<-list()
	out[[1]]<-NULL
	out[[2]]<-NULL
	root<-parsed_df[parsed_df$head_token_id==0,]
	if(root$itemtype=="column"){
		# Identify possible select nodes
		kids<-parsed_df[parsed_df$head_token_id==root$token_id,]
		select_tree<-kids[!(kids$dep_rel %in% c("nmod","obj","acl","obl","conj")) & kids$xpos != "JJR",]
		# additional nodes using conjunction
		r<-get_conj(parsed_df,kids)
		if(nrow(select_tree)>0){
			r<-append(as.integer(select_tree$token_id),r)
		}
		out[[1]]<-rbind(root,get_subtree(parsed_df,r,"nmod"))
		where_tree<-parsed_df[!(parsed_df$token_id %in% out[[1]]$token_id),]	
		out[[2]]<-where_tree
	}else{
		kids<-parsed_df[parsed_df$head_token_id==root$token_id,]
		
		nsubj<-kids[substr(kids$dep_rel,1,5)=="nsubj" & kids$itemtype=="column",]
		if(nrow(nsubj)==0){
			nsubj<-kids[substr(kids$dep_rel,1,5)!="obj" & kids$itemtype=="column",]
		}
		obj<-kids[!(kids$token_id %in% nsubj$token_id),]
		if(nrow(nsubj)>0){
			# Identify possible select nodes
			out[[1]]<-get_subtree(parsed_df,as.integer(nsubj$token_id))
		}
		if(nrow(obj)>0){
			# Identify possible select nodes
			out[[2]]<-get_subtree(parsed_df,as.integer(obj$token_id))
		}
	}
	return(out)
}

get_select<-function(select_df){
	select_part<-vector()
	select_df$processed="no"
	token_id<-select_df[select_df$itemtype=="column",]$token_id
	if(length(token_id)>0){
		for(i in 1:length(token_id)){
			aggr_df<-select_df[select_df$head_token_id==token_id[i] & !(select_df$token_id %in% token_id) & tolower(select_df$token) %in% c("total","average","highest","lowest","best","worst","many") & select_df$processed=="no",]
			if(nrow(aggr_df)>0){
				count_df<-select_df[select_df$head_token_id==token_id[i] & !(select_df$token_id %in% token_id) & tolower(select_df$token) %in% c("many") & select_df$processed=="no",]
				if(nrow(count_df)>0){
					if(tolower(count_df$token[1]) == "many"){
						child<-select_df[select_df$head_token_id==count_df$token_id[1] & !(select_df$token_id %in% token_id) & select_df$processed=="no",]
						if(nrow(child)>0){
							select_part<-append(select_part,map_aggr(aggr_df$token[1],select_df[select_df$token_id==token_id[i],]$item))
							select_df[select_df$token_id==count_df$token_id[1],"processed"]<-"yes"
							select_df[select_df$token_id==token_id[i],"processed"]<-"yes"
							select_df[select_df$token_id==child$token_id[1],"processed"]<-"yes"
						}
					}
				}else{
					select_part<-append(select_part,map_aggr(aggr_df$token[1],select_df[select_df$token_id==token_id[i],]$item))
					select_df[select_df$token_id==aggr_df$token_id[1],"processed"]<-"yes"
					select_df[select_df$token_id==token_id[i],"processed"]<-"yes"
				}
			}else{	
				aggr_df<-select_df[select_df$head_token_id==select_df$head_token_id[as.integer(token_id[i])] & !(select_df$token_id %in% token_id) & tolower(select_df$token) %in% c("total","average","highest","lowest","best","worst") & select_df$processed=="no",]
				if(nrow(aggr_df)>0){
					select_part<-append(select_part,map_aggr(aggr_df$token[1],select_df[select_df$token_id==token_id[i],]$item))
					select_df[select_df$token_id==aggr_df$token_id[1],"processed"]<-"yes"
					select_df[select_df$token_id==token_id[i],"processed"]<-"yes"
				}else{
					select_part<-append(select_part,select_df[select_df$token_id==token_id[i],]$item)
					select_df[select_df$token_id==token_id[i],"processed"]<-"yes"
				}
			}
		}
	}
	gc()
	return(select_part)
}

translate_token<-function(token,discourse){
	if(tolower(token) %in% c("greater","higher","larger")){
		if(discourse){
			oper<-" <= "
		}else{
			oper<-" > "
		}
	}else if(tolower(token) %in% c("less","lower","smaller")){
		if(discourse){
			oper<-" >= "
		}else{
			oper<-" < "
		}
	}else if(tolower(token) %in% c("equals","equal")){
		if(discourse){
			oper<-" <> "
		}else{
			oper<-" = "
		}
	}else{
		oper<-""
	}
	return(oper)
}

get_having<-function(where_df,root){
	# We get here if root is obj and processed column is set to "no"
	having_part<-vector()
	select_aggr<-vector()
	if(root$itemtype=="column"){
		kids<-where_df[where_df$head_token_id==root$token_id & where_df$itemtype=="" & where_df$itemtype=="" & where_df$processed=="no",]
		if(nrow(kids)>0){
			for(i in 1:nrow(kids)){
				kids<-where_df[where_df$head_token_id==root$token_id & where_df$itemtype=="" & where_df$itemtype=="" & where_df$processed=="no",]
				if(nrow(kids)>0){
					kid=kids[1,]
				}else{
					break
				}
				kid1<-kid[tolower(kid$token) %in% c("total","average"),]
				if(nrow(kid1)>0){
					kid2<-kids[tolower(kids$token) %in% c("greater","higher","larger","less","lower","smaller","equals","equal"),]
					if(nrow(kid2)>0){
						# regular where
						oper<-translate_token(kid2$token[1],discourse=FALSE)
						if(oper!=""){
							grandchild<-where_df[where_df$head_token_id==kid2$token_id[1] & where_df$dep_rel=="obl" & where_df$xpos=="CD" & where_df$processed=="no",]
							if(nrow(grandchild)>0){
								having_part<-append(having_part,paste0(map_aggr(kid1$token[1],root$item),oper,grandchild$token[1]))
								where_df[where_df$token_id==kid2$token_id[1],"processed"]<-"yes"
								where_df[where_df$token_id==kid1$token_id[1],"processed"]<-"yes"
								where_df[where_df$token_id==grandchild$token_id[1],"processed"]<-"yes"
								where_df[where_df$token_id==root$token_id,"processed"]<-"yes"
							}else{
								grandchild<-where_df[where_df$head_token_id==kid2$token_id[1] & where_df$dep_rel=="obl" & where_df$itemtype=="column" & where_df$processed=="no",]
								if(nrow(grandchild)>0){
									greatgrandchild<-where_df[where_df$head_token_id==grandchild$token_id[1] & tolower(where_df$token) %in% c("total","average") & where_df$processed=="no",]
									if(nrow(greatgrandchild)>0){
										having_part<-append(having_part,paste0(map_aggr(kid1$token[1],root$item),oper,map_aggr(greatgrandchild$token[1],grandchild$item[1])))
										where_df[where_df$token_id==kid2$token_id[1],"processed"]<-"yes"
										where_df[where_df$token_id==kid1$token_id[1],"processed"]<-"yes"
										where_df[where_df$token_id==grandchild$token_id[1],"processed"]<-"yes"
										where_df[where_df$token_id==greatgrandchild$token_id[1],"processed"]<-"yes"
										where_df[where_df$token_id==root$token_id,"processed"]<-"yes"
									}else{
										having_part<-append(having_part,paste0(map_aggr(kid1$token[1],root$item),oper,grandchild$item[1]))
										having_part<-append(having_part,paste0(map_aggr(kid1$token[1],root$item),oper,map_aggr(greatgrandchild$token[1],grandchild$item[1])))
										where_df[where_df$token_id==kid2$token_id[1],"processed"]<-"yes"
										where_df[where_df$token_id==kid1$token_id[1],"processed"]<-"yes"
										where_df[where_df$token_id==grandchild$token_id[j],"processed"]<-"yes"
										where_df[where_df$token_id==root$token_id,"processed"]<-"yes"
									}
								}
							}
						}
					}else{
						# add aggregate function to select_aggr
						select_aggr<-append(select_aggr,map_aggr(kid1$token[1],root$item))
						where_df[where_df$token_id==kid1$token_id[1],"processed"]<-"yes"
						where_df[where_df$token_id==root$token_id,"processed"]<-"yes"		
					}
				}else{
					# Temporary assignment to skip processing
					where_df[where_df$token_id==kid$token_id,"processed"]<-"maybe"
				}				
			}
		}
	}else if(root$itemtype==""){
		if(tolower(root$token) %in% c("number","count")){
			kids<-where_df[where_df$head_token_id==root$token_id & where_df$itemtype=="column" & where_df$dep_rel %in% c("nmod") & where_df$processed=="no",]
			if(nrow(kids)>0){
				for(i in nrow(kids)){
					grandchild<-where_df[where_df$head_token_id==kids$token_id[i] & tolower(where_df$token) %in% c("greater","higher","larger","less","lower","smaller","equals","equal"),]
					if(nrow(grandchild)>0){
						oper<-translate_token(grandchild$token[1],discourse=FALSE)
						greatgrandchild<-where_df[where_df$head_token_id==grandchild$token_id[1] & where_df$dep_rel=="obl" & where_df$xpos=="CD" & where_df$processed=="no",]
						if(nrow(greatgrandchild)>0){
							having_part<-append(having_part,paste0("UniqueCount(",kids$item[i],")",oper,greatgrandchild$token[1]))
							where_df[where_df$token_id==kids$token_id[i],"processed"]<-"yes"
							where_df[where_df$token_id==grandchild$token_id[1],"processed"]<-"yes"
							where_df[where_df$token_id==greatgrandchild$token_id[1],"processed"]<-"yes"
							where_df[where_df$token_id==root$token_id,"processed"]<-"yes"
						}
					}else{
						sibling<-where_df[where_df$head_token_id==root$head_token_id[i] & tolower(where_df$token) %in% c("greater","higher","larger","less","lower","smaller","equals","equal"),]
						if(nrow(sibling)>0){
							oper<-translate_token(sibling$token[1],discourse=FALSE)
							child<-where_df[where_df$head_token_id==sibling$token_id[1] & where_df$dep_rel=="obl" & where_df$xpos=="CD" & where_df$processed=="no",]
							if(nrow(child)>0){
								having_part<-append(having_part,paste0("UniqueCount(",kids$item[i],")",oper,child$token[1]))
								where_df[where_df$token_id==kids$token_id[i],"processed"]<-"yes"
								where_df[where_df$token_id==sibling$token_id[1],"processed"]<-"yes"
								where_df[where_df$token_id==child$token_id[1],"processed"]<-"yes"
								where_df[where_df$token_id==root$token_id,"processed"]<-"yes"
							}
						}
					}
				}
			}
		}
		
	}
	# Restore unprocessed nodes
	where_df[where_df$processed=="maybe","processed"]<-"no"
	myList<-list()
	myList[[1]]<-where_df
	myList[[2]]<-having_part
	myList[[3]]<-select_aggr
	gc()
	return(myList)
}

validate_like<-function(token){
	# Until wildcard implemented for RDF and other databases
	##if(grepl("%",token,fixed=TRUE) | grepl("_",token,fixed=TRUE))
	if(grepl("%",token,fixed=TRUE)){
		right_part<-paste0(" ?like ",token)
	}else{
		right_part<-paste0(" = ",token)
	}
	return(right_part)
}

expose_column<-function(tbls,joins,value,cur_values,threshold=0.9){
	library(igraph)
	library(stringdist)
	new_col<-vector()
	matches<-stringsim(tolower(cur_values$Value),tolower(value),method='jw', p=0.1)
	ind<-which(matches>=threshold & matches==max(matches))
	col_rows <-unique(cur_values[ind,])
	if(nrow(col_rows)==1){
		new_col[1]<-col_rows$Column[1]
		new_col[2]<-col_rows$Value[1]
	}else if(nrow(col_rows)>1){
		new_tbls<-unique(col_rows$Table)
		if(length(new_tbls)>1){
			g<-graph_from_edgelist(as.matrix(joins[,1:2]),directed=FALSE)
			distances<-distances(g,v=new_tbls,to=tbls)
			d<-apply(distances, 1, function(x) min(x[x!=0]) )
			my_row<-which(d==min(d) & is.finite(d))
			if(length(my_row)>0){
				new_tbl<-new_tbls[my_row[1]]
				new_col[1]<-col_rows[col_rows$Table==new_tbl,]$Column[1]
				new_col[2]<-col_rows[col_rows$Table==new_tbl,]$Value[1]
			}
		}else{
			new_col[1]<-col_rows$Column[1]
			new_col[2]<-col_rows$Value[1]
		}
	}
	return(new_col)
}


get_where<-function(where_df,tbls,joins,cur_values){
	# top node
	where_part<-vector()
	group_part<-vector()
	having_part<-vector()
	select_aggr<-vector()
	exposed_cols<-vector()
	if(nrow(where_df)>0){
		where_df$processed="no"
		top_nodes<-where_df[!(where_df$head_token_id %in% where_df$token_id) & !(where_df$dep_rel %in% c("case","cc")) & where_df$processed=="no",]
		n<-nrow(top_nodes)
		if(n>0){
			for(i in 1:n){
				r<-get_having(where_df,top_nodes[i,])
				where_df<-r[[1]]
				having_part<-append(having_part,r[[2]])
				select_aggr<-append(select_aggr,r[[3]])
			}
		}
		# Refresh top nodes after having clause processing
		top_nodes<-where_df[!(where_df$head_token_id %in% where_df$token_id) & !(where_df$dep_rel %in% c("case","cc")) & where_df$processed=="no",]
		while(nrow(top_nodes)>0){
			node<-top_nodes[1,]
			if(node$dep_rel %in% c("nmod","conj","obl","obj") & node$xpos!="JJR" & node$itemtype!="column"){
				child<-where_df[where_df$head_token_id==node$token_id & where_df$itemtype=="column" & where_df$processed=="no",]
				if(nrow(child)>0){
					right_part<-validate_like(node$token)
					where_part<-append(where_part,paste0(child$item[1],right_part))
					#where_part<-append(where_part,paste0(child$item[1]," = ",node$token))
					where_df[where_df$token_id==child$token_id[1],"processed"]<-"yes"
				}else{
					compare<-where_df[where_df$dep_rel=="conj" & where_df$xpos=="JJR" & where_df$processed=="no",]
					if(nrow(compare)>0){
						nocompare<-where_df[where_df$head_token_id==compare$token_id[1] & tolower(where_df$token) %in% c("not","no") & where_df$processed=="no",]
						discourse<-!nrow(nocompare)==0
						oper<-translate_token(compare$token[1],discourse)
						child<-where_df[where_df$head_token_id==compare$token_id & where_df$itemtype=="column" & where_df$processed=="no",]
						if(nrow(child)>0){
							where_part<-append(where_part,paste0(child$item[1],oper,node$token))
							where_df[where_df$token_id==child$token_id[1],"processed"]<-"yes"
						}
						where_df[where_df$token_id==compare$token_id[1],"processed"]<-"yes"
					}else{
						# Here is the place where we can determine potential column name by matching
						# value node$token to a column that contains such a value for strings
						# if value is non numeric
						options(warn=-1)
						if(is.na(as.numeric(node$token))){
							new_col<-expose_column(tbls,joins,node$token,cur_values)
							if(length(new_col)==2){
								exposed_cols<-append(exposed_cols,new_col[1])
								where_part<-append(where_part,paste0(new_col[1]," = ",gsub(" ","_",new_col[2])))
							}
						}
						options(warn=0)
					}
				}
			}else if(node$xpos=="JJR" & node$itemtype!="column"){
				child<-where_df[where_df$head_token_id==node$token_id[1] & where_df$dep_rel=="obl" & where_df$xpos=="CD" & where_df$processed=="no",]
				if(nrow(child)>0){
					grandchild<-where_df[where_df$head_token_id==child$token_id[1] & where_df$itemtype=="column" & where_df$processed=="no",]
					if(nrow(grandchild)>0){
						grandchild2<-where_df[where_df$head_token_id==child$token_id[1] & tolower(where_df$token) %in% c("not","no") & where_df$processed=="no",]	
						discourse<-!nrow(grandchild2)==0
						oper<-translate_token(node$token[1],FALSE)
						if(oper!=""){
							where_part<-append(where_part,paste0(grandchild$item[1],oper,child$token[1]))
						}
						where_df[where_df$token_id==grandchild$token_id[1],"processed"]<-"yes"
					}else{
						sibling<-where_df[where_df$head_token_id==node$token_id & where_df$itemtype=="column" & where_df$processed=="no",]
						if(nrow(sibling)>0){
							sibling2<-where_df[where_df$head_token_id==node$token_id & tolower(where_df$token) %in% c("not","no") & where_df$processed=="no",]
							discourse<-!nrow(sibling2)==0
							oper<-translate_token(node$token[1],discourse)
							if(oper!=""){
								where_part<-append(where_part,paste0(sibling$item[1],oper,child$token))
							}
							where_df[where_df$token_id==sibling$token_id[1],"processed"]<-"yes"
							if(discourse){
								where_df[where_df$token_id==sibling2$token_id[1],"processed"]<-"yes"
							}
						}
					}
					where_df[where_df$token_id==child$token_id[1],"processed"]<-"yes"
				}
			}else if(node$dep_rel %in% c("nmod","conj","compound") & node$itemtype=="column"){
				child<-where_df[where_df$head_token_id==node$token_id & where_df$itemtype!="column" & where_df$dep_rel %in% c("flat","fixed","compound") & where_df$processed=="no",]
				if(nrow(child)>0){
					right_part<-validate_like(child$token[1])
					where_part<-append(where_part,paste0(node$item,right_part))
					#where_part<-append(where_part,paste0(node$item," = ",child$token[1]))
					where_df[where_df$token_id==child$token_id[1],"processed"]<-"yes"
				}else{
					child<-where_df[where_df$head_token_id==node$token_id & where_df$itemtype!="column" & where_df$dep_rel %in% c("amod","advmod") & where_df$xpos=="JJR" & where_df$processed=="no",]
					if(nrow(child)>0){
						nephew<-where_df[where_df$head_token_id==child$token_id[1] & where_df$dep_rel=="obl" & where_df$xpos=="CD" & where_df$processed=="no",]
						if(nrow(nephew)>0){				
							niece<-where_df[where_df$head_token_id==child$token_id[1] & tolower(where_df$token) %in% c("not","no") & where_df$processed=="no",]
							discourse<-!nrow(niece)==0
							oper<-translate_token(child$token[1],discourse)
							if(oper!=""){
								where_part<-append(where_part,paste0(node$item,oper,nephew$token[1]))
							}
							where_df[where_df$token_id==nephew$token_id[1],"processed"]<-"yes"
							if(discourse){
								where_df[where_df$token_id==niece$token_id[1],"processed"]<-"yes"
							}
						}
						where_df[where_df$token_id==child$token_id[1],"processed"]<-"yes"
					}else{
						sibling<-where_df[where_df$head_token_id==node$head_token_id & where_df$dep_rel=="amod" & where_df$xpos=="JJR" & where_df$processed=="no",]
						if(nrow(sibling)>0){
							nephew<-where_df[where_df$head_token_id==sibling$token_id[1] & where_df$dep_rel=="obl" & where_df$xpos=="CD" & where_df$processed=="no",]
							if(nrow(nephew)>0){
								niece<-where_df[where_df$head_token_id==sibling$token_id[1] & tolower(where_df$token) %in% c("not","no") & where_df$processed=="no",]
								discourse<-!nrow(niece)==0
								oper<-translate_token(sibling$token[1],discourse)
								if(oper!=""){
									where_part<-append(where_part,paste0(node$item,oper,nephew$token[1]))
								}
								where_df[where_df$token_id==nephew$token_id[1],"processed"]<-"yes"
								if(discourse){
									where_df[where_df$token_id==niece$token_id[1],"processed"]<-"yes"
								}
							}
							where_df[where_df$token_id==sibling$token_id[1],"processed"]<-"yes"
						}else{
							cousin<-where_df[where_df$head_token_id==node$head_token_id & where_df$xpos=="JJR" & where_df$processed=="no",]
							if(nrow(cousin)>0){
								nephew<-where_df[where_df$head_token_id==cousin$token_id[1] & where_df$dep_rel=="obl" & where_df$xpos=="CD" & where_df$processed=="no",]
								if(nrow(nephew)>0){
									niece<-where_df[where_df$head_token_id==cousin$token_id[1] & tolower(where_df$token) %in% c("not","no") & where_df$processed=="no",]
									discourse<-!nrow(niece)==0
									oper<-translate_token(cousin$token[1],discourse)
									if(oper!=""){
										where_part<-append(where_part,paste0(node$item,oper,nephew$token[1]))
									}
									where_df[where_df$token_id==nephew$token_id[1],"processed"]<-"yes"
									if(discourse){
										where_df[where_df$token_id==niece$token_id[1],"processed"]<-"yes"
									}
								}
								where_df[where_df$token_id==cousin$token_id[1],"processed"]<-"yes"
							}else{
								group_part<-append(group_part,node$item)
								where_df[where_df$token_id==node$token_id,"processed"]<-"yes"
							}
						}
					}
				}
			}else if(node$dep_rel=="obj" & node$itemtype=="column"){
				child<-where_df[where_df$head_token_id==node$token_id & tolower(where_df$token) %in% c("best","highest","worst","lowest") & where_df$processed=="no",]
				if(nrow(child)>0){
					where_part<-append(where_part,paste0(node$item,"=", map_aggr(child$token[1],node$item)))
					where_df[where_df$token_id==child$token_id[1],"processed"]<-"yes"
				}else{
					child<-where_df[where_df$head_token_id==node$token_id & where_df$dep_rel=="amod" & where_df$xpos=="JJR" & where_df$processed=="no",]
					if(nrow(child)>0){
						grandchild<-where_df[where_df$head_token_id==child$token_id[1] & where_df$dep_rel=="obl" & where_df$xpos=="CD" & where_df$processed=="no",]
						if(nrow(grandchild)>0){
							grandchild2<-where_df[where_df$head_token_id==child$token_id[1] & tolower(where_df$token) %in% c("not","no") & where_df$processed=="no",]
							discourse<-!nrow(grandchild2)==0
							oper<-translate_token(child$token[1],discourse)
							if(oper!=""){
								where_part<-append(where_part,paste0(node$item,oper,grandchild$token[1]))
							}
							where_df[where_df$token_id==grandchild$token_id[1],"processed"]<-"yes"
							if(discourse){
								where_df[where_df$token_id==grandchild2$token_id[1],"processed"]<-"yes"
							}
						}else{
							grandchild<-where_df[where_df$head_token_id==child$token_id[1] & where_df$dep_rel=="obl" & where_df$itemtype=="column" & where_df$processed=="no",]
							if(nrow(grandchild)>0){
								grandchild2<-where_df[where_df$head_token_id==child$token_id[1] & tolower(where_df$token) %in% c("not","no") & where_df$processed=="no",]
								discourse<-!nrow(grandchild2)==0
								oper<-translate_token(child$token[1],discourse)
								if(oper!=""){
									where_part<-append(where_part,paste0(node$item,oper,grandchild$item[1]))
								}
								where_df[where_df$token_id==grandchild$token_id[1],"processed"]<-"yes"
								if(discourse){
									where_df[where_df$token_id==grandchild2$token_id[1],"processed"]<-"yes"
								}
							}
						}
						where_df[where_df$token_id==child$token_id[1],"processed"]<-"yes"
					}else {
						child<-where_df[where_df$head_token_id==node$token_id & where_df$dep_rel=="nmod" & where_df$xpos=="CD" & where_df$processed=="no",]
						if(nrow(child)>0){
							grandchild<-where_df[where_df$head_token_id==child$token_id[1] & where_df$dep_rel %in% c("compound","conj") & where_df$xpos=="CD" & where_df$processed=="no",]
							if(nrow(grandchild)>0){
								where_part<-append(where_part,paste0(node$item," between ",child$token[1]," and ",grandchild$token[1]))
								where_df[where_df$token_id==grandchild$token_id[1],"processed"]<-"yes"
							}
							where_df[where_df$token_id==child$token_id[1],"processed"]<-"yes"
						}else{
							sibling<-where_df[where_df$head_token_id==node$head_token_id & where_df$dep_rel %in% c("xcomp") & where_df$processed=="no",]
							if(nrow(sibling)>0){
								where_part<-append(where_part,paste0(node$item,"=", sibling$token[1]))
								where_df[where_df$token_id==sibling$token_id[1],"processed"]<-"yes"
							}
						}
					}
				}
			}else if(node$dep_rel=="obl" & node$itemtype=="column"){
				child<-where_df[where_df$head_token_id==node$token_id & tolower(where_df$token) %in% c("by") & where_df$processed=="no",]
				if(nrow(child)>0){
					group_part<-append(group_part,node$item)
				}else{
					child<-where_df[where_df$head_token_id==node$token_id & where_df$itemtype!="column" & where_df$dep_rel %in% c("amod","advmod") & where_df$processed=="no",]
					if(nrow(child)>0){
						where_part<-append(where_part,paste0(node$item,"=", child$token[1]))
						where_df[where_df$token_id==child$token_id[1],"processed"]<-"yes"
					}
				}
			}
			where_df[where_df$token_id==node$token_id,"processed"]<-"yes"
			top_nodes<-where_df[!(where_df$head_token_id %in% where_df[where_df$processed=="no",]$token_id) & !(where_df$dep_rel %in% c("case","cc")) & where_df$processed=="no",]
		}
	}
	r<-myList<-list()
	r[[1]]<-where_part
	r[[2]]<-group_part
	r[[3]]<-having_part
	r[[4]]<-select_aggr
	r[[5]]<-exposed_cols
	return(r)
}

map_aggr<-function(aggr,colname){
	if(length(aggr)>0){
		if(tolower(aggr)=="total"){
			r<-paste0("Sum(",colname,")")
		}else if(tolower(aggr)=="average"){
			r<-paste0("Avg(",colname,")")
		}else if(tolower(aggr) %in% c("lowest","worst")){
			r<-paste0("Min(",colname,")")
		}else if(tolower(aggr) %in% c("highest","best")){
			r<-paste0("Max(",colname,")")
		}else if(tolower(aggr)=="many"){
			r<-paste0("UniqueCount(",colname,")")
		}else{
			r<-colname
		}
	}else{
		r<-colname
	}
	return(r)
}

get_subtree<-function(df,leaves,excl=vector()){
# Get a subtree with starting node index ind
	out<-df[0,]
	if(all(leaves)>=0 & all(leaves) <= nrow(df)){
		while(length(leaves)>0){
			out<-rbind(out,df[df$token_id %in% leaves,])
			leaves<-df[df$head_token_id %in% leaves & !(df$dep_rel %in% excl) & !(df$token_id %in% out$token_id),]$token_id
		}
	}
	return(out)	
}

parse_request<-function(df_in,db,refine){
	# map nouns to db items
	if(refine){
		df1<-tag_dbitems(df_in,db)
	}else{
		df1<-map_dbitems(df_in,db)
	}
	# to handle column names that are not words of English language
	df<-refine_parsing(df1)
	gc()
	return(df)
}

tag_dbitems<-function(df_in,db){
	df_in$itemtype<-""
	ind<-which(db$Word != "")
	mydf<-db[ind,]
	# make sure we keep the best score
	mydf<-mydf[order(mydf$AppID,mydf$Word,-mydf$Score),]
	mydf<-mydf[!duplicated(mydf$Word),]
	x<-as.data.frame(df_in$token)
	y<-mydf$Column[match(unlist(x), mydf$Word)]
	z<-mydf$Datatype[match(unlist(x), mydf$Word)]
	y[is.na(y)]<-""
	z[is.na(z)]<-""
	df_in$item<-y
	df_in$itemdatatype<-y
	df_in[df_in$item != "","itemtype"]<-"column"
	return(df_in)
}

parse_question<-function(txt){
	FILE_MODEL<-"english-ud-2.0-170801.udpipe"
	library(udpipe)
	
	if(!exists("tagger")){
		tagger <<- udpipe_load_model(FILE_MODEL)
	}
	doc <- tryCatch({
		udpipe_annotate(tagger, txt)
	}, error = function(e) {
		return(NULL)
	})
	if(is.null(doc)){
		tagger <<- udpipe_load_model(FILE_MODEL)
		doc <- udpipe_annotate(tagger, txt)
	}
	df<-as.data.frame(doc)
	gc()
	return(df[,4:12])
}

parse_question_mgr<-function(txt){
	STOPWORDS<-c("a","the","here","there","it","he","she","they","which","what","who")
	FIRSTWORDS<-c("select","show","display","present","list","find","locate")
	
	# update text by removing stop words
	words<-unlist(strsplit(txt," "))
	if(length(words)>0){
		if(tolower(words[1]) %in% FIRSTWORDS){
			words<-words[-1]
		}
	}
	words<-words[!(tolower(words) %in% STOPWORDS)]
	words<-replace_words(words)
	
	# Removing "-","_" to have the correct annotation
	z<-sapply(words,function(w) gsub("[-_'%]","",w))
	mytxt<-paste(z,collapse=" ")
	df<-parse_question(tolower(mytxt))
	if(nrow(df)>0){
		for(i in 1:nrow(df)){
			token<-df$token[i]
			y<-which(token==tolower(z))
			if(length(y)>0){
				df[i,"token"]<-names(z[y[1]])
				z<-z[-y[1]]
			}
		}
	}else{
		df<-data.frame()
	}
	return(df)
}

replace_words<-function(words){
	orig=c("mean","less","lower")
	repl=c("average","smaller","smaller")
	n<-min(length(orig),length(repl))
	for(i in 1:n){
		ind<-which(tolower(words)==orig[i])
		if(length(ind)>0){
			words[ind]<-repl[i]
		}
	}
	return(words)
}

db_match<-function(db,token,xpos,type="Column"){
	THRESHOLD<-0.9
	db_item<-vector()
	
	db$Column<-as.character(db$Column)
	db$Table<-as.character(db$Table)
		
	# get neighbors from glove if not reserved
	if(is_nlp_reserved(token)){
		neighbors<-token
	}else{
		if(substr(xpos,1,2) %in% c("NN","JJ")){
			neighbors<-glove_nlp_neighbors(token)
			# eclude reserved words from neighbors
			neighbors<-remove_nlp_reserved(neighbors)
		}else{
			neighbors<-token
		}
	}
	if(length(neighbors)>0){
		for(i in 1:length(neighbors)){
			matches<-stringsim(tolower(neighbors[i]),tolower(db[,type]),method='jw', p=0.1)
			if(max(matches) >= THRESHOLD){
				ind<-min(which(matches==max(matches)))
				db_item[1]<-db[ind,type]
				if(type=="Column"){
					db_item[2]<-db[ind,"Datatype"]
				}
				break
			}
		}
	}
	if(length(db_item) != 2){
		db_item[1]<-""
	}
	return(db_item)
}

is_nlp_reserved<-function(words){
	STOPWORDS<-c("not","no","best","highest","worst","lowest","greater","higher","larger","less","lower","smaller","equals","equal","total","average","highest","lowest","best","worst","many","number","count")
	return(any(tolower(words) %in% STOPWORDS))
}

remove_nlp_reserved<-function(words){
	# if nlp stoplist no neighbors needed
	STOPWORDS<-c("not","no","best","highest","worst","lowest","greater","higher","larger","less","lower","smaller","equals","equal","total","average","highest","lowest","best","worst","many","number","count")
	out<-words[!(tolower(words) %in% STOPWORDS)]
	return(out)
}

filter_apps<-function(df_in,db,threshold=0.9){
	library(plyr)
	n<-nrow(df_in)
	if(n>0){
		db$Word<-""
		db$Score<-0
		for(i in 1:n){
			token<-df_in[i,"token"]
			# skip token in single quotes
			if(substr(token,1,1)!="'" & substr(token,nchar(token),nchar(token))!="'"){	
				# exclude reserved words
				# if not reserved get all neighbors
				if(is_nlp_reserved(token)){
					neighbors<-token
				}else{
					# if not ADP or ADJ than continue
					if(substr(df_in$xpos[i],1,2) %in% c("NN","JJ")){
						neighbors<-glove_nlp_neighbors(token)
						# eclude reserved words from neighbors
						neighbors<-remove_nlp_reserved(neighbors)
					}else{
						neighbors<-token
					}
				}
				if(length(neighbors)>0){
					for(j in 1:length(neighbors))
					{
						matches<-stringsim(tolower(neighbors[j]),tolower(db$Column),method='jw', p=0.1)
						ind<-which(matches>=threshold)
						if(length(ind)>0){
							db$Word[ind]<-token
							db$Score[ind]<-matches[ind]
							break
						}
					}
				}
				
			}
			
		}
	}
	df_apps<-unique(db[db$Word != "",c("AppID","Word")])
	df_apps<-count(df_apps,"AppID")
	apps<-df_apps[df_apps$freq==max(df_apps$freq),"AppID"]
	myList<-list()
	myList[[1]]<-apps
	myList[[2]]<-db[db$AppID %in% apps,]
	return(myList)
}

map_dbitems<-function(df,db,pos="ALL"){
	df$item<-""
	df$itemtype<-""
	df$itemdatatype<-""
	if(pos=="ALL"){
		ind<-df$token_id
	}else{
		ind<-df[substr(df$xpos,1,2)=="NN","token_id"]
	}
	n<-length(ind)
	for(i in 1:n){
		token<-df[ind[i],"token"]
		xpos<-df[ind[i],"xpos"]
		if(substr(token,1,1)!="'" & substr(token,nchar(token),nchar(token))!="'"){
			item<-db_match(db,token,xpos,"Column")
			if(item[1]!=""){
				df[ind[i],"item"]<-item[1]
				df[ind[i],"itemtype"]<-"column"
				df[ind[i],"itemdatatype"]<-item[2]
			}
		}
	}
	gc()
	return(df)
}

refine_parsing<-function(df){
	ind<-as.integer(df[df$itemtype %in% c("column","table") & substr(df$xpos,1,2)!="NN","token_id"])
	if(length(ind)>0){
		mydf<-df
		mydf$token[ind]<-"epiphany"
		mytxt<-paste(mydf$token,collapse=" ")
		mydf<-parse_question_mgr(mytxt)
		df[,5:9]<-mydf[,5:9]
	}
	# Remove first and last ' if exist
	df$token<-sapply(df$token,function(w) gsub("^'|'$", '',w))
	gc()
	return(df)
}

optimize_joins<-function(cols,joins,cur_db){
	# get the existing tables with required columns
	if(nrow(joins)>0){
		tbls<-as.character(unique(cur_db[tolower(cur_db$Column) %in% tolower(cols),"Table"]))
		if(length(tbls)==1){
			# if all columns in a single table
			joins<-joins[0,]
			joins<-rbindlist(list(joins,list(tbls[1],tbls[1],"","")))
			colnames(joins)<-c("tbl1","tbl2","joinby1","joinby2")
		}else if(length(tbls)>1){
			mytbls<-connect_tables(tbls,joins)
			joins<-joins[(joins$tbl1 %in% mytbls) & (joins$tbl2 %in% mytbls) & (joins$tbl1!=joins$tbl2),]
			# minimize joins
			joins<-min_joins(joins)
		}else{
			joins<-vector()
		}
		gc()
	}
	return(joins)
}

min_joins<-function(joins){
	g<-graph_from_edgelist(as.matrix(joins[,1:2]),directed=FALSE)
	edges<-as_edgelist(g)
	edges1<-as_edgelist(mst(g))
	if(dim(edges1)[1] < dim(edges)[1] & dim(edges1)[1]>0){
		joins<-joins[paste0(joins$tbl1,joins$tbl2) %in% paste0(edges1[,1],edges1[,2]),]
	}
	return(joins)
}

connect_tables<-function(tbls,joins){
	if(length(tbls)>1){
		g<-graph_from_edgelist(as.matrix(joins[,1:2]),directed=FALSE)
		vertices<-unique(names(V(g)))
		if(length(which(vertices %in% tbls))==length(tbls)){
			distances<-distances(g,v=tbls,to=tbls)
			n<-nrow(distances)
			for(i in 1:(n-1)){
				for(j in (i+1):n){
					if(distances[i,j]>1){
						paths<-all_simple_paths(g,from=tbls[i],to=tbls[j])
						lengths<-sapply(paths, length)
						idx<-which.min(lengths)
						tbls<-unique(append(tbls,names(paths[[idx]])))
					}
				}
			}
		}else{
			tbls<-vector()
		}
	}
	gc()
	return(tbls)
}

verify_joins<-function(cols,joins,cur_db){
	
	myList<-list()
	myList[[1]]<-""
	g<-graph_from_edgelist(as.matrix(joins[,1:2]),directed=FALSE)
	g_mst<-mst(g)
	# verify that all required columns accessible
	tbls<-vertex_attr(g_mst)$name
	tbls_cols<-as.character(cur_db[tolower(cur_db$Table) %in% tolower(tbls),"Column"])
	if(all(tolower(cols) %in% tolower(tbls_cols))){
		myList[[2]]<-joins
	}else{
		myList[[1]]<-"Rephrase the request: could not join all required tables"
	}
	gc()
	return(myList)
}

build_join_clause<-function(joins){
	n<-nrow(joins)
	clause<-""
	from_joins<-data.table(tbl1=character(),tbl2=character(),joinby1=character(),joinby2=character())
	if(n>0){
		if(joins[1,"tbl1"]==joins[1,"tbl2"]){
			clause<-as.character(joins[1,"tbl1"])
			from_joins<-rbindlist(list(from_joins,list(clause,"","","")))
		}else{
			joins$processed<-"no"
			joins$id<-seq(1:n)
			tbls<-vector()
			tbls[1]<-as.character(joins[1,"tbl1"])
			clause<-as.character(joins[1,"tbl1"])
			from_joins<-rbindlist(list(from_joins,list(clause,"","","")))
			while(nrow(joins[(tolower(joins$tbl1) %in% tolower(tbls) | tolower(joins$tbl2) %in% tolower(tbls) ) & joins$processed == "no",])>0){
				cur_rec<-joins[(tolower(joins$tbl1) %in% tolower(tbls) | tolower(joins$tbl2) %in% tolower(tbls) )  & joins$processed == "no",][1,]
				if(length(tbls[tolower(cur_rec$tbl1) %in% tolower(tbls)])>0){
					if(length(tbls[tolower(cur_rec$tbl2) %in% tolower(tbls)])>0){
						clause<-paste0(clause, "on ",cur_rec$tbl1,".",cur_rec$joinby1,"=",cur_rec$tbl2,".",cur_rec$joinby2)
						from_joins<-rbindlist(list(from_joins,list(cur_rec$tbl1,cur_rec$tbl2,cur_rec$joinby1,cur_rec$joinby2)))
					}else{
						tbls[length(tbls)+1]<-cur_rec$tbl2
						clause<-paste0(clause," inner join ",cur_rec$tbl2," on ",cur_rec$tbl1,".",cur_rec$joinby1,"=",cur_rec$tbl2,".",cur_rec$joinby2)
						from_joins<-rbindlist(list(from_joins,list(cur_rec$tbl1,cur_rec$tbl2,cur_rec$joinby1,cur_rec$joinby2)))
					}
				}else{
					tbls[length(tbls)+1]<-cur_rec$tbl1
					clause<-paste0(clause," inner join ",cur_rec$tbl1," on ",cur_rec$tbl2,".",cur_rec$joinby1,"=",cur_rec$tbl1,".",cur_rec$joinby2)
					from_joins<-rbindlist(list(from_joins,list(cur_rec$tbl1,cur_rec$tbl2,cur_rec$joinby1,cur_rec$joinby2)))
				}
				joins[cur_rec$id,"processed"]<-"yes"
			}
		}
	}
	from_joins$tbl1=as.character(from_joins$tbl1)
	from_joins$tbl2=as.character(from_joins$tbl2)
	from_joins$joinby1=as.character(from_joins$joinby1)
	from_joins$joinby2=as.character(from_joins$joinby2)
	myList<-list()
	myList[[1]]<-clause
	myList[[2]]<-from_joins
	return(myList)
}	

join_clause_mgr<-function(cols,cur_db,joins){
	library(plyr)
	myList<-list()
	joins<-optimize_joins(cols,joins,cur_db)
	if(nrow(joins)>0){
		out<-verify_joins(cols,joins,cur_db)
		if(out[[1]]==""){
			joins<-out[[2]]
			out<-build_join_clause(joins)
			myList[[1]]<-""
			myList[[2]]<-out[[1]]
			myList[[3]]<-out[[2]]
		}else{
			myList[[1]]<-out[[1]]
		}
	}else{
		x<-cur_db[cur_db$Column %in% cols,]
		y<-count(x,"Table")
		z<-y[y$freq==length(cols),]
		if(nrow(z)>0){
			myList[[1]]<-""
			myList[[2]]<-z$Table[1]
			myList[[3]]<-data.frame(tbl1=z$Table[1],tbl2="",joinby1="",joinby2="",stringsAsFactors = FALSE)
		}else{
			myList[[1]]<-"Reprase the request: could not locate required tables"
		}
	}
	gc()
	return(myList)
}

glove_nlp_neighbors<-function(word,nbr=10,threshold=0.8){
	
	library(text2vec)
	if(!exists("glove_6B_50d_txt")){
		glove_6B_50d_txt <<- readRDS("glove.rds")
	}
	cur_vector <- tryCatch({
		glove_6B_50d_txt[word, , drop = FALSE]
	}, error = function(e) {
		return(NULL)
	})
	if(is.null(cur_vector)){
		out<-word
	}else{
		cos_sim = sim2(x = glove_6B_50d_txt, y = cur_vector, method = "cosine", norm = "l2")
		cos_sim<-cos_sim[cos_sim >= threshold,]
		if(!is.null(attributes(cos_sim))){
			out<-names(head(sort(cos_sim, decreasing = TRUE), nbr))
		}else{
			out<-word
		}
	}
	return(out)
}



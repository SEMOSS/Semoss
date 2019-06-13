nliapp_mgr<-function(txt,db,joins=data.frame()){

	# get all appids used in the request
	apps<-unique(db$AppID)
	N<-length(apps)
	r<-data.table(Response=character(),AppID=character(),Statement=character())
	p<-data.table(appid=character(),part=character(),item1=character(),item2=character(),item3=character(),item4=character(),item5=character(),item6=character(),item7=character())
	df_in<-parse_question_mgr(txt)
	if(N>0){
		for(i in 1:N){	
			cur_db<-db[db$AppID==apps[i],]
			if(nrow(joins)!=0){
				cur_joins<-joins[joins$AppID==apps[i],1:4]
			}else{
				cur_joins<-joins
			}
			
			#####################################################################################################
			# Prepare request for current db
			df<-parse_request(df_in,cur_db)
			df$processed<-"no"
			out<-get_start(df)
			if(!is.null(out[[1]]) & !is.null(out[[2]])){
				select_part<-get_select(out[[1]])
				mypart<-get_where(out[[2]])
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
				
				
				# get columns used in the request
				cols<-as.character(df[df$itemtype == "column","item"])
				#####################################################################################################
				# get from clause (joins)
				out<-join_clause_mgr(cols,cur_db,cur_joins)
				if(out[[1]]==""){
					from_clause<-out[[2]]
					from_joins<-out[[3]]
					pixel_from<-build_pixel_from(from_joins)
					request_tbls<-unique(c(from_joins$tbl1,from_joins$tbl2))
					request_tbls<-request_tbls[request_tbls!=""]
					response<-"SQL"
				}else{
					sql<-out[[1]]
					response<-"Error"
					next
				}
				
				# add where columns into select section
				if(length(where_part)>0){
					select_part<-unique(append(select_part,select_where(where_part,request_tbls,cur_db)))
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
	return(p)
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

get_start<-function(parsed_df){
	out<-list()
	out[[1]]<-NULL
	out[[2]]<-NULL
	root<-parsed_df[parsed_df$head_token_id==0,]
	if(root$itemtype=="column"){
		# Identify possible select nodes
		kids<-parsed_df[parsed_df$head_token_id==root$token_id,]
		select_tree<-kids[!(kids$dep_rel %in% c("nmod","obj","acl","obl","conj")) & kids$xpos != "JJR",]
		out[[1]]<-rbind(root,get_subtree(parsed_df,as.integer(select_tree$token_id),"nmod"))
		where_tree<-parsed_df[!(parsed_df$token_id %in% out[[1]]$token_id),]	
		out[[2]]<-where_tree
	}else{
		kids<-parsed_df[parsed_df$head_token_id==root$token_id,]
		nsubj<-kids[substr(kids$dep_rel,1,5)=="nsubj" & kids$itemtype=="column",]
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
							select_df[select_df$token_id==count_df$token_id[i],"processed"]<-"yes"
							select_df[select_df$token_id==token_id[i],"processed"]<-"yes"
							select_df[select_df$token_id==child$token_id[i],"processed"]<-"yes"
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

get_where<-function(where_df){
	# top node
	where_part<-vector()
	group_part<-vector()
	having_part<-vector()
	select_aggr<-vector()
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
			if(node$dep_rel %in% c("nmod","conj","obl") & node$itemtype!="column"){
				child<-where_df[where_df$head_token_id==node$token_id & where_df$itemtype=="column" & where_df$processed=="no",]
				if(nrow(child)>0){
					where_part<-append(where_part,paste0(child$item[1],"=",node$token))
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
					}
				}
			}else if(node$dep_rel %in% c("nmod","conj","compound") & node$itemtype=="column"){
				child<-where_df[where_df$head_token_id==node$token_id & where_df$itemtype!="column" & where_df$dep_rel %in% c("flat","fixed","compound") & where_df$processed=="no",]
				if(nrow(child)>0){
					where_part<-append(where_part,paste0(node$item,"=",child$token[1]))
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
						group_part<-append(group_part,node$item)
						where_df[where_df$token_id==node$token_id,"processed"]<-"yes"
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
									where_part<-append(where_part,paste0(node$item,oper,grandchild$token[1]))
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
						}
					}
				}
			}else if(node$dep_rel=="obl" & node$itemtype=="column"){
				child<-where_df[where_df$head_token_id==node$token_id & tolower(where_df$token) %in% c("by") & where_df$processed=="no",]
				if(nrow(child)>0){
					group_part<-append(group_part,node$item)
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

parse_request<-function(df_in,db){
	library(data.table)
	library(plyr)
	library(udpipe)
	library(stringdist)
	library(igraph)
	
	# map nouns to db items
	df1<-map_dbitems(df_in,db)
	# to handle column names that are not words of English language
	df<-refine_parsing(df1)
	return(df)
}

parse_question<-function(txt){
	library(udpipe)
	#STOPWORDS<-c("a","the","here","there","it","he","she","they","is","are","which","what","who")
	STOPWORDS<-c("a","the","here","there","it","he","she","they","which","what","who")
	FILE_MODEL<-"english-ud-2.0-170801.udpipe"
	
	words<-unlist(strsplit(txt," "))
	words<-words[!(tolower(words) %in% STOPWORDS)]
	words<-replace_words(words)
	txt<-paste(words,collapse=" ")
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
	x<-unlist(strsplit(txt," "))
	df<-parse_question(tolower(txt))
	if(nrow(df)>0){
		for(i in 1:nrow(df)){
			token<-df$token[i]
			y<-which(token==tolower(x))
			if(length(y)>0){
				df[i,"token"]<-x[y[1]]
				x<-x[-y[1]]
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

db_match<-function(db,token,type="Column"){
	THRESHOLD<-0.9
	db_item<-vector()

	db$Column<-as.character(db$Column)
	db$Table<-as.character(db$Table)
	matches<-stringsim(tolower(token),tolower(db[,type]),method='jw', p=0.1)
	if(max(matches) >= THRESHOLD){
		ind<-min(which(matches==max(matches)))
		db_item[1]<-db[ind,type]
		if(type=="Column"){
			db_item[2]<-db[ind,"Datatype"]
		}
	}else{
		db_item[1]<-""
	}
	return(db_item)
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
		item<-db_match(db,token,"Column")
		if(item[1]!=""){
			df[ind[i],"item"]<-item[1]
			df[ind[i],"itemtype"]<-"column"
			df[ind[i],"itemdatatype"]<-item[2]
		}
	}
	gc()
	return(df)
}

dbitems_tolower<-function(txt,df){
	words<-unlist(strsplit(txt," "))
	dbitems<-df[df$itemtype %in% c("column","table"),"token"]
	ind<-which(words %in% dbitems)
	if(length(ind)>0){
		words[ind]<-tolower(words[ind])
	}
	mytxt<-paste(words,collapse=" ")
	return(mytxt)
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
		}else{
			joins<-vector()
		}
		gc()
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

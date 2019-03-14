nliapp_mgr<-function(txt,db,joins=data.frame()){
	library(data.table)
	library(plyr)
	library(udpipe)
	library(stringdist)
	library(igraph)
	
	# db includes: Column, Table and AppID columns
	df1<-parse_question(txt)
	# map nouns to db items
	df1<-map_dbitems(df1,db)
	# Convert dbitems to lower case to avoid pos confusion
	mytxt<-dbitems_tolower(txt,df1)
	df<-parse_question(mytxt)
	df<-map_dbitems(df,db)
	# restore original tokens though though keep the correct parsing pos
	df$sentence<-df1$sentence
	df$token<-df1$token
	df$lemma<-df1$lemma
	# to handle column names that are not words of English language
	df<-refine_parsing(df)
	#get pos groups
	chunks<-get_chunks(df)
	
	# get columns used in the request
	cols<-tolower(as.character(df[df$itemtype == "column","item"]))
	# get potential appids used in the request
	apps<-unique(db[tolower(db$Column) %in% cols,"AppID"])
	N<-length(apps)
	r<-data.table(Response=character(),AppID=character(),Statement=character())
	p<-data.table(appid=character(),part=character(),item1=character(),item2=character(),item3=character(),item4=character(),item5=character(),item6=character(),item7=character())
	if(N>0){
		for(i in 1:N){
			df$processed<-"no"
			cur_db<-db[db$AppID==apps[i],]
			if(nrow(joins)!=0){
				cur_joins<-joins[joins$AppID==apps[i],1:4]
			}else{
				cur_joins<-joins
			}
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
			
			# process prepositions
			out<-analyze_prep(df,chunks)
			df<-out[[1]]
			where_part<-out[[2]]
			group_part<-out[[3]]
			
			#process adjectives
			out<-analyze_adj(df,chunks)
			df<-out[[1]]
			select_part1<-out[[2]]
			select_part1<-get_alias(select_part1)
			
			# construct pixel aggregate select part
			pixel_aggr_select<-build_pixel_aggr_select(select_part1,request_tbls,cur_db)
			where_part<-c(where_part,out[[3]])
			
			# construct pixel where based on where_part
			pixel_where<-build_pixel_where(where_part,request_tbls,cur_db)
			having_part<-out[[4]]
			pixel_having<-build_pixel_having(having_part,request_tbls,cur_db)
			
			# the rest of the select
			select_part2<-analyze_noun(df)
			# if having part existing add select singles to groups
			if(length(having_part)>0){
				pixel_group<-build_pixel_group(c(group_part,select_part2),request_tbls,cur_db)
			}else{
				pixel_group<-build_pixel_group(group_part,request_tbls,cur_db)
			}
			# if groups part exists add groups to select singles
			if(length(group_part)>0){
				pixel_single_select<-build_pixel_single_select(c(select_part2,group_part),request_tbls,cur_db)
				# if single select added then we need to recheck from part
				cols<-unique(c(select_part2,cols,group_part))
				# recompute joins
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
			}else{
				pixel_single_select<-build_pixel_single_select(select_part2,request_tbls,cur_db)
			}
			
			# complete building sql nad pixel objects
			if(response == "SQL"){
				sql<-build_sql(select_part1,select_part2,where_part,group_part,having_part,from_clause)
				pixel<-build_pixel(apps[i],pixel_aggr_select,pixel_single_select,pixel_where,pixel_group,pixel_having,pixel_from)
				p<-rbind(p,pixel)
			}
			r<-rbindlist(list(r,list(response,as.character(apps[i]),sql)))
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

build_sql<-function(select_part1,select_part2,where_part,group_part,having_part,from_clause){
	# process select
	if(length(select_part2)==0){
		 if(length(select_part1)==0){
			select_items<-"*"
		 }else{
			select_items<-select_part1
		 }
	}else{
		select_items<-c(select_part2,select_part1)
	}
	# add groups to select if any
	if(length(group_part)>0){
		select_items<-c(group_part,select_items)
	}
	select_clause<-paste0("select ",paste(select_items,collapse=",")," from ",from_clause)
	# process where/group
	if(length(where_part) != 0){
		where_clause<-paste0("where ",paste(where_part,collapse=" and "))
	}else{
		where_clause<-""
	}
	having_clause<-""
	if(length(having_part)>0){
		# group clause includes all items in the having section
		group_items<-select_part2
		group_clause<-paste0("group by ",paste(group_items,collapse=","))
		
		# where clause included in the having clause
		having_clause<-paste0("having ",paste(having_part,collapse=" and "))
		
		select_items<-select_part2
		select_clause<-paste0("select ",paste(select_items,collapse=",")," from ",from_clause)
	}else if(length(group_part) != 0){
			group_clause<-paste0("group by ",paste(group_part,collapse=","))
	}else{
		group_clause<-""
	}
	sql<-paste(select_clause,where_clause,group_clause,having_clause,collapse=" ")
	return(sql)
}


analyze_noun<-function(df){
	clmns<-df[substr(df$xpos,1,2)=="NN" & df$itemtype=="column" & df$processed=="no","token"]
	return(clmns)
}

analyze_prep<-function(df,chunks){
	# chunks is a list of subtrees of nouns, adj, etc.
	n<-length(chunks[["prep"]])
	where_clause<-vector()
	group_clause<-vector()
	if(n>0){
		for(i in 1:n){
			if(i==1){
				cur_recs<-chunks[["prep"]][[1]]
			}else{
				cur_recs<-rbind(cur_recs,chunks[["prep"]][[i]])
			}
		}
		m<-nrow(cur_recs)
		if(m>1){
			cur_recs<-cur_recs[order(-as.integer(cur_recs$head_token_id)),]
		}
		for(i in 1:m){
			# get prep record
			cur_rec<-cur_recs[i,]
			# potentially can be different operation
			token<-cur_rec$token
			parent_rec<-get_parent(df,cur_rec)
			if(nrow(parent_rec)>0){
				if(tolower(parent_rec$itemtype)==""){
					if(tolower(parent_rec$token) %in% c("equals","is")){
						oper<-" = "
						value_rec<-df[df$head_token_id==parent_rec$token_id & df$xpos=="CD" & df$processed=="no",]
						column_rec<-df[df$head_token_id==parent_rec$token_id & substr(df$xpos,1,2)=="NN" & df$processed=="no" & df$itemtype=="column",]
						if(nrow(value_rec)>0 & nrow(column_rec)>0){
							where_clause[length(where_clause)+1]<-paste0(column_rec$token,oper,value_rec$token)
							df[parent_rec$token_id,"processed"]<-"yes"
							df[column_rec$token_id,"processed"]<-"yes"
							df[value_rec$token_id,"processed"]<-"yes"
						}
					}else{
						value<-parent_rec$token
						ext_rec<-parent_rec
						while(TRUE){
							ext_rec<-df[df$head_token_id==ext_rec$token_id & df$dep_rel %in% c("flat","compound") & df$itemtype == "",]
							if(nrow(ext_rec)>0){
								value<-paste0(value," ",ext_rec$token)
							}else{
								break
							}
						}
						sibling_rec<-df[df$head_token_id==parent_rec$token_id & df$token_id != cur_rec$token_id & substr(df$xpos,1,2)=="NN" & df$dep_rel %in% c("compound","flat") & df$itemtype=="column",]
						if(nrow(sibling_rec)>0){
							where_clause[length(where_clause)+1]<-paste0(sibling_rec[1,"item"],"=",value)
							df[parent_rec$token_id,"processed"]<-"yes"
							df[sibling_rec$token_id,"processed"]<-"yes"
							# process conjunction if any
							conj_rec<-df[df$head_token_id==parent_rec$token_id & df$token_id != cur_rec$token_id & substr(df$xpos,1,2)=="NN" & df$dep_rel %in% c("conj") & df$itemtype=="",]
							k<-nrow(conj_rec)
							if(k>0){
								for(j in 1:k){
									child_rec<-df[df$head_token_id==conj_rec$token_id[j] & df$itemtype=="column" & df$processed=="no",]
									if(nrow(child_rec)>0){
										where_clause[length(where_clause)+1]<-paste0(child_rec[1,"item"],"=",conj_rec$token)
										df[conj_rec$token_id,"processed"]<-"yes"
										df[child_rec$token_id,"processed"]<-"yes"
									}
								}
							}
						}
					}
				} else if(tolower(parent_rec$itemtype)=="column"){
					oper_rec<-df[df$head_token_id==parent_rec$token_id & df$token_id!=cur_rec$token_id & df$dep_rel!="det" & df$processed=="no" & df$itemtype == "",]
					if(nrow(oper_rec)>0){
						if(nrow(oper_rec[substr(oper_rec$xpos,1,2)=="JJ",])>0){
							oper_rec<-oper_rec[substr(oper_rec$xpos,1,2)=="JJ",]
							oper<-""
							oper_token<-oper_rec[1,"token"]
							if(oper_token=="greater"){
								if(oper_rec$token_id==1){
									oper<-" > "
								}else if(df[as.integer(oper_rec$token_id)-1,"token"] %in% c("no","not")){
									oper<-" <= "
								}else{
									oper<-" > "
								}
							}else if(oper_token=="smaller"){
								if(oper_rec$token_id==1){
									oper<-" < "
								}else if(df[as.integer(oper_rec$token_id)-1,"token"] %in% c("no","not")){
									oper<-" >= "
								}else{
									oper<-" < "
								}
							}
							if(nchar(oper)>0){
								value_rec<-df[df$head_token_id==oper_rec$token_id & df$xpos=="CD" & df$processed=="no",]
								if(nrow(value_rec)>0){
									where_clause[length(where_clause)+1]<-paste0(parent_rec$token,oper,value_rec$token)
									df[parent_rec$token_id,"processed"]<-"yes"
									df[oper_rec$token_id,"processed"]<-"yes"
									df[value_rec$token_id,"processed"]<-"yes"
								}
							}
						}else if(nrow(oper_rec[oper_rec$xpos=="CD",])>0){
							value_rec<-oper_rec[oper_rec$xpos=="CD",]
							if(tolower(cur_rec$token) == "between"){
								sibling_rec<-df[df$head_token_id==cur_rec$head_token_id & df$xpos=="CD" & df$processed=="no",]
								if(nrow(sibling_rec)>0){
									where_clause[length(where_clause)+1]<-paste0(parent_rec$token," between " ,value_rec$token," and ",sibling_rec$token)
									df[cur_rec$token_id,"processed"]<-"yes"	
									df[parent_rec$token_id,"processed"]<-"yes"
									df[value_rec$token_id,"processed"]<-"yes"
									df[sibling_rec$token_id,"processed"]<-"yes"
								}
							}else if(nrow(oper_rec[tolower(oper_rec$token) == "range",])>0){
								sibling_rec<-df[df$head_token_id==parent_rec$token_id & df$token=="range" & df$token_id != value_rec$token_id & 
								df$processed=="no",]
								if(nrow(sibling_rec)>0){
									child_rec<-df[df$head_token_id==sibling_rec$token_id & df$xpos=="CD" & df$processed=="no",]
									if(nrow(child_rec)>0){
										where_clause[length(where_clause)+1]<-paste0(parent_rec$token," between " ,child_rec$token," and ",value_rec$token)
										df[cur_rec$token_id,"processed"]<-"yes"	
										df[parent_rec$token_id,"processed"]<-"yes"
										df[value_rec$token_id,"processed"]<-"yes"
										df[sibling_rec$token_id,"processed"]<-"yes"
										df[child_rec$token_id,"processed"]<-"yes"
									}
								}
							}else{
								oper<-" = "
								where_clause[length(where_clause)+1]<-paste0(parent_rec$token,oper,oper_rec$token)
								df[parent_rec$token_id,"processed"]<-"yes"
								df[oper_rec$token_id,"processed"]<-"yes"
							}
						}else if(nrow(oper_rec[oper_rec$dep_rel %in% c("flat","compound","appos"),])>0){
							# if appos in oper_rec select it here, if not use flat, then compound
							if(nrow(oper_rec[oper_rec$dep_rel=="appos",])){
								oper_rec<-oper_rec[oper_rec$dep_rel=="appos",]
							}else if(nrow(oper_rec[oper_rec$dep_rel=="flat",])){
								oper_rec<-oper_rec[oper_rec$dep_rel=="flat",]
							}else{
								oper_rec<-oper_rec[oper_rec$dep_rel=="compound",]
							}
							token_ids<-vector()
							token_ids[1]<-oper_rec$token_id
							while(TRUE){
								child_rec<-df[df$head_token_id==token_ids[length(token_ids)] & df$processed == "no",]
								if(nrow(child_rec) > 0){
									token_ids[length(token_ids)+1]<-child_rec$token_id
								}else{
									break
								}
							}
							value<-paste(df[token_ids,"token"],collapse=" ")
							where_clause[length(where_clause)+1]<-paste0(parent_rec$token,"=",value)
							df[parent_rec$token_id,"processed"]<-"yes"
							df[oper_rec$token_id,"processed"]<-"yes"
							for(j in 1:length(token_ids)){
								df[token_ids[j],"processed"]<-"yes"
							}
						}
					}else{
						sibling_rec<-df[df$head_token_id==parent_rec$head_token_id & substr(df$xpos,1,2)=="NN" & df$token_id!=parent_rec$token_id & df$dep_rel == "appos" & df$processed=="no",]
						if(nrow(sibling_rec)>0){
							# where clause
							oper<-" = "
							where_clause[length(where_clause)+1]<-paste0(parent_rec$token,oper,sibling_rec$token)
							df[parent_rec$token_id,"processed"]<-"yes"
							df[sibling_rec$token_id,"processed"]<-"yes"
							df[cur_rec$token_id,"processed"]<-"yes"	
						}else{
							#grouping
							group_clause[length(group_clause)+1]<-parent_rec$token
							df[parent_rec$token_id,"processed"]<-"yes"
							df[cur_rec$token_id,"processed"]<-"yes"
							oper_rec<-df[df$head_token_id==parent_rec$token_id & df$token_id!=cur_rec$token_id & df$processed=="no" & df$itemtype == "column",]
							if(nrow(oper_rec)>0){
								group_clause<-c(group_clause,oper_rec$token)
								df[oper_rec$token_id,"processed"]<-"yes"
							}
						}
					}
				}
			}
		}
	}
	myList<-list()
	myList[[1]]<-df
	myList[[2]]<-where_clause
	myList[[3]]<-group_clause
	return(myList)
}

analyze_adj<-function(df,chunks){
	n<-length(chunks[["adj"]])
	select_clause<-vector()
	where_clause<-vector()
	having_clause<-vector()
	if(n>0){
		for(i in 1:n){
			if(i==1){
				cur_recs<-chunks[["adj"]][[1]]
			}else{
				cur_recs<-rbind(cur_recs,chunks[["adj"]][[i]])
			}
		}
		m<-nrow(cur_recs)
		for(i in 1:m){
			cur_rec<-cur_recs[i,]
			token<-tolower(cur_rec$token)
			parent_rec<-get_parent(df,cur_rec,"column")
			if(nrow(parent_rec)>0){
				if(tolower(parent_rec$itemtype)=="column" & parent_rec$processed=="no"){
					column<-parent_rec$token
					if(parent_rec$dep_rel=="obj"){
						oper<-""
						if(token %in% c("best","greatest","highest")){
							oper<-"max"
						}else if(token %in% c("least","lowest","smallest","worst")){
							oper<-"min"
						}else if(token %in% c("total","sum")){
							oper<-"sum"
						}else if(token %in% c("average")){
							oper<-"avg"
						}else if(oper_token=="greater"){
							if(oper_rec$token_id==1){
								oper<-" > "
							}else if(df[as.integer(oper_rec$token_id)-1,"token"] %in% c("no","not")){
								oper<-" <= "
							}else{
								oper<-" > "
							}
						}else if(oper_token=="smaller"){
							if(oper_rec$token_id==1){
								oper<-" < "
							}else if(df[as.integer(oper_rec$token_id)-1,"token"] %in% c("no","not")){
								oper<-" >= "
							}else{
								oper<-" < "
							}
						}			
						sibling_rec<-df[df$head_token_id==parent_rec$token_id & df$token_id != cur_rec$token_id & df$xpos=="JJ" & df$processed=="no",]
						if(nrow(sibling_rec)>0){
							token1<-sibling_rec[1,"token"]
							oper1<-""
							if(token1 %in% c("total","sum")){
								oper1<-"sum"
							}else if(token1 %in% c("average")){
								oper1<-"avg"
							}
							if(nchar(oper1)>0){
								child_rec<-df[df$head_token_id==cur_rec$token_id & df$token_id != cur_rec$token_id & df$xpos=="CD" & df$processed=="no",]
								if(nrow(child_rec)>0){
									column<-paste0(oper1,"(",column,")")
									having_clause[length(having_clause)+1]<-paste0(column,oper,child_rec$token[1])
								}else{
									column<-paste0(oper1,"(",column,")")
									having_clause[length(having_clause)+1]<-paste0(column,"=",oper,"(",column,")")
									df[parent_rec$token_id,"processed"]<-"yes"
									df[cur_rec$token_id,"processed"]<-"yes"
									df[sibling_rec$token_id,"processed"]<-"yes"
								}
							}
						}else{
							# grouping (having clause)
							if(oper %in% c("max","min")){
								#having_clause[length(where_clause)+1]<-paste0(column,"=",oper,"(",column,")")
								where_clause[length(where_clause)+1]<-paste0(column,"=",oper,"(",column,")")
								df[parent_rec$token_id,"processed"]<-"yes"
								df[cur_rec$token_id,"processed"]<-"yes"
							}else if(oper %in% c("sum","avg")){
								value_rec<-parent_rec
								value<-""
								while(TRUE){
									value_rec<-df[df$head_token_id==value_rec$token_id & df$token_id != cur_rec$token_id & df$itemtype=="" & df$processed=="no",]
									if(nrow(value_rec)>0){
										if(value_rec$xpos=="CD"){
											value<-value_rec$token
											break
										}
									}else{
										break
									}
								}
								if(nchar(value)>0){
									having_clause[length(having_clause)+1]<-paste0(oper,"(",column,") > ",value)
									df[parent_rec$token_id,"processed"]<-"yes"
									df[cur_rec$token_id,"processed"]<-"yes"
									df[value_rec$token_id,"processed"]<-"yes"
								}							
							}
						}
					}else{
						if(token=="many"){
							select_clause[length(select_clause)+1]<-paste0("count(",column,")")
							df[parent_rec$token_id,"processed"]<-"yes"
							df[cur_rec$token_id,"processed"]<-"yes"
						}else if(token %in% c("total","sum")){
							select_clause[length(select_clause)+1]<-paste0("sum(",column,")")
							df[parent_rec$token_id,"processed"]<-"yes"
							df[cur_rec$token_id,"processed"]<-"yes"
						}else if(token %in% c("average")){
							select_clause[length(select_clause)+1]<-paste0("avg(",column,")")
							df[parent_rec$token_id,"processed"]<-"yes"
							df[cur_rec$token_id,"processed"]<-"yes"
						}
					}
				}
			}
		}
	}
	myList<-list()
	myList[[1]]<-df
	myList[[2]]<-select_clause
	myList[[3]]<-where_clause
	myList[[4]]<-having_clause
	return(myList)
}

get_parent<-function(df,cur_rec,itemtype=""){
	head_token_id<-cur_rec$head_token_id
	found<-FALSE
	parent_rec<-df[df$token_id==head_token_id & df$processed=="no",]
	while(nrow(parent_rec)>0){
		if(substr(parent_rec$xpos,1,2)=="NN"){
			if(itemtype == "" | itemtype != "" & parent_rec$itemtype == itemtype){
				found<-TRUE
				break
			}
		}
		head_token_id<-parent_rec$head_token_id
		parent_rec<-df[df$token_id==head_token_id & df$processed=="no",]
	}
	if(!found){
		parent_rec<-data.frame()
	}
	return(parent_rec)
}

get_chunks<-function(df){
	myList<-list()
	myList[["noun"]]<-get_pos(df,"NN")
	myList[["adj"]]<-get_pos(df,c("JJ","RB"))
	myList[["nbr"]]<-get_pos(df,"CD")
	myList[["prep"]]<-get_pos(df,"IN")
	gc()
	return(myList)
}

get_pos<-function(df,xpos){
	df_nouns<-df[substr(df$xpos,1,2) %in% xpos,]
	n<-nrow(df_nouns)
	myList<-list()
	if(n>0){
		for(i in 1:n){
			myList[[i]]<-extract_subtree(df,df_nouns[i,"token_id"])
		}
	}
	gc()
	return(myList)
}


extract_subtree<-function(df,ind,excl=c("NN","JJ","RB","CD","IN")){
	tree<-data.frame(token_id=integer(),token=character(),level=integer(),dep_rel=character(),xpos=character(),head_token_id=integer(),"item"=character(),"itemtype"=character(),stringsAsFactors=FALSE);
	myList<-list()
	level=0
	while(length(ind)>0){
		if(ind[1]!=0){
			level<-level+1
			cur_rec<-df[ind,]
			cur_rec$token_id<-as.integer(cur_rec$token_id)
			cur_rec$level=level
			tree<-rbind(tree,cur_rec[,c("token_id","token","level","head_token_id","dep_rel","upos","xpos","item","itemtype")])
			ind<-which(df$head_token_id %in% ind & !(substr(df$xpos,1,2) %in% excl))
		}
	}
	return(tree)
}

parse_question<-function(txt){
	STOPWORDS<-c("a","the","here","there","it","he","she","they","is","are","which","what","who")
	FILE_MODEL<-"english-ud-2.0-170801.udpipe"
	
	words<-unlist(strsplit(txt," "))
	words<-words[!(tolower(words) %in% STOPWORDS)]
	words<-replace_words(words)
	txt<-paste(words,collapse=" ")
	if(!exists("tagger")){
		tagger <<- udpipe_load_model(FILE_MODEL)
	}
	doc <- udpipe_annotate(tagger, txt)
	df<-as.data.frame(doc)
	gc()
	return(df[,4:12])
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
	
	db$Column<-as.character(db$Column)
	db$Table<-as.character(db$Table)
	matches<-stringsim(tolower(token),tolower(db[,type]),method='jw', p=0.1)
	if(max(matches) >= THRESHOLD){
		ind<-min(which(matches==max(matches)))
		db_item<-db[ind,type]
	}else{
		db_item<-""
	}
	return(db_item)
}

map_dbitems<-function(df,db,pos="ALL"){
	df$item<-""
	df$itemtype<-""
	if(pos=="ALL"){
		ind<-df$token_id
	}else{
		ind<-df[substr(df$xpos,1,2)=="NN","token_id"]
	}
	n<-length(ind)
	for(i in 1:n){
		token<-df[ind[i],"token"]
		item<-db_match(db,token,"Column")
		if(item!=""){
			df[ind[i],"item"]<-item
			df[ind[i],"itemtype"]<-"column"
		}else{
			item<-db_match(db,token,"Table")
			if(item!=""){
				df[ind[i],"item"]<-item
				df[ind[i],"itemtype"]<-"table"
			}
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
		mydf$token[ind]<-"producer"
		mytxt<-paste(mydf$token,collapse=" ")
		mydf<-parse_question(mytxt)
		df[,5:9]<-mydf[,5:9]
	}
	gc()
	return(df)
}

build_joins<-function(cur_db){
	joins<-data.table(tbl1=character(),tbl2=character(),joinby1=character(),joinby2=character())
	# add joins
	dups<-count(cur_db,"Column")
	dups<-dups[dups$freq>=2,]
	n<-nrow(dups)
	if(n>0){
		for(i in 1:n){
			joinby<-dups[i,"Column"]
			joinby_rec<-cur_db[cur_db$Column == joinby,]
			N<-nrow(joinby_rec)
			for(j in 1:(N-1)){
				for(k in (j+1):N){
					joins<-rbindlist(list(joins,list(as.character(joinby_rec[j,"Table"]),as.character(joinby_rec[k,"Table"]),as.character(joinby),as.character(joinby))))
				}
			}
				
		}
	}
	
	return(joins)
}

optimize_joins<-function(cols,joins,cur_db){
	# get the existing tables with required columns
	tbls<-as.character(unique(cur_db[tolower(cur_db$Column) %in% tolower(cols),"Table"]))
	if(length(tbls)==1){
		# if all columns in a single table
		joins<-joins[0,]
		joins<-rbindlist(list(joins,list(tbls[1],tbls[1],"","")))
		colnames(joins)<-c("tbl1","tbl2","joinby1","joinby2")
	}else{
		# remove unneeded leaves
		repeat{
			tbls_freq<-count(c(joins$tbl1,joins$tbl2))
			tbls_todrop<-tbls_freq[tbls_freq$freq==1 & !(tolower(tbls_freq$x) %in% tolower(tbls)),"x"]
			if(length(tbls_todrop)>0){
				joins<-joins[!(tolower(joins$tbl1) %in% tolower(tbls_todrop)) & !(tolower(joins$tbl2) %in% tolower(tbls_todrop)),]
			}else{
				break
			}
		}
	}
	gc()
	return(joins)
}

verify_joins<-function(cols,joins,cur_db){
	
	myList<-list()
	myList[[1]]<-""
	g<-graph_from_edgelist(as.matrix(joins[,1:2]),directed=FALSE)
	g_mst<-mst(g)
	# verifu that all required columns accessible
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
			id<-0
			clause<-as.character(joins[1,"tbl1"])
			from_joins<-rbindlist(list(from_joins,list(clause,"","","")))
			while(nrow(joins[(tolower(joins$tbl1) %in% tolower(tbls) | tolower(joins$tbl2) %in% tolower(tbls) & joins$id != id) & joins$processed == "no",])>0){
				cur_rec<-joins[(tolower(joins$tbl1) %in% tolower(tbls) | tolower(joins$tbl2) %in% tolower(tbls) & joins$id != id)  & joins$processed == "no",][1,]
				if(length(tbls[tolower(cur_rec$tbl1) %in% tolower(tbls)])>0){
					if(length(tbls[tolower(cur_rec$tbl2) %in% tolower(tbls)])>0){
						clause<-paste0(clause, "on ",cur_rec$tbl1,".",cur_rec$joinby1,"=",cur_rec$tbl2,".",cur_rec$joinby2)
						from_joins<-rbindlist(list(from_joins,list(cur_rec$tbl1,cur_rec$tbl2,cur_rec$joinby1,cur_rec$joinby2)))
					}else{
						tbls[length(tbls)+1]<-cur_rec$tbl2
						clause<-paste0(clause," inner join ",cur_rec$tbl2," on ",cur_rec$tbl1,".",cur_rec$joinby1,"=",cur_rec$tbl2,".",cur_rec$joinby2)
						from_joins<-rbindlist(list(from_joins,list(cur_rec$tbl2,cur_rec$tbl1,cur_rec$joinby2,cur_rec$joinby1)))
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
	myList<-list()
	myList[[1]]<-clause
	myList[[2]]<-from_joins
	return(myList)
}	

join_clause_mgr<-function(cols,cur_db,joins){
	
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
		myList[[1]]<-"Reprase the request: could not locate required tables"
	}
	gc()
	return(myList)
}

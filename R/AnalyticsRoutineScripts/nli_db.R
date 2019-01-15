
nlidb_mgr<-function(txt,db){
	df<-parse_question(txt)
	# map nouns to db items
	df<-map_nouns(df,db)
	df$processed<-"no"
	#get pos groups
	chunks<-get_chunks(df)
	
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
	where_part<-c(where_part,out[[3]])
	having_part<-out[[4]]
	
	# the rest of the select
	select_part2<-analyze_noun(df)
	if(select_part2[1] != ""){
		sql<-build_sql(select_part1,select_part2,where_part,group_part,having_part)
	}else{
		sql<-"Rephrase the request"
	}
	gc()
	return(sql)
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

build_sql<-function(select_part1,select_part2,where_part,group_part,having_part){
	# process select
	tbl<-select_part2[1]
	if(length(select_part2)==1){
		 if(length(select_part1)==0){
			select_items<-"*"
		 }else{
			select_items<-select_part1
		 }
	}else{
		select_items<-c(select_part2[2:length(select_part2)],select_part1)
	}
	# add groups to select if any
	if(length(group_part)>0){
		select_items<-c(group_part,select_items)
	}
	select_clause<-paste0("select ",paste(select_items,collapse=",")," from ",tbl)
	# process where/group
	if(length(where_part) != 0){
		where_clause<-paste0("where ",paste(where_part,collapse=" and "))
	}else{
		where_clause<-""
	}
	having_clause<-""
	if(length(having_part)>0){
		# group clause includes all items in the having section
		group_items<-select_part2[2:length(select_part2)]
		group_clause<-paste0("group by ",paste(group_items,collapse=","))
		
		# where clause included in the having clause
		#having_part<-c(where_part,having_part)
		having_clause<-paste0("having ",paste(having_part,collapse=" and "))
		#where_clause<-""
		
		select_items<-select_part2[2:length(select_part2)]
		select_clause<-paste0("select ",paste(select_items,collapse=",")," from ",tbl)
	}else if(length(group_part) != 0){
			group_clause<-paste0("group by ",paste(group_part,collapse=","))
	}else{
		group_clause<-""
	}
	sql<-paste(select_clause,where_clause,group_clause,having_clause,collapse=" ")
	return(sql)
}

analyze_noun<-function(df){
	tbl<-df[substr(df$xpos,1,2)=="NN" & df$itemtype=="table" & df$processed=="no","item"]
	clmns<-df[substr(df$xpos,1,2)=="NN" & df$itemtype=="column" & df$processed=="no","item"]
	if(length(tbl)==0){
		out<-c("",clmns)
	}else{
		out<-c(tbl,clmns)
	}
	return(out)
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
							where_clause[length(where_clause)+1]<-paste0(sibling_rec[1,"item"],"='",value,"'")
							df[parent_rec$token_id,"processed"]<-"yes"
							df[sibling_rec$token_id,"processed"]<-"yes"
							# process conjunction if any
							conj_rec<-df[df$head_token_id==parent_rec$token_id & df$token_id != cur_rec$token_id & substr(df$xpos,1,2)=="NN" & df$dep_rel %in% c("conj") & df$itemtype=="",]
							k<-nrow(conj_rec)
							if(k>0){
								for(j in 1:k){
									child_rec<-df[df$head_token_id==conj_rec$token_id[j] & df$itemtype=="column" & df$processed=="no",]
									if(nrow(child_rec)>0){
										where_clause[length(where_clause)+1]<-paste0(child_rec[1,"item"],"='",conj_rec$token,"'")
										df[conj_rec$token_id,"processed"]<-"yes"
										df[child_rec$token_id,"processed"]<-"yes"
									}
								}
							}
						}
					}
				} else if(tolower(parent_rec$itemtype)=="column"){
					oper_rec<-df[df$head_token_id==parent_rec$token_id & df$token_id!=cur_rec$token_id & df$processed=="no" & df$itemtype == "",]
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
						}else if(oper_rec$dep_rel %in% c("flat","compound","appos")){
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
							where_clause[length(where_clause)+1]<-paste0(parent_rec$token,"='",value,"'")
							df[parent_rec$token_id,"processed"]<-"yes"
							df[oper_rec$token_id,"processed"]<-"yes"
							for(j in 1:length(token_ids)){
								df[token_ids[j],"processed"]<-"yes"
							}
						}
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
					column<-parent_rec$item
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
								column<-paste0(oper1,"(",column,")")
								having_clause[length(where_clause)+1]<-paste0(column,"=",oper,"(",column,")")
								df[parent_rec$token_id,"processed"]<-"yes"
								df[cur_rec$token_id,"processed"]<-"yes"
								df[sibling_rec$token_id,"processed"]<-"yes"
							}
						}else{
							# grouping (having clause)
							if(oper %in% c("max","min")){
								having_clause[length(where_clause)+1]<-paste0(column,"=",oper,"(",column,")")
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
	STOPWORDS<-c("a","the","here","there","it","he","she","they","is","are","which","what")
	FILE_MODEL<-"english-ud-2.0-170801.udpipe"
	library(udpipe)
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
	library(stringdist)
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

map_nouns<-function(df,db){
	df$item<-""
	df$itemtype<-""
	ind<-df[substr(df$xpos,1,2)=="NN","token_id"]
	#ind<-df$token_id
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
trim <- function( x ) {
  gsub("(^[[:space:]]+|[[:space:]]+$)", "", x)
}

build_pixel_single_select<-function(select_part2,req_tbls,cur_db){
	pixel_single_select<-data.table(part=character(),item1=character(),item2=character(),item3=character(),item4=character(),item5=character(),item6=character(),item7=character())
	if(length(select_part2)!=0){
		n<-length(select_part2)
		for(i in 1:n){
			clmn<-select_part2[i]
			tbls<-cur_db[tolower(cur_db$Column) == tolower(clmn) & tolower(cur_db$Table) %in% tolower(req_tbls),"Table"]
			if(length(tbls)>0){
				tbl<-tbls[1]
				pixel_single_select<-rbindlist(list(pixel_single_select,list("select",tbl,clmn,"","","","","")))
			}
		}
	}
	gc()
	return(pixel_single_select)
}

build_pixel_group<-function(group_part,req_tbls,cur_db){
	pixel_single_select<-data.table(part=character(),item1=character(),item2=character(),item3=character(),item4=character(),item5=character(),item6=character(),item7=character())
	if(length(group_part)!=0){
		n<-length(group_part)
		for(i in 1:n){
			clmn<-group_part[i]
			tbls<-cur_db[tolower(cur_db$Column) == tolower(clmn) & tolower(cur_db$Table) %in% tolower(req_tbls),"Table"]
			if(length(tbls)>0){
				tbl<-tbls[1]
				pixel_single_select<-rbindlist(list(pixel_single_select,list("group",tbl,clmn,"","","","","")))
			}
		}
	}
	gc()
	return(pixel_single_select)
}

build_pixel_aggr_select<-function(select_part1,req_tbls,cur_db){
	pixel_aggr_select<-data.table(part=character(),item1=character(),item2=character(),item3=character(),item4=character(),item5=character(),item6=character(),item7=character())
	if(length(select_part1)!=0){
		n<-length(select_part1)
		for(i in 1:n){
			if(length(unlist(strsplit(select_part1[i],"as")))==2){
				x<-unlist(strsplit(select_part1[i],"as"))
				alias<-trim(x[2])
				pos1<-unlist(gregexpr(pattern="[(]",x[1]))
				pos2<-unlist(gregexpr(pattern="[)]",x[1]))
				aggr<-trim(substr(x[1],1,pos1-1))
				clmn<-trim(substr(x[1],pos1+1,pos2-1))
				tbls<-cur_db[tolower(cur_db$Column) == tolower(clmn) & tolower(cur_db$Table) %in% tolower(req_tbls),"Table"]
				if(length(tbls)>0){
					tbl<-tbls[1]
					pixel_aggr_select<-rbindlist(list(pixel_aggr_select,list("select",tbl,clmn,aggr,alias,"","","")))
				}
			}
		}
	}
	gc()
	return(pixel_aggr_select)
}

build_pixel_having<-function(having_part,req_tbls,cur_db){
	pixel_having<-data.table(part=character(),item1=character(),item2=character(),item3=character(),item4=character(),item5=character(),item6=character(),item7=character())
	if(length(having_part)!=0){
		n<-length(having_part)
		for(i in 1:n){
			if(length(unlist(strsplit(having_part[i],"between")))==2){
				# handle between
				oper<-"between"
				x<-unlist(strsplit(having_part[i],"between"))
				
				pos1<-unlist(gregexpr(pattern="[(]",x[1]))
				if(pos1>0){
					pos2<-unlist(gregexpr(pattern="[)]",x[1]))
					aggr<-trim(substr(x[1],1,pos1-1))
					clmn<-trim(substr(x[1],pos1+1,pos2-1))
				}else{
					aggr<-""
					clmn<-trim(x[1])
				}
				tbls<-cur_db[cur_db$Column == clmn & cur_db$Table %in% req_tbls,"Table"]
				if(length(tbls)>0){
					tbl<-tbls[1]
					if(length(unlist(strsplit(x[2],"and")))==2){
						y<-unlist(strsplit(x[2],"and"))
						value<-trim(y[1])
						value2<-trim(y[2])
						# to do: add aggregates for the right part
						pixel_having<-rbindlist(list(pixel_having,list("having",tbl,clmn,aggr,oper,value,value2,"")))
					}
				}
			}else if(length(unlist(strsplit(having_part[i],">=")))==2){
				oper<-">="
				x<-unlist(strsplit(having_part[i],">="))
				pos1<-unlist(gregexpr(pattern="[(]",x[1]))
				if(pos1>0){
					pos2<-unlist(gregexpr(pattern="[)]",x[1]))
					aggr<-trim(substr(x[1],1,pos1-1))
					clmn<-trim(substr(x[1],pos1+1,pos2-1))
				}else{
					aggr<-""
					clmn<-trim(x[1])
				}
				value<-trim(x[2])
				pos1<-unlist(gregexpr(pattern="[(]",value))
				if(pos1>0){
					pos2<-unlist(gregexpr(pattern="[)]",value))
					aggr2<-trim(substr(value,1,pos1-1))
					clmn2<-trim(substr(value,pos1+1,pos2-1))
					tbls<-cur_db[tolower(cur_db$Column) == tolower(clmn2) & tolower(cur_db$Table) %in% tolower(req_tbls),"Table"]
					if(length(tbls)>0){
						tbl2<-tbls[1]
					}else{
						tbl2<-""
					}
				}else{
					aggr2<-""
					tbl2<-value
					clmn2<-""
				}
				tbls<-cur_db[tolower(cur_db$Column) == tolower(clmn) & tolower(cur_db$Table) %in% tolower(req_tbls),"Table"]
				if(length(tbls)>0){
					tbl<-tbls[1]
					pixel_having<-rbindlist(list(pixel_having,list("having",tbl,clmn,aggr,oper,tbl2,clmn2,aggr2)))
				}				
			}else if(length(unlist(strsplit(having_part[i],"<=")))==2){
				oper<-"<="
				x<-unlist(strsplit(having_part[i],"<="))
				pos1<-unlist(gregexpr(pattern="[(]",x[1]))
				if(pos1>0){
					pos2<-unlist(gregexpr(pattern="[)]",x[1]))
					aggr<-trim(substr(x[1],1,pos1-1))
					clmn<-trim(substr(x[1],pos1+1,pos2-1))
				}else{
					aggr<-""
					clmn<-trim(x[1])
				}
				value<-trim(x[2])
				pos1<-unlist(gregexpr(pattern="[(]",value))
				if(pos1>0){
					pos2<-unlist(gregexpr(pattern="[)]",value))
					aggr2<-trim(substr(value,1,pos1-1))
					clmn2<-trim(substr(value,pos1+1,pos2-1))
					tbls<-cur_db[tolower(cur_db$Column) == tolower(clmn2) & tolower(cur_db$Table) %in% tolower(req_tbls),"Table"]
					if(length(tbls)>0){
						tbl2<-tbls[1]
					}else{
						tbl2<-""
					}
				}else{
					aggr2<-""
					tbl2<-value
					clmn2<-""
				}
				tbls<-cur_db[tolower(cur_db$Column) == tolower(clmn) & tolower(cur_db$Table) %in% tolower(req_tbls),"Table"]
				if(length(tbls)>0){
					tbl<-tbls[1]
					pixel_having<-rbindlist(list(pixel_having,list("having",tbl,clmn,aggr,oper,tbl2,clmn2,aggr2)))
				}	
			}else if(length(unlist(strsplit(having_part[i],">")))==2){
				oper<-">"
				x<-unlist(strsplit(having_part[i],">"))
				pos1<-unlist(gregexpr(pattern="[(]",x[1]))
				if(pos1>0){
					pos2<-unlist(gregexpr(pattern="[)]",x[1]))
					aggr<-trim(substr(x[1],1,pos1-1))
					clmn<-trim(substr(x[1],pos1+1,pos2-1))
				}else{
					aggr<-""
					clmn<-trim(x[1])
				}
				value<-trim(x[2])
				pos1<-unlist(gregexpr(pattern="[(]",value))
				if(pos1>0){
					pos2<-unlist(gregexpr(pattern="[)]",value))
					aggr2<-trim(substr(value,1,pos1-1))
					clmn2<-trim(substr(value,pos1+1,pos2-1))
					tbls<-cur_db[tolower(cur_db$Column) == tolower(clmn2) & tolower(cur_db$Table) %in% tolower(req_tbls),"Table"]
					if(length(tbls)>0){
						tbl2<-tbls[1]
					}else{
						tbl2<-""
					}
				}else{
					aggr2<-""
					tbl2<-value
					clmn2<-""
				}
				tbls<-cur_db[tolower(cur_db$Column) == tolower(clmn) & tolower(cur_db$Table) %in% tolower(req_tbls),"Table"]
				if(length(tbls)>0){
					tbl<-tbls[1]
					pixel_having<-rbindlist(list(pixel_having,list("having",tbl,clmn,aggr,oper,tbl2,clmn2,aggr2)))
				}	
			}else if(length(unlist(strsplit(having_part[i],"<")))==2){
				oper<-"<"
				x<-unlist(strsplit(having_part[i],"<"))
				pos1<-unlist(gregexpr(pattern="[(]",x[1]))
				if(pos1>0){
					pos2<-unlist(gregexpr(pattern="[)]",x[1]))
					aggr<-trim(substr(x[1],1,pos1-1))
					clmn<-trim(substr(x[1],pos1+1,pos2-1))
				}else{
					aggr<-""
					clmn<-trim(x[1])
				}
				value<-trim(x[2])
				pos1<-unlist(gregexpr(pattern="[(]",value))
				if(pos1>0){
					pos2<-unlist(gregexpr(pattern="[)]",value))
					aggr2<-trim(substr(value,1,pos1-1))
					clmn2<-trim(substr(value,pos1+1,pos2-1))
					tbls<-cur_db[tolower(cur_db$Column) == tolower(clmn2) & tolower(cur_db$Table) %in% tolower(req_tbls),"Table"]
					if(length(tbls)>0){
						tbl2<-tbls[1]
					}else{
						tbl2<-""
					}
				}else{
					aggr2<-""
					tbl2<-value
					clmn2<-""
				}
				tbls<-cur_db[tolower(cur_db$Column) == tolower(clmn) & tolower(cur_db$Table) %in% tolower(req_tbls),"Table"]
				if(length(tbls)>0){
					tbl<-tbls[1]
					pixel_having<-rbindlist(list(pixel_having,list("having",tbl,clmn,aggr,oper,tbl2,clmn2,aggr2)))
				}	
			}else if(length(unlist(strsplit(having_part[i],"=")))==2){
				oper<-"="
				x<-unlist(strsplit(having_part[i],"="))
				pos1<-unlist(gregexpr(pattern="[(]",x[1]))
				if(pos1>0){
					pos2<-unlist(gregexpr(pattern="[)]",x[1]))
					aggr<-trim(substr(x[1],1,pos1-1))
					clmn<-trim(substr(x[1],pos1+1,pos2-1))
				}else{
					aggr<-""
					clmn<-trim(x[1])
				}
				value<-trim(x[2])
				pos1<-unlist(gregexpr(pattern="[(]",value))
				if(length(pos1)>0){
					pos2<-unlist(gregexpr(pattern="[)]",value))
					aggr2<-trim(substr(value,1,pos1[1]-1))
					# it is actual value for having
					clmn2<-trim(substr(value,pos1[1]+1,pos2[length(pos2)]-1))
					# here is the actual column to validate
					clmn_val<-trim(substr(value,pos1[length(pos1)]+1,pos2[1]-1))
					tbls<-cur_db[tolower(cur_db$Column) == tolower(clmn_val) & tolower(cur_db$Table) %in% tolower(req_tbls),"Table"]
					if(length(tbls)>0){
						tbl2<-tbls[1]
					}else{
						tbl2<-""
					}
				}else{
					aggr2<-""
					tbl2<-value
					clmn2<-""
				}
				tbls<-cur_db[tolower(cur_db$Column) == tolower(clmn) & tolower(cur_db$Table) %in% tolower(req_tbls),"Table"]
				if(length(tbls)>0){
					tbl<-tbls[1]
					pixel_having<-rbindlist(list(pixel_having,list("having",tbl,clmn,aggr,oper,tbl2,clmn2,aggr2)))
				}	
			}
		}
	}
	gc()
	return(pixel_having)
}


build_pixel_where<-function(where_part,req_tbls,cur_db){
	pixel_where<-data.table(part=character(),item1=character(),item2=character(),item3=character(),item4=character(),item5=character(),item6=character(),item7=character())
	if(length(where_part)!=0){
		n<-length(where_part)
		for(i in 1:n){
			if(length(unlist(strsplit(where_part[i],"between")))==2){
				# handle between
				oper<-"between"
				x<-unlist(strsplit(where_part[i],"between"))
				clmn<-trim(x[1])
				tbls<-cur_db[tolower(cur_db$Column) == tolower(clmn) & tolower(cur_db$Table) %in% tolower(req_tbls),"Table"]
				if(length(tbls)>0){
					tbl<-tbls[1]
					if(length(unlist(strsplit(x[2],"and")))==2){
						y<-unlist(strsplit(x[2],"and"))
						value<-trim(y[1])
						value2<-trim(y[2])
						pixel_where<-rbindlist(list(pixel_where,list("where",tbl,clmn,oper,value,value2,"","")))
					}
				}
			}else if(length(unlist(strsplit(where_part[i],">=")))==2){
				# handle >=
				oper<-">="
				x<-unlist(strsplit(where_part[i],">="))
				clmn<-trim(x[1])
				value<-trim(x[2])
				tbls<-cur_db[tolower(cur_db$Column) == tolower(clmn) & tolower(cur_db$Table) %in% tolower(req_tbls),"Table"]
				if(length(tbls)>0){
					tbl<-tbls[1]
					pixel_where<-rbindlist(list(pixel_where,list("where",tbl,clmn,oper,value,"","","")))
				}
			}else if(length(unlist(strsplit(where_part[i],"<=")))==2){
				# handle <=
				oper<-"<="
				x<-unlist(strsplit(where_part[i],"<="))
				clmn<-trim(x[1])
				value<-trim(x[2])
				tbls<-cur_db[tolower(cur_db$Column) == tolower(clmn) & tolower(cur_db$Table) %in% tolower(req_tbls),"Table"]
				if(length(tbls)>0){
					tbl<-tbls[1]
					pixel_where<-rbindlist(list(pixel_where,list("where",tbl,clmn,oper,value,"","","")))
				}
			}else if(length(unlist(strsplit(where_part[i],">")))==2){
				# handle >
				oper<-">"
				x<-unlist(strsplit(where_part[i],">"))
				clmn<-trim(x[1])
				value<-trim(x[2])
				tbls<-cur_db[tolower(cur_db$Column) == tolower(clmn) & tolower(cur_db$Table) %in% tolower(req_tbls),"Table"]
				if(length(tbls)>0){
					tbl<-tbls[1]
					pixel_where<-rbindlist(list(pixel_where,list("where",tbl,clmn,oper,value,"","","")))
				}
			}else if(length(unlist(strsplit(where_part[i],"<")))==2){
				# handle <
				oper<-"<"
				x<-unlist(strsplit(where_part[i],"<"))
				clmn<-trim(x[1])
				value<-trim(x[2])
				tbls<-cur_db[tolower(cur_db$Column) == tolower(clmn) & tolower(cur_db$Table) %in% tolower(req_tbls),"Table"]
				if(length(tbls)>0){
					tbl<-tbls[1]
					pixel_where<-rbindlist(list(pixel_where,list("where",tbl,clmn,oper,value,"","","")))
				}
			}else if(length(unlist(strsplit(where_part[i],"=")))==2){
				# handle =
				oper<-"="
				x<-unlist(strsplit(where_part[i],"="))
				clmn<-trim(x[1])
				value<-trim(x[2])
				tbls<-cur_db[tolower(cur_db$Column) == tolower(clmn) & tolower(cur_db$Table) %in% tolower(req_tbls),"Table"]
				if(length(tbls)>0){
					tbl<-tbls[1]
					pixel_where<-rbindlist(list(pixel_where,list("where",tbl,clmn,oper,value,"","","")))
				}
			}
		}
	}
	gc()
	return(pixel_where)
}

build_pixel_from<-function(from_joins){
	from_pixel<-data.table(part=character(),item1=character(),item2=character(),item3=character(),item4=character(),item5=character(),item6=character(),item7=character())
	if(nrow(from_joins)>0){
		from_joins$joinby1<-as.character(from_joins$joinby1)
		from_joins$joinby2<-as.character(from_joins$joinby2)
		n<-nrow(from_joins)
		for(i in 1:n){
			from_pixel<-rbindlist(list(from_pixel,list("from",from_joins$tbl1[i],from_joins$joinby1[i],from_joins$tbl2[i],from_joins$joinby2[i],"","","")))
		}
	}
	gc()
	return(from_pixel)
}

build_pixel<-function(appid,pixel_aggr_select,pixel_single_select,pixel_where,pixel_group,pixel_having,pixel_from){
	pixel<-data.table(part=character(),item1=character(),item2=character(),item3=character(),item4=character(),item5=character(),item6=character(),item7=character())
	if(nrow(pixel_single_select)>0){
		pixel<-rbind(pixel,pixel_single_select)
	}
	if(nrow(pixel_aggr_select)>0){
		pixel<-rbind(pixel,pixel_aggr_select)
	}
	if(nrow(pixel_from)>0){
		pixel<-rbind(pixel,pixel_from)
	}
	if(nrow(pixel_where)>0){
		pixel<-rbind(pixel,pixel_where)
	}
	if(nrow(pixel_group)>0){
		pixel<-rbind(pixel,pixel_group)
		if(nrow(pixel_having)>0){
			pixel<-rbind(pixel,pixel_having)
		}
	}
	if(nrow(pixel)>0){
		pixel$appid<-appid
		pixel<-pixel[,c(9,1,2,3,4,5,6,7,8)]
	}
	gc()
	return(pixel)
}
	

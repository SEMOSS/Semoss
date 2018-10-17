###########################################################################################3



is.letter <- function(x) grepl("[[:alpha:]]", x)

get_wiki_ids<-function(values,m){
# Description
# Retrieve wiki data item identification for the given set of character values
# Arguments
# values - an array of values to master to a concept
# m - a number of random instances of the values array to be used
	scale<-5
	xpage<-"disambiguation page"
	#n<-length(values)
	#if(m > n){m=n}
	#idx <- sample(1:n, m)
	r<-array()
	k<-0
	for(i in 1:m){
		value<-as.character(values[i])
		if(!is.na(value)){
			if(nchar(value) > 0){
				item<-find_item(value)
				l<-length(item)
				if(l > 0) {
					for(j in 1:l){
						desc<-item[[j]]$description
						if(!is.null(desc)){
							if(str_detect(tolower(desc),xpage)){
								next
							}else{
								id<-item[[j]]$id
								k<-k+1
								r[k]<-id
								break
							}
						} 
					}
				}
				
			}
		}
		
	}
	rm(item,value,idx)
	gc()
	return(r)
}

span<-function(a){
	if(length(a) == 1 & is.na(a[1])){x<-0}
	else{x<-length(a)}
	return(x)
}

get_claims<-function(items,props){
# Description
# Retrive wiki claims about a given items
# Arguments
# items - an array of wiki items
# props - an array of properties to identify claims
# selected - an output array where claims assembled
	n<-length(props)
    if(n > 0){	
		claims<-extract_claims(items,props)
		m<-length(claims)
		selected<-array()
		# loop by item
		for(j in 1:m){
			perform<-c(TRUE,TRUE,TRUE,TRUE,TRUE)
			# loop by properties
			for(i in 1:n){
				if(perform[i]){
					if(!class(claims[[j]][[i]]) == "logical"){
						if(props[i] == "P373"){
							concept<-claims[[j]][[i]]$mainsnak$datavalue$value
							selected[span(selected)+1]<-concept
						}else{
							if(props[i] == "P106"){
								perform[3]<-FALSE
							} else if(props[i] == "P31"){
								perform[5]<-FALSE
							}
							classid<-claims[[j]][[i]]$mainsnak$datavalue$value$id
							class_item<-get_item(classid)
							q<-length(class_item)		
							if(q > 0){
								for(l in 1:q){
									concept<-class_item[[l]]$label$en$value
									selected[span(selected)+1]<-concept
								}
							}
							rm(classid,class_item,q)
						}
					}
				}
			}
		}
		rm(claims,perform,concept)
	}
	gc()
	return(selected)
}

get_concept<-function(qid){
# Description
# Retrieve all claims for a fiven array if items
# Arguments
# qid - an array of wiki items to master to a concept
	PROPS<-c("P373","P106","P31","P279","P361")
	items<-get_item(qid)
	selected<-get_claims(items,PROPS)
	rm(qid,items)
	gc()
	return(selected)
}

most_frequent_concept<-function(concepts,topN){
# Description
# Identify the most frequent concepts
# Arguments
# concepts - an array of given concepts
# topN - the number of top concepts to identify
my.df <- do.call("rbind", lapply(concepts, data.frame))
my.df<-my.df[is.letter(my.df[,1]),]

o<-count(my.df,names(my.df)[1])
names(o)<-c("concept","freq")
o[,2]<-round(o[,2]/length(my.df),4)
o<-o[rank(o$freq) %in% sort(unique(rank(o$freq)),decreasing=TRUE)[1:topN],]
o$concept<-as.character(o$concept)
o<-o[order(-o$freq,o$concept),]
rm(my.df)
return(o)
}

concept_mgr<-function(df,cols,topN,m){
# Description - manages process to infer the concept name from instances of a given dataset
# Arguments
# df - dataframe containing the dataset to master
# cols - is an array of column number to process
# topN - the number of top concept schoices
# m - number of random values to select from the set
# Output - dataframe with column numbers and derived concepts
  
  options(java.parameters = "-Xmx8000m")
  library(WikidataR)
  library(stringr)
  library(plyr)
	
	BASE<-"https://en.wikipedia.org/wiki/"
	nbr<-length(cols)
	r<-data.frame(Original_Column=as.numeric(),Predicted_Concept=as.character(),URL=as.character(),Prob=numeric(),stringsAsFactors = FALSE)
	k<-0
	if(nbr > 0){
		columns<-colnames(df)[cols]
		for(j in 1:nbr){
			wdf<-array()
			concepts <- array()
			values<-gsub("_"," ",unique(df[[cols[j]]]))
			wdf<-get_wiki_ids(values,m)
			if(!is.na(wdf[1])){
				concepts<-get_concept(wdf)
				if(!is.na(concepts[1])){
					top_concepts<-most_frequent_concept(concepts,topN)
					N<-nrow(top_concepts)
					for(i in 1:N){
						k<-k+1
						r[k,1]<-columns[j]
						r[k,2]<-top_concepts[i,1]
						r[k,3]<-paste0(BASE,gsub(" ","_",top_concepts[i,1]))
						r[k,4]<-top_concepts[i,2]
					}
				}
			}
			rm(wdf,values,concepts,top_concepts)
			gc(reset=TRUE)
		}
	}
	gc(reset=TRUE)	
	return(r)
}

concept_xray<-function(df,cols,topN,m){
	N<-10
	input<-concept_mgr(df,cols,topN,m)
	input<-input[,-3]
	
	out<-data.frame(Original_Column=as.numeric(),Predicted_Concept=as.character(),stringsAsFactors = FALSE)
	columns<-unique(input$Original_Column)
	n<-length(columns)
	k<-0
	if(n > 0){
		for(i in 1:n){
			d<-input[input$Original_Column %in% columns[i],]
			d$rank<-round(rank(1-d$Prob,ties.method="average"),0)
			d$weight<-d$Prob/(1+log(d$rank))
			d$count<-round(N*d$weight/sum(d$weight),0)
			d<-d[d$count >=1,]
			d$count<-round(d$count*N/sum(d$count),0)
			m<-nrow(d)
			if(m > 0){
				for(j in 1:m){
					c<-d[j,6]
					if(c > 0){
						for(l in 1:c){
							k<-k+1
							out[k,1]<-columns[i]
							out[k,2]<-paste0(c<-d[j,2],l)
						}
					}
				}
			}
		}
	}
	rm(input,columns,n,k,m,d,c)
	gc()
	return(out)
}


# Note - noun before noun that is column can be dropped

nli_history_mgr<-function(inputfile,db,filename="nli_history.rds"){
	library(data.table)
	library(udpipe)
	library(stringdist)
	library(tokenizers)
	tbl1<-data.table(request=character(),sentence=character(),pos=character(),skeleton=character())
	tbl2<-data.table(word=character(),pos=character(),itemtype=character())
	tbl3<-data.table(before=character(),after=character())
	doc<-readLines(inputfile, warn = FALSE)
	doc<-doc[nchar(doc)>0]
	n<-length(doc)
	if(n>0){
		for(i in 1:n){
			out<-extract_patterns(doc[i],db)
			if(length(out[[1]])>0 & nrow(out[[2]])){
				tbl1<-rbindlist(list(tbl1,list(doc[i],out[[1]]["sentence"],out[[1]]["pos"],out[[1]]["skeleton"])))
				tbl2<-unique(rbind(tbl2,out[[2]]))
				ngram_words<-unlist(strsplit(unlist(tokenize_ngrams(doc[i], n = 2))," "))
				m<-length(ngram_words)/2
				for(j in 1:m){
					tbl3<-rbindlist(list(tbl3,list(ngram_words[2*(j-1)+1],ngram_words[2*j])))
				}
			}
		}
	}
	gc()
	myList<-list()
	myList[[1]]<-tbl1
	myList[[2]]<-tbl2
	tbl3<-unique(tbl3)
	myList[[3]]<-tbl3
	saveRDS(myList,filename)
	#return(myList)
}

parse_sentence<-function(txt,db){
	out<-vector()
	df1<-parse_question(txt)
	if(nrow(df1)>0){
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
	}else{
		df<-data.frame()
	}
	return(df)
}

extract_patterns<-function(txt,db){
	myList<-list()
	df<-parse_sentence(txt,db)
	if(nrow(df)>0){
		tbl<-data.table(word=character(),pos=character(),itemtype=character())
		# get pos plus column indicator
		pos<-df$xpos
		n<-length(pos)
		for(i in 1:n){
			tbl<-rbindlist(list(tbl,list(df$token[i],pos[i],df$itemtype[i])))
		}
		ind<-which(df$itemtype!="")
		pos[ind]<-paste(df$xpos,df$itemtype,sep="-")[ind]
		skeleton<-paste(pos,collapse="=")
		out<-vector()
		out["sentence"]<-df$sentence[1]
		out["pos"]<-paste(df$xpos,collapse="=")
		out["skeleton"]<-skeleton
		myList[[1]]<-out
		myList[[2]]<-tbl	
	}else{
		myList[[1]]<-vector()
		myList[[2]]<-dataframe()
	}
	gc()
	return(myList)
}

next_item<-function(txt,db,histfile="nli_history.rds"){
# Suggest the next element
# Arguments
# txt - the entered text of the current request
# tbl1 - historical skeletons
# tbl2 - historical words used in queries
# output is alist of two items
# table of suggested pos plus column
# vector of suggested words
	nli_hist<-readRDS(histfile)
	tbl1<-nli_hist[[1]]
	tbl2<-nli_hist[[2]]
	tbl3<-nli_hist[[3]]
	options(warn=-1)
	library(stringr)
	library(data.table)
	library(udpipe)
	library(stringdist)
	df<-parse_sentence(txt,db)
	words<-vector()
	if(nrow(df)){
		pos<-df$xpos
		ind<-which(df$itemtype!="")
		pos[ind]<-paste(df$xpos,df$itemtype,sep="-")[ind]
		skeleton<-paste(pos,collapse="=")
		# As we parsed the current request we know which are columns
		# so we can replace tokens with actual column names
		ind<-which(str_detect(tbl1$skeleton,paste0("^",skeleton)))
		matched_skeletons<-tbl1[ind,]
		# we need to make sure that all marked columns
		# have actual column values
		# get the next element pos
		tbl<-data.table(pos=character(),itemtype=character())
		n<-nrow(matched_skeletons)
		if(n>0){
			for(i in 1:n){
				cur<-unlist(matched_skeletons[i,"skeleton"])
				if(nchar(cur)>=nchar(skeleton)+2){
					rest<-substring(cur,nchar(skeleton)+2)
					next_item<-unlist(strsplit(rest,"="))[1]
					parts<-unlist(strsplit(next_item,"-"))
					if(length(parts)==1){
						tbl<-rbindlist(list(tbl,list(parts,"")))
					}else{
						tbl<-rbindlist(list(tbl,list(parts[1],parts[2])))
					}
				}
			}
			# tbl contains options for the next element: pos and column name (optional)
			tbl<-unique(tbl)
			# what parts of speech and not column may be present
			pos<-unique(tbl[tbl$itemtype=="","pos"])
			nbr<-nrow(tbl[tbl$itemtype=="column","itemtype"])
			# if column present add all column names, otherwise exclude
			words<-unlist(tbl2[tbl2$pos %in% unique(tbl$pos) & tbl2$itemtype != "column","word"])
			# if Column possible add all columns
			if(nbr>0){
				words<-append(words,unique(db$Column))
			}
		}
	}else{
		tbl<-data.frame()
	}
	if(length(words)==0){
		mywords<-unlist(strsplit(txt," "))
		m<-length(mywords)
		if(m>0){
			nextwords<-tbl3[tolower(tbl3$before) == tolower(mywords[m]),]$after
			if(length(nextwords)>0){
				ind<-which(tolower(tbl2$word) %in% tolower(nextwords) & tbl2$itemtype=="column")
				if(length(ind)>0){
					# add all columns
					words<-unique(nextwords[!(tolower(nextwords) %in% tolower(tbl2$word[ind]))])
					words<-append(words,unique(db$Column))
				}else{
					words<-nextwords
				}
			
			}else{
				words<-vector()
			}
		}
	}
	names(words)<-NULL
	options(warn=0)
	myList<-list()
	myList[[1]]<-tbl
	myList[[2]]<-words
	return(words)
} 
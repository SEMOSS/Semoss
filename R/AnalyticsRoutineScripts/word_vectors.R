refresh_nlidb_history<-function(inputfile,my_db,filename="nli_training.rds"){
	library(data.table)
	library(udpipe)
	library(stringdist)
	library(tokenizers)
	tbl1<-data.table(request=character(),sentence=character(),pos=character(),skeleton=character())
	tbl2<-data.table(word=character(),pos=character(),itemtype=character(),itemdatatype=character())
	tbl3<-data.table(before=character(),after=character())
	doc<-readLines(inputfile, warn = FALSE)
	doc<-doc[nchar(doc)>0]
	n<-length(doc)
	if(n>0){
		for(i in 1:n){
			out<-extract_patterns(doc[i],my_db)
			if(length(out[[1]])>0 & nrow(out[[2]])){
				tbl1<-rbindlist(list(tbl1,list(doc[i],out[[1]]["sentence"],out[[1]]["pos"],out[[1]]["skeleton"])))
				tbl2<-unique(rbind(tbl2,out[[2]]))
				ngram_words<-unlist(strsplit(unlist(tokenize_ngrams(doc[i], n = 2,lowercase=FALSE))," "))
				m<-length(ngram_words)/2
				for(j in 1:m){
					tbl3<-rbindlist(list(tbl3,list(ngram_words[2*(j-1)+1],ngram_words[2*j])))
				}
			}
		}
	}
	my_words<-unique(append(tbl3$before,tbl3$after))
	myList<-list()
	myList[[1]]<-tbl1
	v<-tbl2[substr(tbl2$pos,1,2)=="NN" & tbl2$itemtype!="column" | tbl2$pos=="CD",]$word
	tbl2[tbl2$word %in% v,]$word<-"<value>"
	myList[[2]]<-tbl2
	tbl3[tbl3$before %in% v,]$before<-"<value>"
	tbl3[tbl3$after %in% v,]$after<-"<value>"
	tbl3<-unique(tbl3)
	myList[[3]]<-tbl3
	myList[[4]]<-my_db
	myList[[5]]<-build_glove_model(inputfile,v)
	myList[[6]]<-my_words
	saveRDS(myList,filename)
	gc()
	#return(myList)
}

build_glove_model<-function(inputfile,my_v){
	library(text2vec)
	txt<-readLines(inputfile, warn = FALSE)
	doc<-tolower(paste(txt,collapse=" "))
	tokens = space_tokenizer(doc)
	t<-unlist(tokens)
	t[t %in% my_v]<-"<value>"
	tokens<-list(t)
	
	# Create vocabulary. Terms will be unigrams (simple words).
	it = itoken(tokens, progressbar = FALSE)
	vocab = create_vocabulary(it)
	# vocab = prune_vocabulary(vocab, term_count_min = 5L)
	vectorizer = vocab_vectorizer(vocab)
	# use window of 5 for context words
	tcm = create_tcm(it, vectorizer, skip_grams_window = 5L)
	
	glove = GlobalVectors$new(word_vectors_size = 50, vocabulary = vocab, x_max = 20)
	wv_main = glove$fit_transform(tcm, n_iter = 10, convergence_tol = 0.01)
	dim(wv_main)
	wv_context = glove$components
	dim(wv_context)
	word_vectors = wv_main + t(wv_context)
	gc()
	return(word_vectors)
}

parse_sentence<-function(txt,my_db){
	out<-vector()
	df1<-parse_question(txt)
	if(nrow(df1)>0){
		# map nouns to db items
		df1<-map_dbitems(df1,my_db)
		# Convert dbitems to lower case to avoid pos confusion
		mytxt<-dbitems_tolower(txt,df1)
		df<-parse_question(mytxt)
		df<-map_dbitems(df,my_db)
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

extract_patterns<-function(txt,my_db){
	myList<-list()
	df<-parse_sentence(txt,my_db)
	if(nrow(df)>0){
		tbl<-data.table(word=character(),pos=character(),itemtype=character(),itemdatatype=character())
		# get pos plus column indicator
		pos<-df$xpos
		n<-length(pos)
		for(i in 1:n){
			tbl<-rbindlist(list(tbl,list(df$token[i],pos[i],df$itemtype[i],df$itemdatatype[i])))
		}
		ind<-which(df$itemtype!="")
		pos[ind]<-paste(df$xpos,df$itemtype,df$itemdatatype,sep="-")[ind]
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

load_glove<-function(glove_source,glove_target="glove.rds"){
	txt<-readLines(glove_source, warn = FALSE)
	x<-strsplit(txt," ")
	y <- matrix(unlist(x), ncol = 51, byrow = TRUE)
	rnames<-y[,1]
	z<-y[,2:51]
	z<-apply(z, 2,as.numeric)
	rownames(z)<-rnames
	colnames(z)<-NULL
	saveRDS(z,glove_target)
	return(z)
}																							

next_word_mgr<-function(txt,mydb,direction="next",histfile="nli_training.rds",size=5,my_vectors=word_vectors){
	myList<-list()
	if(tolower(direction)=="next"){
		myList[["next"]]<-get_next_word(txt,mydb,"next",histfile,size,my_vectors)
	}else if(tolower(direction)=="prev"){
		myList[["prev"]]<-get_next_word(txt,mydb,"prev",histfile,size,my_vectors)
	}else if(tolower(direction)=="both"){
		myList[["next"]]<-get_next_word(txt,mydb,"next",histfile,size,my_vectors)
		myList[["prev"]]<-get_next_word(txt,mydb,"prev",histfile,size,my_vectors)
	}
	return(myList)
}

map_to_training<-function(my_vectors,domain_words,mywords,new_db,old_db,tbl){
	library(openNLP)
	library(openNLPmodels.en)
	
	# new words in the index
	ind<-which(!(tolower(mywords) %in% tolower(domain_words)))
	n<-length(ind)
	if(n > 0){
		sent_token_annotator <- Maxent_Sent_Token_Annotator()
		word_token_annotator <- Maxent_Word_Token_Annotator()
		pos_tag_annotator <- Maxent_POS_Tag_Annotator()
		missed<-vector()
		for(i in 1:n){
			rows<-which(tolower(new_db$Column)==tolower(mywords[ind[i]]))
			if(length(rows)>0){
				# column name
				datatype<-new_db$Datatype[rows[1]]
				cols<-old_db[old_db$Datatype==datatype,]$Column
				if(length(cols)>0){
					# potentially random selection
					mywords[ind[i]]<-cols[1]
				}else{
					missed[length(missied)+1]<-i
				}
			}else{
				# not column name
				# get pos of the word and fin in history the same pos
				s<-NLP::as.String(mywords[ind[i]])
				annotation <- NLP::annotate(s,list(sent_token_annotator,word_token_annotator,pos_tag_annotator))
				new_pos<-annotation[[2]]$features[[1]]$POS
				words<-unique(tbl[tbl$pos==new_pos & tbl$itemtype !="column" & tbl$word != "<value>",]$word)
				if(length(words)==0){
					words<-tbl[substr(tbl$pos,1,2)==substr(new_pos,1,2) & tbl$itemtype !="column" & tbl$word != "<value>",]$word
				}
				if(length(words)>0){
					if(length(words)>1){
						options_word_vectors<-get_word_vector_mgr(my_vectors,words)
						new_word_vector<-get_word_vector_mgr(my_vectors,mywords[ind[i]])
						cur_matrix<-data.matrix(new_word_vector,rownames.force=NA)
						sim_matrix<-sim2(options_word_vectors,cur_matrix, method = "cosine",norm = c("l2"))
						sim_matrix<-sim_matrix[order(-sim_matrix[,1]),]
						mywords[ind[i]]<-names(sim_matrix)[1]
					}else{
						mywords[ind[i]]<-words[1]
					}
				}else{
					missed[length(missed)+1]<-ind[i]
				}
			}
		}
		if(length(missed)>0){
			mywords<-mywords[(!seq(length(mywords)) %in% missed)]
		}
	}
	return(mywords)
}

get_next_word<-function(txt,new_db,direction,histfile="nli_training.rds",size=5,my_vectors=word_vectors){
# Determine potential options for the next/previous word
# Arguments
# txt - the exiting text of the request
# new_db - metadata of the accessible databases
# histfile - history of successful queries
# cutoff -  the cutof similarity value to reduce to toal number of matching columns
# size - is the size in words of the window for request analysis 
# Output
# An array of next/previous word options
	library(text2vec)
	
	nli_hist<-readRDS(histfile)
	#tbl1<-nli_hist[[1]]
	tbl2<-nli_hist[[2]]
	tbl3<-nli_hist[[3]]
	old_db<-nli_hist[[4]]
	local_vectors<-nli_hist[[5]]
	domain_words<-nli_hist[[6]]
	
	options(warn=-1)
	mywords<-unlist(strsplit(txt," "))
	mywords<-tail(mywords,size)
	
	# map new words to most similar in training
	mywords<-map_to_training(my_vectors,domain_words,mywords,new_db,old_db,tbl2)
	
	# get current text local vectors representation
	mywords_vectors<-get_word_vector_mgr(local_vectors,mywords)
	if(nrow(mywords_vectors)>1){
		# discount  previous words
		if(direction=="next"){
			cur_vector<-decay_words(mywords_vectors)
		}else{
			cur_vector<-decay_words(mywords_vectors[seq(dim(mywords_vectors)[1],1),])
		}
	}else{
		cur_vector<-t(mywords_vectors)
	}
	cur_matrix<-t(data.matrix(cur_vector,rownames.force=NA))
	rownames(cur_matrix)<-"request"
	
	if(length(mywords)>0){
		if(direction=="next"){
			nextwords<-tbl3[tolower(tbl3$before) == tolower(mywords[length(mywords)]),]$after
			cur_datatype<-any(tolower(tbl2$word) == tolower(mywords[length(mywords)]) & tbl2$itemtype=="column")
		}else{
			nextwords<-tbl3[tolower(tbl3$after) == tolower(mywords[1]),]$before
			cur_datatype<-any(tolower(tbl2$word) == tolower(mywords[1]) & tbl2$itemtype=="column")
		}
		if(length(nextwords)>0){
			if(length(nextwords)>1){
				nextwords_vectors<-get_word_vector_mgr(local_vectors,nextwords)
				nextword_matrix<-sim2(nextwords_vectors,cur_matrix, method = "cosine",norm = c("l2"))
				nextwords_result<-nextword_matrix[order(-nextword_matrix[,1]),]
				words<-names(nextwords_result)
			}else{
				words<-nextwords[1]
			}
			if(cur_datatype){
				words<-words[!(words %in% old_db$Column)]
			}else{
				tdb<-unique(tbl2[tolower(tbl2$word) %in% tolower(words) & tbl2$itemtype=="column",c(1,4)])
				datatypes<-unique(tdb[order(match(tdb$word,words)),]$itemdatatype)
				tdb<-new_db[new_db$Datatype %in% datatypes,]
				col_names<-unique(tdb[order(match(tdb[,4],datatypes)),]$Column)
				words<-words[!(words %in% old_db$Column)]
				words<-unique(append(words,col_names))
			}
		}else{
			words<-vector()
		}
	}
	gc()
	options(warn=0)
	return(words)
}

get_column_neighbors<-function(my_vectors,mywords,my_db){
# Identifies neighborhood of set of columns
# Arguments
# mywords - a vector of given column
# db - metadata of the accessible databases
# Output
# Named array of neighborhood columns
	options(warn=-1)
	domain_words<-unique(as.character(my_db$Column))
	
	# get glove representation for our domain words
	domain_word_vectors<-get_word_vector_mgr(my_vectors,domain_words)

	mywords_vectors<-get_word_vector_mgr(my_vectors,mywords)
	# discount  previous words
	cur_vector<-decay_words(mywords_vectors,p=1)
	cur_matrix<-t(data.matrix(cur_vector,rownames.force=NA))
	rownames(cur_matrix)<-"request"
	sim_matrix<-sim2(domain_word_vectors,cur_matrix, method = "cosine",norm = c("l2"))
	sim_matrix<-sim_matrix[order(-sim_matrix[,1]),]
	words<-names(sim_matrix)
	# using neighbors as before words we can get after words
	# if after is column get related columns
	options(warn=0)
	return(sim_matrix)
}

decay_words<-function(x,p=0.618){
# Discount glove representations of earlier words in the request
# Arguments
# x - glove representation of the request
# p - discount date
	if(class(x)=="matrix"){
		n<-nrow(x)
		for(i in 1:n){
			if(i==1){
				out<-x[i,]*p^(n-i)
			}else{
				out<-out+x[i,]*p^(n-i)
			}
		}
	}else{
		out<-vector()
	}
	return(out)
}

get_word_vector_mgr<-function(my_vectors,x){
# Determines glove representation of a given array of words
# Arguments
# x - a given array of words 
	n<-length(x)
	out<-vector()
	if(n>0){
		for(i in 1:n){
			x_vector <- get_word_vector(my_vectors,x[i])
			if(length(x_vector)==0){
				x_split<-unlist(strsplit(x[i],"[.,_]"))
				x_split<-unlist(strsplit(trim(gsub('([[:upper:]])', ' \\1', x_split))," "))
				x_vector<-assemble_word_vector(my_vectors,x_split)
			}
			if(length(x_vector)>0){
				rownames(x_vector)<-x[i]
				if(length(out)==0){
					out<-x_vector
				}else{
					out<-rbind(out,x_vector)
				}
			}
		}
	}
	return(out)
}
 
get_word_vector<-function(my_vectors,x){
# Retrieve glove representation of a given set of words
# Arguments
# x - an array of words
# Output
# Glove representation of given array of words
	x_vector <- tryCatch({
		my_vectors[tolower(x), , drop = FALSE]
	}, error = function(e) {
		return(vector())
	})
	return(x_vector)
}

assemble_word_vector<-function(my_vectors,x_split){
# Computing glove representation for concatenated words 
# Arguments
# x_spit - an array of pieces that concatenated into a single words
# Output
# Glove representation of a given array

	x_vector<-vector()
	if(length(x_split)>0){
		n<-length(x_split)
		for(i in 1:n){
			t_vector<-get_word_vector(my_vectors,x_split[i])
			if(length(t)>0){
				if(length(x_vector)==0){
					x_vector<-t_vector
				}else{
					x_vector<-x_vector+t_vector
				}
			}
		}
	}
	return(x_vector)
}

get_forecast_pattern<-function(my_vectors,histfile="nli_history.rds"){
	library(text2vec)
	nli_hist<-readRDS(histfile)
	tbl3<-nli_hist[[3]]
	x<-tbl3$before
	y<-tbl3$after
	x_vector<-get_word_vector_mgr(my_vectors,x)
	y_vector<-get_word_vector_mgr(my_vectirs,y)
	tbl<-tbl3[tbl3$before %in% rownames(x_vector),]
	tbl<-tbl[tbl$after %in% rownames(y_vector),]
	
	x_vector<-x_vector[rownames(x_vector) %in% tbl$before,]
	y_vector<-y_vector[rownames(y_vector) %in% tbl$after,]
	sim<-sim2(x_vector,y_vector, method = "cosine",norm = c("l2"))
	n<-nrow(tbl)
	tbl$sim<-0
	for(i in 1:n){
		tbl$sim[i]<-sim[rownames(sim)==tbl$befor[i],colnames(sim)==tbl$after[i]][1]
	}
	return(sim)
}

get_query_pattern<-function(my_vectors,txt){
	mywords<-unlist(strsplit(txt," "))
	mywords_vectors<-get_word_vector_mgr(my_vectors,mywords)
	n<-nrow(mywords_vectors)-1
	sim<-vector()
	if(n>0){
		for(i in 1:n){
			x<-t(data.matrix(mywords_vectors[i,]))
			y<-t(data.matrix(mywords_vectors[i+1,]))
			sim[i]<-sim2(x,y, method = "cosine",norm = c("l2"))
			names(sim)[i]<-paste0(mywords[i]," ",mywords[i+1])
		}
	}
	return(sim)
}

glove_sim<-function(word_vectors,x,y,mymethod="cosine"){
	x_matrix<-as.matrix(word_vectors[x, , drop = FALSE])
	y_matrix<-as.matrix(word_vectors[y, , drop = FALSE])
	return(sim2(x_matrix,y_matrix, method = mymethod,norm = c("l2")))
}

glove_weighted_sim<-function(word_vectors,x,y,mymethod="cosine",p=1){
	n<-length(x)
	for(i in 1:n){
		if(i==1){
			r<-word_vectors[x, , drop = FALSE]*p^(n-i)
		}else{
			r<-r+word_vectors[x, , drop = FALSE]*p^(n-i)
		}
	}
	x<-matrix(r)
	y_matrix<-as.matrix(word_vectors[y, , drop = FALSE])
	return(sim2(x_matrix,y_matrix, method = mymethod,norm = c("l2")))
}

glove_neighbors<-function(word_vectors,like=vector(),unlike=vector(),nbr=5,p=1,q=1){
	n<-length(like)
	m<-length(unlike)
	if(n>0){
		for(i in 1:n){
			if(i==1){
				neighbors<-word_vectors[like[i], , drop = FALSE]*p^(n-i)
			}else{
				neighbors<-neighbors + word_vectors[like[i], , drop = FALSE]*p^(n-i)
			}
		}
	}
	if(m>0){
		for(i in 1:m){
			if(!exists("neighbors")){
				neighbors<--word_vectors[unlike[i], , drop = FALSE]*q^(m-i)
			}else{
				neighbors<-neighbors-word_vectors[unlike[i], , drop = FALSE]*q^(m-i)
			}
		}
	}
	cos_sim = sim2(x = word_vectors, y = neighbors, method = "cosine", norm = "l2")
	out<-head(sort(cos_sim[,1], decreasing = TRUE), nbr)
	return(out)
}


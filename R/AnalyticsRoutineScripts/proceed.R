
proceed<-function(input,model_size="small",total_tokens=NULL,temperature=1,top_k=2,top_p=1,limit=0){
# input is the text the model will extend
# model_size is the size of the model to choose
# total_tokens controls the length of the output, total_tokens=NULL makes the length output controlled by the model
# temperature controls randomness in the distribution. Temperature close to 0 make a deterministic model
# top_k determine diversity, top_k=1 make it a deterministic model
# top_p is the probability the corpus vocabularity covered for nucleus sampling
# limit is the number of senteces in the output requested
	library(gpt2)
	library(reticulate)
	if(tolower(model_size)=="small"){
		model<-"124M"
	}else if(tolower(model_size)=="large"){
		model<-"774M"
	}else if(tolower(model_size)=="xlarge"){
		model<-"1558M"
	}else{
		model<-""
	}
	if(model!=""){
		#t<-gpt2(prompt = input, model, seed = NULL, batch_size = 1, total_tokens,temperature, top_k, top_p)
		t <- tryCatch(
			{
				gpt2(prompt = input, model, seed = NULL, batch_size = 1, total_tokens,temperature, top_k, top_p)
			},
			error = function(e){
				gpt2(prompt = input, model, seed = NULL, batch_size = 1, total_tokens,temperature, top_k, top_p)
			}
		)
		txt<-gsub("\n|\"","",t)
		names(txt)<-NULL
		if(!is.null(total_tokens)){
			ind<-unlist(gregexpr(pattern ='[.]',txt))
			if(limit>0){
				if(length(ind)>0){
					idx<-min(length(ind),limit)
					txt<-substr(txt,1,ind[idx])
				}
			}else{
				txt<-substr(txt,1,ind[length(ind)])
			}
		}
	}else{
		txt<-"Provide correct model size"
	}
	gc()
	return(txt)
}

infer_viz_desc<-function(chart,src,cols,model_size,total_tokens=100,temperature=1,top_k=2,top_p=1,limit=0){
	if(chart!="" & src!="" & length(cols)!=0){
		CONTEXT1<-"A chart is a graphical representation of data. The data usually reside in columns of database tables. The "
		CONTEXT2<-" below shows how the "
		PURPOSE<-"The purpose of this chart is"
		if(length(cols)>1){
			mycols<-paste0(paste0(cols,collapse=" and ")," depend on ")
		}else{
			mycols<-paste0(cols," depends on ")
		}
		mychart<-paste(chart,"chart",sep=" ")
		input<-paste0(CONTEXT1,mychart,CONTEXT2,mycols,src,'. ',PURPOSE,collapse="")
		desc<-paste0(PURPOSE,proceed(input,model_size,total_tokens,temperature,top_k,top_p,limit))
	}else{
		desc<-"Please provide correct arguments: chart name, dependent variable, independent variable"
	}
	gc()
	return(desc)
}

infer_tbl_desc<-function(cols,model_size='small',total_tokens=100,temperature=1,top_k=2,top_p=1,limit=1,qty=1){
	CONTEXT<-"repository table contains the following columns: "
	PURPOSE<-"The table purpose is"
	input<-paste0(CONTEXT,paste(cols,collapse=", "),'. ',PURPOSE,collapse="")
	desc<-vector()
	for(j in 1:qty){
		desc<-append(desc,paste0(PURPOSE,proceed(input,model_size,total_tokens,temperature,top_k,top_p,limit)))
	}
	gc()
	return(desc)
}

infer_db_desc<-function(cur_db,model_size='small',total_tokens=100,temperature=1,top_k=2,top_p=1,limit=1,qty=1){
	# db is a dataframe containing Column, Table
	CONTEXT<-"Database consists of the following tables: " 
	PURPOSE<-"The purpose of this database is"
	tbls<-unique(cur_db$Table)
	n<-length(tbls)
	if(n>0){
		input<-paste0(CONTEXT,paste(tbls,collapse=", "),". ")
		for(i in 1:n){
			cols<-cur_db[cur_db$Table==tbls[i],]$Column
			tbl_desc<-paste0("Table ",tbls[i]," contains columns ", paste(cols,collapse=", "),". ")
			input<-paste0(input,tbl_desc)
		}
		input<-paste0(input,PURPOSE)
		desc<-vector()
		for(j in 1:qty){
			desc<-append(desc,paste0(PURPOSE,proceed(input,model_size,total_tokens,temperature,top_k,top_p,limit)))
		}
	}else{
		desc<-"Please provide correct argument - a dataframe containing tables and columns of the database"
	}
	gc()
	return(desc)
}


infer_db_desc_alt<-function(cur_db,model_size,total_tokens=100,temperature=1,top_k=2,top_p=1,limit=0){
	CONTEXT<-"Database consists of the following tables. " 
	PURPOSE<-"The purpose of this database is"
	tbls<-unique(cur_db$Table)
	n<-length(tbls)
	input<-"CONTEXT"
	if(n>0){
		for(i in 1:n){
			cols<-cur_db[cur_db$Table==tbls[i],]$Column
			input<-paste0(input,infer_tbl_desc(cols,model_size,total_tokens,temperature,top_k,top_p,limit)," ")
		}
		input<-paste0(input,'. ',PURPOSE)
		desc<-proceed(input,model_size,total_tokens,temperature,top_k,top_p,limit)
	}
	gc()
	return(desc)
}

infer_tags<-function(desc){
	ERR_MSG<-"Could not discover tags"
	desc_part<-unique(unlist(strsplit(desc,"[.]")))
	topics<-assess_nbr_topics(content=desc_part)

	if(topics>1){
		df<-outline_topics(content=desc,topTopics=topics)
		tags<-unique(df$label)
	}else{
		df<-text_keywords(content=desc)
		if(nrow(df)>0){
			tags<-df[df$freq>=2,]$keyword
			if(length(tags)==0){
				tags<-ERR_MSG
			}else{
				tags<-tags[!(tags %in% get_stopwords())]
			}
		}else{
			tags<-ERR_MSG
		}
	}
	gc()
	return(tags)
}

get_stopwords<-function(){
	return(c("number","average","chart","database","column","columns","table","list","item","items"))
}

outline_topics<-function(filename="",page_url="",content="",topTopics=5){
# Description
# Extracts a given number of topics based on latent Dirichlet allocation topic model
# Arguments
# filename is the file name to extract topics from
# page_url is the url of the page to extract topics from
# content is the text to extract topics from
# topTopics is the number of topics to extract
	txt<-prepare_doc(filename,page_url,content,TRUE)
	library(stringr)
	library(textmineR)
	doc <- stringr::str_replace_all(txt, "<br */>", "")
	tcm <- CreateTcm(doc_vec = doc, skipgram_window = 10,verbose = FALSE, cpus = 2)
	model <- FitLdaModel(dtm = tcm, k = topTopics, iterations = 200, burnin = 180, alpha = 0.1,beta = 0.05, optimize_alpha = TRUE,calc_likelihood = FALSE,calc_coherence = FALSE, calc_r2 = FALSE, cpus = 2)
	topics<-SummarizeTopics(model)
	n<-nrow(topics)
	if(n>0){
		colnames(topics)[colnames(topics)=="label_1"]<-"label"
		for(i in 1:n){
			terms<-topics$top_terms_phi[i]
			cur_terms<-unlist(strsplit(terms,", "))
			df<-annotate_doc(cur_terms)
			labels<-df[df$upos=="NOUN" & !(df$token %in% get_stopwords()),]$lemma
			if(length(labels)>0){
				topics$label[i]<-labels[1]
			}else{
				topics$label[i]<-""
			}
		}
	}
	topics<-topics[topics$label!="",]
	gc()
	return(topics)
}

assess_nbr_topics<-function(content){
	library(tm)
	library(ldatuning)
	
	txt = tolower(gsub("[[:punct:]]", " ", content))
	txt<-removeWords(txt, stopwords("english"))
	corpus = Corpus(VectorSource(txt))
	tdm = DocumentTermMatrix(corpus)
	term_tfidf <- tapply(tdm$v/rowSums(as.array(tdm))[tdm$i], tdm$j, mean) * log2(nDocs(tdm)/colSums(as.array(tdm > 0)))
	summary(term_tfidf)
	
	tdm <- tdm[,term_tfidf >= 0.1]
	tdm <- tdm[rowSums(as.array(tdm)) > 0,]
	summary(colSums(as.array(tdm)))
	if(length(content)>=2){
		max.topics<-min(10,length(content))
		z <- tryCatch(
			{
				FindTopicsNumber(tdm, topics = 2:max.topics, metrics = c("Griffiths2004", "CaoJuan2009", "Arun2010", "Deveaud2014"), mc.cores = 1L)
			},
			error = function(e){
				NULL
			}
		)
		if(!is.null(z)){
			arun<-z[z$Arun2010==min(z$Arun2010),"topics"]
			cao<-z[z$CaoJuan2009==min(z$CaoJuan2009),"topics"]
			dev<-z[z$Deveaud2014==max(z$Deveaud2014),"topics"]
			grif<-z[z$Griffiths2004==max(z$Griffiths2004),"topics"]
			optimal.topics<-min(cao,dev)
		}else{
			optimal.topics<-1
		}
	}else{
		optimal.topics<-1
	}
	gc()
	return(optimal.topics)
}


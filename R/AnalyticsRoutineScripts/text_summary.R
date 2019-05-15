
text_summary<-function(filename="",page_url="",content="",topN=5,ord=FALSE,limit=100,min_words=2){
# Description
# Extracts the most stand out sentences based on page rank algorithm and jaccard similarity between sentences
# Arguments
# filename is the file name to summarize_text
# page_url is the url of the page to summarize_text
# content is the text to summarize_text
# topN is the number of stand out sentences to extract 
# ord is the order the sentences listed as they occurred (TRUE) or importance (FALSE)
# limit is the threshold to start limiting candidate selection
# min_word is the minimum number of words in the extracted sentences 
	library(textrank)
	
	txt<-prepare_doc(filename,page_url,content)
	doc<-annotate_doc(txt)
	
	# filter out small sentences
	tsentences <- unique(doc[, c("sentence_id", "sentence")])
	sentences<-tsentences[0,]
	for(i in 1:nrow(tsentences)){
		if(wordcount(tsentences[i,"sentence"])>= min_words){
			sentences<-rbind(sentences,tsentences[i,])
		}
	}
	terminology <- subset(doc, upos %in% c("NOUN", "ADJ"), select = c("sentence_id", "lemma"))
	#head(terminology)
	if(nrow(sentences) <= limit){
		tr <- textrank_sentences(data = sentences, terminology = terminology)
	}else{
		minhash <- minhash_generator(n = 1000, seed = 123456789)
		candidates <- textrank_candidates_lsh(x = terminology$lemma, sentence_id = terminology$sentence_id, minhashFUN = minhash, bands = 500)
		tr <- textrank_sentences(data = sentences, terminology = terminology, textrank_candidates = candidates)
	}
	out<-summary(tr, n = topN,keep.sentence.order = ord)
	out <- gsub(x=out,pattern="\"", replacement="")
	gc()
	return(out)
}

prepare_doc<-function(filename,page_url,content){
	library(textreuse)
	library(textreadr)
	library(readtext)
	sink("log-text-summary.txt")
	if(filename!=""){
		t<-unlist(strsplit(filename,"[.]"))	
		if(length(t)>0){
			ext<-tolower(t[length(t)])
			if(ext %in% c("doc","docx","pdf","txt")){
				if(ext=="doc"){
					s<-read_doc(filename)
					doc<-paste(s,collapse=" ")
				}else if(ext=="docx"){
					s<-read_docx(filename)
					doc<-paste(s,collapse=" ")
				}else if(ext=="pdf"){
					s<-read_pdf(filename)
					doc<-paste(s$text,collapse=" ")
				}else if(ext=="txt"){
					doc<-readtext(filename)
				}
				txt<-clean_text(doc)
				txt<-textreuse::tokenize_sentences(txt, lowercase = FALSE)
				txt<-gsub("[[:punct:]]","",txt)
			}
		}
	}else if(page_url!=""){
		library(xml2)
		library(rvest)
		page = xml2::read_html(page_url)
		txt = clean_text(rvest::html_text(rvest::html_nodes(page, "p")))
		txt<-gsub("[[:punct:]]","",txt)
	}else if(content!=""){
		txt<-clean_text(page_text=content)
		txt<-textreuse::tokenize_sentences(txt, lowercase = FALSE)
		txt<-gsub("[[:punct:]]","",txt)
	}else{
		txt=""
	}
	sink()
	return(txt)
}


summarize_text<-function(filename="",page_url="",content="",topN=5){
# Description
# Extracts the most stand out sentences based on page rank algorithm and similarity grounded in lexical centrality
# Arguments
# filename is the file name to summarize_text
# page_url is the url of the page to summarize_text
# content is the text to summarize_text
# topN is the number of stand out sentences to extract 
	txt<-prepare_doc(filename,page_url,content)
	library(lexRankr,quietly = TRUE)
	out<-lexRankr::lexRank(txt,docId = rep(1, length(txt)),n = topN,continuous = TRUE)
	gc()
	return(gsub(x=out$sentence,pattern="\"", replacement=""))
}

clean_text<-function(page_text){
# Description
# Cleanse the text from non standard characters
# Arguments
# page_text is the text to cleanse
	txt<-gsub("[^\x20-\x7E|\n|\r]", " ", page_text)
	txt<-iconv(txt,to="ASCII//TRANSLIT")
	txt<-gsub(x=txt,pattern="\"", replacement="")
	#txt<-gsub("[[:punct:]]|[[:digit:]]","",txt)
	txt<-gsub("[\r\n]","",txt)
	txt<-gsub("[[:digit:]]","",txt)
	txt<-gsub("[^[:alnum:][:space:].?!\"]", "",txt)
	return(txt)
}

summarize_topics<-function(filename="",page_url="",content="",topTopics=5,topTerms=10) {
# Description
# Extracts a given number of topics based on latent Dirichlet allocation topic model
# Arguments
# filename is the file name to extract topics from
# page_url is the url of the page to extract topics from
# content is the text to extract topics from
# topTopics is the number of topics to extract
	txt<-prepare_doc(filename,page_url,content)
	library(stringr)
	library(textmineR)
	doc <- stringr::str_replace_all(txt, "<br */>", "")
	tcm <- CreateTcm(doc_vec = doc, skipgram_window = 10,verbose = FALSE, cpus = 2)
	model <- FitLdaModel(dtm = tcm, k = topTopics, iterations = 200, burnin = 180, alpha = 0.1,beta = 0.05, optimize_alpha = TRUE,calc_likelihood = FALSE,calc_coherence = FALSE, calc_r2 = FALSE, cpus = 2)
	
	topics<-SummarizeTopics(model)
	rownames(topics)<-NULL
	topics<-topics[,1:5]
	colnames(topics)[c(2,5)]<-c("label","top terms")
	topics<-topics[order(-topics$prevalence,-topics$coherence,topics$label),]
	# Compute and add topic keywords frequency
	topic_keywords<-topic_terms_freq(txt,topics,model$phi,topTerms)
	return(topic_keywords)
}

summarize_topics_text<-function(filename="",page_url="",content="",topTopics=5,topTerms=10,topN=5) {
# Description
# Extracts a given number of topics based on latent Dirichlet allocation topic model
# Arguments
# filename is the file name to extract topics from
# page_url is the url of the page to extract topics from
# content is the text to extract topics from
# topTopics is the number of topics to extract
	txt<-prepare_doc(filename,page_url,content)
	library(stringr)
	library(textmineR)
	doc <- stringr::str_replace_all(txt, "<br */>", "")
	tcm <- CreateTcm(doc_vec = doc, skipgram_window = 10,verbose = FALSE, cpus = 2)
	model <- FitLdaModel(dtm = tcm, k = topTopics, iterations = 200, burnin = 180, alpha = 0.1,beta = 0.05, optimize_alpha = TRUE,calc_likelihood = FALSE,calc_coherence = FALSE, calc_r2 = FALSE, cpus = 2)
	
	topics<-SummarizeTopics(model)
	rownames(topics)<-NULL
	topics<-topics[,1:5]
	colnames(topics)[c(2,5)]<-c("label","top terms")
	topics<-topics[order(-topics$prevalence,-topics$coherence,topics$label),]
	# Compute and add topic keywords frequency
	topic_keywords<-topic_terms_freq(txt,topics,model$phi,topTerms)
	topic_text<-topics_text(txt,topic_keywords,topN)
	out<-bind_results(topic_keywords,topic_text)
	gc()
	return(out)
}

bind_results<-function(topic_keywords,topic_text){
	topic_keywords$text<-""
	topic_text$keyword<-""
	topic_text$freq<-0
	topic_text$topic_share<-0
	topic_text$terms_relevance<-0
	topic_text<-topic_text[,c(1,3,4,5,6,7,8,9,2)]
	topic_keywords<-rbind(topic_keywords,topic_text)
	return(topic_keywords)
}

topics_text<-function(txt,topics,topN){
	mytopics<-unique(topics$topic)
	n<-length(mytopics)
	df<-data.frame()
	if(n>0){
		library(plyr)
		for(i in 1:n){
			mytopic<-topics[topics$topic==mytopics[i],]
			m<-nrow(mytopic)
			v<-vector()
			for(j in 1:m){
				v<-append(v,which(grepl(tolower(mytopic$keyword[j]),tolower(txt))))
			}
			z<-count(v)
			z<-z[order(-z$freq),]
			z<-head(z,topN)
			if(i==1){
				df<-data.frame(topic=mytopics[i],label=mytopic$label[1],prevalence=mytopic$prevalence[1],coherence=mytopic$coherence[1],text=txt[z$x])
			}else{
				df<-rbind(df,data.frame(topic=mytopics[i],label=mytopic$label[1],prevalence=mytopic$prevalence[1],coherence=mytopic$coherence[1],text=txt[z$x]))
			}
		}
		df$text<-format(df$text,justify="left")
	}
	gc()
	return(df)
}

topic_terms_freq<-function(txt,topics,phi,topTerms){
	library(tm)
	weight<-0.6
	ctrl<-list(wordLengths=c(2,Inf))
	term_freq<-tm::termFreq(txt,control=ctrl)
	topics_top_terms<-GetTopTerms(phi,topTerms)
	n<-nrow(topics)
	for(i in 1:n){
		top_terms<-topics_top_terms[,topics$topic[i]]
		top_terms_share<-phi[topics$topic[i],top_terms]
		
		topic_terms_relevance<-weight*log(phi[topics$topic[i],top_terms])+(1-weight)*log(phi[topics$topic[i],top_terms]/(colSums(phi[,top_terms])/n))
		ordered_term_freq<-term_freq[names(term_freq) %in% top_terms][order(match(names(term_freq[names(term_freq) %in% top_terms]),names(top_terms_share)))]

		df<-data.frame(keyword=top_terms,freq=term_freq[names(term_freq) %in% top_terms],
		topic_share=round(top_terms_share*ordered_term_freq,2),terms_relevance=topic_terms_relevance)
		rownames(df)<-NULL
		df$keyword<-as.character(df$keyword)
		df$label<-df[df$terms_relevance==max(df$terms_relevance),]$keyword[1]
		df$topic<-topics$topic[i]
		df$prevalence<-topics$prevalence[i]
		df$coherence<-topics$coherence[i]
		if(i==1){
			out<-df
		}else{
			out<-rbind(out,df)
		}
	}
	out<-out[,c(6,5,7,8,1,2,3,4)]
	gc()
	return(out[order(-out$prevalence,-out$coherence,-out$terms_relevance),])
}

text_keywords<-function(filename="",page_url="",content="",min_ngram=1,min_freq=2){
# Description
# Extracts a set of key words with length greater than one
# Arguments
# filename is the file name to extract key words
# page_url is the url of the page to extract key words
# content is the text to extract key words
	library(textrank,quietly = TRUE)
	txt<-prepare_doc(filename,page_url,content)
	doc<-annotate_doc(txt)	
	stats <- textrank_keywords(doc$lemma, relevant = doc$upos %in% c("NOUN","PROPN","ADJ"), ngram_max = 8, sep = " ")
	stats <- subset(stats$keywords, ngram>=min_ngram & freq>=min_freq )
	gc()
	return(stats)
}

annotate_doc<-function(txt){
	FILE_MODEL<-"english-ud-2.0-170801.udpipe"
	library(udpipe)
	noun_phrase<-"(A|N)*N(P+D*(A|N)*N)*"
	if(!exists("tagger")){
		tagger <<- udpipe_load_model(FILE_MODEL)
	}
	# new version works
	txt<-paste(txt,collapse = "\n")
	doc <- tryCatch({
		udpipe_annotate(tagger, txt)
	}, error = function(e) {
		return(NULL)
	})
	if(is.null(doc)){
		tagger <<- udpipe_load_model(FILE_MODEL)
		doc <- udpipe_annotate(tagger, txt)
	}
	doc<-as.data.frame(doc)
	return(doc)
}

text_summarizer<-function(filename="",page_url="",content="",topN=5,topics=50) {
# Description
# Extracts the most salient sentences based on page eigenvector centrality and Dirichlet allocation topic model
# This script provides likely the deepest text summary of all three 
# Arguments
# filename is the file name to summarize_text
# page_url is the url of the page to summarize_text
# content is the text to summarize_text
# topN is the number of stand out sentences to extract 
# topic is the number of topics used for Dirichlet allocation topic model
	txt<-prepare_doc(filename,page_url,content)
		
	library(igraph) 
	library(textmineR)
	txt <- stringr::str_replace_all(txt, "<br */>", "")
	# Create term co-occurrence matrix
	tcm <- CreateTcm(doc_vec = txt, skipgram_window = 10,verbose = FALSE, cpus = 2)
	# We try to fit a model with a given number of topics into our probability space (term co-occurrence matrix)
	model <- FitLdaModel(dtm = tcm, k = topics, iterations = 200, burnin = 180, alpha = 0.1,beta = 0.05, optimize_alpha = TRUE,calc_likelihood = FALSE,calc_coherence = FALSE, calc_r2 = FALSE, cpus = 2)
	# gamma is  the probability matrix of word and topic relationship
	gamma<-model$gamma

	# parse it into sentences
	sent <- txt
	names(sent) <- seq_along(sent) # so we know index and order
	# embed the sentences in the model
	e <- CreateDtm(sent, ngram_window = c(1,1), verbose = FALSE)
	# remove any documents with 2 or fewer words
	e <- e[ rowSums(e) > 2 , ]
	vocab <- intersect(colnames(e), colnames(gamma))
	e <- e / rowSums(e)
	e <- e[ , vocab ] %*% t(gamma[ , vocab ])
	e <- as.matrix(e)

	# get the pairwise distances between each embedded sentence
	e_dist <- CalcHellingerDist(e)
	# turn into a similarity matrix
	g <- (1 - e_dist) * 100
	# we don't need sentences connected to themselves
	diag(g) <- 0

	# turn into a nearest-neighbor graph
	g <- apply(g, 1, function(x){
	x[ x < sort(x, decreasing = TRUE)[ 3 ] ] <- 0
	x
	})

	# by taking pointwise max, we'll make the matrix symmetric again
	g <- pmax(g, t(g))
	g <- graph.adjacency(g, mode = "undirected", weighted = TRUE)
	# calculate eigenvector centrality
	ev <- evcent(g)

	# format the result
	out <- sent[ names(ev$vector)[ order(ev$vector, decreasing = TRUE)[ 1:topN ] ] ]
	names(out)<-NULL
	gc()
	return(clean_text(out))
}
# The key to similarity is not that terms happen to occur in the same document: 
# it is that the terms happen to occur in the same context - that they have very similar neighbors.

build_tdm<-function(lookup_tbl,vocabulary=NULL,weighted=FALSE){
	# Construct LSA space
	MINWORDLENGTH<-2
	WORDSTOEXCLUDE<-c("a","the","this","these","that","those","then","and","an","with","within","without","when","why","how","in",
	"on","of","from","for","at","so","then","thus","here","there")
	library(LSAfun)
	library(text2vec)
	myList<-list()
	# Construct terms/documents matrix based on a given set of documents or read it from the file
	s<-breakdown(as.character(lookup_tbl[[1]]))
	it <- itoken(s, preprocess_function = tolower,tokenizer = word_tokenizer, chunks_number = 10, progessbar = F)
	if(is.null(vocabulary)){
		vocab<-create_vocabulary(it,stopword=WORDSTOEXCLUDE,ngram=c(ngram_min=1L,ngram_max=1L))
	}else{
		vocab<-vocabulary
	}
	myList[[1]]<-vocab
	vectorizer = vocab_vectorizer(vocab)
	dtm = create_dtm(it, vectorizer)
	myMatrix<-as.textmatrix(t(as.matrix(dtm)))
	colnames(myMatrix)<-paste0("D",seq(1,ncol(myMatrix)))
		
	# apply tfidf weights to the terms/documents matrix
	if(weighted){
		myMatrix<-apply_tfidf(myMatrix)
	}
	myList[[2]]<-myMatrix
	gc()
	return(myList)
}

append_doc<-function(lookup_tbl,myLSAspace,myVocabulary,filename_lsa,weighted=FALSE){
	existTDM<-as.textmatrix(myLSAspace)
	myList<-build_tdm(lookup_tbl,myVocabulary,weighted)
	myTDM<-myList[[2]]
	newTDM<-fold_in(myTDM,myLSAspace)
	n<-ncol(existTDM)
	m<-ncol(newTDM)
	colnames(newTDM)<-paste0("D",seq(n+1,n+m))
	combTDM<-cbind(existTDM,newTDM)
	existSum<-sum(myLSAspace$sk)
	combLSAspace = lsa(combTDM,dims=dimcalc_ndocs(existSum))
	saveRDS(combLSAspace, paste0(filename_lsa,".rds"))
	gc()
	return(combLSAspace)
}

append_docs<-function(lookup_tbl,myLSAspace,myVocabulary,filename_lsa,weighted=FALSE){
	# Get LSa space components
	tk<-existLSAspace$tk
	sk<-existLSAspace$sk
	dk<-existLSAspace$dk
	n<-nrow(dk)
	# Construct additional docs term document matrix
	s<-breakdown(as.character(tbl2[[1]]))
	it <- itoken(s, preprocess_function = tolower,tokenizer = word_tokenizer, chunks_number = 10, progessbar = F)
	vocab<-readRDS("original-lsa-vocab.rds")
	vectorizer = vocab_vectorizer(vocab)
	dtm = create_dtm(it, vectorizer)
	q<-as.textmatrix(t(as.matrix(dtm)))
	colnames(q)<-paste0("D",seq(n+1,n+ncol(q)))
	# If term frequency if needed
	if(weighted){
		q<-q/colSums(q)
	}
	# Map new docs into the existing LSa space
	s<-diag(sk)
	q<-t(q) %*% as.matrix(tk) %*% solve(s)
	# Extend existing LSA space with new docs
	dk<-rbind(dk,q)
	combLSAspace<-existLSAspace
	combLSAspace$dk<-dk
	saveRDS(combLSAspace, paste0(filename_lsa,".rds"))
	gc()
	return(combLSAspace)

}

apply_tfidf<-function(matrix){
	# Compute tfidf weights and apply them to a given terms/documents matrix
	totals<-colSums(matrix)
	tfidf_matrix<-matrix/totals*gw_idf(matrix)
	return(tfidf_matrix)
}

lsi_mgr<-function(lookup_tbl,share=SHARE,filename_lsa=LSA_filename,weighted=FALSE){
	# build LSA space
	SHARE<-0.8
	LSA_filename<-"LSAspace"
	# Construct term document matrix
	myList<-build_tdm(lookup_tbl,NULL,weighted)
	myVocabulary<-myList[[1]]
	myMatrix<-myList[[2]]
	# Build LSa space
	myLSAspace = lsa(myMatrix, dims=dimcalc_share(share))
	saveRDS(myLSAspace, paste0(filename_lsa,".rds"))
	saveRDS(myVocabulary, paste0(filename_lsa,"-vocab.rds"))
	rm(myLSAspace,myVocabulary,myList)
}

match_desc<-function(terms, desc){
	# mapping query terms to LSI terms
	MAXDIST<-1
	library(stringdist)
	v_terms<-terms[terms %in% desc]
	unmatched_desc<-desc[!(desc %in% v_terms)]
	if(length(unmatched_desc) > 0){
		v_terms1<-terms[amatch(unmatched_desc,terms,maxDist=MAXDIST)]
		v_terms1<-v_terms1[!is.na(v_terms1)]	
		if(length(v_terms1) > 0){
			desc<-c(v_terms,v_terms1)
		} 
	}
	gc()
	return(desc)
}

lsi_lookup<-function(item,lookup_tbl,margin=0.01,myLSAspace,weighted=FALSE){
	# perform lsi lookup
	library(lsa)
	library(plyr)
	tk<-myLSAspace$tk
	dk<-myLSAspace$dk
	sk<-myLSAspace$sk
	myList<-list()

	terms<-rownames(tk)
	# parse a given item
	desc<-tolower(unlist(strsplit(item," ")))
	# map the item to actual terms in case it is not exact match to terms
	desc<-match_desc(terms,desc)
	if(length(desc) > 0){
		# calculate query terms frequency
		o<-count(desc)
		o$tf<-o$freq/sum(o$freq)
		w<-ifelse(terms %in% o$x, o$tf, 0)
		# query in original coordinates
		q<-ifelse(terms %in% desc, 1, 0)
		# if weighted apply term frequency
		if(weighted) {
			q<-q*w
		}
		# express query in new coordinates
		# diagonal eigenvalues matrix
		s<-diag(sk)
		q<-t(q) %*% as.matrix(tk) %*% solve(s)
	} else{
		return(myList)
	}
	# perform cosine similarity between query and the documents
	# to identify the most similar match
	m<-nrow(dk)
	v<-vector()
	for(i in 1:m){
		v[i]<-cosine(as.numeric(q),as.numeric(dk[i,]))
	}
	r<-max(v)
	row<-which(v>(max(v)-margin))
	nbr<-as.integer(substring(rownames(dk)[row],2))
	myList[[1]]<-as.character(lookup_tbl[nbr,1])
	myList[[2]]<-paste0(as.character(lookup_tbl[nbr,2]))
	myList[[3]]<-v[row]
	rm(tk,dk,sk,desc,w,o,m,v,r)
	#return(paste0(as.character(lookup_tbl[nbr,"Category"]),"-",as.character(lookup_tbl[nbr,"DoD_SE_Category"])))
	return(myList)
}	

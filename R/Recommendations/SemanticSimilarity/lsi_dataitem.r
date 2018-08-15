# The key to similarity is not that terms happen to occur in the same document: 
# it is that the terms happen to occur in the same context - that they have very similar neighbors.

build_tdm<-function(desc_tbl,desc_col,freq_col,vocabulary=NULL,weighted=TRUE){
	# Construct LSA space
	# Arguments
	# desc_tbl - dataframe which data will be used to construct the term document matrix
	# desc_col - a column in the desc_tbl dataframe containing the test used for term document construction
	# freq_col - a column in the desc_tbl dataframe containing the frequencies of the respected rows
	# vocabulary - either null for initial construction or the existing vocabulary for adding documents to the existing matrix
	# weighted - logical value indicating whether tf-idf weighting should be applied
	MINWORDLENGTH<-2
	WORDSTOEXCLUDE<-c("a","the","this","these","their","that","those","then","and","an","as","over","with","within","without","when","why","how","in","on","of",
	"or","to","into","by","from","for","at","so","then","thus","here","there","whether","is","are","not","other","too","his","her","they","oh","number")
	#library(LSAfun)
	#library(stringi)
	library(text2vec)
	library(data.table)
	library(plyr)
	library(stringr)
	myList<-list()
	# Construct terms/documents matrix based on a given set of documents or read it from the file
	s<-breakdown(str_replace_all(desc_tbl[[desc_col]],"[^[:graph:]]", " "))
	#s<-stri_trans_general(str_replace_all(desc_tbl[[desc_col]],"[^[:graph:]]", " "), "latin-ascii")
	it <- itoken(s, preprocess_function = tolower,tokenizer = word_tokenizer, chunks_number = 10, progessbar = F)
	if(is.null(vocabulary)){
		vocab<-create_vocabulary(it,stopword=WORDSTOEXCLUDE,ngram=c(ngram_min=1L,ngram_max=1L))
	}else{
		vocab<-vocabulary
	}
	# exclude words consisting of a single character
	vocab<-vocab[nchar(vocab$term) >= 2,]
	# exclude words appeared only one time
	vocab<-vocab[vocab$term_count > 1,]
	myList[[1]]<-vocab
	vectorizer = vocab_vectorizer(vocab)
	dtm = create_dtm(it, vectorizer)
	myMatrix<-as.textmatrix(t(as.matrix(dtm)))
	colnames(myMatrix)<-paste0("D",seq(1,ncol(myMatrix)))
		
	# apply tfidf weights to the terms/documents matrix
	if(weighted){
		myMatrix<-apply_tfidf(myMatrix)
		# Adjustment for different descriptions based on frequency
		cmd<-paste0("v<-as.numeric(desc_tbl$",freq_col,")")
		eval(parse( text=cmd ))
		v2 <- rep(v,each=dim(myMatrix)[1])
		myMatrix<-myMatrix*v2
	}
	
	# aggregate concept descriptions documents for each column into a single document
	# setDT(desc_tbl)[,freq := .N, by = c("Original_Column")]
	# freq<-desc_tbl[!duplicated(desc_tbl$Original_Column),]
	
	# Aggregate scores and compute count
	freq<-count(desc_tbl$Original_Column)
	names(freq)[1]<-"Original_Column"
	freq1<-aggregate(desc_tbl$score,list(desc_tbl$Original_Column),sum)
	names(freq1)[1]<-"Original_Column"
	freq<-merge(freq,freq1,by.x="Original_Column",by.y="Original_Column")
	names(freq)[3]<-"score"
	
	m<-nrow(myMatrix)
	tm<-matrix(nrow=m,ncol=0)
	n<-nrow(freq)
	end<-0
	for(i in 1:n){
		start<-as.integer(end+1)
		end<-as.integer(start+freq[i,"freq"]-1)
		if(start == end){
			doc<-myMatrix[,start]
		}else{
			cmd<-paste0("doc<-rowSums(myMatrix[,",start,":",end,"])")
			eval(parse( text=cmd ))
		}
		tm<-cbind(tm,doc)
	}
	colnames(tm)<-paste0("D",seq(1:n))
	
	myList[[2]]<-tm
	myList[[3]]<-freq1
	gc()
	return(myList)
}


apply_tfidf<-function(matrix){
	# Compute tfidf weights and apply them to a given terms/documents matrix
	# Arcguments
	# matrix - a term document matrix to which tf-idf weighing should be applied
	
	totals<-colSums(matrix)
	totals[totals==0]=1
	tfidf_matrix<-matrix/totals*gw_idf(matrix)
	tfidf_matrix[is.nan(tfidf_matrix)]=0
	
	return(tfidf_matrix)
}


lsi_mgr<-function(lookup_tbl,desc_col,freq_col,share=SHARE,filename_lsa=LSA_filename,weighted=TRUE){
	# build LSA space
	# Arguments
	# lookup_tbl - a dataframe which data will be used to construct the term document matrix
	# desc_col - a column in the desc_tbl dataframe containing the test used for term document construction
	# freq_col - a column in the desc_tbl dataframe containing the frequencies of the respected rows
	# share - share of eigenvalues to retain
	# filename_lsa - the name of the file to save LSA space and its vocabulary
	# weighted - logical value indicating whether tf-idf weighting should be applied
	
	SHARE<-0.8
	LSA_filename<-"column-desc-set-lsa"
	library(lsa)
	# Construct term document matrix
	myList<-build_tdm(lookup_tbl,desc_col,freq_col,NULL,weighted)
	myVocabulary<-myList[[1]]
	myMatrix<-myList[[2]]
	# Build LSa space
	if(share == 1){
		myLSAspace = lsa(myMatrix, dims=dimcalc_raw())
	}else{
		myLSAspace = lsa(myMatrix, dims=dimcalc_share(share))
	}
	saveRDS(myLSAspace, paste0(filename_lsa,".rds"))
	saveRDS(myVocabulary, paste0(filename_lsa,"-vocab.rds"))
	rm(myLSAspace,myVocabulary,myList)
}

match_desc<-function(terms, desc){
	# mapping query terms to LSI terms
	# Arguments
	# terms - the terms in the vocabulary
	# the existing words that needs to be mapped

	library(stringdist)
	o<-count(desc)
	colnames(o)[1]<-"desc"
	o$desc<-as.character(o$desc)
	v_terms<-terms[terms %in% o$desc]
	r1<-o[o$desc %in% v_terms,]
	gc()
	return(r1)
}

build_query_tdm<-function(lookup_tbl,myLSAspace,myVocabulary,desc_col="Concept_Description",freq_col="Prob",weighted=TRUE){
	# Construct from query concept descriptions a matrix in the LSA space (newer and faster method)
	# Arguments
	# lookup_tbl - a dataframe containing new documents
	# myLSAspace - the existing LSA space
	# myVocabulary - the existing vocabulary
	# desc_col - a column in the desc_tbl dataframe containing the test used for term document construction
	# weighted - logical value indicating whether tf-idf weighting should be applied
	#library(LSAfun)
	#library(stringi)
	library(text2vec)
	
	tk<-myLSAspace$tk
	sk<-myLSAspace$sk
	dk<-myLSAspace$dk
	n<-nrow(dk)
	
	# Construct additional docs term document matrix
	s<-breakdown(as.character(lookup_tbl[[desc_col]]))
	#s<-stri_trans_general(as.character(lookup_tbl[[desc_col]]), "latin-ascii")
	
	it <- itoken(s, preprocess_function = tolower,tokenizer = word_tokenizer, chunks_number = 10, progessbar = F)
	vectorizer = vocab_vectorizer(myVocabulary)
	dtm = create_dtm(it, vectorizer)
	myMatrix<-as.textmatrix(t(as.matrix(dtm)))
	
	if(weighted){
		myMatrix<-apply_tfidf(myMatrix)
		# Adjustment for different descriptions based on frequency
		cmd<-paste0("v<-as.numeric(lookup_tbl$",freq_col,")")
		eval(parse( text=cmd ))
		v2 <- rep(v,each=dim(myMatrix)[1])
		myMatrix<-myMatrix*v2
	}
	tm<-rowSums(myMatrix)
	
	# Map new docs into the existing LSa space
	s<-diag(sk)
	q<-t(tm) %*% as.matrix(tk) %*% solve(s)
	# Extend existing LSA space with new docs
	return(q)
}

build_query_doc<-function(df,myLSAspace,descname="Concept_Description",freqname="Prob",weighted=TRUE){
	# Construct from query concept descriptions a matrix in the LSA space (original method)
	# Arguments
	# df - a new column concept description dataframe
	# descname - the name of the column with concept description
	# freqname - the column name with the probability of the respected concept
	# weighted - default the we are using tf-idf weighting (TRUE)
	library(lsa)
	library(plyr)
	tk<-myLSAspace$tk
	dk<-myLSAspace$dk
	sk<-myLSAspace$sk

	terms<-rownames(tk)
	n<-nrow(df)
	m<-length(terms)
	q_matrix<-matrix(0,ncol=m,nrow=0)
	for(i in 1:n){
		# parse a given item
		item<-as.character(df[i,descname])
		desc<-tolower(unlist(strsplit(item," ")))
		# map the item to actual terms in case it is not exact match to terms
		o<-match_desc(terms,desc)
		if(nrow(o) > 0){
			# calculate query terms frequency
			o$tf<-o$freq/sum(o$freq)
			w<-ifelse(terms %in% o$desc, o$tf, 0)
			# query in original coordinates
			q<-ifelse(terms %in% desc, 1, 0)
			# if weighted apply term frequency
			if(weighted) {
				q<-q*w*as.numeric(df[i,freqname])
				q_matrix<-rbind(q_matrix,q)
			}
		}
	}
	# weighted query in old coordinates
	q<-colSums(q_matrix)
	# express query in new coordinates
	# diagonal eigenvalues matrix
	s<-diag(sk)
	q<-t(q) %*% as.matrix(tk) %*% solve(s)
	return(q)
}

get_similar_doc<-function(column,lookup_tbl,q_doc,myLSAspace,margin=0.01,low_limit=0.5,orig_col="Original_Column"){
	# Identify the most similar documents
	# Arguments
	# column - the name of column that matches the original one
	# lookup_tbl - the original lookup table
	# q_doc query document
	# myLSAspace LSI environment
	# margin - the range of similarity to keep documents compare with the best match
	# low_limit - the lowest level for the document similarity
	# orig_column - the name of original column for which we are searching for matches	
	library(lsa)
	library(data.table)
	dk<-myLSAspace$dk
	m<-nrow(dk)
	v<-vector()
	for(i in 1:m){
		v[i]<-cosine(as.double(q_doc),as.double(dk[i,]))
	}
	v[is.nan(v)]=0
	r<-max(v)
	row<-which(v>(max(v)-margin) & v>low_limit)
	
	z<-data.table(item_id=character(),similarity=numeric());
	size<-length(row)
	if(size>0){
		for(i in 1:size){
			nbr<-as.integer(substring(rownames(dk)[row[i]],2))
			cmd<-paste0("exist_col<-as.character(lookup_tbl[nbr,\"",orig_col,"\"])")
			eval(parse( text=cmd ))
			z<-rbindlist(list(z,list(exist_col,v[row[i]])))
		}
		z<-z[order(-z$similarity),]
	}
	rm(dk,size,m,v,r,row)
	gc()
	return(z)

}

breakdown <- function(x){
  
  x <- tolower(x)
  
  ## Umlaute
  
  x <- gsub(x=x,pattern="\xe4",replacement="ae")
  x <- gsub(x=x,pattern="\xf6",replacement="oe")
  x <- gsub(x=x,pattern="\xfc",replacement="ue")  
  
  ## Accents
  
  x <- gsub(x=x,pattern="\xe0",replacement="a")
  x <- gsub(x=x,pattern="\xe1",replacement="a")
  x <- gsub(x=x,pattern="\xe2",replacement="a")
  
  x <- gsub(x=x,pattern="\xe8",replacement="e")
  x <- gsub(x=x,pattern="\xe9",replacement="e")
  x <- gsub(x=x,pattern="\xea",replacement="e")
  
  x <- gsub(x=x,pattern="\xec",replacement="i")
  x <- gsub(x=x,pattern="\xed",replacement="i")
  x <- gsub(x=x,pattern="\xee",replacement="i")
  
  x <- gsub(x=x,pattern="\xf2",replacement="o")
  x <- gsub(x=x,pattern="\xf3",replacement="o")
  x <- gsub(x=x,pattern="\xf4",replacement="o")
  
  x <- gsub(x=x,pattern="\xf9",replacement="u")
  x <- gsub(x=x,pattern="\xfa",replacement="u")
  x <- gsub(x=x,pattern="\xfb",replacement="u")
 
  
  x <- gsub(x=x,pattern="\xdf",replacement="ss")
  
  ## Convert to ASCII
  
  x <- iconv(x,to="ASCII//TRANSLIT")
  
  ## Punctation, Numbers and Blank lines
  
  x <- gsub(x=x,pattern="[[:punct:]]", replacement=" ")
  x <- gsub(x=x,pattern="[[:digit:]]", replacement=" ")
  x <- gsub(x=x,pattern="\n", replacement=" ")
  x <- gsub(x=x,pattern="\"", replacement=" ")

  
  return(x)  
}

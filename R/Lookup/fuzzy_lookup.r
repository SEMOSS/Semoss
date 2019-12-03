fuzzy_lookup_light<-function(catalog,catalog_col,request,topMatches=5){
	library(stringdist)
	n<-length(request)
	if(n>0){
		df<-data.frame(Request=request,stringsAsFactors=FALSE)
		request_col="Request"
		cmd<-paste0("out<-data.frame(",request_col,"=character(),",catalog_col,"=character(),Similarity=numeric(),stringsAsFactors=FALSE)")
		eval(parse(text=cmd))
		for(i in 1:n){
			cmd<-paste0("z<-stringsim(df$",request_col,"[i],catalog[[catalog_col]],method=\"jw\",p=0.1)")
			eval(parse(text=cmd))
			ind<-order(z, decreasing=TRUE)[1:topMatches]
			if(length(ind)>0){
				cmd<-paste0("cur_out<-data.frame(",request_col,"=rep(df$",request_col,"[i],length(ind)),",catalog_col,"=catalog[[catalog_col]][ind],Similarity=round(z[ind],4),stringsAsFactors=FALSE)")
				eval(parse(text=cmd))
				out<-rbind(out,cur_out)
			}else{
				cmd<-paste0("cur_out<-data.frame(",request_col,"=df$",request_col,"[i],",catalog_col,"=\"no match found\",Similarity=0,stringsAsFactors=FALSE)")
				eval(parse(text=cmd))
				out<-rbind(out,cur_out)
			}
		}
	}else{
		return("Request is empty")
	}
	gc()
	#write.csv(out,"fuzzymapping.csv")
	return(out)
}


fuzzy_lookup<-function(catalog,catalog_fn,request,topMatches=5){
	library(stringdist)
	library(stringi)
	library(lsa)
	library(data.table)
	library(text2vec)
	
	# get fileroot
	fileroot=paste0(catalog_fn,"_blocks")
	# get parameters
	params<-readRDS(paste0(fileroot,"_params.rds"))
	catalog_col<-params[5]
	params<-as.integer(params[1:4])
	blocksize<-params[2]
	stagelimit<-params[3]
	topBlocks<-min(ceiling(stagelimit/blocksize),10)
	
	if(sum(params)==0){
		return("Number of records in the catalog exceeded the upper limit")
	}else if(sum(params[1:2])==0){
		df<-data.frame(Request=request,stringsAsFactors=FALSE)
		request_col="Request"
		cmd<-paste0("out<-data.frame(",request_col,"=character(),",catalog_col,"=character(),Similarity=numeric(),stringsAsFactors=FALSE)")
		eval(parse(text=cmd))
		for(i in 1:n){
			cmd<-paste0("z<-stringsim(df$",request_col,"[i],catalog[[catalog_col]],method=\"jw\",p=0.1)")
			eval(parse(text=cmd))
			ind<-order(z, decreasing=TRUE)[1:topMatches]
			if(length(ind)>0){
				cmd<-paste0("cur_out<-data.frame(",request_col,"=rep(df$",request_col,"[i],length(ind)),",catalog_col,"=catalog[[catalog_col]][ind],Similarity=round(z[ind],4),stringsAsFactors=FALSE)")
				eval(parse(text=cmd))
				out<-rbind(out,cur_out)
			}else{
				cmd<-paste0("cur_out<-data.frame(",request_col,"=df$",request_col,"[i],",catalog_col,"=\"no match found\",Similarity=0,stringsAsFactors=FALSE)")
				eval(parse(text=cmd))
				out<-rbind(out,cur_out)
			}
		}
		gc()
		#write.csv(df,"fuzzymapping.csv")
		return(out)
		
	}else {
		df<-data.frame(Request=request,stringsAsFactors=FALSE)
		request_col="Request"
		df<-map_blocks(df,request_col,topBlocks,fileroot)
		
		# add column for lookup match and the respected similarity
		cmd<-paste0("df$",catalog_col,"<-\"\"")
		eval(parse(text=cmd))
		df$Similarity<-0
		n<-nrow(df)
		if(n>0){
			N<-nrow(catalog)

			cmd<-paste0("out<-data.frame(",request_col,"=character(),",catalog_col,"=character(),Similarity=numeric(),stringsAsFactors=FALSE)")
			eval(parse(text=cmd))
			for(i in 1:n){
				if(df$Block[i]!="0"){
					# get the required records by blocks
					blocks<-as.integer(unlist(strsplit(df$Block[i],",")))

					# get catalog rows defined by mapped blocks
					rows<-paste(sapply(blocks,function(blocks) paste0((blocks-1)*blocksize+1,":",min(+blocks*blocksize,N))),collapse=",")
					cmd<-paste0("catalog_block<-catalog[c(",rows,"),]")
					eval(parse(text=cmd))
					
					# perform fuzzy matching and get the top matches
					cmd<-paste0("z<-stringsim(stringi::stri_unescape_unicode(df$",request_col,"[i]),stringi::stri_unescape_unicode(catalog_block[[catalog_col]]),method=\"jw\",p=0.1)")
					eval(parse(text=cmd))
					ind<-order(z, decreasing=TRUE)[1:topMatches]
					
					# add the matching records to the output dataframe
					if(length(ind)>0){
						cmd<-paste0("cur_out<-data.frame(",request_col,"=rep(df$",request_col,"[i],length(ind)),",catalog_col,"=catalog_block[[catalog_col]][ind],Similarity=round(z[ind],4),stringsAsFactors=FALSE)")
						eval(parse(text=cmd))
						out<-rbind(out,cur_out)
					}else{
						cmd<-paste0("cur_out<-data.frame(",request_col,"=df$",request_col,"[i],",catalog_col,"=\"no match found\",Similarity=0,stringsAsFactors=FALSE)")
						eval(parse(text=cmd))
						out<-rbind(out,cur_out)
					}
				}else{
					cmd<-paste0("df$",catalog_col,"[i]<-\"no match found\"")
					eval(parse(text=cmd))
				}
			}
		}
		#write.csv(out,"fuzzymapping.csv")
		gc()
		return(out)
	}
}

map_blocks<-function(request_df,request_col,topN,fileroot){
	
	myLSAspace<-readRDS(paste0(fileroot,"-lsa.rds"))
	myVocabulary<-readRDS(paste0(fileroot,"-vocab.rds"))
	
	dk<-myLSAspace$dk
	tk<-myLSAspace$tk
	m<-nrow(dk)
	
	request_df$Block<-0
	request_df$BlockSimilarity=""
	n<-nrow(request_df)
	for(i in 1:n){
		cmd<-paste0("request<-request_df$",request_col,"[i]")
		eval(parse(text=cmd))

		# build quesry document
		q_doc<-build_query_tdm(request,tk,dk,myVocabulary)
		
		if(any(q_doc!=0)){
			v<-vector()
			for(j in 1:m){
				v[j]<-cosine(as.double(q_doc),as.double(dk[j,]))
			}
			if(length(v)>0){
				v[is.nan(v)]=0
				row<-order(v, decreasing=TRUE)[1:topN]
				request_df$Block[i]<-paste(row,collapse=",")
				request_df$BlockSimilarity[i]<-paste(round(v[row],4),collapse=",")
			}
		}
	}
	gc()
	return(request_df)
}

build_query_tdm<-function(request,tk,dk,myVocabulary){
	# Construct from query concept descriptions a matrix in the LSA space (newer and faster method)
	# Arguments
	# request - a string request describing the respected zoning/vlan
	# myLSAspace - the existing LSA space
	# myVocabulary - the existing vocabulary

	n<-nrow(dk)	
	# Construct additional docs term document matrix
	s<-breakdown(request)
	it <- itoken(s, preprocess_function = tolower,tokenizer = space_tokenizer, chunks_number = 10, progressbar = F)
	vectorizer = vocab_vectorizer(myVocabulary)
	dtm = create_dtm(it, vectorizer)
	myMatrix<-as.textmatrix(t(as.matrix(dtm)))
	
	q<-t(myMatrix) %*% as.matrix(tk)
	return(q)
}

prepare_catalog<-function(catalog,catalog_fn="ocm",catalog_col="Legal.Name",share=0.8){
# Prepares original data input frame
# Arguments
# catalog - nput dataframe
# catalog_fn - the name the catalog will be saved
# catalog_col - the name of the catalog column that serves as a lookup
# blocksize - the size of the block
# share - the share of overall dimensions to keep
	LOWLIMIT<-2e+05
	N<-nrow(catalog)
	if(N>0){
		params<-compute_params(catalog_fn,N,catalog_col)
		params<-as.integer(params[1:4])
		if(sum(params)>0){
			# no processing into blocks necessary if N <= 20000
			if(N>LOWLIMIT){
				fileroot<-paste0(catalog_fn,"_blocks")
				build_catalog_blocks(catalog,catalog_fn,catalog_col,params)
				fuzzy_lsi_mgr(fileroot,catalog_col,params,share)
			}
			gc()
			return("Catalog preparation completed")
		}else{
			return("Input dataframe size out of range")
		}
	}else{
		return("Cannot prepare catalog. Input dataframe has 0 records")
	}
}

compute_params<-function(catalog_fn,N,catalog_col){
	LOWLIMIT<-2e+05
	HIGHLIMIT<-1e+07
	
	if(N<=LOWLIMIT){
		out<-c(0,0,LOWLIMIT,HIGHLIMIT,catalog_col)
	}else if(N>LOWLIMIT & N<=HIGHLIMIT){
		blocksize<-ceiling(N/250)
		if(blocksize<=20000){
			blocks<-250
		}else{
			blocks<-ceiling(250*blocksize/20000)
			blocksize<-20000
		}
		out<-c(blocks,blocksize,LOWLIMIT,HIGHLIMIT,catalog_col)
	}else{
		out<-c(0,0,0,0,catalog_col)
		print("Number of record in the catalog exceed the upper limit 10,000,000")
	}
	saveRDS(out,paste0(catalog_fn,"_blocks_params.rds"))
	return(out)
}

build_catalog_blocks<-function(catalog,catalog_fn="ocm",catalog_col="Legal.Name",params){
# Builds blocks of original data input frame
# Arguments
# catalog - nput dataframe
# catalog_fn - the name the catalog will be saved
# catalog_col - the name of the catalog column that serves as a lookup
# blocks_fn - the name of the file containing blocks of original data
# blocksize - the size of the block
	
	fileroot<-paste0(catalog_fn,"_blocks")
	saveRDS(catalog,paste0(catalog_fn,".rds"))
	# get clock size info
	if(params[1]!=0){
		# build blocks
		N<-nrow(catalog)
		blocksize<-params[2]
		n<-params[1]
		txt<-vector()
		for(i in 1:n){
			start<-(i-1)*blocksize+1
			end<-min(i*blocksize,N)
			cmd<-paste0("txt<-append(txt,paste(catalog$",catalog_col,"[start:end],collapse=\" \"))")
			eval(parse(text=cmd))
		}
		cmd<-paste0("df<-data.frame(",catalog_col,"=txt,stringsAsFactors=FALSE)")
		eval(parse(text=cmd))
		saveRDS(df,paste0(fileroot,".rds"))
	}
	gc()
}


fuzzy_lsi_mgr<-function(fileroot="ocm_blocks",catalog_col="Legal.Name",params,share=0.8){
	# build LSA space
	# Arguments
	# filename - filename for a lookup_tbl
	# share - share of eigenvalues to retain
	# filename_lsa - the name of the file to save LSA space and its vocabulary
	
	library(lsa)
	fileroots<-fileroot
	blocks<-params[1]
	n<-length(fileroots)
	for(i in 1:n){
		fileroot<-fileroots[i]
		# Construct term document matrix
		myList<-build_tdm(fileroot,catalog_col,NULL)
		myVocabulary<-myList[[1]]
		myMatrix<-myList[[2]]
		# Build LSA space
		if(share == 1){
			myLSAspace = lsa(myMatrix, dims=dimcalc_raw())
		}else{
			myLSAspace = lsa(myMatrix, dims=dimcalc_share(share))
		}
		saveRDS(myLSAspace, paste0(fileroot,"-lsa.rds"))
		saveRDS(myVocabulary, paste0(fileroot,"-vocab.rds"))
		rm(myLSAspace,myVocabulary,myList)
	}
	gc()
}

build_tdm<-function(fileroot,catalog_col,vocabulary=NULL){
	# Construct LSA space
	# Arguments
	# desc_tbl - dataframe which data will be used to construct the term document matrix
	# desc_col - a column in the desc_tbl dataframe containing the test used for term document construction
	# vocabulary - either null for initial construction or the existing vocabulary for adding documents to the existing matrix
	MINWORDLENGTH<-2
	WORDSTOEXCLUDE<-c("a","the","this","these","their","that","those","then","and","an","as","over","with","within","without","when","why","how","in","on","of",
	"or","to","into","by","from","for","at","so","then","thus","here","there","whether","is","are","not","other","too","his","her","they","oh","number")

	library(text2vec)
	library(data.table)
	library(plyr)
	library(stringr)
	library(lsa)
	myList<-list()
	desc_tbl<-readRDS(paste0(fileroot,".rds"))
	#desc_col="Legal.Name"
	# Construct terms/documents matrix based on a given set of documents or read it from the file
	s<-breakdown(str_replace_all(desc_tbl[[catalog_col]],"[^[:graph:]]", " "))
	it <- itoken(s, preprocess_function = tolower,tokenizer = word_tokenizer, chunks_number = 10, progressbar = F)
	if(is.null(vocabulary)){
		vocab<-create_vocabulary(it,stopword=WORDSTOEXCLUDE,ngram=c(ngram_min=1L,ngram_max=1L))
	}else{
		vocab<-vocabulary
	}
	# exclude words consisting of a single character
	vocab<-vocab[nchar(vocab$term) >= 2,]
	vocab<-vocab[vocab$term_count > 1,]
	vocab<-vocab[vocab$doc_count!=nrow(desc_tbl),]
	myList[[1]]<-vocab
	vectorizer = vocab_vectorizer(vocab)
	dtm = create_dtm(it, vectorizer)
	# Use the next line if tf-idf is not needed!!!
	myMatrix<-as.textmatrix(t(as.matrix(dtm)))
	myMatrix = lw_logtf(myMatrix) * gw_idf(myMatrix)

	myList[[2]]<-myMatrix
	gc()
	return(myList)
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

trim <- function (x) gsub("^\\s+|\\s+$", "", x)




span<-function(a){
  if(length(a) == 1 & is.na(a[1])){
    x<-0;
  }else{
    x<-length(a);
  }
  return(x);
}

splitCamelCase<-function(a){
  a<-paste(strsplit(a, "(?<=[a-z])(?=[A-Z])", perl = TRUE)[[1]], collapse=" ");
  a<-gsub("_", " ", a);
  return(a);
}

getGoogleSearchUrl <- function(searchTerm, domain = '.com', quotes=TRUE) {
  if(quotes) {
    searchTerm <- paste("\"", searchTerm, "\"", sep='');
  }
  searchTerm <- URLencode(searchTerm);
  searchURL <- paste('http://www.google', domain, '/search?q=', searchTerm, sep='');
  return(searchURL)
}

searchGoogle <- function(term) {
  library(XML);
  library(httr);
  library(RCurl);
  library(stringr);
  
  EXCLUDE<-"Advanced searchSearch Help Send feedback"
  EXCLUDE1<-"In order to show you the most relevant results"
  searchURL<-getGoogleSearchUrl(searchTerm=term);
  response<- GET(searchURL);
  if (response$status_code!=200){ # HTTP request failed!!
    # do some stuff...
    return("");
  }
  doc.html<-htmlTreeParse(searchURL,useInternal = TRUE)
  doc.text<-unlist(xpathApply(doc.html, '//p', xmlValue))
  doc.text<-str_trim(gsub('\\n', ' ', doc.text),"both")
  doc.text<-doc.text[!(doc.text==EXCLUDE)]
  doc.text<-doc.text[!(substr(doc.text,1,nchar(EXCLUDE1))==EXCLUDE1)]
  return(doc.text)
}

generateDescriptionFrame<-function(uniqueValues){
  library(WikidataR);
  
  uniqueDescriptions <- array();
  numUnique = length(uniqueValues);
  for(i in 1:numUnique) {
    cleanValue <- splitCamelCase(uniqueValues[i]);
    values <- c();
    if(cleanValue != "") {
      values <- tryCatch({
        find_item(cleanValue);
      });
    }
    if(length(values) == 0) {
      # wiki returned nothing, try google
      googleResults <- searchGoogle(cleanValue);
      uniqueDescriptions[span(uniqueDescriptions)+1] <- paste(googleResults, collapse='; ');
    } else {
      wikiResults <- lapply(values, function(x) { cbind(x$label, x$description) } );
      uniqueDescriptions[span(uniqueDescriptions)+1] <- paste(lapply(wikiResults, function(x) { paste(x, collapse= ' ') }), collapse='; ');
    }
  }
  
  colToDescriptionFrame <- data.table(uniqueValues, uniqueDescriptions);
  names(colToDescriptionFrame) <- c('column', 'description');
  rm(numUnique);
  return(colToDescriptionFrame);
}

getDocumentCosineSimilarity<-function(allTables, allColumns) {
  library(text2vec);
  library(data.table);
  library(lsa);
  
  uniqueColumnNames = unique(allColumns);
  colToDescriptionFrame <- generateDescriptionFrame(uniqueColumnNames);
  
  MINWORDLENGTH<-2;
  WORDSTOEXCLUDE<-c('a',"the","this","these","their","that","those","then","and","an","as","over","with","within","without","when","why","how","in",
                    "on","of","or","to","by","from","for","at","so","then","thus","here","there");
  
  prep_fun<-tolower;
  tok_fun<-word_tokenizer;
  tokens<-itoken(colToDescriptionFrame$description, 
                  preprocessor = prep_fun, 
                  tokenizer = tok_fun, 
                  ids = colToDescriptionFrame$column, 
                  progressbar = FALSE);
  vocab <- create_vocabulary(tokens, stopwords = WORDSTOEXCLUDE, ngram=c(ngram_min=1L,ngram_max=1L));
  vocab <- vocab[nchar(vocab$term) >= MINWORDLENGTH,];
  vectorizer <- vocab_vectorizer(vocab);
  dtm <- create_dtm(tokens, vectorizer);
  myMatrix <- as.textmatrix(t(as.matrix(dtm)));
  myLSAspace = lsa(myMatrix, dims=dimcalc_share(.8));
  cosineSimMatrix <- cosine(t(myLSAspace$dk));
  
  # need to go through and merge the cosine distance
  # in addition to the column / tables
  dimensions <- dim(cosineSimMatrix);
  cosine_distance <- round(as.vector(cosineSimMatrix), 4);
  col1 <- rep(uniqueColumnNames, each=dimensions[2]);
  col2 <- rep(uniqueColumnNames, dimensions[1]);
  
  similarity_frame <- as.data.table(as.data.frame(cbind(col1, col2, cosine_distance)));
  names(similarity_frame) <- c('sourceCol', 'targetCol', 'distance');
  # remove exact column name matches
  similarity_frame <- similarity_frame[toupper(sourceCol) != toupper(targetCol)];
  
  # make sure column is numeric
  similarity_frame$distance <- as.numeric(as.character(similarity_frame$distance));
  # if we couldn't get a description
  # the matching will be NaN
  # so we will drop those rows
  similarity_frame <- similarity_frame[!is.na(distance)]; 
  
  # merge back the table column names
  # create the table and column frame
  tableToCol <- data.table(allTables, allColumns);
  names(tableToCol) <- c('table', 'column');
  similarity_frame <- merge(similarity_frame,tableToCol, by.x='sourceCol', by.y='column', allow.cartesian=TRUE);
  colnames(similarity_frame)[which(names(similarity_frame) == "table")] <- "sourceTable";
  similarity_frame <- merge(similarity_frame,tableToCol, by.x='targetCol', by.y='column', allow.cartesian=TRUE);
  colnames(similarity_frame)[which(names(similarity_frame) == "table")] <- "targetTable";
  # ignore same table joins
  similarity_frame <- similarity_frame[targetTable != sourceTable];
  
  # add in descriptions
  similarity_frame <- merge(similarity_frame,colToDescriptionFrame, by.x='sourceCol', by.y='column', allow.cartesian=TRUE);
  colnames(similarity_frame)[which(names(similarity_frame) == "description")] <- "sourceColumnDescription";
  # ignore where description is empty
  similarity_frame <- similarity_frame[similarity_frame$sourceColumnDescription != ""];
  
  # add in descriptions
  similarity_frame <- merge(similarity_frame,colToDescriptionFrame, by.x='targetCol', by.y='column', allow.cartesian=TRUE);
  colnames(similarity_frame)[which(names(similarity_frame) == "description")] <- "targetColumnDescription";
  # ignore where description is empty
  similarity_frame <- similarity_frame[similarity_frame$targetColumnDescription != ""];
  
  rm(allTables, allColumns, uniqueColumnNames, prep_fun, tok_fun, tokens, vocab, vectorizer, dtm, myMatrix, cosineSimMatrix,
    dimensions, cosine_distance, col1, col2, tableToCol);
  
  return(similarity_frame);
}


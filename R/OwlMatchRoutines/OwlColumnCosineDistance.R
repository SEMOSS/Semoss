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

generateDescriptionFrame<-function(uniqueValues){
  library(WikidataR);
  
  uniqueDescriptions <- array();
  numUnique = length(uniqueValues);
  for(i in 1:numUnique) {
    values <- find_item(splitCamelCase(uniqueValues[i]));
    data <- lapply(values, function(x) { cbind(x$label, x$description) } );
    uniqueDescriptions[span(uniqueDescriptions)+1] <- paste(lapply(data, function(x) { paste(x, collapse= ' ') }), collapse=' ');
  }
  
  colToDescriptionFrame <- data.table(uniqueValues, uniqueDescriptions);
  names(colToDescriptionFrame) <- c('column', 'description');
  rm(numUnique);
  return(colToDescriptionFrame);
}

getDocumentCostineSimilarityMatrix<-function(allTables, allColumns) {
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
  cosineSimMatrix <- cosine(myMatrix);
  
  # need to go through and merge the cosine distance
  # in addition to the column / tables
  dimensions <- dim(cosineSimMatrix);
  cosine_distance <- round(as.vector(cosineSimMatrix), 4);
  col1 <- rep(uniqueColumnNames, each=dimensions[2]);
  col2 <- rep(uniqueColumnNames, dimensions[1]);
  
  similarity_frame <- as.data.table(as.data.frame(cbind(col1, col2, cosine_distance)));
  similarity_frame[,3] <- as.numeric(as.character(similarity_frame$cosine_distance));
  names(similarity_frame) <- c('sourceCol', 'targetCol', 'distance');
  
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
  
  rm(allTables, allColumns, uniqueColumnNames, prep_fun, tok_fun, tokens, vocab, vectorizer, dtm, myMatrix, cosineSimMatrix,
     dimensions, cosine_distance, col1, col2, tableToCol);
  
  return(similarity_frame);
}


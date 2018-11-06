getDocumentCosineSimilarity <- function(colToDescriptionFrame) {
library(text2vec);
library(data.table);
library(lsa);

MINWORDLENGTH<-2;
WORDSTOEXCLUDE<-c('a',"the","this","these","their","that","those","then","and","an","as","over","with","within","without","when","why","how","in","on","of","or","to","by","from","for","at","so","then","thus","here","there");

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
cosineSimMatrix

# need to go through and merge the cosine distance
# in addition to the column / tables
dimensions <- dim(cosineSimMatrix);
cosine_distance <- round(as.vector(cosineSimMatrix), 4);
cosine_distance
col1 <- rep(colToDescriptionFrame$column, each=dimensions[2]);
col2 <- rep(colToDescriptionFrame$column, dimensions[1]);

similarity_frame <- as.data.table(as.data.frame(cbind(col1, col2, cosine_distance)));
names(similarity_frame) <- c('sourceCol', 'targetCol', 'distance');
# remove exact column name matches
similarity_frame <- similarity_frame[sourceCol != targetCol];

# make sure column is numeric
similarity_frame$distance <- as.numeric(as.character(similarity_frame$distance));
# if we couldn't get a description
# the matching will be NaN
# so we will drop those rows
similarity_frame <- similarity_frame[!is.na(distance)]; 
return(similarity_frame);

}


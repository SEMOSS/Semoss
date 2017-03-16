# Utility functions that are sourced by RRoutine.java

# Default result function
# The user can override this in his or her analytics script
# RRoutine.java looks for this function,
# so having this avoids an error
GetResult <- function() {
  # Return nothing
}

# Function to run locality sensitive hashing match
run_lsh_matching <- function(path,N,b,cutoff){
  library(textreuse)
  corpus_minhash <- minhash_generator(n = N, seed = 953)
  corpus <- TextReuseCorpus(dir = path,tokenizer = tokenize_ngrams, n = 1,minhash_func = corpus_minhash)
  
  buckets <- lsh(corpus, bands = b)
  
  candidates<-lsh_candidates(buckets)
  subset<-lsh_subset(candidates)
  
  mycorpus<-corpus[subset]
  m<-pairwise_compare(mycorpus,ratio_of_matches,directional=TRUE)
  
  n<-nrow(m)
  df<-data.frame(item=character(50),match=character(50),score=numeric(5),stringsAsFactors=FALSE)
  r<-0
  
  for (i in 1:n){
    for (j in 1:n){
      if(i!=j & m[i,j] > cutoff/2){
        r<-r+1
        df[r,2]<-rownames(m)[i]
        df[r,1]<-colnames(m)[j]
        df[r,3]<-m[i,j]
      }
    }
  }
  df<-df[df$score > cutoff,]
  df<-df[order(df$item,-df$score,df$match),]
  setDT(df)
  return(df)
}

# For test cases
#path<-"C:/Users/tbanach/Workspace/SemossDev/R/MatchingRepository";
#run_lsh_matching(path,210,70,0.5)

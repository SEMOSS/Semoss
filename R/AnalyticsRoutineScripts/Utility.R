# Utility functions that are sourced by RRoutine.java

# Default result function
# The user can override this in his or her analytics script
# RRoutine.java looks for this function,
# so having this avoids an error
GetResult <- function() {
  # Return nothing
}

# Function to run locality sensitive hashing match
run_lsh_matching <- function(path, N, b, similarityThreshold, instancesThreshold, delimiter){
  
  # Library the necessary packages
  library(textreuse)
  library(data.table)
  
  # Create a minhash generator
  corpus_minhash <- minhash_generator(n = N, seed = 953)
  
  # Throws an error if there are no files in the corpus
  corpus <- tryCatch({
    TextReuseCorpus(dir = path, tokenizer = tokenize_ngrams, n = 1, minhash_func = corpus_minhash)
  }, error = function(e) {
    return("error")
  })
  
  # Return an empty frame if there was an error
  if (!is.list(corpus)) {
    df <- data.frame(item_engine = character(1), item_concept = character(1), match_engine = character(1), match_concept = character(1), score = numeric(1), stringsAsFactors = FALSE)
    df[1, 1] <- "(No Concepts Found)"
    return(df)
  }

  # Determine candidates
  buckets <- lsh(corpus, bands = b)
  candidates <- lsh_candidates(buckets)
  subset <- lsh_subset(candidates)
  mycorpus <- corpus[subset]
  
  # Throws an error when there are not enough concepts to consider matches
  m <- tryCatch({
    pairwise_compare(mycorpus, ratio_of_matches, directional = TRUE)
  }, error = function(e) {
    return("error")
  })

  # Return an empty frame if there was an error
  if (!is.matrix(m)) {
    df <- data.frame(item_engine = character(1), item_concept = character(1), match_engine = character(1), match_concept = character(1), score = numeric(1), stringsAsFactors = FALSE)
    df[1, 1] <- "(No Matches Found)"
    return(df)
  }
  
  # Create the data frame
  df <- pairwise_candidates(m, directional = TRUE)
  df <- df[c(2, 1, 3)]
  names(df) <- c("item", "match", "score")

  # Add in the word count
  wordcounts <- wordcount(corpus)
  df$item_instances <- NA
  df$match_instances <- NA
  for (r in 1:nrow(df)) {
    df$item_instances[r] <- wordcounts[df[r, 1][[1]]][[1]]
    df$match_instances[r] <- wordcounts[df[r, 2][[1]]][[1]]
  }
  
  # Only return records above the thresholds
  df <- df[df$score > similarityThreshold, ]
  df <- df[df$item_instances > instancesThreshold, ]
  df <- df[df$match_instances > instancesThreshold, ]
  
  # Order the data frame properly
  df <- df[order(df$item, -df$score, df$match), ]

  # Split out the engine and concept
  setDT(df)
  df[, c("item_engine", "item_concept") := tstrsplit(item, delimiter, fixed=TRUE)]
  df[, c("match_engine", "match_concept") := tstrsplit(match, delimiter, fixed=TRUE)]
  
  # Delete redundant columns
  df[, item := NULL]
  df[, match := NULL]
  return(df)
}

# For test cases
#path <- "C:\\Users\\tbanach\\Documents\\SEMOSS Dev Projects\\Semantic Matching\\TAP_Core_Data_Corpus"
#path <- "C:\\Users\\tbanach\\Workspace\\SemossDev\\R\\MatchingRepository"
#N <- 200
#b <- 40
#similarityThreshold <- 0.5
#instancesThreshold <- 1000
#delimiter = ";"
#dt <- run_lsh_matching(path, N, b, similarityThreshold, instancesThreshold, delimiter)

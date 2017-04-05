# Utility functions that are sourced by RRoutine.java

# Default result function
# The user can override this in his or her analytics script
# RRoutine.java looks for this function,
# so having this avoids an error
GetResult <- function() {
  # Return nothing
}

# Function to run locality sensitive hashing match
run_lsh_matching <- function(path, N, b, similarityThreshold, instancesThreshold, delimiter, tempPath){
  
  # Library the necessary packages
  library(textreuse)
  library(data.table)
  
  
  ##################################################
  # Run LSH
  ##################################################
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
  df[, c("item_engine", "item_concept", "item_property") := tstrsplit(item, delimiter, fixed=TRUE)]
  df[, c("match_engine", "match_concept", "match_property") := tstrsplit(match, delimiter, fixed=TRUE)]
  
  # Delete redundant columns
  df[, item := NULL]
  df[, match := NULL]
  
  
  ##################################################
  # Create the files needed to persist the data in the right format
  ##################################################
  dt <- setDT(df)
  
  # Save the original concept and property names
  dt$item_concept_original <- dt$item_concept
  dt$match_concept_original <- dt$match_concept
  dt$item_property_original <- dt$item_property
  dt$match_property_original <- dt$match_property
  
  
  ##################################################
  # Make concept/property a unique identifier
  ##################################################
  
  # Specify what to separate engines, concepts, and properties by
  ecp.separator <- "~"
  
  # For the item
  dt$item_engine <- as.character(dt$item_engine)
  dt$item_concept <- as.character(dt$item_concept)
  dt$item_concept <- paste(dt$item_concept, dt$item_engine, sep = ecp.separator)
  dt$item_property <- as.character(dt$item_property)
  item.is.property <- which(!is.na(dt$item_property))
  dt$item_property[item.is.property] <- paste(dt$item_property[item.is.property], dt$item_concept[item.is.property], sep = ecp.separator)
  
  # For the match
  dt$match_engine <- as.character(dt$match_engine)
  dt$match_concept <- as.character(dt$match_concept)
  dt$match_concept <- paste(dt$match_concept, dt$match_engine, sep = ecp.separator)
  dt$match_property <- as.character(dt$match_property)
  match.is.property <- which(!is.na(dt$match_property))
  dt$match_property[match.is.property] <- paste(dt$match_property[match.is.property], dt$match_concept[match.is.property], sep = ecp.separator)
  
  
  ##################################################
  # Create a column for the match
  ##################################################
  
  # Specify what to separate matches by
  match.separator <- "%"
  
  # concept-concept
  c.c <- which(is.na(dt$item_property) & is.na(dt$match_property))
  dt$match[c.c] <- paste(dt$item_concept[c.c], dt$match_concept[c.c], sep = match.separator)
  
  # concept-property
  c.p <- which(is.na(dt$item_property) & !is.na(dt$match_property))
  dt$match[c.p] <- paste(dt$item_concept[c.p], dt$match_property[c.p], sep = match.separator)
  
  # property-concept
  p.c <- which(!is.na(dt$item_property) & is.na(dt$match_property))
  dt$match[p.c] <- paste(dt$item_property[p.c], dt$match_concept[p.c], sep = match.separator)
  
  # propery-property
  p.p <- which(!is.na(dt$item_property) & !is.na(dt$match_property))
  dt$match[p.p] <- paste(dt$item_property[p.p], dt$match_property[p.p], sep = match.separator)
  
  
  ##################################################
  # Create tables for loops
  ##################################################
  
  # concept-match
  c.m <- which(is.na(dt$item_property))
  concept.match <- dt[c.m, c("item_concept", "match")]
  colnames(concept.match) <- c("concept_id", "match_1")
  
  # match-concept
  m.c <- which(is.na(dt$match_property))
  match.concept <- dt[m.c, c("match", "match_concept")]
  colnames(match.concept) <- c("match_1", "concept_id")
  
  # property-match
  p.m <- which(!is.na(dt$item_property))
  property.match <- dt[p.m, c("item_property", "match")]
  colnames(property.match) <- c("property_id", "match_1")
  
  # match-property
  m.p <-which(!is.na(dt$match_property))
  match.property <- dt[m.p, c("match", "match_property")]
  colnames(match.property) <- c("match_1", "property_id")
  
  
  ##################################################
  # Create a table for the unique engines, concepts, and properties
  ##################################################
  
  # Start with the item engine/concepts
  unique.ecp <- dt[, c("item_engine", "item_concept", "item_concept_original", "item_property", "item_property_original", "item_instances")]
  
  # Temporarily change the column names to match
  colnames(unique.ecp) <- c("match_engine", "match_concept", "match_concept_original", "match_property", "match_property_original", "match_instances")
  
  # Append match engine and concpets
  unique.ecp <- rbind(unique.ecp, dt[, c("match_engine", "match_concept", "match_concept_original", "match_property", "match_property_original", "match_instances")])
  
  # Change the names to just engine and concept
  colnames(unique.ecp) <- c("engine", "concept_id", "concept", "property_id", "property", "instances")
  
  # Remove duplicated values
  setkeyv(unique.ecp, c("engine", "concept_id", "property_id"))
  unique.ecp <- unique(unique.ecp)
  
  
  ##################################################
  # Create a table that has just the match and its score
  ##################################################
  match <- dt[, c("match", "score")]
  
  ##################################################
  # Write all the tables
  ##################################################
  #write.csv(dt, paste0(tempPath, "\\", "0_flat_table.csv"), row.names = FALSE, na = "")
  write.csv(unique.ecp, paste0(tempPath, "\\", "1_unique_ecp.csv"), row.names = FALSE, na = "")
  write.csv(match, paste0(tempPath, "\\", "2_match.csv"), row.names = FALSE, na = "")
  write.csv(concept.match, paste0(tempPath, "\\", "3_concept_match.csv"), row.names = FALSE, na = "")
  write.csv(match.concept, paste0(tempPath, "\\", "4_match_concept.csv"), row.names = FALSE, na = "")
  write.csv(property.match, paste0(tempPath, "\\", "5_property_match.csv"), row.names = FALSE, na = "")
  write.csv(match.property, paste0(tempPath, "\\", "6_match_property.csv"), row.names = FALSE, na = "")
  
  # Finally return the data frame
  return(df)
}

# For test cases
#path <- "C:\\Users\\tbanach\\Documents\\SEMOSS Dev Projects\\Semantic Matching\\TAP_Core_Data_Corpus"
#path <- "C:\\Users\\tbanach\\Workspace\\SemossDev\\R\\MatchingRepository"
#N <- 200
#b <- 40
#similarityThreshold <- 0.5
#instancesThreshold <- 1
#delimiter = ";"
#tempPath <- "C:\\Users\\tbanach\\Workspace\\SemossDev\\R\\Temp"
#dt <- run_lsh_matching(path, N, b, similarityThreshold, instancesThreshold, delimiter, tempPath)

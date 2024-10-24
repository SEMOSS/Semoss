# Function to run locality sensitive hashing match
run_lsh_matching <- function(path, N, b, similarityThreshold, instancesThreshold, delimiter, rdfPath, rdbmsPath, metadataPath){
  
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
  
  # Used to get total instance count
  df$item_total_count <- NA
  df$match_total_count <- NA
  metadata<-read.csv(metadataPath, header = TRUE, sep = ",")
  df$item_has_properties <- NA
  df$match_has_properties <- NA


  df$match_instances <- NA
  for (r in 1:nrow(df)) {
	item.file.name <- df[r, 1][[1]]
	match.file.name <- df[r, 2][[1]]
    df$item_instances[r] <- wordcounts[item.file.name][[1]]
    df$match_instances[r] <- wordcounts[match.file.name][[1]]
	
	# Get total instance count for item and match based on metadata file
	if(grepl("%%%",item.file.name)){
	pos <- regexpr('%%%', item.file.name)
	item.file.name <- substr(item.file.name, 0, pos-1)
	item.file.name <- paste0(item.file.name, ";")
	}
	if(grepl(".*;",item.file.name)){
	pos <- regexpr(';', item.file.name)
	values <- strsplit(item.file.name, ";")
	item.file.name <-paste0(values[[1]][1],';', values[[1]][2])
	item.file.name <- paste0(item.file.name, ";")
	}
	
	if(grepl("%%%",match.file.name)){
	pos <- regexpr('%%%', match.file.name)
	match.file.name <- substr(match.file.name, 0, pos-1)
	match.file.name <- paste0(match.file.name, ";")
	}
	if(grepl(".*;",match.file.name)){
	pos <- regexpr(';', match.file.name)
	values <- strsplit(match.file.name, ";")
	match.file.name <-paste0(values[[1]][1],';', values[[1]][2])
	match.file.name <- paste0(match.file.name, ";")
	}
	
	item.total.count <- metadata[metadata$sourceFileName == item.file.name,][[2]]
	match.total.count <- metadata[metadata$sourceFileName == match.file.name,][[2]]
	if(length(item.total.count) == 0) {
		df$item_total_count[r] = 0
	} 
	else {
	    df$item_total_count[r] = item.total.count
	}
	
	if(length(match.total.count) == 0) {
		df$match_total_count[r] = 0
	} 
	else {
	    df$match_total_count[r] = match.total.count
	}
	
	item.has.properties <- metadata[metadata$sourceFileName == item.file.name,][[3]]
	if(length(item.has.properties) == 0) {
		df$item_has_properties[r] = 0
	}
	else {
	df$item_has_properties[r] = item.has.properties
	}
	
	match.has.properties <- metadata[metadata$sourceFileName == match.file.name,][[3]]
	if(length(match.has.properties) == 0) {
		df$match_has_properties[r] = 0
	}
	else {
	df$match_has_properties[r] = match.has.properties
	}
  }
  
  # Only return records above the thresholds
  df <- df[df$score > similarityThreshold, ]
  df <- df[df$item_instances > instancesThreshold, ]
  df <- df[df$match_instances > instancesThreshold, ]
  
  # Order the data frame properly
  df <- df[order(df$item, -df$score, df$match), ]

  #obtain unique count to total count ratio
  
  # Split out the engine and concept
  # Add a period at the end in case there are no properties
  setDT(df)
  df[, c("item_engine", "item_concept", "item_property") := tstrsplit(paste0(item, "."), delimiter, fixed=TRUE)]
  df[, c("match_engine", "match_concept", "match_property") := tstrsplit(paste0(match, "."), delimiter, fixed=TRUE)]
  
  # Remove the period
  df$item_property <- substring(df$item_property, 1, nchar(df$item_property) - 1)
  df$match_property <- substring(df$match_property, 1, nchar(df$match_property) - 1)
  
  # If the length of the property is zero, make NA (needed for checks below)
  df$item_property[which(df$item_property == "")] <- NA
  df$match_property[which(df$match_property == "")] <- NA
  
  # Delete redundant columns
  df[, item := NULL]
  df[, match := NULL]
  
  
  ##################################################
  # Create the files needed to persist the data in the right format
  ##################################################
  # We return df below, so start working with a new data table
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
  colnames(concept.match) <- c("concept_id", "match_id")
  
  # match-concept
  m.c <- which(is.na(dt$match_property))
  match.concept <- dt[m.c, c("match", "match_concept")]
  colnames(match.concept) <- c("match_id", "concept_id")
  
  # property-match
  p.m <- which(!is.na(dt$item_property))
  property.match <- dt[p.m, c("item_property", "match")]
  colnames(property.match) <- c("property_id", "match_id")
  
  # match-property
  m.p <-which(!is.na(dt$match_property))
  match.property <- dt[m.p, c("match", "match_property")]
  colnames(match.property) <- c("match_id", "property_id")
  

  ##################################################
  # Create a table for the unique engines, concepts, and properties
  ##################################################
  
  # Start with the item engine/concepts
  unique.ecp <- dt[, c("item_engine", "item_concept", "item_concept_original", "item_property", "item_property_original", "item_instances", "item_total_count", "item_has_properties")]
  
  # Temporarily change the column names to match
  colnames(unique.ecp) <- c("match_engine", "match_concept", "match_concept_original", "match_property", "match_property_original", "match_instances", "match_total_count", "match_has_properties")
  
  # Append match engine and concpets
  unique.ecp <- rbind(unique.ecp, dt[, c("match_engine", "match_concept", "match_concept_original", "match_property", "match_property_original", "match_instances", "match_total_count", "match_has_properties")])
  
  # Change the names to just engine and concept
  colnames(unique.ecp) <- c("engine", "concept_id", "concept", "property_id", "property", "instances", "total_count", "has_properties")
  
  # Remove duplicated values
  setkeyv(unique.ecp, c("engine", "concept_id", "property_id"))
  unique.ecp <- unique(unique.ecp)
  
  
  ##################################################
  # Create a table that has just the match and its score
  ##################################################
  match <- dt[, c("match", "score")]
  colnames(match) <- c("match_id", "score")
  colnames(dt) <- c("score","item_instances", "item_total_count", "match_total_count", "item_has_properties", "match_has_properties", "match_instances","item_engine","item_concept_id","item_property_id","match_engine","match_concept_id","match_property_id","item_concept","match_concept","item_property","match_property","match_id")

  ##################################################
  # Add dummy nodes to make sure there is data to generate the metamodel properly
  ##################################################
  s.e <- "SourceEngineExample"
  s.c <- "SourceConcept"
  s.c.id <- paste0(s.c, ecp.separator, s.e)
  s.p <- "SourceProperty"
  s.p.id <- paste0(s.p, ecp.separator, s.c.id)
  t.e <- "TargetEngineExample"
  t.c <- "TargetConcept"
  t.c.id <- paste0(t.c, ecp.separator, t.e)
  t.p <- "TargetProperty"
  t.p.id <- paste0(t.p, ecp.separator, t.c.id)
  c.c <- paste0(s.c.id, match.separator, t.c.id)
  c.p <- paste0(s.c.id, match.separator, t.p.id)
  p.c <- paste0(s.p.id, match.separator, t.c.id)
  p.p <- paste0(s.p.id, match.separator, s.p.id)
  
  # unique.ecp
  # ("engine", "concept_id", "concept", "property_id", "property", "instances", "total_count", "has_properties")
  unique.ecp <- rbindlist(list(unique.ecp, list(s.e, s.c.id, s.c, NA, NA, 0, 0, 0)))
  unique.ecp <- rbindlist(list(unique.ecp, list(t.e, t.c.id, t.c, NA, NA, 0, 0, 0)))
  unique.ecp <- rbindlist(list(unique.ecp, list(s.e, s.c.id, s.c, s.p.id, s.p, 0, 0, 0)))
  unique.ecp <- rbindlist(list(unique.ecp, list(t.e, t.c.id, t.c, t.p.id, t.p, 0, 0, 0)))
  
  # match
  # ("match_id", "score")
  match <- rbindlist(list(match, list(c.c, 1)))
  match <- rbindlist(list(match, list(c.p, 1)))
  match <- rbindlist(list(match, list(p.c, 1)))
  match <- rbindlist(list(match, list(p.p, 1)))
  
  # concept.match
  # ("concept_id", "match_id")
  concept.match <- rbindlist(list(concept.match, list(s.c.id, c.c)))
  concept.match <- rbindlist(list(concept.match, list(s.c.id, c.p)))
  
  # match.concept
  # ("match_id", "concept_id")
  match.concept <- rbindlist(list(match.concept, list(c.c, t.c.id)))
  match.concept <- rbindlist(list(match.concept, list(p.c, t.c.id)))
  
  # property.match
  # ("property_id", "match_id")
  property.match <- rbindlist(list(property.match, list(s.p.id, p.c)))
  property.match <- rbindlist(list(property.match, list(s.p.id, p.p)))
  
  # match.property
  # ("match_id", "property_id")
  match.property <- rbindlist(list(match.property, list(c.p, t.p.id)))
  match.property <- rbindlist(list(match.property, list(p.p, t.p.id)))
  
  #check if item id is for concept or property
  is.concept.item <- NA
  
  property.ids.item <- dt$item_property_id
  for (i in seq_along(property.ids.item)) {
	if(is.na(property.ids.item[i])) {
		is.concept.item[i] = 1
	} else {
		is.concept.item[i] =0}
	}
	
	dt<-cbind(dt, is.concept.item)
	
	#check if match id is for concept or property

	is.concept.match <- NA
  
  property.ids.match <- dt$match_property_id
  for (i in seq_along(property.ids.match)) {
	if(is.na(property.ids.match[i])) {
		is.concept.match[i] = 1
	} else {
		is.concept.match[i] =0}
	}
	
	dt<-cbind(dt, is.concept.match)
	
	#check if item id if for relation
	is.relation.item <- NA
  
	relation.ids.item <- dt$item_concept_id
	for (i in seq_along(relation.ids.item)) {
	if(grepl('%%%', relation.ids.item[i])) {
		is.relation.item[i] = 1
	} else {
		is.relation.item[i] =0}
	}
	
	dt<-cbind(dt, is.relation.item)
	
	#check if item id if for relation
	is.relation.match <- NA
  
	relation.ids.match <- dt$match_concept_id
	for (i in seq_along(relation.ids.match)) {
	if(grepl('%%%',relation.ids.match[i])) {
		is.relation.match[i] = 1
	} else {
		is.relation.match[i] =0}
	}
	
	dt<-cbind(dt, is.relation.match)
	df$PKI <- round(df$item_instances / df$item_total_count, 3)
	dt$PKI <- round(dt$item_instances / dt$item_total_count, 3)

  ################################################
  # Rename columns for matching database
  ##################################################
  colnames(dt) <- c("score", "source_instances", "source_total_count", "target_total_count", "source_has_columns", "target_has_columns", "target_instances", "source_database", "source_table_id", "source_column_id", "target_database", "target_table_id", "target_column_id", "source_table", "target_table", "source_column", "target_column", "match_id", "is_table_source", "is_table_target", "is_relation_source", "is_relation_target", "PKI")

  ################################################
  # Rename columns for dataframe returned
  ##################################################
  colnames(df) <- c("score", "source_instances", "source_total_count", "target_total_count", "source_has_columns", "target_has_columns", "target_instances", "source_database", "source_table", "source_column", "target_database", "target_table", "target_column", "PKI")  
  
  ##################################################
  # Write all the tables
  ##################################################
  write.csv(dt, paste0(rdbmsPath, "\\", "0_flat_table.csv"), row.names = FALSE, na = "")
  #write.csv(unique.ecp, paste0(rdfPath, "\\", "1_unique_ecp.csv"), row.names = FALSE, na = "")
  #write.csv(match, paste0(rdfPath, "\\", "2_match.csv"), row.names = FALSE, na = "")
  #write.csv(concept.match, paste0(rdfPath, "\\", "3_concept_match.csv"), row.names = FALSE, na = "")
  #write.csv(match.concept, paste0(rdfPath, "\\", "4_match_concept.csv"), row.names = FALSE, na = "")
  #write.csv(property.match, paste0(rdfPath, "\\", "5_property_match.csv"), row.names = FALSE, na = "")
  #write.csv(match.property, paste0(rdfPath, "\\", "6_match_property.csv"), row.names = FALSE, na = "")
  
  # Finally return the data frame
  write.csv(df, paste0(rdbmsPath, "\\", "final.csv"), row.names = FALSE, na = "")

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

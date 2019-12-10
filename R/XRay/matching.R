# Function to run locality sensitive hashing match
run_lsh_matching <- function(path, N, b, similarityThreshold, instancesThreshold, delimiter=';', matchingSameDB=TRUE, outputFolder=NULL){
  
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
		TextReuseCorpus(paths = path, tokenizer = tokenize_ngrams, n = 1, minhash_func = corpus_minhash)
	}, error = function(e) {
		return("error")
	})
  
	# Return an empty frame if there was an error
	if (!is.list(corpus)) {
		return(NULL)
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
		return(NULL)
	})

	# Return an empty frame if there was an error
	if (!is.matrix(m)) {
		return(NULL)
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
		item.file.name <- df[r, 1][[1]]
		match.file.name <- df[r, 2][[1]]
		df$item_instances[r] <- wordcounts[item.file.name][[1]]
		df$match_instances[r] <- wordcounts[match.file.name][[1]]
	}
  
	# Only return records above the thresholds
	df <- df[df$score > similarityThreshold, ]
	df <- df[df$item_instances > instancesThreshold, ]
	df <- df[df$match_instances > instancesThreshold, ]

	# Order the data frame properly
	df <- df[order(df$item, -df$score, df$match), ]
  
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
  
  	###################################################################
	# Remove matching columns within a same database
	###################################################################
	if(!matchingSameDB) {
		removeMatchingSourceTargetDF <- df
		removeMatchingSourceTargetDF <- df[which(df$item_engine != df$match_engine)]
		df <- removeMatchingSourceTargetDF
		#check for empty df
		size <- dim(df)
		if(size[1] == 0) {
			return(NULL)
		}
	}

	################################################
	# Rename columns for dataframe returned
	##################################################
	colnames(df) <- c("Score", "Source_Instances", "Target_Instances", "Source_Database_Id", "Source_Table", "Source_Property", "Target_Database_Id", "Target_Table", "Target_Property")
  
	#########################################################
	# Merge concept and property as one value to be displayed
	#########################################################
	#Source_Column <- NA
	#source.property.values <- df$Source_Property
	
	#for (i in seq_along(source.property.values)) {
	#	if(is.na(source.property.values[i])) {
	#		Source_Column[i] = df$Source_Table[i]
	#	} 
	#	else {
	#		Source_Column[i] = source.property.values[i]
	#	}
	#}
	#df<-cbind(df, Source_Column)
  
	#Target_Column <- NA
	#target.property.values <- df$Target_Property
	#for (i in seq_along(target.property.values)) {
	#	if(is.na(target.property.values[i])) {
	#		Target_Column[i] = df$Target_Table[i]
	#	} 
	#	else {
	#		Target_Column[i] = target.property.values[i]
	#	}
	#}
	#df<-cbind(df, Target_Column)
  
	##################################################
	# Adding match count column
	##################################################
	df$Match_Count <- round(df$Score * df$Source_Instances)
	
	# JK, doing a round
	df$Score <- round(df$Score, 2)
	# Adding unique ids
	df$Source_GUID <- paste(df$Source_Table, df$Source_Property, df$Source_Database_Id, sep="--")
	df$Target_GUID <- paste(df$Target_Table, df$Target_Property, df$Target_Database_Id, sep="--")
	
	##################################################
	# Write all the tables
	##################################################
	# Finally return the data frame
	if(!is.null(outputFolder)) {
		write.csv(df, paste0(outputFolder, "\\", "final.csv"), row.names = FALSE, na = "")
	}
	
	return(df)
}
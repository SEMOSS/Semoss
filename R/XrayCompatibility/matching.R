# Function to run locality sensitive hashing match
run_lsh_matching <- function(path, N, b, similarityThreshold, instancesThreshold, delimiter, matchingSameDB, outputFolder){
  
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
		df <- data.frame(Source_Database = numeric(1), Source_Table = numeric(1), Source_Column = numeric(1), Target_Database = numeric(1), Target_Table = numeric(1), Target_Column = numeric(1), Score = numeric(1), Source_Instances = numeric(1), Target_Instances = numeric(1), Match_Count = numeric(1), stringsAsFactors = FALSE)
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
		df <- data.frame(Source_Database = numeric(1), Source_Table = numeric(1), Source_Column = numeric(1), Target_Database = numeric(1), Target_Table = numeric(1), Target_Column = numeric(1), Score = numeric(1), Source_Instances = numeric(1), Target_Instances = numeric(1), Match_Count = numeric(1), stringsAsFactors = FALSE)
		return(df)
	})

	# Return an empty frame if there was an error
	if (!is.matrix(m)) {
		df <- data.frame(Source_Database = numeric(1), Source_Table = numeric(1), Source_Column = numeric(1), Target_Database = numeric(1), Target_Table = numeric(1), Target_Column = numeric(1), Score = numeric(1), Source_Instances = numeric(1), Target_Instances = numeric(1), Match_Count = numeric(1), stringsAsFactors = FALSE)
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

	###################################################################
	# Remove matching columns within a same database
	###################################################################
	if(!matchingSameDB) {
		removeMatchingSourceTargetDF <- dt
		removeMatchingSourceTargetDF <- dt[which(dt$item_engine != dt$match_engine)]
		dt <- removeMatchingSourceTargetDF
		#check for empty df
		size <- dim(dt)
		if(size[1] == 0) {
			df <- data.frame(Source_Database = numeric(1), Source_Table = numeric(1), Source_Column = numeric(1), Target_Database = numeric(1), Target_Table = numeric(1), Target_Column = numeric(1), Score = numeric(1), Source_Instances = numeric(1), Target_Instances = numeric(1), Match_Count = numeric(1), stringsAsFactors = FALSE)
			return(df)
		}
	}
  
	###################################################################
	# Check if match is for concept or property
	###################################################################
	#check if item id 
	is.concept.item <- NA
	property.ids.item <- dt$item_property
	for (i in seq_along(property.ids.item)) {
		if(is.na(property.ids.item[i])) {
			is.concept.item[i] = 1
		} else {
			is.concept.item[i] =0
		}
	}
	dt<-cbind(dt, is.concept.item)

	#check if match id is for concept or property
	is.concept.match <- NA
	property.ids.match <- dt$match_property
	for (i in seq_along(property.ids.match)) {
		if(is.na(property.ids.match[i])) {
			is.concept.match[i] = 1
		} else {
			is.concept.match[i] =0
		}
	}
	dt<-cbind(dt, is.concept.match)

	################################################
	# Rename columns for matching database
	##################################################
	colnames(dt) <- c("Score", "Source_Instances", "Target_Instances", "Source_Database", "Source_Table_Id", "Source_Column_Id", "Target_Database", "Target_Table_Id", "Target_Column_Id", "Source_Table", "Target_Table", "Source_Property", "Target_Property", "Match_Id", "Is_Table_Source", "Is_Table_Target")

	################################################
	# Rename columns for dataframe returned
	##################################################
	colnames(df) <- c("Score", "Source_Instances", "Target_Instances", "Source_Database", "Source_Table", "Source_Column", "Target_Database", "Target_Table", "Target_Column")
  
	#########################################################
	# Merge concept and property as one value to be displayed
	#########################################################
	Source_Column <- NA
	source.property.values <- dt$Source_Property

	for (i in seq_along(source.property.values)) {
		if(is.na(source.property.values[i])) {
			Source_Column[i] = dt$Source_Table[i]
		} 
		else {
			Source_Column[i] = source.property.values[i]
		}
	}
	dt<-cbind(dt, Source_Column)
  
  
	Target_Column <- NA
	target.property.values <- dt$Target_Property
	for (i in seq_along(target.property.values)) {
		if(is.na(target.property.values[i])) {
			Target_Column[i] = dt$Target_Table[i]
		} 
		else {
			Target_Column[i] = target.property.values[i]
		}
	}
	dt<-cbind(dt, Target_Column)
  
	##################################################
	# Adding match count column
	##################################################
	dt$Match_Count <- round(dt$Score * dt$Source_Instances)
	
	##################################################
	# Write all the tables
	##################################################
	write.csv(dt, paste0(outputFolder, "\\", "0_flat_table.csv"), row.names = FALSE, na = "")
	# Finally return the data frame
	write.csv(df, paste0(outputFolder, "\\", "final.csv"), row.names = FALSE, na = "")

	return(dt)
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

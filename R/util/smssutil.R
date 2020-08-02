# all of the util functions go here

	
canLoad <- function(file)
{
	library(qdapRegex);
	scriptLines <- readLines(file);
	allPackages <- rm_between(scriptLines, c("library(", "require("), c(")", ")"), extract=TRUE);
	#output <- paste(na.omit(unlist(allPackages)), collapse=", ")
	#unvlist <-  which(as.logical(lapply(allPackages, require, character.only=TRUE))==F)
	output <- array(na.omit(unlist(allPackages)));
	output <- as.matrix(output)[,1]
	libs <- library()$result[,1]
	diff <- setdiff(output, libs)
	#if(length(diff) > 0)
	diff <- paste(unlist(diff), collapse=", ")
	#else
		
	return (diff)
}	


canLoad2 <- function(file)
{
	library(qdapRegex);
	scriptLines <- readLines(file);
	allPackages <- rm_between(scriptLines, c("library(", "require("), c(")", ")"), extract=TRUE);
	#output <- paste(na.omit(unlist(allPackages)), collapse=", ")
	unvlist <-  which(as.logical(lapply(allPackages, require, character.only=TRUE))==F)
	
	
	return (unvlist)
}	

# get the latest 
# if(exists variable then ask for the latest
# do a compare 
# if the compare returns true assign then 

getCurMeta <- function(frame, allframe)
{
	key = deparse(substitute(frame))
	cur_meta <- list()
	
	cur_cols <- colnames(frame)
	for (col in cur_cols)
	{
		type <- class(frame[[col]])
		cur_meta[[col]] <- type		
	}	
	#if (!exists('allframe')) 
	if(!exists(paste0(key, "_meta_old"), allframe))
	{
		print("setting all frame")
		allframe <- list()
		allframe[[paste0(key, "_meta_new")]] <- cur_meta
		allframe[[paste0(key, "_meta_old")]] <- cur_meta
	}
	else
	{
		print("adding new meta")
		allframe[[paste0(key, "_meta_new")]] <- cur_meta
	}
	return (allframe)
}

hasFrameChanged <- function(key, allframe)
{	
	
	cur_meta <- allframe[[paste0(key, "_meta_new")]]
	prev_meta <- allframe[[paste0(key, "_meta_old")]]
	cur_cols <- names(cur_meta)
	# now we start comparing
	changed <- FALSE
	for (col in cur_cols)
	{
		type <- cur_meta[[col]]
		col_exists <- exists(col, prev_meta)		
		if(!col_exists)
		{
			print("mismatch : added column -> true")
			allframe[[paste0(key, "_meta_old")]] <- cur_meta
			changed <- TRUE
			#allframes[[key]] <<- cur_meta
			#print(col_compared)
			break
		}
		newtype <- prev_meta[[col]]
		if(is.null(newtype) || type != newtype)
		{
			print("mismatch : changed type -> true")
			allframe[[paste0(key, "_meta_old")]] <- cur_meta
			#allframes[[key]] <<- cur_meta
			#return ("true")
			break
		}
		
	}	
	# need to compare for removed columns as well 
	prev_cols <- names(prev_meta)
	if(!changed)
	{
		for (col in prev_cols)
		{
			#print("checking removed cols")		
			if(!exists(col, cur_meta))
			{
				print("mismatch : removed column -> true")
				allframe[[paste0(key, "_meta_old")]] <- cur_meta
				#return ("true")
				break
			}
		}
	}
	
	return (allframe);
}

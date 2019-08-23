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

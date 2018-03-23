escapeRegexR <- function(greplExpr){
	splitRule <- strsplit(as.character(greplExpr), '"')[[1]]
	lengthMinusOne <- length(splitRule) - 1
	regex <- paste(splitRule[2:as.double(lengthMinusOne)], sep="")
	regex <- stri_escape_unicode(regex)
	greplExpr <- paste(splitRule[1],'"',regex,'"', splitRule[length(splitRule)], sep="")
}

validateRule <- function(rule) {
	lapply(list('validate', 'stringi'), require, character.only = TRUE)

	if(grepl("^(grepl).*", rule)) rule <- escapeRegexR(rule)
	
	df <- data.frame(rule = c(rule), label = c("rule"))
	tryCatch({
			v <- validator(.data=df) 
			if(exists('v')) {
				return("TRUE")
			}
		  }, error = function(e) {
				return("FALSE")
		})
}

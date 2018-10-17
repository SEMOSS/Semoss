getSmryDt <- function(dt, colName){
	if (is.numeric(dt[[colName]])){
		smry <- round(summary(dt[[colName]]), 3)
		if (length(smry) == 6) smry["NA's"] <- 0 
		smryNames <-  names(smry) %>% gsub("Qu.", "Quartile", .) %>% gsub("[.]", "", .) 
		return (data.table(smryNames[order(match(smryNames, c("NA's", "Min", "1st Quartile", "Median", "Mean", "3rd Quartile", "Max")))]) %>% setnames(., names(.), paste0(colName, "_Stats")) %>% .[, (colName) := as.numeric(c(smry[7], smry[1:6]))])
	} else {
		return (data.table(c("NA's", "Top 3 Values")) %>% setnames(., names(.), paste0(colName, "_Stats")) %>% .[, (colName) := c(sum(is.na(dt[[colName]])), paste(names(sort(table(dt[[colName]]), decreasing=T))[1:3],collapse="|"))])
	}
}

################################################
# Generate a list of descriptions for a list of instances
#################################################
library(data.table)
generateDescriptionColumn <- function(instances) {
	library(WikidataR);
	numInstances <- length(instances);
	instanceDescription <- vector();
	for(j in 1:numInstances) {
		# grab for this instance the values
		# print(paste(i, " ::: ", j, " ::: ", splitCamelCase(instances[j]) ));
		cleanInstance <- splitCamelCase(instances[j]);
		values <- find_item(cleanInstance);
		if(length(values) == 0) {
			# wiki returned nothing, try google
			googleResults <- searchGoogle(cleanInstance);
			# push into the description array for this instance
			instanceDescription[span(instanceDescription)+1] <- paste(googleResults, collapse='; ');
		} else {
			datavalues <- lapply(values, function(x) { cbind(x$label, x$description) } );
			# push into the description array for this instance
			instanceDescription[span(instanceDescription)+1] <- paste(lapply(datavalues, function(x) { paste(x, collapse= '; ') }), collapse='; ');
		}
	}
	return (instanceDescription)
}

span<-function(a){
  if(length(a) == 1 & is.na(a[1])){
    x<-0;
  }else{
    x<-length(a);
  }
  return(x);
}

splitCamelCase<-function(a){
  a<-paste(strsplit(a, "(?<=[a-z])(?=[A-Z])", perl = TRUE)[[1]], collapse=" ");
  a<-gsub("_", " ", a);
  return(a);
}

getGoogleSearchUrl <- function(searchTerm, domain = '.com', quotes=TRUE) {
  searchTerm <- gsub(' ', '%20', searchTerm);
  if(quotes) {
    searchTerm <- paste('%22', searchTerm, '%22', sep='');
  }
  searchURL <- paste('http://www.google', domain, '/search?q=', searchTerm, sep='');
  return(searchURL)
}

searchGoogle <- function(term) {
  library(XML);
  library(httr);
  library(RCurl);
  library(stringr);
  
  EXCLUDE<-"Advanced searchSearch Help Send feedback"
  EXCLUDE1<-"In order to show you the most relevant results"
  searchURL<-getGoogleSearchUrl(searchTerm=term);
  response<- GET(searchURL);
  if (response$status_code!=200){ # HTTP request failed!!
    # do some stuff...
    return("");
  }
  doc.html<-htmlTreeParse(searchURL,useInternal = TRUE)
  doc.text<-unlist(xpathApply(doc.html, '//p', xmlValue))
  doc.text<-str_trim(gsub('\\n', ' ', doc.text),"both")
  doc.text<-doc.text[!(doc.text==EXCLUDE)]
  doc.text<-doc.text[!(substr(doc.text,1,nchar(EXCLUDE1))==EXCLUDE1)]
  return(doc.text)
}
  
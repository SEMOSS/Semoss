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
  if(quotes) search.term <- paste('%22', searchTerm, '%22', sep='');
  searchURL <- paste('http://www.google', domain, '/search?q=',searchTerm, sep='');
  return(searchURL)
}

searchGoogle <- function(searchTerm) {
  library(XML);
  library(RCurl);
  library(stringr);
  
  EXCLUDE<-"Advanced searchSearch Help Send feedback"
  EXCLUDE1<-"In order to show you the most relevant results"
  searchURL<-getGoogleSearchUrl(searchTerm=searchTerm)
  doc.html<-htmlTreeParse(searchURL,useInternal = TRUE)
  doc.text<-unlist(xpathApply(doc.html, '//p', xmlValue))
  doc.text<-str_trim(gsub('\\n', ' ', doc.text),"both")
  doc.text<-doc.text[!(doc.text==EXCLUDE)]
  doc.text<-doc.text[!(substr(doc.text,1,nchar(EXCLUDE1))==EXCLUDE1)]
  return(doc.text)
}


predictDescriptions <- function(uniqueValues) {
  library(data.table);
  library(WikidataR);
  
  uniqueDescriptions <- array();
  numUnique <- length(uniqueValues);
  for(i in 1:numUnique) {
    cleanValue = splitCamelCase(uniqueValues[i]);
    values <- find_item(cleanValue);
    if(length(values) == 0) {
      # wiki returned nothing, try google
      googleResults <- searchGoogle(cleanValue);
      uniqueDescriptions[span(uniqueDescriptions)+1] <- paste(googleResults, collapse='***NEW LINE***');
    } else {
      wikiResults <- lapply(values, function(x) { cbind(x$label, x$description) } );
      uniqueDescriptions[span(uniqueDescriptions)+1] <- paste(lapply(wikiResults, function(x) { paste(x, collapse= ' ') }), collapse='***NEW LINE***');
    }
  }
  
  rm(numUnique);
  return(unique(uniqueDescriptions));
}
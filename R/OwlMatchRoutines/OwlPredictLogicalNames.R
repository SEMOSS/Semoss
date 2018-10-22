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

predictLogicalNames <- function(uniqueValues) {
  library(WikidataR);
  
  logicalNames <- array();
  
  props<-c("P31","P106","P279","P361","P373");
  numUnique <- length(uniqueValues);
  for(i in 1:numUnique) {
    cleanValue <- splitCamelCase(uniqueValues[i]);
    possibleItems <- find_item(cleanValue);
    wikiIds <- sapply(possibleItems, function (x) {cbind(x$id)} );
    wikiItems <- get_item(wikiIds);
    claims <- extract_claims(wikiItems, props);
    numClaims <- length(claims);
    if(numClaims > 0) {
      for(j in 1:numClaims) {
        # okay, we need special processing based on the property type
        claimItem <- claims[[j]];
        p31 <- claimItem[[1]];
        p106 <- claimItem[[2]];
        p279 <- claimItem[[3]];
        p361 <- claimItem[[4]];
        p373 <- claimItem[[5]];
        
        if(!class(p31) == "logical") {
          classid <- p31$mainsnak$datavalue$value$id;
          classItems <- get_item(classid);
          numItems <- length(classItems);
          if(numItems > 0) {
            for(i in 1:numItems) {
              logicalNames[span(logicalNames)+1]<-classItems[[i]]$label$en$value
            }
          }
        }
        
        if(!class(p106) == "logical") {
          classid <- p106$mainsnak$datavalue$value$id;
          classItems <- get_item(classid);
          numItems <- length(classItems);
          if(numItems > 0) {
            for(i in 1:numItems) {
              logicalNames[span(logicalNames)+1]<-classItems[[i]]$label$en$value
            }
          }
        }
        
        if(!class(p279) == "logical") {
          classid <- p279$mainsnak$datavalue$value$id;
          classItems <- get_item(classid);
          numItems <- length(classItems);
          if(numItems > 0) {
            for(i in 1:numItems) {
              logicalNames[span(logicalNames)+1]<-classItems[[i]]$label$en$value
            }
          }
        }
        
        if(!class(p361) == "logical") {
          classid <- p361$mainsnak$datavalue$value$id;
          classItems <- get_item(classid);
          numItems <- length(classItems);
          if(numItems > 0) {
            for(i in 1:numItems) {
              logicalNames[span(logicalNames)+1]<-classItems[[i]]$label$en$value
            }
          }
        }
        
        if(!class(p373) == "logical") {
          logicalNames[span(logicalNames)+1]<-p373$mainsnak$datavalue$value;
        }
      }
    }
  }
  
  if(length(logicalNames) == 0) {
    return(logicalNames);
  }
  
  logicalNameFreqTable <- sort(table(logicalNames));
  maxToGrab <- if (length(logicalNameFreqTable) < 10) 0 else (length(logicalNameFreqTable)-10);
  topLogicalNames <- names(logicalNameFreqTable[length(logicalNameFreqTable):maxToGrab]);
  
  rm(numUnique, uniqueValues, cleanValue, wikiItems, p31, p106, p279, p361, p373, logicalNames, logicalNameFreqTable);
  gc();
  return(topLogicalNames);
}
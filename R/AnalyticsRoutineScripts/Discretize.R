mainMethod <- function() {
  library(datasets);
  library(data.table);
  data(iris);
  iris;
  iris <- as.data.table(iris);
  iris$Sepal.Length[1] = NULL;
  
  inputList <- list(
    Petal.Length=list(), 
    Sepal.Length=list(breaks=c(4.3, 5.5, 6.7, 7.9), labels=c('Short','Medium','Long')), 
    Petal.Width=list(breaks=c(0:5*.5))
  );
  
  discretizeColumnsDt(iris, inputList);
}

# method to discretize a numeric column and add it back to the frame
discretizeColumnsDt <- function(dt, inputList){
  
  # this options string is used for the discretize method
  discretizeOptions <- ",include.lowest = TRUE, right=TRUE,ordered_result=TRUE)";
  
  #we will construct R syntax to execute based on the type of input that is provided
  colNames <- names(inputList);
  # loop through each input
  for(name in colNames) {
    discretizedColumnName <- getNewColumnName(name, names(dt))
    
    options <- inputList[[name]];

    # case 1 - no input
    # even though is is a subset of the other
    # will treat it different for optimization
    if(length(options) == 0) {
      # we need to determine the optimal breaks
      numBins <- length(hist(dt[[name]], plot=FALSE)$counts);
      
      # this is easy to set
      # will just add this
      dt[, (discretizedColumnName):=eval(parse(text = paste0("discretize(dt[['", name, "']], breaks=", numBins, discretizeOptions)))];
    } else {
      
      # alright, we got some input
      # we could have the following
      # breaks: can be an array or a positive integer
      # lables: must be an array
      # dig.lab: must be a positive integer
      
      # we really only need to take into consideration breaks
      # since if that is not there, we need to calculate it using hist
      
      if( !("breaks" %in% names(options)) ) {
        numBins <- length(hist(dt[[name]], plot=FALSE)$counts);
        # add the numBins into the options list
        # so when we loop through all the options
        # we have it considered
        options$breaks = numBins;
      }
      
      # if we have defined the breaks as an array of values
      # we need to tell the algorithm this
      # placeholder for the variable
      method <- "";
      
      # now we loop through and generate the expression we want
      distOptions <- "";
      for(distOptionName in names(options)) {
        thisOptionExpression <- "";
        
        distOptionValue = options[[distOptionName]];
        # if array, we will wrap around as a vector
        if(distOptionName == "dig.lab") {
          thisOptionExpression <- paste(distOptionValue);
        } else if(distOptionName == "breaks") {
          if(length(distOptionValue) == 1) {
            thisOptionExpression <- paste0(distOptionValue);
          } else {
            method = "fixed";
            thisOptionExpression <- paste0("c(", paste(distOptionValue, collapse=", "), ")");
          }
        } else if(distOptionName == "labels") {
          if(length(distOptionValue) == 1) {
            thisOptionExpression <- paste0("\"", distOptionValue, "\"");
          } else {
            thisOptionExpression <- paste0("c(\"", paste(distOptionValue, collapse="\", \""), "\")");
          }
        }
        
        # append to entire expression
        distOptions <- paste(distOptions, paste0(distOptionName, "=", thisOptionExpression), sep=",");
      }
      
      # add the method if necessary
      if(method != "") {
        distOptions <- paste(distOptions, paste0("method=\"", method, "\""), sep=",");
      }
      # now execute
      dt[, (discretizedColumnName):=eval(parse(text = paste0("discretize(dt[['", name, "']]", distOptions, discretizeOptions)))];
    }
  }
  
  return (dt);
}



## get a new column name that is unique
getNewColumnName <- function(requestedColName, allColNames, colSuffix="_Discretized"){
  proposedColName <- paste0(requestedColName, colSuffix)
  nameGrepVec <- grep(proposedColName, allColNames)
  if (length(nameGrepVec) == 0) {
    proposedColName
  } else {
    existingColNames <- allColNames[nameGrepVec]
    if (length(existingColNames) == 1) {
      paste0(requestedColName, colSuffix, "_1")
    } else {
      largestIndex <- strsplit(existingColNames, paste0(colSuffix,'_')) %>% unlist(lapply(1:length(existingColNames), function(i) paste0(.[[i]][2]))) %>% as.integer(.) %>% max(., na.rm=TRUE) 
      paste0(requestedColName, colSuffix, "_", largestIndex+1)
    }
  }
}

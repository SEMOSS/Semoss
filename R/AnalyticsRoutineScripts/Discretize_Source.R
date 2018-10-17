discretize <- function(x, method = "frequency", breaks = 3, 
  labels = NULL, include.lowest = TRUE, right = FALSE, dig.lab = 3,
  ordered_result = FALSE, infinity = FALSE, onlycuts = FALSE, categories = NULL, ...) {
  
  if(!is.null(categories)) {
    warning("Parameter categories is deprecated. Use breaks instead! Also, the default method is now frequency!")
    breaks <- categories
  }
  
  methods = c("interval", "frequency", "cluster", "fixed")
  
  method <- methods[pmatch(tolower(method), methods)]
  if(is.na(method)) stop("Unknown method!")
      
  if(method == "fixed" && length(breaks) < 2) 
    stop("fixed needs at least two values for breaks.")
  if(method != "fixed" && (length(breaks) != 1 || breaks < 1))
    stop("breaks needs to be a single positive integer for this method.")
  
  breaks <- switch(method,
    interval = unique(seq(from=min(x, na.rm=TRUE), to=max(x, na.rm=TRUE), length.out=breaks+1)),
		
    frequency = unique(quantile(x, probs = seq(0,1, length.out = breaks+1), na.rm = TRUE)),

    cluster = {
      cl <-  stats::kmeans(stats::na.omit(x), breaks)  #removed ellipsis argument which was causing error during testing
      centers <- sort(cl$centers[,1])
      unique(as.numeric(c(min(x, na.rm=TRUE), head(centers, 
        length(centers)-1) + diff(centers)/2, max(x, na.rm=TRUE))))
    },
    
    fixed = breaks
  )
    
  if(any(duplicated(breaks))) {
		stop("Some breaks are not unique, use fewer breaks for the data.")
	}

  ### fix first and last to -/+Inf
  if(infinity) {
    breaks[1] <- -Inf
    breaks[length(breaks)] <- Inf
  }
  
  if(onlycuts) return(as.vector(breaks))
  
  structure(
    cut(x, breaks = breaks, labels = labels, 
      include.lowest = include.lowest, right = right, dig.lab,
			ordered_result = ordered_result),
    'discretized:breaks' = as.vector(breaks),
    'discretized:method' = method
  ) 
}

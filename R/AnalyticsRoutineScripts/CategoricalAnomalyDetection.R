library(AnomalyDetection)

# For test cases
# library(data.table)
# setwd("C:\\Users\\tbanach\\RWorkingDirectory")
# dt <- read.csv("test_categorical_anom.csv")
# args <- list('date', 'event', 'group', 'count distinct', 0.25, 'both', 0.05, 7)
# setDT(dt)
# head(dt)

# Store the arguments in more readable form
# Note that the args[[i]] string must be different from the variable name for the case of headers
# For example, time <- "time" would cause an issue when calling get(time)
# So make these variables contain a period, which is an illegal header character in Semoss
# That way they will be distinct
time.col <- args[[1]]
event.col <- args[[2]]
group.col <- args[[3]]

# Get the aggregate function
agg.string <- args[[4]]
if (agg.string == 'count distinct') {
  agg <- uniqueN
  agg.string <- sub(" ", "_", args[[4]])
} else if (agg.string == 'count') {
  agg <- length  
} else {
  agg <- get(agg.string)
}
max.anoms <- args[[5]]
direction <- args[[6]]
alpha <- args[[7]]
period <- args[[8]]

# Make sure group is a factor, because the levels of group must be accessed later
dt[, toString(group.col):=as.factor(get(group.col))]

# For cases where the group is just the time
DetectWithoutGroups <- function(dt, time.col, event.col, agg, agg.string, max.anoms, direction, alpha, period) {

  # Set the time column as the key
  setkeyv(dt, time.col)
  
  # Store the levels of the group that had an anomaly at the most recent time
  anom.levels <- list()
  i <- 1
  
  # Aggregate for each time using the user-specified aggregate function
  agg.col <- paste0(agg.string, "_", event.col)
  dt[, toString(agg.col) := as.numeric(agg(get(event.col))), keyby = time.col]
  
  # Detect anomalies on only one value for each group per unique time
  unique.times <- unique(dt, by=time.col)[, c(time.col, agg.col), with=FALSE]
  
  # So that a graphics window does not pop up
  options(device=pdf)
  
  # Detect anomalies
  res <- AnomalyDetectionVec(unique.times[, get(agg.col)], max_anoms=max.anoms, direction=direction, alpha=alpha, period=period, plot=FALSE)
  anom.col <- paste0("anom_", agg.col)
  unique.times[, toString(anom.col) := 0]
  unique.times[res$anoms$index, toString(anom.col) := res$anoms$anoms]
  
  # Store the most recent time if it was an anomaly
  if (!is.null(res$anoms$index) && res$anoms$index[length(res$anoms$index)] == nrow(unique.times)) {
    anom.levels[[i]] <- unique.times[nrow(unique.times), get(time.col)]
  }
  return(list(dt=dt, unique.times=unique.times, anom.levels=anom.levels))
}

# For cases where the user specifies groups
DetectWithGroups <- function(dt, time.col, event.col, group.col, agg, agg.string, max.anoms, direction, alpha, period) {

  # Set the time column as the key
  setkeyv(dt, c(time.col, group.col))
  
  # Store the levels of the group that had an anomaly at the most recent time
  anom.levels <- list()
  i <- 1
  
  # Aggregate for each group and time using the user-specified aggregate function
  agg.col <- paste0(agg.string, "_", event.col, "_BY_", group.col)
  dt[, toString(agg.col) := as.numeric(agg(get(event.col))), keyby = c(time.col, group.col)]
  
  # Detect anomalies on only one value for each group per unique time
  unique.times <- unique(dt, by=time.col)[, time.col, with=FALSE]
  
  # So that a graphics window does not pop up
  options(device=pdf)
  
  # Loop through each level of the group,
  # and determine the series for that level
  for (level in levels(dt[, get(group.col)])) {

    # Determine the series for this particular level
    group.series <- unique(dt, by=c(time.col, group.col))[get(group.col) == level, c(time.col, group.col, agg.col), with=FALSE]
    
    # Perform an outer join between the series for a particular group,
    # and the data table of all days, so that each day is represented
    vec <- merge(unique.times, group.series, by=time.col, all=TRUE)[, get(agg.col)]
    
    # Replace NA's from the outer join (that occur when this level is not represented for a given time) with zeros
    vec[is.na(vec)] <- 0
    
    # Detect anomalies
    res <- AnomalyDetectionVec(vec, max_anoms=max.anoms, direction=direction, alpha=alpha, period=period, plot=FALSE)
    
    # Add columns
    unique.times[, paste0("level_", level) := vec]
    anom.col <- paste0("anom_", level)
    unique.times[, toString(anom.col) := 0]
    unique.times[res$anoms$index, toString(anom.col) := res$anoms$anoms]
    
    # Store the levels of the group that had an anomaly at the most recent time
    if (!is.null(res$anoms$index) && res$anoms$index[length(res$anoms$index)] == nrow(unique.times)) {
      anom.levels[[i]] <- level
      i <- i + 1
    }
  }
  return(list(dt=dt, unique.times=unique.times, anom.levels=anom.levels))
}

# If the group is time, then detect without groups
# Default to detect with groups
result <- list()
if (group.col == time.col) {
  result <- DetectWithoutGroups(dt, time.col, event.col, agg, agg.string, max.anoms, direction, alpha, period)
} else {
  result <- DetectWithGroups(dt, time.col, event.col, group.col, agg, agg.string, max.anoms, direction, alpha, period)
}
dt <- result$dt
unique.times <- result$unique.times
anom.levels <- result$anom.levels

# Return whether an anomaly occured at the most recent time
GetResult <- function() {
  return(anom.levels)
}

# For test cases
# See the final results
# dt
# unique.times
# GetResult()

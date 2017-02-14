library(AnomalyDetection)

# For test cases
# library(data.table)
# setwd("C:\\Users\\tbanach\\RWorkingDirectory")
# dt <- read.csv("anomaly_detection_test.csv")
# args <- list('timestamp_1', 'count_1', 'sum', 0.01, 'both', 0.05, 1440)
# dt <- read.csv("test_numeric_anom.csv")
# args <- list('date', 'numeric_series', 'sum', 0.25, 'both', 0.05, 7)
# setDT(dt)
# head(dt)

# Store the arguments in more readable form
# Note that the args[[i]] string must be different from the variable name for the case of headers
# For example, time <- "time" would cause an issue when calling get(time)
# So make these variables contain a period, which is an illegal header character in Semoss
# That way they will be distinct
time.col <- args[[1]]
series.col <- args[[2]]

# Get the aggregate function
agg.string <- args[[3]]
if (agg.string == 'count distinct') {
  agg <- uniqueN
} else if (agg.string == 'count') {
  agg <- length  
} else {
  agg <- get(agg.string)
}
max.anoms <- args[[4]]
direction <- args[[5]]
alpha <- args[[6]]
period <- args[[7]]

# Set the time column as the key
setkeyv(dt, time.col)

# Aggregate for each time value using the user-specified aggregate function
agg.col <- paste0(toupper(agg.string), "_", series.col)
dt[, toString(agg.col) := as.numeric(agg(get(series.col))), keyby = c(time.col)]

# Detect anomalies on only one value per unique time
unique.times <- unique(dt, by=time.col)[, c(time.col, agg.col), with=FALSE]

# So that a graphics window does not pop up
options(device=pdf)

# Run anomaly detection on the aggregated column
res <- AnomalyDetectionVec(unique.times[, get(agg.col)], max_anoms=max.anoms, direction=direction, alpha=alpha, period=period, plot=FALSE)

# Add anomaly column
anom.col <- paste0("ANOM_", agg.col)
unique.times[, toString(anom.col) := 0]
unique.times[res$anoms$index, toString(anom.col) := res$anoms$anoms] 

# Perfom the inner join on the original data table
dt <- merge(dt, unique.times[, c(time.col, anom.col), with=FALSE], by=time.col, all=FALSE)

# Return whether an anomaly occured at the most recent time
GetResult <- function() {
  if (is.null(res$anoms$index)) {
    return(FALSE)
  } else {
    return(res$anoms$index[length(res$anoms$index)] == nrow(unique.times))
  }
}

# For test cases
# See the final results
# dt
# unique.times
# GetResult()
# res$anoms

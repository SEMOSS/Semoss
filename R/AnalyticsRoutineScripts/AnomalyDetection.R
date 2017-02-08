library(AnomalyDetection)

# For test cases
# library(data.table)
# df <- read.csv("C:\\Users\\tbanach\\Workspace\\SemossDev\\R\\AnalyticsRoutineScripts\\anomaly_detection_test.csv")
# head(df)
# args <- list('count_1', 0.01, 'both', 0.05, 100, FALSE)
# setDT(df)

# Because Semoss headers can be lowercase,
# but the frame is always schronized with capital letters,
# make this argument (series column name) uppercase
# Comment out for test case
args[[1]] <- toupper(args[[1]])

# So that a graphics window does not pop up
options(device=pdf)

# Run anomaly detection
res <- AnomalyDetectionVec(df[, c(args[[1]]), with=FALSE], max_anoms=args[[2]], direction=args[[3]], alpha=args[[4]], period=args[[5]], plot=args[[6]])

# Create anomaly column
df$ANOM <- 0
df$ANOM[res$anoms$index] <- res$anoms$anoms

# Return the number of anomalies
GetResult <- function() {
  length(res$anoms$index)
}
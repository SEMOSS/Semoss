# Anomaly detection packages
install.packages("RCurl")
install.packages("httr")
install.packages("devtools")
library(RCurl)
library(httr)
library(devtools)
set_config( config( ssl_verifypeer = 0L ) )
devtools::install_github("twitter/AnomalyDetection")

# General Semoss packages
install.packages("Rserve")
install.packages("splitstackshape")
install.packages("data.table")
install.packages("reshape2")
install.packages("RJDBC")
install.packages("sqldf")
install.packages("stringr")
install.packages("rJava")

# Semantic matching package
install.packages("textreuse")

# Fuzzy Join
install.packages("fuzzyjoin")

# Fuzzy matching
install.packages("stringdist")

# Semantic matching
install.packages("plyr")
install.packages("WikidataR")

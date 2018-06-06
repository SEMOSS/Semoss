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
# Collision Resolver
# Fuzzy matching
install.packages("fuzzyjoin")
install.packages('RJSONIO')
install.packages("rlang")
install.packages("tibble")
install.packages("R6")
install.packages("bindrcpp")
install.packages("crayon")
install.packages("pkgconfig")
install.packages("glue")
install.packages("plogr")
install.packages("dplyr")
install.packages("tidyr")
install.packages("purrr")
install.packages("stringdist")

# Business rules
install.packages("arules")

# Semantic matching
install.packages("plyr")
install.packages("WikidataR")
install.packages("curl")
install.packages("openssl")
install.packages("httr")
install.packages("jsonlite")
install.packages("WikipediR")
install.packages("LSAfun")
install.packages("text2vec")

# Google Analytics
url <- "https://cran.r-project.org/src/contrib/Archive/RGoogleAnalytics/RGoogleAnalytics_0.1.1.tar.gz"
install.packages(url, repos=NULL)
install.packages("lubridate")

# Edit Rules
install.packages("validate")
install.packages("yaml")
install.packages("settings")



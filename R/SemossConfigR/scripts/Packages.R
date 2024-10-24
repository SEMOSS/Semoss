options(repos="https://CRAN.R-project.org")
###############################################################################
# General Semoss Packages
###############################################################################
install.packages("Rserve", dependencies=TRUE)
install.packages("rJava", dependencies=TRUE)
install.packages("splitstackshape", dependencies=TRUE)
install.packages("data.table", dependencies=TRUE)
install.packages("reshape2", dependencies=TRUE)
install.packages("stringr", dependencies=TRUE)
install.packages("lubridate", dependencies=TRUE)
install.packages("dplyr", dependencies=TRUE)
install.packages("fst", dependencies=TRUE)

###############################################################################
# Clean Routines
###############################################################################

# Decode/Encode URI/URL
install.packages("urltools", dependencies=TRUE)

# ToProperCase
install.packages("snakecase", dependencies=TRUE)

# Impute Null Values
install.packages("Amelia", dependencies=TRUE)

###############################################################################
# Recommendations
###############################################################################

# Database Recommmendations
install.packages("XML", dependencies=TRUE)
install.packages("doParallel", dependencies=TRUE)
#install.packages("RCurl", dependencies=TRUE)

# Recommendations
install.packages("Rcpp", dependencies=TRUE)
install.packages("lattice", dependencies=TRUE)
install.packages("codetools", dependencies=TRUE)
install.packages("digest", dependencies=TRUE)
install.packages("foreach", dependencies=TRUE)
install.packages("SnowballC", dependencies=TRUE)
install.packages("futile.options", dependencies=TRUE)
install.packages("futile.logger", dependencies=TRUE)
install.packages("magrittr", dependencies=TRUE)
install.packages("formatR", dependencies=TRUE)
install.packages("RcppParallel", dependencies=TRUE)
install.packages("stringi", dependencies=TRUE)
install.packages("Matrix", dependencies=TRUE)
install.packages("lambda.r", dependencies=TRUE)
install.packages("doParallel", dependencies=TRUE)
install.packages("grid", dependencies=TRUE)
install.packages("tools", dependencies=TRUE)
install.packages("iterators", dependencies=TRUE)
install.packages("mlapi", dependencies=TRUE)
install.packages("compiler", dependencies=TRUE)
install.packages("lubridate", dependencies=TRUE)
install.packages("parallel", dependencies=TRUE)
#install.packages("stringr", dependencies=TRUE)
#install.packages("text2vec", dependencies=TRUE)
#install.packages("lsa", dependencies=TRUE)
#install.packages("plyr", dependencies=TRUE)
#install.packages("R6", dependencies=TRUE)
#install.packages("doParallel", dependencies=TRUE)

# Semantic matching package
install.packages("textreuse", dependencies=TRUE)

# Advanced federation
install.packages("stringdist", dependencies=TRUE)

# Fuzzy matching
# Fuzzy Join 
install.packages("fuzzyjoin", dependencies=TRUE)
install.packages("rlang", dependencies=TRUE)
install.packages("tibble", dependencies=TRUE)
install.packages("R6", dependencies=TRUE)
install.packages("bindrcpp", dependencies=TRUE)
install.packages("crayon", dependencies=TRUE)
install.packages("pkgconfig", dependencies=TRUE)
install.packages("glue", dependencies=TRUE)
install.packages("plogr", dependencies=TRUE)
install.packages("purrr", dependencies=TRUE)
#install.packages("devtools", dependencies=TRUE)
#install.packages("dplyr", dependencies=TRUE)

# Semantic matching
install.packages("plyr", dependencies=TRUE)
install.packages("WikidataR", dependencies=TRUE)
install.packages("curl", dependencies=TRUE)
install.packages("openssl", dependencies=TRUE)
install.packages("jsonlite", dependencies=TRUE)
install.packages("WikipediR", dependencies=TRUE)
install.packages("LSAfun", dependencies=TRUE)
install.packages("text2vec", dependencies=TRUE)
#install.packages("httr", dependencies=TRUE)

# semantic description
#install.packages("WikidataR", dependencies=TRUE)
#install.packages("WikipediR", dependencies=TRUE)
#install.packages("curl", dependencies=TRUE)
#install.packages("jsonlite", dependencies=TRUE)
#install.packages("httr", dependencies=TRUE)

# Database semantic
install.packages("lsa", dependencies=TRUE)
#install.packages("WikidataR", dependencies=TRUE)
#install.packages("text2vec", dependencies=TRUE)
#install.packages("plyr", dependencies=TRUE)
#install.packages("stringdist", dependencies=TRUE)

# Xray
install.packages("RcppProgress", dependencies=TRUE)
install.packages("withr", dependencies=TRUE)
install.packages("NLP", dependencies=TRUE)
install.packages("memoise", dependencies=TRUE)
install.packages("tidyselect", dependencies=TRUE)
#install.packages("textreuse", dependencies=TRUE)
#install.packages("purrr", dependencies=TRUE)
install.packages("tidyr", dependencies=TRUE)
#install.packages("devtools", dependencies=TRUE)
#install.packages("digest", dependencies=TRUE)
#install.packages("WikidataR", dependencies=TRUE)
#install.packages("WikipediR", dependencies=TRUE)
#install.packages("httr", dependencies=TRUE)
#install.packages("curl", dependencies=TRUE)
#install.packages("jsonlite", dependencies=TRUE)

# Xray Metamodel
#install.packages("jsonlite", dependencies=TRUE)






###############################################################################
#  Graph Visualizations
###############################################################################
install.packages("igraph", dependencies=TRUE)


###############################################################################
#  Analytics
###############################################################################

# Apriori
install.packages("arules", dependencies=TRUE)
#install.packages("data.table", dependencies=TRUE)
#install.packages("dplyr", dependencies=TRUE)

# Classification
install.packages("partykit", dependencies=TRUE)
install.packages("naniar", dependencies=TRUE)
install.packages("missRanger", dependencies=TRUE)
#install.packages("dplyr", dependencies=TRUE)
#install.packages("data.table", dependencies=TRUE)

# Clustering
install.packages("cluster", dependencies=TRUE)

# LOF
install.packages("Rlof", dependencies=TRUE)
install.packages("VGAM", dependencies=TRUE)
#install.packages("data.table", dependencies=TRUE)
#install.packages("dplyr", dependencies=TRUE)

# Outlier
install.packages("HDoutliers", dependencies=TRUE)
install.packages("mcclust", dependencies=TRUE)
install.packages("flashClust", dependencies=TRUE)
install.packages("leaps", dependencies=TRUE)
install.packages("FactoMineR", dependencies=TRUE)
#install.packages("data.table", dependencies=TRUE)

# Random Forest
install.packages("randomForest", dependencies=TRUE)
#install.packages("data.table", dependencies=TRUE)
#install.packages("dplyr", dependencies=TRUE)

# Numerical Model
install.packages("nueralnet", dependencies=TRUE)
#install.packages("data.table", dependencies=TRUE)

# LSA Column
#install.packages("LSAfun", dependencies=TRUE)
#install.packages("text2vec", dependencies=TRUE)

# Anomaly detection packages
install.packages("RCurl", dependencies=TRUE)
install.packages("httr", dependencies=TRUE)
install.packages("devtools", dependencies=TRUE)
set_config( config( ssl_verifypeer = 0L ) )
devtools::install_github("twitter/AnomalyDetection", dependencies=TRUE)

# Business rules
#install.packages("arules", dependencies=TRUE)

# Document Similarity
#install.packages("R6", dependencies=TRUE)
#install.packages("Matrix", dependencies=TRUE)
#install.packages("formatR", dependencies=TRUE)
#install.packages("Rcpp", dependencies=TRUE)
#install.packages("codetools", dependencies=TRUE)
#install.packages("grid", dependencies=TRUE)
#install.packages("iterators", dependencies=TRUE)
#install.packages("foreach", dependencies=TRUE)
#install.packages("data.table", dependencies=TRUE)
#install.packages("mlapi", dependencies=TRUE)
#install.packages("digest", dependencies=TRUE)
#install.packages("RcppParallel", dependencies=TRUE)
#install.packages("lattice", dependencies=TRUE)
#install.packages("futile.logger", dependencies=TRUE)
#install.packages("futile.options", dependencies=TRUE)
#install.packages("lambda.r", dependencies=TRUE)
#install.packages("text2vec", dependencies=TRUE)

# Document Summary
install.packages("readtext", dependencies=TRUE)
install.packages("xml2", dependencies=TRUE)
install.packages("rvest", dependencies=TRUE)
install.packages("lexRankr", dependencies=TRUE)
install.packages("textrank", dependencies=TRUE)
install.packages("textmineR", dependencies=TRUE)
install.packages("textreadr", dependencies=TRUE)
install.packages("pdftools", dependencies=TRUE)
install.packages("antiword", dependencies=TRUE)
install.packages("tm", dependencies=TRUE)
#install.packages("textreuse", dependencies=TRUE)
#install.packages("stringr", dependencies=TRUE)
#install.packages("dplyr", dependencies=TRUE)
#install.packages("udpipe", dependencies=TRUE)

# Natural Language Search
install.packages("udpipe", dependencies=TRUE)
install.packages("tokenizers", dependencies=TRUE)
install.packages("http://cran.r-project.org/src/contrib/Archive/SteinerNet/SteinerNet_3.0.1.tar.gz", repos=NULL, type="source")
#install.packages("igraph", dependencies=TRUE)
#install.packages("data.table", dependencies=TRUE)
#install.packages("stringr", dependencies=TRUE)
#install.packages("stringdist", dependencies=TRUE)

# Implied insights
install.packages("Boruta", dependencies=TRUE)
install.packages("FSelector", dependencies=TRUE)
install.packages("skimr", dependencies=TRUE)
#install.packages("rlang", dependencies=TRUE)
#install.packages("tidyselect", dependencies=TRUE)
#install.packages("data.table", dependencies=TRUE)
#install.packages("arules", dependencies=TRUE)

# Sentiment Analysis
install.packages("sentimentr", dependencies=TRUE)

# Fuzzy Lookup
install.packages("qs", dependencies=TRUE)

# Pivot Tables
install.packages("pivottabler", dependencies=TRUE)

#Complex Joins
install.packages("sqldf", dependencies=TRUE)

#NetworkViz
install.packages("visNetwork", dependencies=TRUE)
install.packages("networkD3", dependencies=TRUE)
install.packages("htmlwidgets", dependencies=TRUE)

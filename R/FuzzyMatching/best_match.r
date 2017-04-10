match_metrics <- function(df){
library(stringdist)
c<-ncol(df)
c1<-as.character(unique(df[,1]))
c2<-unique(df[,2])
d<-as.character(c2)

r <- data.frame(item=character(), match=character(),similarity=numeric(),appliedmethod = character(),stringsAsFactors=FALSE)
r<-best_match(r,c1,d,method="lv",q=1,p=0)
r<-best_match(r,c1,d,method="dl",q=1,p=0)
r<-best_match(r,c1,d,method="qgram",q=2,p=0)
r<-best_match(r,c1,d,method="cosine",q=2,p=0)
r<-best_match(r,c1,d,method="osa",q=1,p=0)
r<-best_match(r,c1,d,method="jw",q=1,p=0.1)
r<-best_match(r,c1,d,method="lcs",q=1,p=0)
r<-best_match(r,c1,d,method="soundex",q=1,p=0)
r<-best_match(r,c1,d,method="jaccard",q=2,p=0)
return(r)
}

best_match <- function(r,c1,d,method=method,q,p){
methods<-list()
methods[["lv"]]<-"Levenshtein"
methods[["dl"]]<-"Damerau-Levenshtein"
methods[["qgram"]]<-"q-gram"
methods[["cosine"]]<-"Cosine q-gram"
methods[["osa"]]<-"Optimal string aligment"
methods[["jw"]]<-"Jaro-Winker"
methods[["lcs"]]<-"Longest common substring"
methods[["jaccard"]]<-"Jaccard q-gram"
methods[["soundex"]]<-"Soundex"
n<-length(c1)
m<-stringdistmatrix(c1,d,method=method,q=q,p=p)

j<-nrow(r)
for (i in 1:n){
  i.min<-min(m[i,],na.rm=TRUE)
  j<-j+1
  ind<-which(m[i,]== i.min)[1]
  sim<-stringsim(c1[i],d[ind],method=method,q=q,p=p)
  r[j,1]<-c1[i]
  r[j,2]<-d[ind] 
  r[j,3]<-sim
  r[j,4]<-methods[[method]]  
}
r$similarity<-round(r$similarity,2)
r<-r[order(r$item,-r$similarity,r$match,r$appliedmethod),]
return(r)
}

df<-read.csv("C:/fuzzyMatching.csv")
match_metrics(df)


match_metrics <- function(df1,df2,col1,col2,samplesize){
# df1 is the dataframe of the first concept (only first column will be used)
# df2 is the dataframe of the first concept  (only first column will be used)
# col1 is the column name from df1 to match
# col2 is column name from df12 to match
# samplesize is the sample size randomly selected from df1
library(stringdist)

if(samplesize < nrow(df1)) {
  idx<-sample(1:nrow(df1),samplesize)
  df1 <- df1[idx,]
  df1<-as.data.frame(df1)
}
if(ncol(df1) == 1){
	names(df1)[1]<-col1
}
cmd1<-paste("c1<-as.character(unique(df1$",col1,"))",sep="")
eval(parse(text=cmd1))

cmd2<-paste("d<-as.character(unique(df2$",col2,"))",sep="")
eval(parse(text=cmd2))

r <- data.frame(item=character(), match=character(),similarity=numeric(),appliedmethod = character(),stringsAsFactors=FALSE)
r<-best_match(r,c1,d,method="lv",q=1,p=0)
r<-best_match(r,c1,d,method="dl",q=1,p=0)
r<-best_match(r,c1,d,method="qgram",q=3,p=0)
r<-best_match(r,c1,d,method="cosine",q=3,p=0)
r<-best_match(r,c1,d,method="osa",q=1,p=0)
r<-best_match(r,c1,d,method="jw",q=1,p=0.1)
r<-best_match(r,c1,d,method="lcs",q=1,p=0)
r<-best_match(r,c1,d,method="soundex",q=1,p=0)
r<-best_match(r,c1,d,method="jaccard",q=3,p=0)
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
m<-stringdistmatrix(c1,d,method=method,q=q,p=p,weight=c(d=1,i=0.95,s=0.9,t=0.85))

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

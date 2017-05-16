fuzzy_single_join <- function(df1,df2,col1,col2,istest,mode,max_dist,method, q, p){
# df1 the dataframe of the first concept
# df2 the dataframe of the first concept
# col1 column name from df1 to join
# col2 column name from df12 to join
# mode of the join: inner, left, right, full, semi, anti
# max_dist maximum distance(lv,dl,qgram,osa,lcs,soundex) or minimum similarity(jw,cosine,jaccard) required to join the concept records
# method for fuzzy matching
# q the size of grams for q-gram matching
# p - penalty for some methods for fuzzy matching
# 
library(fuzzyjoin)
#write.csv(df1,"C:\\Users\\rramirezjimenez\\workspace\\Semoss\\R\\FuzzyJoin\\Temp\\sourceDataFrame.csv")
#write.csv(df2,"C:\\Users\\rramirezjimenez\\workspace\\Semoss\\R\\FuzzyJoin\\Temp\\targetDataFrame.csv")
a<-paste(col1,"=","\"",col2,"\")",sep="")
b<-paste(",mode=\"",mode,"\",max_dist=",max_dist,",method=\"",method,"\",q=",q,",p=",p,",weight=c(d=1,i=0.95,s=0.9,t=0.85))",sep="")
c<-paste("r<-","stringdist_join(df1,df2,by=c(",a,b,sep="")
eval(parse(text=c))
#write.csv(r,"C:\\Users\\rramirezjimenez\\workspace\\Semoss\\R\\FuzzyJoin\\Temp\\final.csv")
if(ncol(df1) == 1){
r<-as.data.frame(r)
names(r)[1]<-col1
}
if(istest){
	r<-r[,c(col1,col2)]
}
return(r)
}

#df1<-read.csv("C:\\workspace\\Semoss\\R\\FuzzyJoin\\Temp\\sourceDataFrame.csv")
#df2<-read.csv("C:\\workspace\\Semoss\\R\\FuzzyJoin\\Temp\\targetDataFrame.csv")
# Jaro - Winkler
#df<-fuzzy_join(df1,df2,"t1c1","t2c1","inner",0.1,method="jw",q=1,p=0.1)
# Damirau - Levenshtein
#df<-fuzzy_join(df1,df2,"t1c1","t2c1","inner",1,method="dl",q=1,p=0.1)
# qgram
#df<-fuzzy_join(df1,df2,"t1c1","t2c1","inner",2,method="qgram",q=2,p=0)
# cosine
#df<-fuzzy_join(df1,df2,"t1c1","t2c1","inner",0.1,method="cosine",q=2,p=0)
# osa
#df<-fuzzy_join(df1,df2,"t1c1","t2c1","inner",2,method="osa",q=1,p=0)

fuzzy_join(df1,df2,"t1c1","t2c1",TRUE,"inner",0.1,method="jw",q=1,p=0.1)
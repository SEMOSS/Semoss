#for loop over the length of  Description
library(data.table)
DescriptionLength<-length( Description[[1]])
Match=0;
LSA_Score=0;
LSA_Category=0;
joinDescription=0;
for(i in 1: DescriptionLength){
temp<-lsi_lookup(toString( Description[[i,1]]),lookup_tbl,1,LSAspace)
if(length(temp)!=0){
tempDescription<-list(rep(toString( Description[[i,1]]),length(temp[[1]])))
Match<-append( Match,temp[[1]])
LSA_Category<-append(LSA_Category,temp[[2]])
LSA_Score<-append(LSA_Score,temp[[3]])
joinDescription<-append(joinDescription,tempDescription[[1]])
}
}
df<-data.frame(joinDescription, LSA_Category,LSA_Score, Match);
df<-df[-c(1),]

sortdf<-df[with(df, order( joinDescription, -LSA_Score)), ]


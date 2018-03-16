#for loop over the length of  Description
DescriptionLength<-length( Description[[1]])
dfFinal=data.frame()
for(i in 1: DescriptionLength){
temp<-data.frame(lsi_lookup(toString( Description[i,1]),lookup_tbl,.6,LSAspace))
if(length(temp)!=0){
colnames(temp) <- c("Match","LSA_Category","LSA_Score")
temp2<-data.frame(head(temp[order(temp$LSA_Score,decreasing=TRUE),],numMatch))
colnames(temp2) <- c("Match","LSA_Category","LSA_Score")
tempDescription<-list(rep(toString( Description[i,1]),numMatch))
temp3<-data.frame(c(temp2,Description=tempDescription))
dfFinal<-rbind(dfFinal,temp3)
}
}
rm(temp,temp2,temp3,tempDescription)


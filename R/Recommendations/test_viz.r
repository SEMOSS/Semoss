# constructing the visualization history
rm(list=ls())
source("C:\\Users\\pnadolny\\Documents\\Deloitte Information\\SEMOSS\\GA\\commons.r")
source("C:\\Users\\pnadolny\\Documents\\Deloitte Information\\SEMOSS\\GA\\viz_tracking.r")
library(data.table)
library(jsonlite)
library(plyr)
startDate="2018-01-01"
endDate=toString(Sys.Date())
df<-get_userdata(startDate,endDate)
r<-viz_history(df)
write.csv(r,file="C:\\Users\\pnadolny\\Documents\\Deloitte Information\\SEMOSS\\GA\\results\\viz_history.csv",row.names=FALSE,na="") 


###################################################################################################################################
# Generating recommendations
rm(list=ls())
source("C:\\Users\\pnadolny\\Documents\\Deloitte Information\\SEMOSS\\GA\\viz_tracking.r")
df<-read.csv("viz_history.csv")
df1<-head(df,20)[1:3]
r<-viz_recom(df,df1)




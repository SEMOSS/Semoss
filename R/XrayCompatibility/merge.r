xray_merge<-function(data,semantic){
	N<-nrow(data)
	M<-nrow(semantic)
	if(N > 0 & M > 0){
		data$link<-paste0(data$Source_Database,"$",data$Source_Table,"$",data$Source_Column,"$",data$Target_Database,"$",data$Target_Table,"$",data$Target_Column)
		df1<-data[,c(1,23,22)]
		semantic$link<-paste0(semantic$Source_Database,"$",semantic$Source_Table,"$",semantic$Source_Column,"$",semantic$Target_Database,"$",semantic$Target_Table,"$",semantic$Target_Column)
		df2<-semantic[,c(1,23)]
		names(df2)[1]<-"Semantic_Score"

		merged<-merge(x=df1, y= df2, by = "link", all=TRUE)
		n<-nrow(merged)
		out<-data.frame(Source_Database=character(),Source_Table=character(),Source_Column=character(),Target_Database=character(),Target_Table=character(),Target_Column=character(),
		Score=numeric(),Match_Count=integer(),Semantic_Score=numeric(),stringsAsFactors = FALSE)

		for(i in 1:n){
			x<-unlist(strsplit(as.character(merged[i,1]), "[$]"))
			m<-length(x)
			for(j in 1:m){
				out[i,j]<-x[j]
			}
			out[i,7]<-merged[i,2]
			out[i,8]<-merged[i,3]
			out[i,9]<-merged[i,4]
		}
		out[order(out$Source_Database,out$Source_Table,out$Source_Column,-out$Score,-out$Semantic_Score,out$Target_Database,out$Target_Table,out$Target_Column),]
		rm(df1,df2,m,n)
		gc()
	} else if(N > 0 & M == 0){
		out<-data[,c(4,5,6,7,8,9,1)]
		out$Matches<-round(data$Score*data$Source_Instances,0)
		out$Semantic_Score<-"NA"
		out[order(out$Source_Database,out$Source_Table,out$Source_Column,-out$Score),]
	} else if(N == 0 & M > 0){
		out<-semantic[,c(4,5,6,7,8,9)]
		out$Score<-"NA"
		out$Matches<-"NA"
		out$Semantic_Score<-semantic$Score
		out[order(out$Source_Database,out$Source_Table,out$Source_Column,-out$Semantic_Score,out$Target_Database,out$Target_Table,out$Target_Column),]
	} else{
		out<-data.frame(Source_Database=character(),Source_Table=character(),Source_Column=character(),Target_Database=character(),Target_Table=character(),Target_Column=character(),
		Score=numeric(),Match_Count=integer(),Semantic_Score=numeric(),stringsAsFactors = FALSE)
	}
	return(out)
}


#data<-read.csv("c:/Users/gmordinson/Documents/final_data.csv")
#semantic<-read.csv("c:/Users/gmordinson/Documents/final_semantic.csv")
#xray_merge(data,semantic)

#data<-data[0,]
#xray_merge(data,semantic)

#data<-read.csv("c:/Users/gmordinson/Documents/final_data.csv")
#semantic<-semantic[0,]
#xray_merge(data,semantic)

#data<-data[0,]
#semantic<-semantic[0,]
#xray_merge(data,semantic)
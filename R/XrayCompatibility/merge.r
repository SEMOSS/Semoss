xray_merge<-function(data,semantic){
	N<-nrow(data)
	M<-nrow(semantic)
	if(N > 0 & M > 0){
		data$link<-paste0(data$Source_Database,"$",data$Source_Table,"$",data$Source_Property,"$",data$Target_Database,"$",data$Target_Table,"$",data$Target_Property,"$")
		df1<-data[,c(1,2,3,4,5,10,11,12,15)]
		semantic$link<-paste0(semantic$Source_Database,"$",semantic$Source_Table,"$",semantic$Source_Property,"$",semantic$Target_Database,"$",semantic$Target_Table,"$",semantic$Target_Property,"$")
		df2<-semantic[,c(3,15)]
		names(df2)[1]<-"Semantic_Score"

		merged<-merge(x=df1, y= df2, by = "link", all=TRUE)
		n<-nrow(merged)
		out<-data.frame(Source_Database_Id=character(),Source_Database=character(),Source_Table=character(),Source_Property=character(),Source_Column=character(),Source_Instances=integer(),Target_instances=integer(),Target_Database_Id=character(),Target_Database=character(),Target_Table=character(),Target_Property=character(),Target_Column=character(),Score=numeric(),Match_Count=integer(),Semantic_Score=numeric(),stringsAsFactors = FALSE)
		for(i in 1:n){
			x<-unlist(strsplit(as.character(merged[i,1]), "[$]"))
			out[i,1]<-as.character(merged[i,3])
			out[i,2]<-x[1]
			out[i,3]<-x[2]
			out[i,4]<-x[3]
			out[i,5]<-as.character(merged[i,7])
			out[i,6]<-merged[i,5]
			out[i,7]<-merged[i,6]
			out[i,8]<-as.character(merged[i,2])
			out[i,9]<-x[4]
			out[i,10]<-x[5]
			out[i,11]<-x[6]
			out[i,12]<-as.character(merged[i,8])
			out[i,13]<-merged[i,4]
			out[i,14]<-merged[i,9]
			out[i,15]<-merged[i,10]
		}
		out[order(out$Source_Database,out$Source_Table,out$Source_Property,-out$Score,-out$Semantic_Score,out$Target_Database,out$Target_Table,out$Target_Property),]
		rm(df1,df2,n)
		gc()
	} else if(N > 0 & M == 0){
		out<-data[,c(2,13,6,7,10,4,5,1,14,8,9,11,3)]
		out$Match_Count<-round(data$Score*data$Source_Instances,0)
		out$Semantic_Score<-"NA"
		out[order(out$Source_Database,out$Source_Table,out$Source_Property,-out$Score),]
	} else if(N == 0 & M > 0){
		out<-semantic[,c(2,13,6,7,10,4,5,1,14,8,9,11,3)]
		names(out)[ncol(out)]<-"Semantic_Score"
		out$Source_Instances<-"NA"
		out$Target_Instances<-"NA"
		out$Score<-"NA"
		out$Match_Count<-"NA"
		out<-out[,c(1:12,14,15,13)]
		out[order(out$Source_Database,out$Source_Table,out$Source_Property,-out$Semantic_Score,out$Target_Database,out$Target_Table,out$Target_Property),]
	} else{
		out<-data.frame(Source_Database_Id=character(),Source_Database=character(),Source_Table=character(),Source_Property=character(),Source_Column=character(),Source_Instances=integer(),Target_instances=integer(),Target_Database_Id=character(),Target_Database=character(),Target_Table=character(),Target_Property=character(),Target_Column=character(),Score=numeric(),Match_Count=integer(),Semantic_Score=numeric(),stringsAsFactors = FALSE)
	}
	gc()
	return(out)
}

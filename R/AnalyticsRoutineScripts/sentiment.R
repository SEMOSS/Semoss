score_sentiment<-function(df,review_col="review",aggr_col="name",emtn=FALSE){
# Arguments: dataframe with one column: review/optinion	
# output: a dataframe with the sentiment scores for the respected rows of the dataframe
# and the aggregate score
	w1<-0.9
	w2<-1
	library(sentimentr)
	out<-numeric()
	# restore original dataframe
	df<-restore_source(df)

	n<-nrow(df)
	if(n>0){
		for(i in 1:n){
			cmd<-paste("review<-gsub(\"_|break|BREAK|#|@\",\" \",as.character(df$",review_col,"[i]))",sep="")
			eval(parse(text=cmd))
			score<-mean(sentiment(review)$sentiment)
			out<-append(out,score)
			if(emtn){
				if(i==1){
					emotions<-score_emotions(review)
				}else{
					emotions<-rbind(emotions,score_emotions(review))
				}
			}
		}
		df$sentiment<-out
		if(emtn){
			df<-cbind(df,emotions)
		}
	}
	if(!is.null(aggr_col)){
		cmd<-paste("items<-unique(df$",aggr_col,")",sep="")
		eval(parse(text=cmd))
		n<-length(items)
		for(i in 1:n){
			cmd<-paste("df_item<-df[df$",aggr_col,"==items[i],]",sep="")
			eval(parse(text=cmd))
			vg<-length(which(df_item$sentiment>0.5))
			gd<-length(which(df_item$sentiment<=0.5 & df_item$sentiment>0))
			bd<-length(which(df_item$sentiment>=-0.5 & df_item$sentiment<0))
			vb<-length(which(df_item$sentiment<(-0.5)))
			df_item$aggregate_sentiment<-(w2*(vg-vb)+w1*(gd-bd))/nrow(df_item)
			# add aggregated emotions for the item
			if(emtn){
				df_emotions<-aggregate_emotions(df_item)
				df_item<-cbind(df_item,df_emotions)
			}
			
			if(i==1){
				out<-df_item
			}else{
				out<-rbind(out,df_item)
			}
		}
	}else{
		out<-df
	}
	rownames(out)<-NULL
	colnames(out)<-paste(colnames(out),paste("of_",review_col,sep=""),sep="_")
	gc()
	return(out)
}

restore_source<-function(df,review_col="review",aggr_col="name"){
	ind<-which(colnames(df) == paste0(review_col,"_of_",review_col))
	if(length(ind)==1){
		colnames(df)[ind]<-review_col
		ind<-which(colnames(df) == paste0(aggr_col,"_of_",review_col))
		if(length(ind)==1){
			colnames(df)[ind]<-aggr_col
		}
	}
	if(review_col %in% colnames(df)){
		if(!is.null(aggr_col)){
			cmd<-paste("df<-df[,c(\"",review_col,"\",\"",aggr_col,"\")]",sep="")
		}else{
			cmd<-paste("df<-data.frame(\"",review_col,"\"=df$\"",review_col,"\",stringsAsFactors=FALSE)",sep="")
		}
		eval(parse(text=cmd))
	}
	return(df)
}

aggregate_emotions<-function(df_item){
	w1<-0.9
	w2<-0.1
	em<-c("anticipation","anger","disgust","fear","joy","sadness","surprise","trust")
	n<-length(em)
	item_emotions<-vector()
	for(i in 1:n){
		cmd<-paste("gd<-length(which(df_item$",em[i],">0))",sep="")
		eval(parse(text=cmd))
		cmd<-paste("vg<-length(which(df_item$",em[i],">0.5))",sep="")
		eval(parse(text=cmd))
		cmd<-paste("bd<-length(which(df_item$",em[i],"<0))",sep="")
		eval(parse(text=cmd))
		cmd<-paste("vb<-length(which(df_item$",em[i],"<(-0.5)))",sep="")
		eval(parse(text=cmd))
		item_emotions<-append(item_emotions,w1*(gd-bd)+w2*(vg-vb))
	}
	item_emotions<-item_emotions/nrow(df_item)
	
	row_emotions<-cbind.data.frame(split(item_emotions,rep(1:8, times=1, stringsAsFactors=F)))
	df_emotions<-row_emotions
	m<-nrow(df_item)-1
	for(i in 1:m){
		df_emotions<-rbind(df_emotions,row_emotions)
	}
	names(df_emotions)<-paste("aggregate_",em,sep="")
	rownames(df_emotions)<-NULL
	return(df_emotions)
}

score_emotions<-function(review){
	em<-c("anticipation","anger","disgust","fear","joy","sadness","surprise","trust")
	emotions<-suppressMessages({emotion(review)})
	emotions<-aggregate(emotions$emotion_count,by=list(emotions$emotion_type),sum)
	names(emotions)<-c("emotion_type","emotion_count")
	n<-length(em)
	row_emotions<-vector()
	for(i in 1:n){
		pos<-as.integer(emotions[emotions$emotion_type==em[i],"emotion_count"])
		cmd<-paste("neg<-as.integer(emotions[emotions$emotion_type==\"",em[i],"_negated","\",\"emotion_count\"])",sep="")
		eval(parse(text=cmd))
		row_emotions<-append(row_emotions,pos-neg)
	}
	names(row_emotions)<-em
	total<-sum(row_emotions)
	if(total != 0){
		row_emotions<-row_emotions/sum(row_emotions)
	}
	return(row_emotions)
}

allocate_portfolio<-function(df,threshold=1,max_share=0.25){
# Determines portfolio allocations
# Arguments:
# name - the name of the stock
# beta - the stock volatility vs matrket
# return - the stock return 
# 
	library(lpSolve)
	n<-nrow(df)
	dir<-"max"
	f.obj<-as.numeric(df$return)
	con<-matrix(0,ncol=n,nrow=0)
	con_shared<-rep(1,n)
	con<-rbind(con,con_shared)
	con.dir<-"="
	con.rhs<-1
	
	con_beta<-as.numeric(df$beta)
	con<-rbind(con,con_beta)
	con.dir<-append(con.dir,"<=")
	con.rhs<-append(con.rhs,threshold)
	
	con_args<-diag(1,n,n)
	con<-rbind(con,con_args)
	con.dir<-append(con.dir,rep(">=",n))
	con.rhs<-append(con.rhs,rep(0,n))
	
	con<-rbind(con,con_args)
	con.dir<-append(con.dir,rep("<=",n))
	con.rhs<-append(con.rhs,rep(max_share,n))
	
	out<-lp(dir,f.obj,con,con.dir,con.rhs)
	
	if(out$status == 0){
		df$share<-round(out$solution,2)
		df$total<-round(df$return*df$share,2)
	}else{
		df$share<-0.00
		df$total<-0.00
	}
	gc()
	return(df)
}



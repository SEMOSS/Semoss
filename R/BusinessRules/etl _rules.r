
etl_rules <- function(df1,df2,list1,list2,sprt,conf){
# This function tries to infer rules for the instances that are present in df1, but not in df2
#
# df1 is the first dataframe
# df2 is the second dataframe that has the same structure as df1, but less data
# list1 is the primary key of the first dataframe
# list2 is the primary key of the second dataframe
# show defines whether to show only critical variables or all above average importance

if(length(list1) == 0 | length(list2) == 0) {
	cat("Warning: list is empty")
	flush.console()
	return()
} else {
		if(length(list1) != length(list2)) {
				cat("Error: size of list1=",length(list1)," is not equal size of list2=",length(list2))
				flush.console()
				return()
		} else{
			c1<-ncol(df1)
			for(i in 1:c1){
				if(class(df1[,i]) == "factor" |  class(df1[,i]) == "integer") {
					df1[,i]<-as.factor(df1[,i])
				}
			}
			c2<-ncol(df2)
			for(i in 1:c2){
				if(class(df2[,i]) == "factor" |  class(df2[,i]) == "integer") {
					df2[,i]<-as.factor(df2[,i])
				}
			}
			n<-length(list1)
			m<-length(list2)
			col<-list1[1]
			cmd<-paste0("df1$comp1<-as.character(df1$",col,")")
			eval(parse(text=cmd))
			if(n >1){
				for (i in 2:n){
					col<-list1[i]
					a<-paste0("as.character(df1$",col,")")
					b<-paste0("paste0(df1$comp1,",a,")")
					cmd<-paste0("df1$comp1<-",b)
					eval(parse(text=cmd))
				}
			}
			col<-list2[1]
			cmd<-paste0("df2$comp2<-as.character(df2$",col,")")
			eval(parse(text=cmd))
			if(m >1){
				for (i in 2:m){
					col<-list2[i]
					a<-paste0("as.character(df2$",col,")")
					b<-paste0("paste0(df2$comp2,",a,")")
					cmd<-paste0("df2$comp2<-",b)
					eval(parse(text=cmd))
				}
			}
		}

		df1$test<-"No"
		if(nrow(df1[(df1$comp1 %in% df2$comp2),]) > 0) {
			df1[(df1$comp1 %in% df2$comp2),]$test<-"Yes"
			df1<-df1[,-(ncol(df1)-1)]
			df1$test<-as.factor(df1$test)
			df<-df1
		} else{
			# return(data.frame(rule=character(),support=numeric(),confidence=numeric(),lift=numeric()))
			return(empty_dataframe())
		}
		
}


for(i in 1:ncol(df)){
	if(class(df[,i]) == "numeric"){
		bins<-nclass.FD(df[,i])
		df[,i]<-discretize(df[,i],"interval",categories=bins)
	}
} 

library(arules)
dft<-df[df$test == "No",]
if(nrow(dft) > 0) {
	rules.all<-apriori(df,parameter=list(support=sprt,confidence=conf,maxlen=6),appearance=list(rhs="test=No"))
	rules<-subset(rules.all,subset=rhs %in% "test=No")
	
	if(nrow(rules@quality) > 0) {
		quality(rules) <- round(quality(rules), digits=4)
		rules.pruned<-rules[!is.redundant(rules)]
		r = data.frame(rule=labels(rules.pruned@lhs),support=rules.pruned@quality$support,confidence=rules.pruned@quality$confidence,lift=rules.pruned@quality$lift)
		return(r[order(r$support,r$confidence,r$lift,decreasing=c(T,T,T)),])
	} else {
		#return(data.frame(rule=character(),support=numeric(),confidence=numeric(),lift=numeric()))
		return(empty_dataframe())
	}
} else {
	#return(data.frame(rule=character(),support=numeric(),confidence=numeric(),lift=numeric()))
	return(empty_dataframe())
}
}

empty_dataframe <- function(){
	dft<-data.frame(rule=character(),support=numeric(),confidence=numeric(),lift=numeric())
	temprow <- matrix(c(rep.int("",length(dft))),nrow=1,ncol=length(dft))
	newrow <- data.frame(temprow)
	colnames(newrow) <- colnames(dft)
	dft <- rbind(dft,newrow)
	return(dft)
}

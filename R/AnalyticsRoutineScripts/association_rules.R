get_association_rules<-function(in_df,attrs,premise=NULL,outcome=NULL,support=0.01,confidence=0.8,lift=1,nbr_int=5,mult=3){
# Identify association rules
# Args
# in_df - table/dataframe to be used for rules search
# attrs - an array of columns (character or factor) in in_df to be used for rules
# premise - left side of the rule 
# outcome - rught side of the rule {example c("HIGH_COST=1","HIGH_COST=0")}
# support - minimum support level for the rule (default 0.01)
# confidence - minimum confidence level for the rule (default 0.8)
# lift - minimum lift level for the rule (default 1)

	library(arules)
	if(length(attrs)>0){
		cols<-which(colnames(in_df) %in% attrs)
		cmd<-paste0('df<-in_df[,',paste0('c(',paste(cols,collapse=','),')'),']')
		eval(parse(text=cmd))
		# discretize
		# integer columns with few unique values become factors
		types<-unlist(lapply(df,class))
		cols<-names(types[types=="integer"])
		for(col in cols){
			cnt<-length(unique(df[[col]]))
			if(cnt <= nbr_int*mult){
				df[[col]]<-as.factor(df[[col]])
			}
		}
		df<-discretizeDF(df,default=list(method="interval",breaks=nbr_int))
		for(i in 1:length(attrs)){
			if(class(df[[attrs[i]]]) != "factor"){
				df[[attrs[i]]]<-as.factor(df[[attrs[i]]])
			}
		}
		trans<-as(df,"transactions")
		
		# build and execute rule detection command
		myparameter <- list()
		if (!is.null(support)) {myparameter$supp <- support}
		if (!is.null(confidence)) {myparameter$conf <- confidence}
		myparameter$target<-"rules"
		
		myappearance<-list()
		if (!is.null(premise)){
			myappearance$lhs <- premise
		}
		if (!is.null(outcome)){
			myappearance$rhs <- outcome
		}
		if (!is.null(premise) && is.null(outcome)) {
			myappearance$default <- "rhs"
		} else if (is.null(premise) && !is.null(outcome)) {
			myappearance$default <- "lhs"
		} else if (!is.null(premise) && !is.null(outcome)) {
			myappearance$default <- "none"
		}
		# discover rules
		rules<-apriori(data=trans,parameter=myparameter,appearance=myappearance,control = list (verbose=F))
		if(length(rules)>0){
			# keep rules with lift greater than 1
			cmd<-paste0("rules<-subset(rules,subset = lift > ",lift,")")
			eval(parse(text=cmd))
			
			# drop redundant rules
			ind<-which(is.redundant(rules, measure = "confidence"))
			if(length(ind)>0){
				rules<-rules[ind]
			}
			rules_labels<-labels(rules)
			r<-parse_labels(rules_labels)
			rules_premise<-r[[1]]
			rules_outcome<-r[[2]]
			rules_quality<-quality(rules)
			out<-data.frame(Premise=rules_premise,Outcome=rules_outcome,Support=round(rules_quality$support,2),Confidence=round(rules_quality$confidence,2),
			Lift=round(rules_quality$lift,2),Count=rules_quality$count,stringsAsFactors=TRUE)
			out<-out[order(-out$Support,-out$Confidence,-out$Lift),]
		}else{
			out<-data.frame(Premise=character(),Outcome=character(),Support=numeric(),Confidence=numeric(),Lift=numeric(),Count=integer(),stringsAsFactors=TRUE)
		}
	}else{
		out<-data.frame(Premise=character(),Outcome=character(),Support=numeric(),Confidence=numeric(),Lift=numeric(),Count=integer(),stringsAsFactors=TRUE)
	}
	gc()
	return(out)
}

parse_labels<-function(rules_labels){
	premise<-vector()
	outcome<-vector()
	for(i in 1:length(rules_labels)){
		items<-trim(unlist(strsplit(rules_labels[i],'=>')))
		premise<-append(premise,substr(items[1],2,nchar(items[1])-1))
		outcome<-append(outcome,substr(items[2],2,nchar(items[2])-1))
	}
	r<-list()
	r[[1]]<-premise
	r[[2]]<-outcome
	gc()
	return(r)
}

trim<-function(x) {
  gsub("(^[[:space:]]+|[[:space:]]+$)", "", x)
}
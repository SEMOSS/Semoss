get_userdata<-function(startDate,endDate){
# Description
# Retrieve the event data from GA project site then parse 
# and assemble them with the requested level of details
# Arguments
# startDate - starting date for analysis
# endDate - en date for analysis

set_config(config(ssl_verifypeer = 0L))

load("./token_file")
# Validate the token
ValidateToken(token)

viewID="ga:151491080"
dim=c("1","2","3","4","5")
query.list<-Init(
            start.date=startDate,
            end.date=endDate, 
            dimension=toString(paste("ga:dimension", dim, sep="")),
            metrics="ga:totalEvents",
            sort=toString(paste("-ga:dimension", dim, sep="")),
            max.results=10000,
            table.id=viewID  
        )
ga.query<-QueryBuilder(query.list)
#ga.df <- GetReportData(ga.query, token)
ga.df <- tryCatch({
    GetReportData(ga.query, token)
  }, error = function(e) {
    return(data.frame(dimension1 = character(), dimension2 = character(), dimension3 = character(), dimension4 = character(),dimension5 = character(),date = character(), totalEvents=numeric(1)))
  })
gc()
return(ga.df)
}

parse_dataitem<-function(item){
	x<-unlist(strsplit(item, "[:]"))
	y<-unlist(strsplit(x[2], "__"))
	z<-array()
	z[1]<-x[1]
	z[2]<-y[1]
	if(length(y) > 1){
		z[3]<-y[2]
	}else{
		z[3]<-""
	}
	rm(x,y)
	return(z)
}

parse_details<-function(details,level){
# Description
# parse the data details based on requested level
# Arguments
# details - data details
# level - the level of details requested
	x<-unlist(strsplit(details, "[;]"))
	n<-length(x)
	if(level == 2) {
		for(i in 1:n){
			x[i]<-unlist(strsplit(x[i], "[:]"))[1]
		}
		x<-unique(x)
	} else if (level == 3){
		for(i in 1:n){
			x[i]<-unlist(strsplit(x[i], "__"))[1]
		}
		x<-unique(x)
	}
	s<-""
	n<-length(x)
	for(i in 1:n){
		s<-paste0(s,x[i],";")
	}
	rm(x,n)
	return(s)
}

recom<-function(df,method,type,topN){
# Objective: compute recommendations dataframe using valious methods
# ARGUMENTS
# dataframe of events pairs counts
# method - "UBCF","IBCF","ALS","POPULAR"
# type - "ratings" for missing values, "ratingMatrix" for all values, "topN" 
# topN - for type="topN" the number of top values to show 
rm<-as(df, "realRatingMatrix") 
# rm
# getRatingMatrix(rm)
recommenderRegistry$get_entries(dataType = "realRatingMatrix")
r <- Recommender(rm, method = method)
names(getModel(r))
#getModel(r)$topN
recom <- predict(r, rm, n=topN,type=type)
x<-getList(recom)

n<-length(x)
z<-data.frame(event1=as.character(),event2=as.character(),count=as.numeric())
for(i in 1:n){
	m<-length(x[[i]])
	if(m > 0) {
		for(j in 1:m){
			if(type == "topN") {
				newrow<-data.frame(names(x[i]),x[[i]][j],m-j+1)
			} else {
				newrow<-data.frame(names(x[i]),names(x[[i]][j]),x[[i]][j])
			}
			z<-rbind(z,newrow)
		}
	}

}
return(z)
}

evaluate<-function(df,train, given, goodRating){
# Objective: evaluate recommendations by different methods
# ARGUMENTS
# dataframe of events pairs counts
# portion of data allocated to training
# number of given items for each observation
# good rating score
library(recommenderlab)
rm<-as(df, "realRatingMatrix") 
e <- evaluationScheme(rm, method="split", train=train, given=given, goodRating=goodRating)
r1 <- Recommender(getData(e, "train"), "UBCF")
r2 <- Recommender(getData(e, "train"), "IBCF")
r3 <- Recommender(getData(e, "train"), "ALS")
r4 <- Recommender(getData(e, "train"), "POPULAR")
p1 <- predict(r1, getData(e, "known"), type="ratings")
p2 <- predict(r2, getData(e, "known"), type="ratings")
p3 <- predict(r3, getData(e, "known"), type="ratings")
p4 <- predict(r4, getData(e, "known"), type="ratings")
error <- rbind( UBCF = calcPredictionAccuracy(p1, getData(e, "unknown")),IBCF = calcPredictionAccuracy(p2, getData(e, "unknown")),ALS = calcPredictionAccuracy(p3, getData(e, "unknown")),
POPULAR = calcPredictionAccuracy(p4, getData(e, "unknown")))
print(error)
flush.console()
}

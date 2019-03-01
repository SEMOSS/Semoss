build_model<-function(mydata,neurons=c(5,3),limit=0.0001){
	library(neuralnet)
	maxs <- apply(mydata, 2, max)
	mins <- apply(mydata, 2, min)
	scaled <- as.data.frame(scale(mydata, center = mins, scale = maxs - mins))
	ind <- sample(1:nrow(mydata),round(0.8*nrow(mydata)))
	train<-scaled[ind,]
	test<-scaled[-ind,]
	n<-colnames(train)
	f <- as.formula(paste("Output ~", paste(n[!n %in% "Output"], collapse = " + ")))
	model <- neuralnet(f,data=train,hidden=neurons,linear.output=TRUE,threshold=limit)
	# plot(model)
	# compute rmse for training
	# for a good fit the rmse for training and prediction should be in the same range
	# if rmse test > rmse train the model is overfitting
	# if rmse test < rmse train the model is underfitting
	tr.model<-compute(model,train[,1:2])
	tr.rescaled <- tr.model$net.result*(max(mydata$Output)-min(mydata$Output))+min(mydata$Output)
	tr.out<-cbind(mydata[ind,"One"],mydata[ind,"Two"],mydata[ind,"Output"],as.data.frame(tr.rescaled))
	colnames(tr.out)<-c("One","Two","Output","NN_Output")
	tr.rmse <- sum((mydata[ind,"Output"] - tr.rescaled)^2)/nrow(train)
	# compute rmse for prediction
	pr.model<-compute(model,test[,1:2])
	pr.rescaled <- pr.model$net.result*(max(mydata$Output)-min(mydata$Output))+min(mydata$Output)
	pr.out<-cbind(mydata[-ind,"One"],mydata[-ind,"Two"],mydata[-ind,"Output"],as.data.frame(pr.rescaled))
	colnames(pr.out)<-c("One","Two","Output","NN_Output")
	pr.rmse <- sum((mydata[-ind,"Output"] - pr.rescaled)^2)/nrow(test)
	
	# Construct output for the user
	x<-rbind(tr.out,pr.out)
	new<-as.data.frame(mydata$Output)
	colnames(new)<-"Output"
	new$NN_Output<-x$NN_Output[match(unlist(mydata$Output),x$Output)]
	
	myList<-list()
	myList[["Training RSME"]]<-tr.rmse
	myList[["Prediction RSME"]]<-pr.rmse
	#myList[["Training"]]<-tr.out
	#myList[["Prediction"]]<-pr.out
	myList[["Comparison"]]<-new
	saveRDS(model,"neralnet_model.rds")
	gc()
	return(myList)
}

cross_validate<-function(mydata,neurons=c(5,3),limit=0.0001,chunks=10){
	# Cross validation
	set.seed(1248)
	cv.error <- NULL
	n<-colnames(mydata)
	f <- as.formula(paste("Output ~", paste(n[!n %in% "Output"], collapse = " + ")))
	for(i in 1:chunks){
		maxs <- apply(mydata, 2, max)
		mins <- apply(mydata, 2, min)
		scaled <- as.data.frame(scale(mydata, center = mins, scale = maxs - mins))
		ind <- sample(1:nrow(mydata),round((chunks-1)/chunks*nrow(mydata)))
		train.cv <- scaled[ind,]
		test.cv <- scaled[-ind,]
		model <- neuralnet(f,data=train.cv,hidden=neurons,linear.output=TRUE,threshold=limit)   
		pr.model <- compute(model,test.cv[,1:2])
		pr.model <- pr.model$net.result*(max(mydata$Output)-min(mydata$Output))+min(mydata$Output)   
		test.cv.r <- (test.cv$Output)*(max(mydata$Output)-min(mydata$Output))+min(mydata$Output)   
		cv.error[i] <- sqrt(sum((test.cv.r - pr.model)^2)/length(test.cv.r))  
	}
	myList<-list()
	myList[["Mean RMSE"]]<-mean(cv.error)
	myList[["Cross Validation Errors"]]<-cv.error
	gc()
	return(myList)
}

validate_model<-function(mydata,neurons=c(5,3),limit=0.0001){
	model<-readRDS("neralnet_model.rds")
	# Cross validation
	set.seed(1248)
	maxs <- apply(mydata, 2, max)
	mins <- apply(mydata, 2, min)
	scaled <- as.data.frame(scale(mydata, center = mins, scale = maxs - mins))
  	pr.model <- compute(model,scaled[,1:2])
	pr.model <- pr.model$net.result*(max(mydata$Output)-min(mydata$Output))+min(mydata$Output)   
	test.cv.r <- (scaled$Output)*(max(mydata$Output)-min(mydata$Output))+min(mydata$Output)   
	err<- sqrt(sum((test.cv.r - pr.model)^2)/length(test.cv.r))  
	gc()
	return(err)
}

prepare_data<-function(series){
	library(data.table)
	mydata<-data.table(One=numeric(),Two=numeric(),Output=numeric());
	n<-length(series)-2
	if(n>0){
		series<-series[order(series)]
		for(i in 1:n){
			mydata<-rbindlist(list(mydata,list(series[i],series[i+1],series[i+2])))
		}
	}
	return(mydata)
}

compose_model<-function(df_series,neurons=c(5,3),limit=0.0001,chunks=10){
	series<-df_series[,1]
	mydata<-prepare_data(series)
	myList<-tryCatch({
		build_model(mydata,neurons,limit)
	},error = function(e) {
		myList<-list()
		myList[["Training RSME"]]<--1
		myList[["Prediction RSME"]]<--1
		myList[["Comparison"]]<-data.frame()
		myList[["Cross Validation"]]<<-data.frame()
		return(myList)
    })
	#myList<-build_model(mydata,neurons,limit)
	myList[["Cross Validation"]]<-tryCatch({
		cross_validate(mydata,neurons,limit,chunks)
	},error = function(e) {
		myList<-list()
		myList[["Training RSME"]]<--1
		myList[["Prediction RSME"]]<--1
		myList[["Comparison"]]<-data.frame()
		myList[["Cross Validation"]]<<-data.frame()
		return(myList)
		
	})
	#myList[["Cross Validation"]]<-cross_validate(mydata,neurons,limit,chunks)
	return(myList)
}

ga.df <- tryCatch({
    GetReportData(ga.query, token)
  }, error = function(e) {
    return(data.frame(dimension1 = character(1), dimension2 = character(1), date = character(1), totalEvents=numeric(1)))
  })
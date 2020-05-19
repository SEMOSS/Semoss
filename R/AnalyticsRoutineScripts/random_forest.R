
build_rfmodel<-function(ind_vars,dep_var,trainFrame,depth){
# build random forest model and save it as an asset
# Args
# ind_vars - a vector of independent variables
# dep_var - dependent variable
# trainFrame - training table with column names redlecting dependent and independet variables
# depth - max depth a decission tree (0 means unlimited)
	library(data.table)
	library(ranger)
	
	# construct formula
	formula<-paste0(dep_var,"~",paste(ind_vars,collapse=" + "))
	# generate model
	model<-ranger(formula,data=trainFrame,importance="impurity",max.depth=depth,write.forest=TRUE)

	# assemble output
	out<-list()
	if(class(trainFrame[[dep_var]]) %in% c("factor","character")){
		out[[1]]<-model$prediction.error
		out[[2]]<-as.factor(trainFrame[[dep_var]])
		out[[3]]<-model$predictions
		out[[4]]<-data.frame(Variables=names(model$variable.importance),Importance=round(unname(model$variable.importance),4))
	}else{
		err<-abs(trainFrame[[dep_var]]-model$predictions)
		values<-vector()
		values[1]<-round(min(err),4)
		values[2]<-round(max(err),4)
		values[3]<-round(mean(err),4)
		values[4]<-round(sd(err),4)
		out[[1]]<-model$prediction.error
		out[[2]]<-data.frame(Attributes=c("Min","Max","Mean","Stdev"),Prediction_Error=values)
		out[[3]]<-out[[2]]
		out[[4]]<-data.frame(Variables=names(model$variable.importance),Importance=round(unname(model$variable.importance),4))
	}
	out[[5]]<-model
	gc()
	return(out)
}

drop_missing<-function(ind_vars,dep_var,tbl){
	library(data.table)
	tbl[tbl=="null"]<-NA
	tbl[tbl=="NULL"]<-NA
	mynames<-paste0("c('",gsub(",","','",paste(append(ind_vars,dep_var),collapse=",")),"')")
	cmd<-paste0("out<-na.omit(tbl,cols=",mynames,")")
	eval(parse(text=cmd))
	return(out)
}

rfmodel_mgr<-function(ind_vars,dep_var,trainFrame,model_fn="rf",mis_data="as_is",sample_size=1000,sample_blocks=100,threshold=10000,depth=3){
# Perform classification and regression for large datasets using random forest
# Args
# ind_vars - independent variables (columns in the training dataset)
# dep_var - dependent variable (column in the training dataset)
# trainFrame - training dataset
# model_fn - the root of the rain forest model to save as an asset
# sample_sixe - size of the same (optional)
# sample_blocks - number of samples (optional)
# depth - max depth a decission tree (0 means unlimited)
# Output
# a list with attributes of the random forest model
# random forest model and its parameters saved as rds files
	library(plyr)
	ACTIONS<-c("impute","drop","as_is")
	if(mis_data %in% ACTIONS){
		# remove missing values
		MODEL_NUMBER<-1
		TREE_NUMBER<-1
		
		# fill in missing data per user action
		trainFrame<-mis_data_action_rf(trainFrame,c(ind_vars,dep_var),mis_data)[[1]]
		n<-nrow(trainFrame)
		if(n>0){
			models<-list()
			if(n > threshold){
				out<-list()
				cnt<-vector()
				for(i in 1:sample_blocks){
					trainBlock<-trainFrame[unique(sample(n,sample_size,T)),]
					out[[i]]<-build_rfmodel(ind_vars,dep_var,trainBlock,depth)
					if(i==MODEL_NUMBER){
						tree<-build_tree(dep_var,out[[i]][[5]],TREE_NUMBER)
					}
					cnt[i]<-nrow(trainBlock)
					models[[i]]<-out[[i]][[5]]
				}
				if(class(trainFrame[[dep_var]]) %in% c("factor","character")){
					type<-1   # classification
					opts<-sort(unique(trainFrame[[dep_var]]))
					confusion_matrix<-matrix(0,nrow=length(opts),ncol=length(opts))
					colnames(confusion_matrix)<-opts
					rownames(confusion_matrix)<-opts
					confusion_matrix<-as.table(confusion_matrix)
					
					for(i in 1:sample_blocks){
						cur<-out[[i]]
						if(i==1){
							err<-cur[[1]]
							
							cur_matrix<-models[[i]]$confusion.matrix
							cur_cols<-colnames(cur_matrix)
							ind<-which(opts %in% cur_cols)
							for(j in 1:length(ind)){
								for(k in 1:length(ind)){
									confusion_matrix[ind[j],ind[k]]<-cur_matrix[j,k]
								}
							}
							imp<-cur[[4]]
						}else{
							err<-err+cur[[1]]
							
							cur_matrix<-models[[i]]$confusion.matrix
							cur_cols<-colnames(cur_matrix)
							ind<-which(opts %in% cur_cols)
							for(j in 1:length(ind)){
								for(k in 1:length(ind)){
									confusion_matrix[ind[j],ind[k]]<-confusion_matrix[ind[j],ind[k]]+cur_matrix[j,k]
								}
							}
							imp<-cbind(imp,cur[[4]][,2])
						}
					}
					z<-list()
					z[[1]]<-round(err/sample_blocks,4)
					z[[2]]<-confusion_matrix
					z[[3]]<-data.frame(Variable=imp[,1],Importance=apply(imp[,-1],1,sum))
				}else{
					type<-2   # regression
					for(i in 1:sample_blocks){
						cur<-out[[i]]
						if(i==1){
							err<-cur[[1]]
							tbl<-cur[[2]]
							imp<-cur[[4]]
						}else{
							err<-err+cur[[1]]
							tbl<-cbind(tbl,cur[[2]][,2])
							imp<-cbind(imp,cur[[4]][,2])
						}
					}
					z<-list()
					z[[1]]<-round(err/sample_blocks,4)
					z[[2]]<-data.frame(Attributes=c("Min","Max","Mean","Stdev"),Prediction_Error=c(min(tbl[1,-1]),max(tbl[2,-1]),sum(tbl[3,-1]*cnt)/sum(cnt),sqrt(sum(tbl[4,-1]^2*cnt)/sum(cnt))))
					z[[3]]<-data.frame(Variable=imp[,1],Importance=apply(imp[,-1],1,sum))
				}
				z[[4]]<-sample_blocks
				z[[5]]<-500
			}else{
				z<-list()
				sample_blocks<-1
				cnt<-vector()
				type<-0
				x<-build_rfmodel(ind_vars,dep_var,trainFrame,depth)
				tree<-build_tree(dep_var,x[[5]],TREE_NUMBER)
				z[[1]]<-x[[1]]
				if(class(trainFrame[[dep_var]]) %in% c("factor","character")){
					z[[2]]<-table(true=x[[2]],predicted=x[[3]])
					rownames(z[[2]])<-colnames(z[[2]])
				}else{
					z[[2]]<-x[[2]]
				}
				z[[3]]<-x[[4]]
				z[[4]]<-1
				z[[5]]<-500
				models<-x[[5]]
			}
			params<-list()
			params[[1]]<-ind_vars
			params[[2]]<-dep_var
			params[[3]]<-sample_blocks
			params[[4]]<-cnt
			params[[5]]<-type
			saveRDS(params,paste0(model_fn,"_rfparams.rds"))
			saveRDS(models,paste0(model_fn,"_rfmodels.rds"))
		}else{
			z<-"Training dataset without missing values is empty"
		}
	}else{
		z<-"Possible missing data actions are: impute, drop and as is"
	}
	gc()
	return(z)
}

get_tree<-function(model_fn,model_nbr=1,tree_nbr=1){
	params<-readRDS(paste0(model_fn,"_rfparams.rds"))
	models<-readRDS(paste0(model_fn,"_rfmodels.rds"))
	cnts<-params[[4]]
	n<-length(cnts)
	if(n==0){
		model<-models
	}else{
		model<-models[[min(n,model_nbr)]]
	}
	tree<-build_tree(params[[2]],model,min(500,tree_nbr))
	gc()
	return(tree)
	
}

predict_rfmodel<-function(newFrame,model_fn="rf",mis_data="as_is"){
# Retrieve the previously saved model and perform the predictions
# Args
# newFrame - a dataframe of new independent variables used in the model
# model_fn - a file name of the model
	ACTIONS<-c("impute","drop","as_is")
	
	library(ranger)
	library(data.table)
	
	# store the initial frame
	in_frame<-newFrame
	if(nrow(in_frame)>0){
		if(mis_data %in% ACTIONS){
			# fill in missing data per user action
			r<-mis_data_action_rf(newFrame,colnames(newFrame),mis_data)
			newFrame<-r[[1]]
			valid_rows<-r[[2]]
			
			params<-readRDS(paste0(model_fn,"_rfparams.rds"))
			models<-readRDS(paste0(model_fn,"_rfmodels.rds"))
			out<-list()
			if(all(params[[1]] %in% names(newFrame))){
				cnts<-params[[4]]
				n<-length(cnts)
				if(n==0){
					prediction<-predict(models,newFrame)$prediction
					if(length(valid_rows)==0){
						cur_frame<-newFrame
						cur_frame[,params[[2]]]<-prediction
					}else{
						cur_frame<-in_frame
						dropped_rows<-which(!(seq(nrow(in_frame)) %in% valid_rows))
						cur_frame[,params[[2]]]<-prediction[1]
						cur_frame[valid_rows,params[[2]]]<-prediction
						cur_frame[dropped_rows,params[[2]]]<-NA
					}
					colnames(cur_frame)[ncol(cur_frame)]<-paste0("Predicted_",params[[2]])
				}else{
					for(i in 1:n){
						model<-models[[i]]
						if(i==1){
							pred<-data.frame(m1=predict(model,newFrame)$predictions)
						}else{
							pred[,paste0("m",i)]<-predict(model,newFrame)$predictions
						}
					}
					m<-nrow(pred)
					if(params[[5]]==1){
						for(i in 1:n){
							pred[,paste0("m",i)]<-as.character(pred[,paste0("m",i)])
						}
						p<-vector()
						for(i in 1:m){
							cur<-data.frame(Prediction=as.character(pred[i,]),Freq=cnts)
							grp<-aggregate(cur[,2],by=list(cur$Prediction),FUN=sum)
							grp<-grp[order(-grp$x),]
							p[i]<-as.character(grp[1,1])
						}
					}else{
						p<-apply(pred,1,mean)
					}
					if(length(valid_rows)==0){
						cur_frame<-newFrame
						cur_frame[,params[[2]]]<-p
					}else{
						cur_frame<-in_frame
						dropped_rows<-which(!(seq(nrow(cur_frame)) %in% valid_rows))
						cur_frame[,params[[2]]]<-p[1]
						cur_frame[valid_rows,params[[2]]]<-p
						cur_frame[dropped_rows,params[[2]]]<-NA
					}
					colnames(cur_frame)[ncol(cur_frame)]<-paste0("Predicted_",params[[2]])
				}
				out[[1]]<-0
				out[[2]]<-params[[1]]
				out[[3]]<-cur_frame
			}else{
				out[[1]]<--1
				out[[2]]<-params[[1]]
				out[[3]]<-newFrame
			}
		}else{
			out<-"Possible missing data actions are: impute, drop and as is"
		}
	}else{
		out<-"Input dataset is empty"
	}
	gc()
	return(out)
}


build_tree<-function(dep_var,model,nbr){
	library(ranger)
	df<-treeInfo(model,nbr)
	term.ind<-which(df$terminal)
	n<-length(term.ind)
	if(n>0){
		max.items<-0
		result<-list()
		for(i in 1:n){
			items<-1
			myrow<-vector()
			curid<-df$nodeID[term.ind[i]]
			if(is.numeric(df$prediction[term.ind[i]])){
				value<-round(df$prediction[term.ind[i]],4)
			}else{
				value<-df$prediction[term.ind[i]]
			}
			myrow<-append(myrow,paste0(dep_var,"=",value),after=0)			
			while(length(which(curid==append(df$leftChild,df$rightChild)))>0){
				items<-items+1
				ind<-which(curid==df$leftChild)
				if(length(ind)>0){
					myrow<-append(myrow,paste0(df$splitvarName[ind],"<=",df$splitval[ind]),after=0)
					curid<-df$nodeID[ind]
				}else{
					ind<-which(curid==df$rightChild)
					myrow<-append(myrow,paste0(df$splitvarName[ind],">",df$splitval[ind]),after=0)
					curid<-df$nodeID[ind]
				}
			}
			result[[i]]<-myrow
			if(items > max.items){
				max.items<-items
			}
		}
	}
	out<-matrix(0,ncol=max.items,nrow=0)
	for(i in 1:n){
		myrow<-result[[i]]
		currow<-append(myrow,rep("",max.items-length(myrow)))
		out<-rbind(out,currow)
	}
	out<-as.matrix(cbind("root",out))
	colnames(out)<-paste("level",0:max.items)
	rownames(out)<-NULL
	gc()
	return(out)
}


mis_data_action_rf<-function(in_df,attrs,mis_data){
# Performs missing data action
# Args
# in_df - a dataframe to perform missing data action on
# attrs - a list of column names of df to perform missing data action
# mis_data - missing data action to perform (possible options are "impute","drop","as_is")
	ACTIONS<-c("impute","drop","as_is")
	row_id<-paste(sample(c(1:9,letters,LETTERS),15),collapse="")
	
	# add row index
	in_df[[row_id]]<-seq(nrow(in_df))
	attrs<-append(attrs,row_id)
	# subset of required columns
	cols<-which(colnames(in_df) %in% attrs)
	cmd<-paste0('df<-in_df[,',paste0('c(',paste(cols,collapse=','),')'),']')
	eval(parse(text=cmd))
	
	valid_rows<-vector()
	# handle missing data per user request
	if(tolower(mis_data)==ACTIONS[1]){
		df<-impute_data(df,attrs)
		# replace part of the dataframe with imputed data
		df<-replace_columns(in_df,df,row_id)
	}else if(tolower(mis_data)==ACTIONS[2]){
		df[df=="null" | df=="NULL"]<-NA
		df<-df[complete.cases(df),]
		if(nrow(in_df) > nrow(df)){
			valid_rows<-which(in_df[[row_id]] %in% df[[row_id]])
		}
	}else{
		# replace part of the dataframe with imputed data
		df<-in_df
	}
	# drop temporary column row_id
	ind<-which(colnames(df)==row_id)
	cmd<-paste0("df<-df[,-",ind,"]")
	eval(parse(text=cmd))
	# assemble output
	out<-list()
	out[[1]]<-df
	out[[2]]<-valid_rows
	
	gc()
	return(out)
}










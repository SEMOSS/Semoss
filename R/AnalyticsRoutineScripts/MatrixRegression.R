
getRegressionCoefficientsFromScript <- function(rScript, predictionCol) {


# create the model
model <- eval(parse(text=rScript))
modelTable <- as.data.table(coef(model), keep.rownames = TRUE)
names(modelTable) <- c("Column Header", "Coefficient")

# get residuals vs fitted

Actual <- predictionCol
Fitted <- predict(model)

values <- data.frame(Actual,Fitted)

l <- list(modelTable, values )

# the list that we are returning as two objects - the first is our model table and the second is the table of values for residuals vs fitted 
return(l)
}





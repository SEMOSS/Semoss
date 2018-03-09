editRules <- function(df, E) {
# get constraints in frame
editFrame <- as.data.frame(E)
editNames <- editFrame$name

# check violations
ve <- violatedEdits(E, df)

# get subset of df that violates the conditions
veFrame <- as.data.frame(ve)
subsetScript <- 'subset <- veFrame[which('
subsetScriptCondition <- ""
for(i in 1:nrow(editFrame)) {
	col <- editNames[i]
	subsetScriptCondition <- paste(subsetScriptCondition, "veFrame$", col, "== TRUE ", sep="") 
	if(i < nrow(editFrame)) {
	subsetScriptCondition <- paste(subsetScriptCondition, " | ")
	}
}
subsetScript <- paste(subsetScript, subsetScriptCondition, "),]", sep="")
eval(parse(text=subsetScript))
return (subset)
}
package prerna.algorithm.impl;

import java.util.List;

import prerna.ds.R.RSyntaxHelper;

public class RCountReactor extends RBaseReducer {

	/*
	 * This class is not like the other math reactors
	 * This is because the execution of the method 
	 * does not follow the same format as count requires 2 function calls
	 */
	
	public RCountReactor() {
		
	}

	public String process(String tableName, String column) {
		// generate a string similar to 
		// datatable[, sum(na.omit(distance))]
		StringBuilder builder = new StringBuilder();
		builder.append(tableName).append("[ , as.numeric(length(unique(na.omit(").append(column).append("))))]");
		return builder.toString();
	}
	
	public String processGroupBy(String tableName, String column, List<String> groupByCols) {
		// generate a string similar to 
		// datatable[, sum(na.omit(distance)), by =c("year")]
		StringBuilder builder = new StringBuilder();
		builder.append(tableName).append("[ , as.numeric(length(unique(na.omit(").append(column).append(")))), by = ")
			.append( RSyntaxHelper.createStringRColVec(groupByCols.toArray()) ).append(" ]");
		return builder.toString();
	}

}

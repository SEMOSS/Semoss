package prerna.algorithm.impl;

import java.util.List;

public class SqlBasicMathReactor extends SqlBaseReducer {

	protected String mathRoutine = null;
	
	public String process(String tableName, String script) {
		// generate a string similar to 
		// select distinct mathRoutine ( col_name * 2 ) from tableName
		StringBuilder builder = new StringBuilder();
		builder.append("SELECT DISTINCT ").append(mathRoutine).append("(").append(script).append(")").append(" FROM ").append(tableName);		 
		return builder.toString();
	}
	
	public String processGroupBy(String tableName, String script, List<String> groupByCols) {
		// generate a string similar to 
		// select distinct mathRoutine ( col_name * 2 ) groupby1 groupby2 from tableName group by groupby1 groupby2 
		StringBuilder groupBuilder = new StringBuilder();
		for(String groupBy : groupByCols) {
			if(groupBuilder.length() == 0) {
				groupBuilder.append(groupBy);
			} else {
				groupBuilder.append(" , ").append(groupBy);
			}
		}
		String groupByStrings = groupBuilder.toString();
		
		StringBuilder builder = new StringBuilder();
		builder.append("SELECT DISTINCT ").append(mathRoutine).append("(").append(script).append(") , ").append(groupByStrings)
			.append(" FROM ").append(tableName).append(" GROUP BY ").append(groupByStrings);
		return builder.toString();
	}
	
	public void setMathRoutine(String mathRoutine) {
		this.mathRoutine = mathRoutine;
	}
}
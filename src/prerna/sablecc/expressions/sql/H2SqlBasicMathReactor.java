package prerna.sablecc.expressions.sql;

import prerna.ds.H2.H2Frame;
import prerna.util.Utility;

public class H2SqlBasicMathReactor extends AbstractSqlBaseReducer {

	protected String mathRoutine = null;
	
	public String process(H2Frame frame, String script) {
		// generate a string similar to 
		// select distinct mathRoutine ( col_name * 2 ) from tableName
		StringBuilder builder = new StringBuilder();
		builder.append("SELECT DISTINCT ").append(mathRoutine).append("(").append(script).append(")").append(" FROM ").append(frame.getTableName());
		String filters = frame.getSqlFilter();
		if(filters != null && !filters.isEmpty()) {
			builder.append(" ").append(filters);
		}
		return builder.toString();
	}
	
	public H2SqlExpressionIterator processGroupBy(H2Frame frame, String script, String[] groupByCols) {
		String newCol = "EXPRESSION_" + Utility.getRandomString(6);
		String newScript = mathRoutine + "(" + script + ")";
		H2SqlExpressionIterator it = new H2SqlExpressionIterator(frame, newScript, newCol, null, groupByCols);
		return it;
//		// generate a string similar to 
//		// select distinct mathRoutine ( col_name * 2 ) groupby1 groupby2 from tableName group by groupby1 groupby2 
//		StringBuilder groupBuilder = new StringBuilder();
//		for(String groupBy : groupByCols) {
//			if(groupBuilder.length() == 0) {
//				groupBuilder.append(groupBy);
//			} else {
//				groupBuilder.append(" , ").append(groupBy);
//			}
//		}
//		String groupByStrings = groupBuilder.toString();
//		
//		StringBuilder builder = new StringBuilder();
//		builder.append("SELECT DISTINCT ").append(mathRoutine).append("(").append(script).append(") , ").append(groupByStrings)
//			.append(" FROM ").append(tableName);
//		if(filters != null && !filters.isEmpty()) {
//			builder.append(" ").append(filters);
//		}
//		builder.append(" GROUP BY ").append(groupByStrings);
//		return builder.toString();
	}
	
	public void setMathRoutine(String mathRoutine) {
		this.mathRoutine = mathRoutine;
	}
}
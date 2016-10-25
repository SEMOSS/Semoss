package prerna.sablecc.expressions.sql;

import prerna.ds.H2.H2Frame;
import prerna.util.Utility;

public class H2SqlBasicMathReactor extends AbstractSqlBaseReducer {

	protected String mathRoutine = null;
	
	public String process(H2Frame frame, String script) {
		// generate a string similar to 
		// select distinct mathRoutine ( col_name * 2 ) from tableName
		StringBuilder builder = new StringBuilder();
		
		// changing the name to be using the view name
		String colName = frame.getTableColumnName(script);
		if(colName != null) {
			script = colName;
		}
		
		builder.append("SELECT DISTINCT ").append(mathRoutine).append("(").append(script).append(")").append(" FROM ");
		if(frame.isJoined()) {
			builder.append(frame.getViewTableName());
		} else {
			builder.append(frame.getTableName());
		}
		
		String filters = frame.getSqlFilter();
		if(filters != null && !filters.isEmpty()) {
			builder.append(" ").append(filters);
		}
		return builder.toString();
	}
	
	public H2SqlExpressionIterator processGroupBy(H2Frame frame, String script, String[] groupByCols) {
		String newCol = "EXPRESSION_" + Utility.getRandomString(6);
		String colName = frame.getTableColumnName(script.trim());
		if(colName != null) script = colName;
		String newScript = mathRoutine + "(" + script + ")";
		H2SqlExpressionIterator it = new H2SqlExpressionIterator(frame, newScript, newCol, null, groupByCols);
		return it;
	}
	
	public void setMathRoutine(String mathRoutine) {
		this.mathRoutine = mathRoutine;
	}
}
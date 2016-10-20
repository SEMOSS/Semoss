package prerna.sablecc.expressions.r;

import java.util.List;

import prerna.ds.R.RSyntaxHelper;

public abstract class RBasicMathReactor extends AbstractRBaseReducer {

	protected String mathRoutine = null;
	
	public String process(String tableName, String column) {
		// generate a string similar to 
		// datatable[, sum(na.omit(distance))]
		StringBuilder builder = new StringBuilder();
		builder.append(tableName).append("[ , as.numeric(").append(mathRoutine).append("(na.omit(").append(column).append(")))]");
		return builder.toString();
	}
	
	public String processGroupBy(String tableName, String column, List<String> groupByCols) {
		// generate a string similar to 
		// datatable[, sum(na.omit(distance)), by =c("year")]
		StringBuilder builder = new StringBuilder();
		builder.append(tableName).append("[ , as.numeric(").append(mathRoutine).append("(na.omit(").append(column).append("))), by = ")
			.append( RSyntaxHelper.createStringRColVec(groupByCols.toArray()) ).append(" ]");
		return builder.toString();
	}
	
	public void setMathRoutine(String mathRoutine) {
		this.mathRoutine = mathRoutine;
	}
}

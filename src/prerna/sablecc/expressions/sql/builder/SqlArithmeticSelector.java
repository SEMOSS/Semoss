package prerna.sablecc.expressions.sql.builder;

import java.util.List;
import java.util.Vector;

public class SqlArithmeticSelector implements ISqlSelector{

	private ISqlSelector leftObj;
	private ISqlSelector rightObj;
	private String arithmetic;
	
	/*
	 * Perform arithmetic between a set of selectors
	 */
	
	public SqlArithmeticSelector(ISqlSelector leftObj, ISqlSelector rightObj, String arithmetic) {
		this.leftObj = leftObj;
		this.rightObj = rightObj;
		this.arithmetic = arithmetic;
	}

	@Override
	public List<String> getTableColumns() {
		List<String> tableColumns = new Vector<String>();
		if(leftObj instanceof ISqlSelector) {
			tableColumns.addAll( ((ISqlSelector) leftObj).getTableColumns() );
		}
		if(rightObj instanceof ISqlSelector) {
			tableColumns.addAll( ((ISqlSelector) rightObj).getTableColumns() );
		}
		return tableColumns;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(leftObj.toString()).append(" ").append(arithmetic).append(" ").append(rightObj.toString());
		return builder.toString();
	}
	
}

package prerna.sablecc.expressions.r.builder;

import java.util.List;
import java.util.Vector;

public class RArithmeticSelector implements IRExpressionSelector{

	private IRExpressionSelector leftObj;
	private IRExpressionSelector rightObj;
	private String arithmetic;
	
	/*
	 * Perform arithmetic between a set of selectors
	 */
	
	public RArithmeticSelector(IRExpressionSelector leftObj, IRExpressionSelector rightObj, String arithmetic) {
		this.leftObj = leftObj;
		this.rightObj = rightObj;
		this.arithmetic = arithmetic;
	}

	@Override
	public List<String> getTableColumns() {
		List<String> tableColumns = new Vector<String>();
		if(leftObj instanceof IRExpressionSelector) {
			tableColumns.addAll( ((IRExpressionSelector) leftObj).getTableColumns() );
		}
		if(rightObj instanceof IRExpressionSelector) {
			tableColumns.addAll( ((IRExpressionSelector) rightObj).getTableColumns() );
		}
		return tableColumns;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(leftObj.toString()).append(" ").append(arithmetic).append(" ").append(rightObj.toString());
		return builder.toString();
	}

	@Override
	public String getName() {
		StringBuilder builder = new StringBuilder();
		builder.append(leftObj.getName()).append("_").append(arithmetic).append("_").append(rightObj.getName());
		return builder.toString();
	}
	
}

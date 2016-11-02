package prerna.sablecc.expressions.r.builder;

import java.util.List;
import java.util.Vector;

import prerna.sablecc.expressions.IExpressionSelector;

public class RArithmeticSelector implements IExpressionSelector{

	private IExpressionSelector leftObj;
	private IExpressionSelector rightObj;
	private String arithmetic;
	
	/*
	 * Perform arithmetic between a set of selectors
	 */
	
	public RArithmeticSelector(IExpressionSelector leftObj, IExpressionSelector rightObj, String arithmetic) {
		this.leftObj = leftObj;
		this.rightObj = rightObj;
		this.arithmetic = arithmetic;
	}

	@Override
	public List<String> getTableColumns() {
		List<String> tableColumns = new Vector<String>();
		if(leftObj instanceof IExpressionSelector) {
			tableColumns.addAll( ((IExpressionSelector) leftObj).getTableColumns() );
		}
		if(rightObj instanceof IExpressionSelector) {
			tableColumns.addAll( ((IExpressionSelector) rightObj).getTableColumns() );
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

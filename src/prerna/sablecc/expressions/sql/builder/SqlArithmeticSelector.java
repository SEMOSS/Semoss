package prerna.sablecc.expressions.sql.builder;

import java.util.List;
import java.util.Vector;

import prerna.sablecc.expressions.IExpressionSelector;

public class SqlArithmeticSelector implements IExpressionSelector{

	private IExpressionSelector leftObj;
	private IExpressionSelector rightObj;
	private String arithmetic;
	
	/*
	 * Perform arithmetic between a set of selectors
	 */
	
	public SqlArithmeticSelector(IExpressionSelector leftObj, IExpressionSelector rightObj, String arithmetic) {
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
		builder.append(leftObj.toString()).append(" ").append(arithmetic);
		
		//We want to handle the case of dividing by 0, so accounting for this case by creating query
		// leftObj / NULLIF(rightObj, 0)
		if(arithmetic.equals("/") || arithmetic.equals("\\")) {
			builder.append("NULLIF(").append(rightObj.toString()).append(",0)");
		} else {
			builder.append(" ").append(rightObj.toString());
		}
		return builder.toString();
	}

	@Override
	public String getName() {
		String arithmeticName = null;
		if(arithmetic.equals("+")) {
			arithmeticName = "Plus";
		} else if(arithmetic.equals("-")) {
			arithmeticName = "Minus";
		} else if(arithmetic.equals("/") || arithmetic.equals("\\") ) {
			arithmeticName = "Divide";
		} else if(arithmetic.equals("*")) {
			arithmeticName = "Multiply";
		} else if(arithmetic.equals("%")) {
			arithmeticName = "Mod";
		}
		
		StringBuilder builder = new StringBuilder();
		builder.append(leftObj.getName()).append("_").append(arithmeticName).append("_").append(rightObj.getName());
		return builder.toString();
	}
	
}

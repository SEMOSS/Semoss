package prerna.query.querystruct;

import net.sf.jsqlparser.statement.select.FromItem;

// set operation is typically used for 

public class OpExpression extends SelectQueryStruct
{
	
	boolean composite = false;
	
	Object rightItem = null;
	Object leftItem = null;
	
	String rightExpr = null;
	String leftExpr = null;
	String on = null;
	
	String expression = null;
	SelectQueryStruct item = null;
	String comparator = null;
	
	public void setRightExpresion(Object rightItem)
	{
		this.rightItem = rightItem;
	}
	
	public void setLeftExpresion(Object leftItem)
	{
		this.leftItem = leftItem;
	}

	public void setExpression(String expression)
	{
		this.expression = expression;
	}
	
	public void setFromItem(SelectQueryStruct item)
	{
		this.item = item;
	}
	
	public void setComposite(boolean composite)
	{
		this.composite = composite;
	}
	
	public void setLeftExpr(String expr)
	{
		this.leftExpr = expr;
	}

	public void setRightExpr(String expr)
	{
		this.rightExpr = expr;
	}

	public void setOn(String on)
	{
		this.expression = on;
	}
	
	

}

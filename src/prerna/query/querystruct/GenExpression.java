package prerna.query.querystruct;

import java.util.List;

import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;

public class GenExpression extends SelectQueryStruct implements IQuerySelector {
	
	// if composite is set then you basically have 2 sides
	// if not just one side
	boolean composite = false;
	
	public boolean recursive = false; // ((a+b)+c)*d) - 2 values - tree out of the expression
	public boolean telescope = false; // join - <- within it join join (select a, b,c from d) a - GenExpression1(telescope = true, alias=a).body(GenExpression2(select a, b,c from d))
	
	public Object rightItem = null; // rightitem - GenExpression / Value / Scalar etc. 
	public Object leftItem = null; // left item blah blah
	
	
	String rightAlias = null;
	String rightExpr = null;
	
	String leftAlias = null; // used all the time
	String leftExpr = null; // left expression - string of the sql being used
	String on = null;
	
	String expression = null; // expression
	SelectQueryStruct item = null;
	
	String alias = null;
	public String aQuery = null;
	
	public boolean paranthesis = false;
	
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
	
	public void setLeftAlias(String leftAlias)
	{
		this.leftAlias = leftAlias;
	}

	public String getLeftAlias()
	{
		return leftAlias;
	}
	
	public void setOn(String on)
	{
		this.expression = on;
		this.on = on;
	}
	
	public void setOperation(String operation)
	{
		this.operation = operation;
	}
	
	public String getLeftExpr()
	{
		return this.leftExpr;
	}

	public String getOperation()
	{
		return this.operation;
	}

	@Override
	public SELECTOR_TYPE getSelectorType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getAlias() {
		// TODO Auto-generated method stub
		return alias;
	}

	@Override
	public void setAlias(String alias) {
		// TODO Auto-generated method stub
		this.alias = alias;
	}

	@Override
	public boolean isDerived() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String getDataType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getQueryStructName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<QueryColumnSelector> getAllQueryColumns() {
		// TODO Auto-generated method stub
		return null;
	}
	
	public String toString()
	{
		return aQuery;
	}

	
	public StringBuffer printQS(GenExpression qs, StringBuffer buf)
	{
		// if the type is join.. you need to do other things
		//System.err.println("Processing  " + qs.aQuery + " <>" + qs.expression + "<>" + qs.operation);
		if(buf == null)
			buf = new StringBuffer();
		boolean processed = false;
		if(qs.operation != null)
		{
			if(qs.operation.equalsIgnoreCase("select") || qs.operation.equalsIgnoreCase("querystruct"))
			{
				buf.append("\n");
				buf.append("SELECT  ");
				for(int selIndex = 0;selIndex < qs.nselectors.size();selIndex++)
				{
					GenExpression sqs = qs.nselectors.get(selIndex);
					
					// need to handle telescope
					
					if(selIndex > 0)
						buf.append(",");
					
					if(sqs.operation != null && sqs.operation.equalsIgnoreCase("querystruct"))		
					{
						buf.append("(");
					}
					printQS(sqs, buf);
					if(sqs.operation != null && sqs.operation.equalsIgnoreCase("querystruct"))		
					{
						buf.append(")");
					}
					/*// if it is a column I need to put alias too
					else if(sqs.operation.equalsIgnoreCase("column") || sqs.operation.equalsIgnoreCase("double") || sqs.operation.equalsIgnoreCase("date") || sqs.operation.equalsIgnoreCase("time") || sqs.operation.equalsIgnoreCase("string") )
					{
						buf.append(sqs.leftExpr);
						if(sqs.leftAlias != null)
							buf.append(" as ").append(sqs.leftAlias);
					}
					// need to handle the union, case, between
					
					// add the alias
					if(sqs.alias != null)
						buf.append("as " + sqs.alias);
					*/
				}
				processed = true;
			}
			else if(qs.operation.equalsIgnoreCase("column"))
			{
				buf.append(qs.leftExpr);
				if(qs.leftAlias != null && qs.leftAlias.length() > 0)
					buf.append(" as ").append(qs.leftAlias);
				processed = true;
			}
			else if(qs.operation.equalsIgnoreCase("double") || qs.operation.equalsIgnoreCase("date") || qs.operation.equalsIgnoreCase("time") || qs.operation.equalsIgnoreCase("string") || qs.operation.equalsIgnoreCase("long"))
			{
				buf.append(qs.leftItem);
				if(qs.leftAlias != null && qs.leftAlias.length() > 0)
					buf.append(" as ").append(qs.leftAlias);
				processed = true;
			}
			else if(qs.operation.equalsIgnoreCase("opaque"))
			{
				buf.append(qs.getLeftExpr());
				if(qs.leftAlias != null && qs.leftAlias.length() > 0)
					buf.append(" as ").append(qs.leftAlias);
				processed = true;
			}
			else if(qs.operation.equalsIgnoreCase("from") && !qs.composite)
			{
				buf.append(qs.getLeftExpr());
				if(qs.leftAlias != null && qs.leftAlias.length() > 0)
					buf.append(" as ").append(qs.leftAlias);
				processed = true;
			}
			else if(qs.composite)
			{
				//System.err.println(" hmm.. from is composite.. but simple ?  " + qs.aQuery);
			}
		}
		if(qs.operation != null && qs.operation.contains("union"))
		{
			//System.err.println("And now we are getting the union " + qs);
			// process the left and right
			// 
			OperationExpression opex = (OperationExpression)qs;
			List <GenExpression> operands = opex.operands;
			List <String> opNames = opex.opNames;
			
			for(int opIndex = 0;opIndex < opNames.size();opIndex++)
			{
				if(opIndex == 0)
					printQS((GenExpression)operands.get(opIndex), buf);
				buf.append("  ").append(opNames.get(opIndex)).append("  ");
				printQS((GenExpression)operands.get(opIndex+1), buf);
			}
			
			processed = true;
		}
		
		if(qs.operation != null && qs.operation.equalsIgnoreCase("between"))
		{
			processed = true;
			buf.append("  "); 
			printQS((GenExpression)qs.body, buf);
			buf.append("  BETWEEN ");
			printQS((GenExpression)qs.leftItem, buf);
			buf.append("  AND  ");
			printQS((GenExpression)qs.rightItem, buf);
			processed = true;
			
		}
		// need to handle telescope

		if(!processed)
		{
			if(qs.recursive)
			{
				printQSRecursive(qs, buf);
			}
			else if(qs instanceof WhenExpression)
			{
				buf.append(((WhenExpression)qs).printOutput());
			}
			else if(qs.leftItem != null && qs.rightItem != null) // this is expression
			{
				// dont know how to handle this yet
				// fun stuff we are going to go into a recursion again
				Object leftItem = (GenExpression)qs.leftItem;
				Object rightItem = (GenExpression)qs.rightItem;
				
				if(leftItem instanceof GenExpression && ((GenExpression)leftItem).paranthesis)
					buf.append("(");
				// this is where we need to do the paranthesis again I think
				printQS((GenExpression)qs.leftItem, buf);
				if(leftItem instanceof GenExpression && ((GenExpression)leftItem).paranthesis)
					buf.append(")");
				buf.append(qs.operation);

				if(rightItem instanceof GenExpression && ((GenExpression)rightItem).paranthesis)
					buf.append("(");
				printQS((GenExpression)qs.rightItem, buf);
				if(rightItem instanceof GenExpression && ((GenExpression)rightItem).paranthesis)
					buf.append(")");

			}
			else if(qs.telescope )
			{
				buf.append("(");
				printQS((GenExpression)qs.body, buf);
				buf.append(")");
				
				if(qs.leftAlias != null && qs.leftAlias.length() > 0)
					buf.append(" ").append(qs.leftAlias);				
			}
			else if(qs.operation == null)
			{			
				//System.err.println("Dont know to handle " + qs.aQuery + "<> " + qs.operation);
			}
		}
		// like the filter

		// add the from
		if(qs.from != null)
		{
			// put the paranthesis if it is another select 
			//if(qs.from.operation != null && qs.from.operation.equalsIgnoreCase("querystruct"))
			{
				//System.err.println("From operation is set to " + qs.from.operation + " composite ?" + qs.from.composite );
				buf.append("\n");
				buf.append("  FROM " );
				if(qs.from.composite)
					buf.append("( ");
				
				printQS(qs.from, buf);
				
				if(qs.from.composite)
				{
					buf.append(") ");
				
					if(qs.from.getLeftAlias() != null)
						buf.append("AS " + qs.from.getLeftAlias());
				}
			}
			/*
			else
			{
				buf.append("  FROM " );
				if(qs.from.leftExpr != null)
					buf.append(qs.from.leftExpr).append("  ");
				if(qs.from.leftAlias != null)
					buf.append(qs.from.leftAlias).append(" ");
			}*/
		}
		

		// add the joins finally			
		for(int joinIndex = 0;joinIndex < qs.joins.size();joinIndex++)
		{
			//System.err.println("Selector Buf so far " + buf);
			GenExpression sqs = qs.joins.get(joinIndex);	
			buf.append("  ");
			String joinType = sqs.on;
			String open = "";
			String close = "";
			// I also need to pick the from here
			// this is is the inner join on <from>
			buf.append("\n");
			buf.append(sqs.on);
			buf.append("  ");
			// how to tell if a join is a subjoin ?
			if(sqs.from != null && sqs.from.composite)
				buf.append("(");
			printQS(sqs.from, buf);
			if(sqs.from != null && sqs.from.composite)
				buf.append(")");
			if(sqs.from.leftAlias != null)
				buf.append(sqs.from.leftAlias);
			buf.append("  on ");
			if(sqs.body != null && sqs.body.operation.equalsIgnoreCase("querystruct"))
			{
				open = "(";
				close = ")  ";
			}
			// process this as a query struct
			buf.append(open);
			printQS(sqs.body, buf);
			buf.append(close);
		}

		// add the where
		if(qs.filter != null)
		{
			buf.append("\n");
			buf.append("  WHERE " );
			printQS(qs.filter, buf);
		}
		
		
		// add the groupby
		if(qs.ngroupBy.size() > 0)
		{
			buf.append("  GROUP BY " );
			for(int groupIndex = 0;groupIndex < qs.ngroupBy.size();groupIndex++)
			{
				if(groupIndex != 0)
					buf = buf.append(" , ");
				GenExpression gep = qs.ngroupBy.get(groupIndex);

				if(gep.composite)
					buf.append("(");
				printQS(gep, buf);
				if(gep.composite)
					buf.append(")");
				
			}
		}

		// add the order
		if(qs.norderBy.size() > 0)
		{
			buf.append("  ORDER BY " );
			for(int orderIndex = 0;orderIndex < qs.norderBy.size();orderIndex++)
			{
				if(orderIndex != 0)
					buf = buf.append(" , ");
				GenExpression gep = qs.norderBy.get(orderIndex).body;

				if(gep.composite)
					buf.append("(");
				printQS(gep, buf);
				if(gep.composite)
					buf.append(")");
				buf.append("  ");
				String direction = ((OrderByExpression)qs.norderBy.get(orderIndex)).direction;
				if(direction.length() > 0)
					buf.append(direction);
			}
		}
		
		// limit and offset
		if(qs.limit != -1)
			buf.append(" LIMIT ").append(qs.limit);

		if(qs.offset != -1)
			buf.append(" OFFSET ").append(qs.offset);

		
		return buf;
	}
		
	public StringBuffer printQSRecursive(GenExpression qs, StringBuffer buf)
	{
		Object leftItem = qs.leftItem;
		Object rightItem = qs.rightItem;
		
		if(leftItem instanceof GenExpression)
		{
			// need to know if I need to put a paranthesis around this or not
			Object childLeftItem = ((GenExpression)leftItem).leftItem;
			Object childRightItem = ((GenExpression)leftItem).leftItem;
			
			StringBuffer leftBuf = printQS((GenExpression)leftItem, new StringBuffer());
			
			if((childLeftItem != null && ((GenExpression)childLeftItem).composite) && (childRightItem != null && ((GenExpression)childRightItem).composite))
			{
				buf.append("(");
				buf.append(leftBuf);
				buf.append(")");
			}
			else
				buf.append(leftBuf).append(" ");
		}
		else 
			buf.append(qs.leftItem).append(" ");
		
		if(qs.operation != null)
			buf.append(" ").append(qs.operation).append(" ");
		
		if(rightItem instanceof GenExpression)
		{
			// need to know if I need to put a paranthesis around this or not
			Object childLeftItem = ((GenExpression)rightItem).leftItem;
			Object childRightItem = ((GenExpression)rightItem).rightItem;
			
			StringBuffer rightBuf = printQS((GenExpression)rightItem, new StringBuffer());
			
			if((childLeftItem != null && childLeftItem instanceof GenExpression && ((GenExpression)childLeftItem).composite) && (childRightItem != null && childLeftItem instanceof GenExpression && ((GenExpression)childRightItem).composite))
			{
				buf.append("(");
				buf.append(rightBuf);
				buf.append(")");
			}
			else
				buf.append(rightBuf).append(" ");
		}
		else 
			buf.append(qs.rightItem).append(" ");
		return buf;
	}
	


}

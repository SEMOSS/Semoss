package prerna.query.querystruct;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;

public class GenExpression extends SelectQueryStruct implements IQuerySelector, Serializable {
	
	// if composite is set then you basically have 2 sides
	// if not just one side
	public boolean composite = false;
	public GenExpression parent = null;
	
	public boolean recursive = false; // ((a+b)+c)*d) - 2 values - tree out of the expression
	public boolean telescope = false; // join - <- within it join join (select a, b,c from d) a - GenExpression1(telescope = true, alias=a).body(GenExpression2(select a, b,c from d))
	
	public Object rightItem = null; // rightitem - GenExpression / Value / Scalar etc. 
	public Object leftItem = null; // left item blah blah
	
	// put the table name as well
	public String tableName = null;
	public String tableAlias = null;
	
	String rightAlias = null;
	String rightExpr = null;
	
	public String leftAlias = null; // used all the time
	String leftExpr = null; // left expression - string of the sql being used
	String on = null;
	
	String expression = null; // expression
	SelectQueryStruct item = null;
	
	String alias = null;
	public String aQuery = null;
	public boolean neutralize = false; // removes this from the overall string when printed
	
	public boolean paranthesis = false;
	
	public String userTableName = null;
	public String userTableAlias = null;
	
	public boolean distinct = false;

	public List<String> withFrom = null;
	public List<GenExpression> withList = null;
	
	public void setRightExpresion(Object rightItem)
	{
		this.rightItem = rightItem;
		if(rightItem instanceof GenExpression)
			((GenExpression)rightItem).parent = this;
		
	}
	
	public void setLeftExpresion(Object leftItem)
	{
		this.leftItem = leftItem;
		if(leftItem instanceof GenExpression)
			((GenExpression)leftItem).parent = this;
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

	public void setWithFrom(List<String> withFrom) 
	{
	    this.withFrom = withFrom;
	}
	
	public List<String> getWithFrom()
	{
	    return this.withFrom;
	}
	
    public void setWithList(List<GenExpression> withList) 
    {
        this.withList = withList;
    }
    
    public List<GenExpression> getWithList()
    {
        return this.withList;
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

	
	public static StringBuffer printQS(GenExpression qs, StringBuffer buf)
	{
		// if the type is join.. you need to do other things
		//System.err.println("Processing  " + qs.aQuery + " <>" + qs.expression + "<>" + qs.operation);
		String newLine = "\n";
		if(buf == null) {
			buf = new StringBuffer();
		}
		boolean processed = false;
		
		if(qs.withFrom != null && qs.withFrom.size() > 0)
		{   
		    buf.append("WITH ");
		    for(int i=0;i<qs.withFrom.size();i++) {
				if(i>0){
					buf.append(",");
				}
		        buf.append(qs.withFrom.get(i));
		        buf.append(" AS (");
		        printQS(qs.withList.get(i),buf);
		        buf.append(") ");
		    }
		}

		if(qs != null && qs.operation != null && !qs.neutralize) {
			if(qs.operation.equalsIgnoreCase("select") || qs.operation.equalsIgnoreCase("querystruct")) {
				buf.append(newLine);
				buf.append("SELECT  ");
				if(qs.distinct) {
					buf.append(" DISTINCT ");
				}
				for(int selIndex = 0;selIndex < qs.nselectors.size();selIndex++) {
					GenExpression sqs = qs.nselectors.get(selIndex);
					// need to handle telescope
					StringBuffer newBuf = printQS(sqs, null);
					if(newBuf != null && newBuf.length() > 0) {
						if(selIndex > 0) {
							buf.append(", ");
						}
						if(sqs.operation != null && sqs.operation.equalsIgnoreCase("querystruct")) {
							buf.append("(");
						}
						buf.append(newBuf);
						if(sqs.operation != null && sqs.operation.equalsIgnoreCase("querystruct")) {
							buf.append(")");
						}
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
				String columnName = qs.leftExpr;
				//if(!columnName.startsWith("\""))
				//	columnName = "\"" + columnName + "\"";
				if(qs.paranthesis) {
					buf.append("(");
				}
				
				// table prefix on selectors necessary to disambiguate when join columns have same name
				if(qs.userTableAlias != null && !qs.userTableAlias.isEmpty()) {
					buf.append(qs.userTableAlias).append(".");
				} else if(qs.userTableName != null && !qs.userTableName.isEmpty()) {
					buf.append(qs.userTableName).append(".");
				}
//				else if(qs.tableAlias != null && !qs.tableAlias.isEmpty()) {
//					buf.append(qs.tableAlias).append(".");
//				} else if(qs.tableName != null && !qs.tableName.isEmpty()){
//					buf.append(qs.tableName).append(".");
//				}
				
				buf.append(columnName);
				if(qs.leftAlias != null && qs.leftAlias.length() > 0) {
					buf.append(" as ").append(qs.leftAlias);
				}
				if(qs.paranthesis) {
					buf.append(")");
				}
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
			else if(qs.operation.equalsIgnoreCase("allcol"))
			{
				buf.append("*");
				processed = true;
			}
			else if(qs.composite)
			{
				//System.err.println(" hmm.. from is composite.. but simple ?  " + qs.aQuery);
			}
		}
		if(qs.operation != null && qs.operation.contains("union") && !qs.neutralize)
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
		
		if(qs.operation != null && qs.operation.equalsIgnoreCase("between") && !qs.neutralize)
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
		if(qs.operation != null && qs.operation.equalsIgnoreCase("cast") && !qs.neutralize)
		{
			// name of the function is in the left alias
			buf.append("CAST ").append("(");
			if(qs.leftItem != null && qs.leftItem instanceof GenExpression) {
				printQS((GenExpression)qs.leftItem, buf);
			} else {
				buf.append(qs.leftItem);
			}
			buf.append(")");
			if(qs.leftAlias != null) {
                buf.append(" AS ").append(qs.leftAlias);
			}
			processed = true;
		}
		
		if(qs.operation != null && qs.operation.equalsIgnoreCase("function") && !qs.neutralize)
		{
			FunctionExpression thisExpr = (FunctionExpression)qs;
			if(!thisExpr.neutralizeFunction)
			{
				buf.append(thisExpr.expression).append("(");	
				if(qs.distinct)
					buf.append(" DISTINCT ");
			}
			else if(thisExpr.expressions.size() == 1)
			{
				buf.append("(");
				if(qs.distinct)
					buf.append(" DISTINCT ");
			}
			// name of the function is in the left alias
			//buf.append(qs.expression).append("(");
			List <GenExpression> parameters = thisExpr.expressions;
			for(int paramIndex = 0;paramIndex < parameters.size();paramIndex++)
			{
				if(paramIndex > 0)
					buf.append(", ");
				printQS(parameters.get(paramIndex), buf);
			}
			buf.append(")");
			if(qs.leftAlias != null)
				buf.append(" AS ").append(qs.leftAlias);
			processed = true;
		}
		if(qs.operation != null && qs.operation.equalsIgnoreCase("isnull") && !qs.neutralize)  
		{
			// name of the function is in the left alias
			printQS((GenExpression)qs.leftItem, buf);
			buf.append(" IS NULL ");
			if(qs.leftAlias != null)
				buf.append(" AS ").append(qs.leftAlias);
			processed = true;
		}
		// need to handle telescope

		if(!processed && !qs.neutralize)
		{
			if(qs.recursive)
			{
				printQSRecursive(qs, buf);
				if(qs.leftAlias != null)
					buf.append(qs.leftAlias);
			}
			else if(qs instanceof WhenExpression)
			{
				buf.append(((WhenExpression)qs).printOutput());
			}
			else if(qs instanceof InGenExpression)
			{
				InGenExpression ig = (InGenExpression)qs;
				if(qs.leftItem != null) {
					printQS((GenExpression)qs.leftItem, buf);
				}
				if(ig.isNot()) {
					buf.append("  NOT IN  ");
				} else {
					buf.append("  IN  ");
				}
				if(ig.inList.size() > 0 && ig.rightItem == null)
				{
					// process the list
					// I have made this opaque
					for(int itemIndex = 0;itemIndex < ig.inList.size();itemIndex++)
					{
						if(itemIndex != 0)
							buf.append(", ");
						StringBuffer newBuf = printQS(ig.inList.get(itemIndex), null);
						if(!newBuf.toString().startsWith("("))
							buf.append("(");
						buf.append(newBuf);
						if(!newBuf.toString().endsWith(")"))
							buf.append(")");
							
					}
					//buf.append(")");
				}
				else if(qs.rightItem != null && qs.rightItem instanceof GenExpression)
				{
					buf.append("(");
					// when you have where column1 in ( subquery ) 
					// the subquery is stored it is stored in the body
					if( ((GenExpression)qs.rightItem).telescope) {
						printQS( ((GenExpression)qs.rightItem).body, buf);
					} else {
						printQS((GenExpression)qs.rightItem, buf);
					}
					buf.append(")");
				}
				processed = true;
			}
			
			// this also needs to be neutralized
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
			else if(qs.telescope && !qs.body.neutralize)
			{
				// need to acomodate when it is neutralize
				buf.append("(");
				printQS((GenExpression)qs.body, buf);
				buf.append(")");
				
				if(qs.leftAlias != null && qs.leftAlias.length() > 0)
					buf.append(" AS ").append(qs.leftAlias);				
			}
			else if(qs.leftExpr != null) // accomodating for the paranthesis from
			{
				buf.append(qs.leftExpr);
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
				buf.append(newLine);
				buf.append("  FROM " );
				if(qs.from.composite)
					buf.append("( ");
				
				printQS(qs.from, buf);
				
				if(qs.from.composite)
				{
					buf.append(") ");
				
					if(qs.from.getLeftAlias() != null && qs.from.getLeftAlias().length() > 0)
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
			String open = "";
			String close = "";
			// I also need to pick the from here
			// this is is the inner join on <from>
			// how to tell if a join is a subjoin ?
			if(sqs.from != null && !sqs.neutralize)
			{
				buf.append(newLine);
				buf.append(sqs.on);
				buf.append("  ");
				if(sqs.from != null && sqs.from.composite)
					buf.append("(");
				printQS(sqs.from, buf);
				if(sqs.from != null && sqs.from.composite)
					buf.append(")");
				if(sqs.from.leftAlias != null)
					buf.append("  AS ").append(sqs.from.leftAlias);
			}
			if(sqs.body != null && sqs.body.operation.equalsIgnoreCase("querystruct") && !sqs.neutralize)
			{
				open = "(";
				close = ")  ";
			}
			// process this as a query struct
			if(sqs.body != null && !sqs.neutralize)
			{
				buf.append("  on ");
				buf.append(open);
				printQS(sqs.body, buf);
				buf.append(close);
			}
		}

		// add the where
		if(qs.filter != null)
		{
			StringBuffer newBuf = printQS(qs.filter, null);
			if(newBuf != null && newBuf.length() > 0)
			{
				buf.append(newLine);
				buf.append("  WHERE " );
				buf.append(newBuf);
			}
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

				if(!gep.neutralize) // accomodating if it has been removed
				{
					if(gep.composite)
						buf.append("(");
					printQS(gep, buf);
					if(gep.composite)
						buf.append(")");
				}				
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

				if(!gep.neutralize) // accomodating if it has been removed
				{
					
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
		}
		
		// limit and offset
		if(qs.limit != -1)
			buf.append(" LIMIT ").append(qs.limit);

		if(qs.offset != -1)
			buf.append(" OFFSET ").append(qs.offset);

		
		return buf;
	}
		
	public static StringBuffer printQSRecursive(GenExpression qs, StringBuffer buf)
	{
		Object leftItem = qs.leftItem;
		Object rightItem = qs.rightItem;
		
		String operation = qs.operation;
		
		// Account for when it is an or and both left item and right item are neutralized
		//if(leftItem != null && leftItem instanceof GenExpression && ((GenExpression)leftItem).neutralize && rightItem != null && rightItem instanceof GenExpression && ((GenExpression)rightItem).neutralize)
		//	return buf.append("(1 = 1) ");
		
		if(leftItem instanceof GenExpression)
		{
			if(!((GenExpression)leftItem).neutralize)
			{
				// need to know if I need to put a paranthesis around this or not
				Object childLeftItem = ((GenExpression)leftItem).leftItem;
				Object childRightItem = ((GenExpression)leftItem).rightItem;
				
				StringBuffer leftBuf = printQS((GenExpression)leftItem, new StringBuffer()); // need to account for neutralize here

				if(leftBuf.length() > 0)
				{
					if((childLeftItem != null && childLeftItem instanceof GenExpression && ((GenExpression)childLeftItem).composite) 
							&& (childRightItem != null && childRightItem instanceof GenExpression && ((GenExpression)childRightItem).composite))
					{
						buf.append("(");
						buf.append(leftBuf);
						buf.append(")");
					}
					else
						buf.append(leftBuf).append(" ");
				}
				else if(operation.equalsIgnoreCase("and"))
				{
					// get the "and" gen expression to be added
					// True and True = True
					// False and True = false;
					buf.append("1 = 1");
				}
				else if(operation.equalsIgnoreCase("or"))
				{
					// get the "or" gen expression to be added
					// True or False = True
					// False or False = False
					buf.append("1 = 0");
				}
			}
		}
		else 
			buf.append(qs.leftItem).append(" ");
		
		if(operation != null)
			buf.append(" ").append(operation).append(" ");
		
		if(rightItem instanceof GenExpression)
		{
			if(!((GenExpression)rightItem).neutralize)
			{
			// need to know if I need to put a paranthesis around this or not
				Object childLeftItem = ((GenExpression)rightItem).leftItem;
				Object childRightItem = ((GenExpression)rightItem).rightItem;
				
				StringBuffer rightBuf = printQS((GenExpression)rightItem, new StringBuffer());
				
				if(rightBuf.length() > 0)
				{
					if((childLeftItem != null && childLeftItem instanceof GenExpression && ((GenExpression)childLeftItem).composite) && (childRightItem != null && childLeftItem instanceof GenExpression && ((GenExpression)childRightItem).composite))
					{
						buf.append("(");
						buf.append(rightBuf);
						buf.append(")");
					}
					else
						buf.append(rightBuf).append(" ");
				}
				else if(operation.equalsIgnoreCase("and"))
				{
					// get the "and" gen expression to be added
					// True and True = True
					// False and True = false;
					buf.append("1 = 1 ");
				}
				else if(operation.equalsIgnoreCase("or"))
				{
					// get the "or" gen expression to be added
					// True or False = True
					// False or False = False
					buf.append("1 = 0 ");
				}
			}
		}
		else 
			buf.append(qs.rightItem).append(" ");
		return buf;
	}
	
//	public void replaceTableAlias(GenExpression gep, String oldName, String newName)
//	{
//		// if this is an operation
//		if(gep.operation.equalsIgnoreCase("column"))
//			gep.tableName = newName;
//		
//		// operation
//		else if(gep.leftItem != null && gep.rightItem != null)
//		{
//			if(gep.leftItem instanceof GenExpression)
//				replaceTableAlias((GenExpression)gep.leftItem, oldName, newName);
//			if(gep.rightItem instanceof GenExpression)
//				replaceTableAlias((GenExpression)gep.rightItem, oldName, newName);
//		}
//	}
	
	
	// recursively goes through and replaces column table name
	
	public void replaceTableAlias2(GenExpression qs, String oldName, String newName)
	{
		// if the type is join.. you need to do other things
		//System.err.println("Processing  " + qs.aQuery + " <>" + qs.expression + "<>" + qs.operation);
		boolean processed = false;
		if(qs != null && qs.operation != null)
		{
			if(qs.operation.equalsIgnoreCase("select") || qs.operation.equalsIgnoreCase("querystruct"))
			{
				for(int selIndex = 0;selIndex < qs.nselectors.size();selIndex++)
				{
					GenExpression sqs = qs.nselectors.get(selIndex);					
					replaceTableAlias2(sqs, oldName, newName);
				}
				processed = true;
			}
			if(qs.operation.equalsIgnoreCase("column"))
			{
				if(qs.tableName != null && oldName!= null &&qs.tableName.equalsIgnoreCase(oldName))
					qs.tableName = newName;
				else if(oldName == null)
					qs.tableName = newName;
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
					replaceTableAlias2((GenExpression)operands.get(opIndex), oldName, newName);
				replaceTableAlias2((GenExpression)operands.get(opIndex+1), oldName, newName);
			}
			processed = true;
		}
		
		if(qs.operation != null && qs.operation.equalsIgnoreCase("between"))
		{
			processed = true;
			replaceTableAlias2((GenExpression)qs.body, oldName, newName);
			replaceTableAlias2((GenExpression)qs.leftItem, oldName, newName);
			replaceTableAlias2((GenExpression)qs.rightItem, oldName, newName);
			processed = true;
			
		}
		if(qs.operation != null && qs.operation.equalsIgnoreCase("cast"))
		{
			// name of the function is in the left alias
			if(qs.leftItem != null && qs.leftItem instanceof GenExpression)
				replaceTableAlias2((GenExpression)qs.leftItem, oldName, newName);
			processed = true;
		}
		
		if(qs.operation != null && qs.operation.equalsIgnoreCase("function"))
		{
			// name of the function is in the left alias
			FunctionExpression thisExpr = (FunctionExpression)qs;
			List <GenExpression> parameters = thisExpr.expressions;
			for(int paramIndex = 0;paramIndex < parameters.size();paramIndex++)
			{
				replaceTableAlias2(parameters.get(paramIndex), oldName, newName);
			}
			processed = true;
		}
		if(qs.operation != null && qs.operation.equalsIgnoreCase("isnull"))
		{
			// name of the function is in the left alias
			replaceTableAlias2((GenExpression)qs.leftItem, oldName, newName);
			processed = true;
		}

		// need to handle telescope

		if(!processed)
		{
			if(qs.recursive)
			{
				Object leftItem = qs.leftItem;
				Object rightItem = qs.rightItem;
				
				if(leftItem instanceof GenExpression)
				{
					replaceTableAlias2((GenExpression)leftItem, oldName, newName);
					
				}
				if(rightItem instanceof GenExpression)
				{
					replaceTableAlias2((GenExpression)rightItem, oldName, newName);
				}
			}
			else if(qs instanceof WhenExpression)
			{
				// this shoudl already do liskov substitution
				replaceTableAlias2(qs, oldName, newName);;
			}
			else if(qs instanceof InGenExpression)
			{
				InGenExpression ig = (InGenExpression)qs;
				if(qs.leftItem != null)
					replaceTableAlias2((GenExpression)qs.leftItem, oldName, newName);

				if(qs.rightItem != null && qs.rightItem instanceof GenExpression)
				{
					replaceTableAlias2((GenExpression)qs.rightItem, oldName, newName);
				}
			}
			else if(qs.leftItem != null && qs.rightItem != null) // this is expression
			{
				// dont know how to handle this yet
				// fun stuff we are going to go into a recursion again
				Object leftItem = (GenExpression)qs.leftItem;
				Object rightItem = (GenExpression)qs.rightItem;
				// this is where we need to do the paranthesis again I think
				replaceTableAlias2((GenExpression)qs.leftItem, oldName, newName);
				replaceTableAlias2((GenExpression)qs.rightItem, oldName, newName);
			}
			else if(qs.telescope )
			{
				replaceTableAlias2((GenExpression)qs.body, oldName, newName);
			}
		}
		// like the filter

		// add the from
		if(qs.from != null)
			replaceTableAlias2((GenExpression)qs.from, oldName, newName);				
		

		// add the joins finally			
		for(int joinIndex = 0;joinIndex < qs.joins.size();joinIndex++)
		{
			//System.err.println("Selector Buf so far " + buf);
			GenExpression sqs = qs.joins.get(joinIndex);	
			// I also need to pick the from here
			// this is is the inner join on <from>
			// how to tell if a join is a subjoin ?
			if(sqs.from != null)
				replaceTableAlias2((GenExpression)sqs.from, oldName, newName);
			if(sqs.body != null)
				replaceTableAlias2((GenExpression)sqs.body, oldName, newName);
		}

		// add the where
		if(qs.filter != null)
			replaceTableAlias2((GenExpression)qs.filter, oldName, newName);
		
		
		// add the groupby
		if(qs.ngroupBy.size() > 0)
		{
			for(int groupIndex = 0;groupIndex < qs.ngroupBy.size();groupIndex++)
			{
				GenExpression gep = qs.ngroupBy.get(groupIndex);
				replaceTableAlias2(gep, oldName, newName);
			}
		}

		// add the order
		if(qs.norderBy.size() > 0)
		{
			for(int orderIndex = 0;orderIndex < qs.norderBy.size();orderIndex++)
			{
				GenExpression gep = qs.norderBy.get(orderIndex).body;
				replaceTableAlias2(gep, oldName, newName);
			}
		}
		
	}

	public void addQuoteToColumn(GenExpression qs, String quote)
	{
		// if the type is join.. you need to do other things
		//System.err.println("Processing  " + qs.aQuery + " <>" + qs.expression + "<>" + qs.operation);
		boolean processed = false;
		if(qs != null && qs.operation != null)
		{
			if(qs.operation.equalsIgnoreCase("select") || qs.operation.equalsIgnoreCase("querystruct"))
			{
				for(int selIndex = 0;selIndex < qs.nselectors.size();selIndex++)
				{
					GenExpression sqs = qs.nselectors.get(selIndex);					
					addQuoteToColumn(sqs, quote);
				}
				processed = true;
			}
			if(qs.operation.equalsIgnoreCase("column"))
			{
				qs.leftExpr = quote + qs.leftExpr + quote;
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
					addQuoteToColumn((GenExpression)operands.get(opIndex), quote);
				addQuoteToColumn((GenExpression)operands.get(opIndex+1), quote);
			}
			processed = true;
		}
		
		if(qs.operation != null && qs.operation.equalsIgnoreCase("between"))
		{
			processed = true;
			addQuoteToColumn((GenExpression)qs.body, quote);
			addQuoteToColumn((GenExpression)qs.leftItem, quote);
			addQuoteToColumn((GenExpression)qs.rightItem, quote);
			processed = true;
			
		}
		if(qs.operation != null && qs.operation.equalsIgnoreCase("cast"))
		{
			// name of the function is in the left alias
			if(qs.leftItem != null && qs.leftItem instanceof GenExpression)
				addQuoteToColumn((GenExpression)qs.leftItem, quote);
			processed = true;
		}
		
		if(qs.operation != null && qs.operation.equalsIgnoreCase("function"))
		{
			// name of the function is in the left alias
			FunctionExpression thisExpr = (FunctionExpression)qs;
			List <GenExpression> parameters = thisExpr.expressions;
			for(int paramIndex = 0;paramIndex < parameters.size();paramIndex++)
			{
				addQuoteToColumn(parameters.get(paramIndex),quote);
			}
			processed = true;
		}
		if(qs.operation != null && qs.operation.equalsIgnoreCase("isnull"))
		{
			// name of the function is in the left alias
			addQuoteToColumn((GenExpression)qs.leftItem,quote);
			processed = true;
		}

		// need to handle telescope

		if(!processed)
		{
			if(qs.recursive)
			{
				Object leftItem = qs.leftItem;
				Object rightItem = qs.rightItem;
				
				if(leftItem instanceof GenExpression)
				{
					addQuoteToColumn((GenExpression)leftItem, quote);
					
				}
				if(rightItem instanceof GenExpression)
				{
					addQuoteToColumn((GenExpression)rightItem,quote);
				}
			}
			else if(qs instanceof WhenExpression)
			{
				// this shoudl already do liskov substitution
				addQuoteToColumn(qs, quote);;
			}
			else if(qs.leftItem != null && qs.rightItem != null) // this is expression
			{
				// dont know how to handle this yet
				// fun stuff we are going to go into a recursion again
				Object leftItem = (GenExpression)qs.leftItem;
				Object rightItem = (GenExpression)qs.rightItem;
				// this is where we need to do the paranthesis again I think
				addQuoteToColumn((GenExpression)qs.leftItem, quote);
				addQuoteToColumn((GenExpression)qs.rightItem, quote);
			}
			else if(qs.telescope )
			{
				addQuoteToColumn((GenExpression)qs.body, quote);
			}
		}
		// like the filter

		// add the from
		if(qs.from != null)
			addQuoteToColumn((GenExpression)qs.from, quote);				
		

		// add the joins finally			
		for(int joinIndex = 0;joinIndex < qs.joins.size();joinIndex++)
		{
			//System.err.println("Selector Buf so far " + buf);
			GenExpression sqs = qs.joins.get(joinIndex);	
			// I also need to pick the from here
			// this is is the inner join on <from>
			// how to tell if a join is a subjoin ?
			if(sqs.from != null)
				addQuoteToColumn((GenExpression)sqs.from, quote);
			if(sqs.body != null)
				addQuoteToColumn((GenExpression)sqs.body, quote);
		}

		// add the where
		if(qs.filter != null)
			addQuoteToColumn((GenExpression)qs.filter, quote);
		
		
		// add the groupby
		if(qs.ngroupBy.size() > 0)
		{
			for(int groupIndex = 0;groupIndex < qs.ngroupBy.size();groupIndex++)
			{
				GenExpression gep = qs.ngroupBy.get(groupIndex);
				addQuoteToColumn(gep, quote);
			}
		}

		// add the order
		if(qs.norderBy.size() > 0)
		{
			for(int orderIndex = 0;orderIndex < qs.norderBy.size();orderIndex++)
			{
				GenExpression gep = qs.norderBy.get(orderIndex).body;
				addQuoteToColumn(gep, quote);
			}
		}
		
	}

	public void addSelect(GenExpression selector)
	{
		// if it is duplicate dont add it
			// check the alias
		for(int selectorIndex = 0;selectorIndex < nselectors.size();selectorIndex++)
		{
			GenExpression thisSelector = nselectors.get(selectorIndex);
			if(thisSelector.leftAlias != null && selector.leftAlias != null)
				if(thisSelector.leftAlias.contentEquals(selector.leftAlias)) return;
			else if(thisSelector.leftAlias != null && selector.getLeftExpr() != null)
				if(thisSelector.leftAlias.contentEquals(selector.getLeftExpr())) return;
			else if(thisSelector.getLeftExpr() != null && selector.leftAlias != null)
				if(thisSelector.getLeftExpr().contentEquals(selector.leftAlias)) return;
			else if(thisSelector.getLeftExpr() != null && selector.getLeftExpr() != null)
				if(thisSelector.getLeftExpr().contentEquals(selector.getLeftExpr())) return;
		}
		nselectors.add(selector);
	}
	
	public boolean compareSelectors(GenExpression inputExpression)
	{
		// if it is duplicate dont add it
			// check the alias
		boolean match = true;
		for(int selectorIndex = 0;match && selectorIndex < inputExpression.nselectors.size();selectorIndex++)
		{
			GenExpression thisSelector = nselectors.get(selectorIndex);
			boolean foundMatch = false;
			for(int thisSelectorIndex = 0;!foundMatch  && thisSelectorIndex < this.nselectors.size();thisSelectorIndex++)
			{
				GenExpression selector = this.nselectors.get(thisSelectorIndex);
				if(thisSelector.leftAlias != null && selector.leftAlias != null)
					foundMatch = thisSelector.leftAlias.contentEquals(selector.leftAlias);
				else if(thisSelector.leftAlias != null && selector.getLeftExpr() != null)
					foundMatch = thisSelector.leftAlias.contentEquals(selector.getLeftExpr());
				else if(thisSelector.getLeftExpr() != null && selector.leftAlias != null)
					foundMatch = thisSelector.getLeftExpr().contentEquals(selector.leftAlias);
				else if(thisSelector.getLeftExpr() != null && selector.getLeftExpr() != null)
					foundMatch = thisSelector.getLeftExpr().contentEquals(selector.getLeftExpr());
			}
			match = foundMatch && match;
		}
		return match;
	}
	
	public static StringBuffer printLevel2(GenExpression qs, 
			List <String> realTables, // this is where all the real tables are kept
			int level, 
			GenExpression derivedKey, 
			Map <GenExpression, List <GenExpression>> derivedColumns,
			Map <Integer, List <GenExpression>> levelProjections, // all the columns in a given level
			Map <String, List <GenExpression>> tableColumns, // all the columns in a given level do we need this ?
			Map <String, String> tableAliases, // aliases for this column - also need something that will keep // also keeps aliases for table
			String tableName,
			boolean derived, 
			boolean projection)
	{
		// if the type is join.. you need to do other things
		// if derived is true only add it to the derived columns not to anything else
		//System.err.println("Processing  " + qs.aQuery + " <>" + qs.expression + "<>" + qs.operation);
		boolean processed = false;
		List <GenExpression> derivedColumnList = new Vector<GenExpression>();
		List <GenExpression> allColumnList = new Vector<GenExpression>();
		
		List <GenExpression> projectionList = new Vector<GenExpression>();

		
		if(levelProjections.containsKey(level))
			projectionList = levelProjections.get(level);

		if(derivedColumns.containsKey(derivedKey))
			derivedColumnList = derivedColumns.get(derivedKey);
		
		if(qs != null && qs.operation != null && !qs.neutralize) 
		{
			if(qs.operation.equalsIgnoreCase("select") || qs.operation.equalsIgnoreCase("querystruct")) 
			{
				level++;
				
				tableName = qs.currentTable;
				
				for(int selIndex = 0;selIndex < qs.nselectors.size();selIndex++) 
				{

					GenExpression sqs = qs.nselectors.get(selIndex);
					// need to handle telescope
					printLevel2(sqs, realTables, level, null, derivedColumns, levelProjections, tableColumns, tableAliases, tableName, false, true);
				}
				processed = true;
			}
			else if(qs.operation.equalsIgnoreCase("column"))
			{
				String columnName = qs.leftExpr;	
				//System.err.println(qs.currentTable + "<<>>" + qs.tableName + "<<>>" + qs.tableAlias + "<<>>" + tableName);
				
				processColumn(qs, derived, projection, tableColumns, tableAliases, projectionList, allColumnList, derivedColumns, derivedKey, qs.tableName);
				// otherwise add this to the derived columns
				
				// add it to projection if it is
				
				
				processed = true;
			}
			
			else if(qs.operation.equalsIgnoreCase("double") || qs.operation.equalsIgnoreCase("date") || qs.operation.equalsIgnoreCase("time") || qs.operation.equalsIgnoreCase("string") || qs.operation.equalsIgnoreCase("long"))
			{
				// dont need this ?
				// may be need it only for alias
				String columnName = qs.leftExpr;
				processColumn(qs, derived, projection, tableColumns, tableAliases, projectionList, allColumnList, derivedColumns, derivedKey, tableName);
				processed = true;
			}
			else if(qs.operation.equalsIgnoreCase("opaque"))
			{
				// not sure how we are handling this.
				String columnName = qs.leftExpr;
				processColumn(qs, derived, projection, tableColumns, tableAliases, projectionList, allColumnList, derivedColumns, derivedKey, tableName);
			}
			else if(qs.operation.equalsIgnoreCase("from") && !qs.composite)
			{
				String columnName = qs.leftExpr;
				realTables.add(columnName);

				if(qs.leftAlias != null && qs.leftAlias.length() > 0)
				{
					// need to account for when there are conflicts
					tableAliases.put(qs.leftAlias, tableName);
				}
				processed = true;
			}
			else if(qs.operation.equalsIgnoreCase("allcol"))
			{
				String columnName = "*";
				// need to highlight they are trying to select all col
				if(qs.tableName != null)
					tableName = qs.tableName;
				processColumn(qs, derived, projection, tableColumns, tableAliases, projectionList, allColumnList, derivedColumns, derivedKey,  tableName);				
				
				processed = true;
			}
			else if(qs.composite)
			{
				//System.err.println(" hmm.. from is composite.. but simple ?  " + qs.aQuery);
			}
		}
		
		if(qs.operation != null && qs.operation.contains("union") && !qs.neutralize)
		{
			//System.err.println("And now we are getting the union " + qs);
			// process the left and right
			// level gets set when we hit a select
			OperationExpression opex = (OperationExpression)qs;
			List <GenExpression> operands = opex.operands;
			List <String> opNames = opex.opNames;
			
			for(int opIndex = 0;opIndex < opNames.size();opIndex++)
			{
				printLevel2((GenExpression)operands.get(opIndex), realTables, level, null, derivedColumns, levelProjections, tableColumns,tableAliases, tableName, derived, projection);
			}
			
			processed = true;
		}
		
		if(qs.operation != null && qs.operation.equalsIgnoreCase("between") && !qs.neutralize)
		{
			processed = true;
			String columnName = qs.leftExpr;
			if(derivedKey == null)
			{
				derivedKey = qs;
				processColumn(qs, derived, projection, tableColumns, tableAliases, projectionList, allColumnList, derivedColumns, derivedKey,  tableName);
				derived = true;
			}
			
			printLevel2((GenExpression)qs.body, realTables, level, derivedKey, derivedColumns, levelProjections, tableColumns,tableAliases, tableName, true, projection);
			printLevel2((GenExpression)qs.leftItem, realTables, level, derivedKey, derivedColumns, levelProjections, tableColumns,tableAliases, tableName, true, projection);
			printLevel2((GenExpression)qs.rightItem, realTables, level, derivedKey, derivedColumns, levelProjections, tableColumns,tableAliases, tableName, true, projection);
			processed = true;

			if(qs.aQuery != null && qs.aQuery.equalsIgnoreCase(derivedKey.aQuery))
				derivedKey = null;
			
		}
		if(qs.operation != null && qs.operation.equalsIgnoreCase("cast") && !qs.neutralize)
		{
			// name of the function is in the left alias
			if(derivedKey == null)
			{
				derivedKey = qs;
				processColumn(qs, derived, projection, tableColumns, tableAliases, projectionList, allColumnList, derivedColumns, derivedKey,  tableName);
				derived = true;
			}
			if(qs.leftItem != null && qs.leftItem instanceof GenExpression) {
				printLevel2((GenExpression)qs.leftItem, realTables, level, derivedKey, derivedColumns, levelProjections, tableColumns,tableAliases, tableName, true, projection);
			} else {
				// do the columnname magic
				String columnName = qs.leftExpr;
				processColumn(qs, derived, projection, tableColumns, tableAliases, projectionList, allColumnList, derivedColumns, derivedKey,  tableName);
			}
			if(qs.aQuery != null && qs.aQuery.equalsIgnoreCase(derivedKey.aQuery))
				derivedKey = null;

			processed = true;
		}
		
		if(qs.operation != null && qs.operation.equalsIgnoreCase("function") && !qs.neutralize)
		{
			FunctionExpression thisExpr = (FunctionExpression)qs;
			if(derivedKey == null)
			{
				derivedKey = qs;
				processColumn(qs, derived, projection, tableColumns, tableAliases, projectionList, allColumnList, derivedColumns, derivedKey,  tableName);
				derived = true;
			}
			
			// name of the function is in the left alias
			//buf.append(qs.expression).append("(");
			List <GenExpression> parameters = thisExpr.expressions;
			for(int paramIndex = 0;paramIndex < parameters.size();paramIndex++)
			{
				printLevel2((GenExpression)parameters.get(paramIndex), realTables, level, derivedKey, derivedColumns, levelProjections, tableColumns,tableAliases, tableName, true, projection);
			}
			if(qs.aQuery != null && qs.aQuery.equalsIgnoreCase(derivedKey.aQuery))
				derivedKey = null;
			processed = true;
		}
		if(qs.operation != null && qs.operation.equalsIgnoreCase("isnull") && !qs.neutralize)  
		{
			// name of the function is in the left alias
			if(derivedKey == null)
			{
				derivedKey = qs;
				processColumn(qs, derived, projection, tableColumns, tableAliases, projectionList, allColumnList, derivedColumns, derivedKey,  tableName);
				derived = true;
			}

			printLevel2((GenExpression)qs.leftItem, realTables, level, derivedKey, derivedColumns, levelProjections, tableColumns,tableAliases, tableName, true, projection);
			if(qs.aQuery != null && qs.aQuery.equalsIgnoreCase(derivedKey.aQuery))
				derivedKey = null;
			
			// dont know if I need the alias yet
			//if(qs.leftAlias != null)
			//	buf.append(" AS ").append(qs.leftAlias);
			processed = true;
		}
		
		
		// need to handle telescope
		
		if(!processed && !qs.neutralize)
		{
			if(qs.recursive)
			{
				// add this to the 
				// recursive will never have a table name fully baked
				// because it can use multiple columns
				if(derivedKey == null)
				{
					derivedKey = qs;
					processColumn(qs, derived, projection, tableColumns, tableAliases, projectionList, allColumnList, derivedColumns, derivedKey,  tableName);
					derived = true;
				}
				printLevel2Recursive(qs, realTables, level, derivedKey, derivedColumns, levelProjections, tableColumns,tableAliases, tableName, derived, projection);
				if(qs.aQuery != null && qs.aQuery.equalsIgnoreCase(derivedKey.aQuery))
					derivedKey = null;
			}
			else if(qs instanceof WhenExpression)
			{
				//buf.append(((WhenExpression)qs).printOutput());
				if(derivedKey == null)
				{
					derivedKey = qs;
					processColumn(qs, derived, projection, tableColumns, tableAliases, projectionList, allColumnList, derivedColumns, derivedKey,  tableName);
					derived = true;
				}
				((WhenExpression)qs).printLevel(qs, realTables, level, derivedKey, derivedColumns, levelProjections, tableColumns, tableAliases, tableName, derived, projection);
				if(qs.aQuery != null && qs.aQuery.equalsIgnoreCase(derivedKey.aQuery))
					derivedKey = null;
			}
			else if(qs instanceof InGenExpression)
			{
				// add this to the 
				if(derivedKey == null)
				{
					derivedKey = qs;
					derived = true;
				}
				InGenExpression ig = (InGenExpression)qs;
				if(qs.leftItem != null) {
					printLevel2((GenExpression)qs.leftItem, realTables, level, derivedKey, derivedColumns, levelProjections, tableColumns,tableAliases, tableName, derived, projection);
				}
				if(ig.inList.size() > 0 && ig.rightItem == null)
				{
					// process the list
					// I have made this opaque
					for(int itemIndex = 0;itemIndex < ig.inList.size();itemIndex++)
					{
						printLevel2(ig.inList.get(itemIndex), realTables, level, derivedKey, derivedColumns, levelProjections, tableColumns,tableAliases, tableName, derived, projection);
					}
					//buf.append(")");
				}
				else if(qs.rightItem != null && qs.rightItem instanceof GenExpression)
				{
					// when you have where column1 in ( subquery ) 
					// the subquery is stored it is stored in the body
					if( ((GenExpression)qs.rightItem).telescope) {
						printLevel2(((GenExpression)qs.rightItem).body, realTables, level, derivedKey, derivedColumns, levelProjections, tableColumns,tableAliases, tableName, derived, projection);
					} else {
						printLevel2((GenExpression)qs.rightItem, realTables, level, derivedKey, derivedColumns, levelProjections, tableColumns,tableAliases, tableName, derived, projection);
					}
				}
				processed = true;
				if(qs.aQuery != null && qs.aQuery.equalsIgnoreCase(derivedKey.aQuery))
					derivedKey = null;
			}
			
			// this also needs to be neutralized
			else if(qs.leftItem != null && qs.rightItem != null) // this is expression
			{
				// add this to the 
				if(derivedKey == null)
				{
					derivedKey = qs;
					derived = true;
				}

				// dont know how to handle this yet
				// fun stuff we are going to go into a recursion again
				Object leftItem = (GenExpression)qs.leftItem;
				Object rightItem = (GenExpression)qs.rightItem;
				
				// this is where we need to do the paranthesis again I think
				printLevel2((GenExpression)qs.leftItem, realTables, level, derivedKey, derivedColumns, levelProjections, tableColumns,tableAliases, tableName, derived, projection);
				printLevel2((GenExpression)qs.rightItem, realTables, level, derivedKey, derivedColumns, levelProjections, tableColumns,tableAliases, tableName, derived, projection);
				if(qs.aQuery != null && qs.aQuery.equalsIgnoreCase(derivedKey.aQuery))
					derivedKey = null;
			}
			else if(qs.telescope && !qs.body.neutralize)
			{
				if(derivedKey == null)
				{
					derivedKey = qs;
					derived = true;
				}
				processColumn(qs, derived, projection, tableColumns, tableAliases, projectionList, allColumnList, derivedColumns, derivedKey,  tableName);

				// need to acomodate when it is neutralize
				printLevel2((GenExpression)qs.body, realTables, level, derivedKey, derivedColumns, levelProjections, tableColumns,tableAliases, tableName, derived, projection);
				if(qs.aQuery != null && qs.aQuery.equalsIgnoreCase(derivedKey.aQuery))
					derivedKey = null;
				
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

				if(qs.from.composite)
				{
					printLevel2((GenExpression)qs.from, realTables, level, derivedKey, derivedColumns, levelProjections, tableColumns,tableAliases, tableName, derived, false);
					if(qs.from.leftAlias != null)
						tableAliases.put(qs.from.leftAlias, qs.from.aQuery);
				}
				else
				{
					if(!realTables.contains(qs.from.leftExpr) &&qs.from.leftExpr != null && qs.from.leftExpr.length() > 0)
					realTables.add(qs.from.leftExpr);
					if(qs.from.leftAlias != null)
						tableAliases.put(qs.from.leftAlias, qs.from.leftExpr);
					// add the alias
				}
				
				// if we need to keep the table alias keep it here
			}
		}
		

		// add the joins finally			
		for(int joinIndex = 0;joinIndex < qs.joins.size();joinIndex++)
		{
			//System.err.println("Selector Buf so far " + buf);
			GenExpression sqs = qs.joins.get(joinIndex);	

			// I also need to pick the from here
			// this is is the inner join on <from>
			// how to tell if a join is a subjoin ?
			if(sqs.from != null && !sqs.neutralize)
			{
				if(sqs.from != null && sqs.from.composite)
				{
					printLevel2(sqs.from, realTables, level, derivedKey, derivedColumns, levelProjections, tableColumns,tableAliases, tableName, derived, false);
				}
				else
				{
					printLevel2((GenExpression)sqs.from, realTables, level, derivedKey, derivedColumns, levelProjections, tableColumns,tableAliases, tableName, derived, false);
					if(!realTables.contains(sqs.currentTable) && sqs.currentTable != null && sqs.currentTable.length() > 0)
						realTables.add(sqs.currentTable); // why is this alias null ?
					
				}
				
				// need to process alias possibl
			}
			if(sqs.body != null && sqs.body.operation.equalsIgnoreCase("querystruct") && !sqs.neutralize)
			{
//				open = "(";
//				close = ")  ";
			}
			// process this as a query struct
			if(sqs.body != null && !sqs.neutralize)
			{
				printLevel2((GenExpression)sqs.body, realTables, level, derivedKey, derivedColumns, levelProjections, tableColumns,tableAliases, tableName, derived, false);
			}
		}

		// add the where
		if(qs.filter != null)
		{
			printLevel2((GenExpression)qs.filter, realTables, level, derivedKey, derivedColumns, levelProjections, tableColumns,tableAliases, tableName, derived, false);
		}
		
		
		// add the groupby
		if(qs.ngroupBy.size() > 0)
		{
			for(int groupIndex = 0;groupIndex < qs.ngroupBy.size();groupIndex++)
			{
				GenExpression gep = qs.ngroupBy.get(groupIndex);
				printLevel2((GenExpression)gep, realTables, level, derivedKey, derivedColumns, levelProjections, tableColumns,tableAliases, tableName, derived, false);
			}
		}

		// add the order
		if(qs.norderBy.size() > 0)
		{
			for(int orderIndex = 0;orderIndex < qs.norderBy.size();orderIndex++)
			{
				GenExpression gep = qs.norderBy.get(orderIndex).body;
				printLevel2((GenExpression)gep, realTables, level, derivedKey, derivedColumns, levelProjections, tableColumns,tableAliases, tableName, derived, false);
			}
		}
		
		levelProjections.put(level, projectionList);
		if(derived)
			derivedColumns.put(derivedKey, derivedColumnList);

		return null;
	}

	public static void printLevel2Recursive(
			GenExpression qs, 
			List <String> realTables, // this is where all the real tables are kept
			int level, 
			GenExpression derivedKey, 
			Map <GenExpression, List <GenExpression>> derivedColumns,
			Map <Integer, List <GenExpression>> levelProjections, // all the columns in a given level
			Map <String, List <GenExpression>> levelColumns, // all the columns in a given level
			Map <String, String> aliases, // aliases for this column - also need something that will keep 
			String tableName,
			boolean derived, 
			boolean projection)
	{
		Object leftItem = qs.leftItem;
		Object rightItem = qs.rightItem;
		
		String operation = qs.operation;
		
		// Account for when it is an or and both left item and right item are neutralized
		//if(leftItem != null && leftItem instanceof GenExpression && ((GenExpression)leftItem).neutralize && rightItem != null && rightItem instanceof GenExpression && ((GenExpression)rightItem).neutralize)
		//	return buf.append("(1 = 1) ");
		
		if(leftItem instanceof GenExpression)
		{
			if(!((GenExpression)leftItem).neutralize)
			{
				// need to know if I need to put a paranthesis around this or not
				Object childLeftItem = ((GenExpression)leftItem).leftItem;
				Object childRightItem = ((GenExpression)leftItem).rightItem;
				
				printLevel2((GenExpression)leftItem, realTables, level, derivedKey, derivedColumns, levelProjections, levelColumns,aliases, tableName, derived, projection); // need to account for neutralize here

			}
		}
		
		
		if(rightItem instanceof GenExpression)
		{
			if(!((GenExpression)rightItem).neutralize)
			{
			// need to know if I need to put a paranthesis around this or not
				Object childLeftItem = ((GenExpression)rightItem).leftItem;
				Object childRightItem = ((GenExpression)rightItem).rightItem;
				
				printLevel2((GenExpression)rightItem, realTables, level, derivedKey, derivedColumns, levelProjections, levelColumns,aliases, tableName, derived, projection); // need to account for neutralize here
			}
		}
	}

	// I wonder if we need to account for the level
	// since there could be the same alias being used across levels for different tables
	public static void processColumn(GenExpression qs, 
			boolean derived, 
			boolean projection, 
			Map <String, List <GenExpression>> tableColumns,
			Map <String, String> aliases, 
			List projectionList, 
			List allColumnList, 
			Map <GenExpression, List<GenExpression>> derivedColumns, 
			GenExpression derivedKey, 
			String tableName)
	{
		// table could have an alias
		// but we will process it as something that is available from the columns
		
		
		List <GenExpression> tableColumnList = null;

		List <GenExpression> derivedColumnList = null;
		
		if(tableColumns.containsKey(tableName))
			tableColumnList = tableColumns.get(tableName);
		else
			tableColumnList = new Vector<GenExpression>();
				
		String prefix = tableName + "__";
		// if there is an alias to do 
		//if(qs.leftAlias == null && derivedKey == null && !projection)
		//	prefix = "";
		
		if(derivedColumns.containsKey(derivedKey))
			derivedColumnList = derivedColumns.get(derivedKey);
		else
			derivedColumnList = new Vector<GenExpression>();

		if( (qs.operation.equalsIgnoreCase("column") || qs.operation.equalsIgnoreCase("allcol") ))
		{
			boolean found = false;
			for(int tableColumnIndex = 0;!found && tableColumnIndex < tableColumnList.size();tableColumnIndex++)
				found = qs.aQuery.equalsIgnoreCase(tableColumnList.get(tableColumnIndex).aQuery);
			
			if(!found)
				tableColumnList.add(qs);
		}
		if(derived)
		{
			derivedColumnList.add(qs);
		}
		
		else if(projection)
		{
			projectionList.add(qs);
		}
		

		
		tableColumns.put(tableName, tableColumnList);
		
		derivedColumns.put(derivedKey, derivedColumnList);
	}
	
	
}

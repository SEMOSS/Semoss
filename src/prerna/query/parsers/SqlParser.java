package prerna.query.parsers;

import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.util.SelectUtils;
import net.sf.jsqlparser.util.TablesNamesFinder;
import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryArithmeticSelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;

public class SqlParser {
	
	// keep table alias
	private Map<String, String> tableAlias = null;
	// keep column alias
	private Map<String, String> columnAlias = null;
	
	public SqlParser() {
		this.tableAlias = new Hashtable <String, String>();
		this.columnAlias = new Hashtable <String, String>();
	}
	
	public void processQuery(String query) throws Exception
	{
		Statement stmt = CCJSqlParserUtil.parse(query);
		Select select = ((Select)stmt);
		PlainSelect sb = (PlainSelect)select.getSelectBody();
		List <SelectItem> items = sb.getSelectItems();

		OrExpression expr = (OrExpression)sb.getWhere();

		QueryStruct2 qs = new QueryStruct2();

		// joins first - I need the table alias for selects
		Table fromTable = (Table)sb.getFromItem();
		//tableAlias.put(fromTable.getName(), fromTable.getAlias().getName());
		tableAlias.put(fromTable.getAlias().getName(), fromTable.getName());
		fillJoins(qs, fromTable.getName(), sb.getJoins());
		
		// fill the selects next
		
		fillSelects(qs, items);

		// then filters
		fillFilters(qs, null, expr);
		
		// fill the groups
		fillGroups(qs, sb.getGroupByColumnReferences());

		// fill the order by
		fillOrder(qs, sb.getOrderByElements());

		// fill the limit
		fillLimit(qs, sb.getLimit());
		
		// getting all the tables
		TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
		List<String> tableList = tablesNamesFinder.getTableList(select);
		
		
		System.out.println("Tables are .. " + tableList);
		
		// adding a from item
		// I dont know if it will ever happen ?
		// the only thing I can think of is when we federate, but at that point you are still in the pixel zone
		Table newTable = new Table("moron");
		newTable.setAlias(new Alias("m"));
		
		select.setSelectBody(sb);
		//sb.setFromItem(new FromItem().setAlias(new Alias("Moron"));
		
		SelectUtils.addExpression(select, new Column("b"));
		
		// adding wheres
		// this is really where it all will come down to
		// so we should also have parenthesis
		EqualsTo newExpr = new EqualsTo();
		//newExpr.se
		newExpr.setLeftExpression(new Column("random"));
		newExpr.setRightExpression(new LongValue(20));
		expr.setRightExpression(newExpr);
		//expr.setLeftExpression(new );
		
		//select.
		
		// so the from is not coming in properly
		
		System.out.println("Statement is " + stmt);
		
	}
	
	public void fillLimit(QueryStruct2 qs, Limit limit)
	{
		
		if(limit.getRowCount() instanceof LongValue)
		{
			long limitRow =  ((LongValue)limit.getRowCount()).getValue();
			qs.setLimit(limitRow);
		}

		if(limit.getOffset() instanceof LongValue)
		{
			long offset =  ((LongValue)limit.getOffset()).getValue();
			qs.setOffSet(offset);
		}
	}
	
	public void fillOrder(QueryStruct2 qs, List <OrderByElement> orders)
	{
		for(int orderIndex = 0;orderIndex < orders.size();orderIndex++)
		{
			OrderByElement thisElement = orders.get(orderIndex);
			Expression expr = thisElement.getExpression();
			String sortDir = "ASC";
			if(thisElement.isAscDescPresent() && !thisElement.isAsc())
					sortDir = "DESC";
			if(expr instanceof Column)
			{
				String colName = ((Column)expr).getColumnName();
				if(columnAlias.containsKey(colName))
				{
					String fullColumn = columnAlias.get(colName);
					String [] colParts = fullColumn.split("__");
					String concept = colParts[0];
					String property = colParts[1];
					qs.addOrderBy(concept, property, sortDir);
				}
			}			
		}
	}
	
	public void fillGroups(QueryStruct2 qs, List <Expression> groups)
	{
		for(int groupIndex = 0;groupIndex < groups.size();groupIndex++)
		{
			Expression expr = groups.get(groupIndex);
			
			// this has to be a column
			// right now this assumption is wrong
			// it could be an alias of a derived column
			if(expr instanceof Column)
			{
				String colName = ((Column)expr).getColumnName();
				if(columnAlias.containsKey(colName))
				{
					String fullColumn = columnAlias.get(colName);
					String [] colParts = fullColumn.split("__");
					String concept = colParts[0];
					String property = colParts[1];
					qs.addGroupBy(concept, property);
				}
			}
			
		}
	}
	
	// need the selects here
	public void fillSelects(QueryStruct2 qs, List <SelectItem> selects)
	{
		for(int selectIndex = 0;selectIndex < selects.size();selectIndex++)
		{
			SelectItem si = selects.get(selectIndex);
			if(si instanceof SelectExpressionItem)
			{
				SelectExpressionItem sei = (SelectExpressionItem)si;
				Expression expr = sei.getExpression();

				// so this could be a sum
				// or whatever else
				// I need to run through similar route as 
				// filter
				// column
				// or a full expression
				// in which case, I just need to put the columns
				// not sure how we handle it today
				// for now I will handle simple ones
				// try simple route first
				IQuerySelector thisSelect = fillSelectExpr(expr);
				if(thisSelect != null)
				{
					// simple could be anything
					// need to get the alias here to be utilized
					if(thisSelect instanceof QueryColumnSelector)
					{
						Alias alias = sei.getAlias();
						String aliasName = null;
						if(alias == null)
							// set it to the same name
							aliasName = ((Column)expr).getColumnName();
						else
							aliasName = alias.getName();

						((QueryColumnSelector)thisSelect).setAlias(aliasName);
						columnAlias.put(aliasName, ((QueryColumnSelector)thisSelect).getTable() + "__" + ((QueryColumnSelector)thisSelect).getColumn());
					}
					qs.addSelector(thisSelect);
				}
				else
				{
					// need to add the alias here
					processSelect(expr, qs, null, true, sei.getAlias().getName());				
				}
			}
		}
	}
	
	public void processSelect(Expression expr, QueryStruct2 qs, IQuerySelector qas, boolean left, String alias) {
		Expression lexpr = null;
		Expression rexpr = null;
		IQuerySelector thisSelector = null;
		
		// possibilities are
		// addition, subtraction, multiplication, division or paranthesis so keep it moving
		
		// addition
		if(expr instanceof Addition)
		{
			if(qas == null)
			{
				qas = new QueryArithmeticSelector();
				qas.setAlias(alias);
				((QueryArithmeticSelector)qas).setMathExpr("+");
				thisSelector = qas;
				// should possibly sendd in querystruct as well
				qs.addSelector(qas);
			}
			else
			{
				thisSelector = new QueryArithmeticSelector();
				((QueryArithmeticSelector)thisSelector).setMathExpr("+");
				
				// I need to make an evaluation to see if this is a ArithmeticSelector
				// or is this something else
				if(left)
					((QueryArithmeticSelector)qas).setLeftSelector(thisSelector);
				else
					((QueryArithmeticSelector)qas).setRightSelector(thisSelector);
			}
				
			Addition aExpr = (Addition)expr;
			lexpr = aExpr.getLeftExpression();
			rexpr = aExpr.getRightExpression();
			
		}

		// subtraction
		else if(expr instanceof Subtraction)
		{
			if(qas == null)
			{
				qas = new QueryArithmeticSelector();
				qas.setAlias(alias);
				((QueryArithmeticSelector)qas).setMathExpr("-");
				thisSelector = qas;
				// should possibly sendd in querystruct as well
				qs.addSelector(qas);
			}
			else
			{
				thisSelector = new QueryArithmeticSelector();
				((QueryArithmeticSelector)thisSelector).setMathExpr("-");
				if(left)
					((QueryArithmeticSelector)qas).setLeftSelector(thisSelector);
				else
					((QueryArithmeticSelector)qas).setRightSelector(thisSelector);
			}
				
			Subtraction aExpr = (Subtraction)expr;
			lexpr = aExpr.getLeftExpression();
			rexpr = aExpr.getRightExpression();
		}

		// multiplication
		else if(expr instanceof Multiplication)
		{
			if(qas == null)
			{
				qas = new QueryArithmeticSelector();
				qas.setAlias(alias);
				((QueryArithmeticSelector)qas).setMathExpr("*");
				thisSelector = qas;
				// should possibly sendd in querystruct as well
				qs.addSelector(qas);
			}
			else
			{
				thisSelector = new QueryArithmeticSelector();
				((QueryArithmeticSelector)thisSelector).setMathExpr("*");
				if(left)
					((QueryArithmeticSelector)qas).setLeftSelector(thisSelector);
				else
					((QueryArithmeticSelector)qas).setRightSelector(thisSelector);
			}
				
			Multiplication aExpr = (Multiplication)expr;
			lexpr = aExpr.getLeftExpression();
			rexpr = aExpr.getRightExpression();
		}

		
		// division
		else if(expr instanceof Division)
		{
			if(qas == null)
			{
				qas = new QueryArithmeticSelector();
				qas.setAlias(alias);
				((QueryArithmeticSelector)qas).setMathExpr("/");
				thisSelector = qas;
				// should possibly sendd in querystruct as well
				qs.addSelector(qas);
			}
			else
			{
				thisSelector = new QueryArithmeticSelector();
				((QueryArithmeticSelector)thisSelector).setMathExpr("/");
				if(left)
					((QueryArithmeticSelector)qas).setLeftSelector(thisSelector);
				else
					((QueryArithmeticSelector)qas).setRightSelector(thisSelector);
			}
				
			Division aExpr = (Division)expr;
			lexpr = aExpr.getLeftExpression();
			rexpr = aExpr.getRightExpression();
		}
		
		// function
		if(expr instanceof Function)
		{
			if(qas == null)
			{
				qas = new QueryArithmeticSelector();
				qas.setAlias(alias);
				((QueryArithmeticSelector)qas).setMathExpr("+");
				thisSelector = qas;
				// should possibly sendd in querystruct as well
				qs.addSelector(qas);
			}
			else
			{
				thisSelector = new QueryArithmeticSelector();
				((QueryArithmeticSelector)thisSelector).setMathExpr("+");
				if(left)
					((QueryArithmeticSelector)qas).setLeftSelector(thisSelector);
				else
					((QueryArithmeticSelector)qas).setRightSelector(thisSelector);
			}
				
			Function aExpr = (Function)expr;
			// most of them seem to have one argument, so should I try to get that first
			aExpr.getParameters().getExpressions().get(0);
			
		}


		else if (expr instanceof Parenthesis)
		{
			// move into the next piece
			processSelect(((Parenthesis)expr).getExpression(), qs, qas, left, alias);
		}
		else
		{
			/** this should be common portion**/
			// need to find if these are simple ones or complex
			// if complex then I need to recurse
			IQuerySelector leftS = fillSelectExpr(lexpr);
			IQuerySelector rightS = fillSelectExpr(rexpr);
			
			if(leftS != null)
				((QueryArithmeticSelector)thisSelector).setLeftSelector(leftS);
			else // recurse on it
				processSelect(lexpr, qs, thisSelector, true, alias);
				
			if(rightS != null)
				((QueryArithmeticSelector)thisSelector).setRightSelector(rightS);
			else // recurse on it
				processSelect(rexpr, qs, thisSelector, false, alias);
		}
	}
	
	public IQuerySelector fillSelectExpr(Expression expr) {
		
		// Column
		// DoubleValue
		// DateValue
		// LongValue
		// TimestampValue
		// TimeValue
		// StringValue
		// NullValue

		IQuerySelector constSelector = null;
		
		
		if(expr instanceof LongValue)
		{
			long longValue = ((LongValue) expr).getValue();
			constSelector = new QueryConstantSelector();
			((QueryConstantSelector)constSelector).setConstant(longValue);
		}
		if(expr instanceof DoubleValue)
		{
			double longValue = ((DoubleValue) expr).getValue();
			constSelector = new QueryConstantSelector();
			((QueryConstantSelector)constSelector).setConstant(longValue);
		}
		if(expr instanceof StringValue)
		{
			String strValue = ((StringValue) expr).getValue();
			constSelector = new QueryConstantSelector();
			((QueryConstantSelector)constSelector).setConstant(strValue);
		}
		if(expr instanceof DateValue)
		{
			// need to see about this
			Date dateValue = ((DateValue) expr).getValue();
			constSelector = new QueryConstantSelector();
			((QueryConstantSelector)constSelector).setConstant(dateValue);
		}
		if(expr instanceof NullValue)
		{
			// need to see about this as well
			String strValue = ((NullValue) expr).toString();
			constSelector = new QueryConstantSelector();
			((QueryConstantSelector)constSelector).setConstant(strValue);
		}
		
		if(expr instanceof Column)
		{
			String colValue = ((Column) expr).getColumnName();
			// need a way to get the alias
			//String alias = ((Column)expr).getFullyQualifiedName();
			String tableValue = ((Column)expr).getTable().getName();
			constSelector = new QueryColumnSelector();
			if(tableAlias.containsKey(tableValue))
				tableValue = tableAlias.get(tableValue);
			
			colValue = tableValue + "__" + colValue;
			constSelector = new QueryColumnSelector(colValue);
		}

		return constSelector;
	}

	
	
	public void fillJoins(QueryStruct2 qs,String tableName, List <Join> joins) {
		// joins are all sitting on
		// select.getJoins()
		// each one of which is telling what type of join it is
		// for the case of engineconcept ec and engine e
		// the join seems to say simple is true
		// the last one it says simple is false and it also puts an equation to it
		// each join has a table associated with it
		// sb.joins.get(index).rightitem - table and alias
		// sb.join.get(index).onExpression - tells you the quals to expression

		for(int joinIndex = 0;joinIndex < joins.size();joinIndex++)
		{
			Join thisJoin = joins.get(joinIndex);
			Table rightTable = (Table)thisJoin.getRightItem();
			
			// add the alias
			String rightTableName = rightTable.getName();
			String rightTableAlias = rightTable.getAlias().getName();
			
			
			// if somebody -- need to see if sql grammar can accomodate for stupidity where alias and table are same kind of
			//tableAlias.put(rightTableName, rightTableAlias);
			tableAlias.put(rightTableAlias, rightTableName);
			boolean simple = thisJoin.isSimple();
			
			if(!simple)
			{
				EqualsTo joinExpr = (EqualsTo)thisJoin.getOnExpression();
				String toTable = ((Column)joinExpr.getRightExpression()).getTable().getName();
				String toColumn = ((Column)joinExpr.getRightExpression()).getColumnName();
				String fromTable = tableName;
				String fromColumn = ((Column)joinExpr.getLeftExpression()).getColumnName();
				String joinType = null;
				
				// need to translate the alias into column name
				
				String full_from = fromTable  + "__" + fromColumn;
				String full_To = rightTableName + "__" + toColumn;
				
				if(thisJoin.isInner())
					joinType = "inner.join";
				if(thisJoin.isOuter())
					joinType = "outer.join";
				if(thisJoin.isFull())
					joinType = "full.join";
			
				qs.addRelation(full_from, full_To, joinType);
			}			
			else
			{
				// Need to understand how implicit join is being handled
			}
		}
	}
	
	// things I need to recurse
	// Main Query Struct
	// What was previously executed
	// for instance if the previous piece was and and this is or, I need to close and start it ?
	// Need some way to jump back to the previous level
	
	// I need some way to go in and go out kind of like the sablecc here
	// so I will do this artifically
	// Start of with a simple query filter or even a null
	// if the first one is an and/or 
	// I create the and/or filter and call this with left expression
	// and right expression
	// With the filter as a curFilter
	// if the expression is simple
	// it will continue to add itself as a simple filter
	// if the expression is complex then it will create another filter and add it into it - FE cant handle it right now
	// Once complete, there is nothing to change, since at this point it is all done
	// everytime I finish up with the expression which is a simple one like equals etc. 
	// 
	
	public void fillFilters(QueryStruct2 qs, IQueryFilter curFilter, Expression expr) {
		// this is a simple one just go ahead and process it like anything else
		// this should go first.. 
		// if unable to process it is only then we should attempt to create other pieces
		IQueryFilter filter = processFilter(expr);
		
		
		if(filter != null)
		{
			if(curFilter != null)
			{		
				if(curFilter instanceof AndQueryFilter)
					((AndQueryFilter)curFilter).addFilter(filter);
				else
					((OrQueryFilter)curFilter).addFilter(filter);
			}
			else
			{
				curFilter = filter;
				qs.addFilter(curFilter);				
			}
		}
		else
		{	
			if(expr instanceof AndExpression)
			{
				AndQueryFilter newFilter = null;
				
				if(curFilter == null)
				{
					curFilter = new AndQueryFilter();
					qs.addFilter(curFilter);
					
				}
				else if(!(curFilter instanceof AndQueryFilter))
				{
					newFilter = new AndQueryFilter();
					// I need something which adds this to the curFilter
					// at this point the cur filter has to be an or
					// it could be a subfilter
					// for now I will process it as a or
					((OrQueryFilter)curFilter).addFilter(newFilter);				
					curFilter = newFilter;
					
				}		
				// process left
				fillFilters(qs,curFilter, ((AndExpression) expr).getLeftExpression());
				// process right
				fillFilters(qs,curFilter, ((AndExpression) expr).getRightExpression());
	
			}
			else if(expr instanceof OrExpression)
			{
				OrQueryFilter newFilter = null;
				
				if(curFilter == null)
				{
					curFilter = new OrQueryFilter();
					qs.addFilter(curFilter);
					
				}
				else if(!(curFilter instanceof OrQueryFilter))
				{
					newFilter = new OrQueryFilter();
					// I need something which adds this to the curFilter
					// at this point the cur filter has to be an or
					// it could be a subfilter
					// for now I will process it as a or
					((AndQueryFilter)curFilter).addFilter(newFilter);				
					curFilter = newFilter;
				}		
	
				// process left
				fillFilters(qs,curFilter, ((OrExpression) expr).getLeftExpression());
				// process right
				fillFilters(qs, curFilter, ((OrExpression) expr).getRightExpression());
			}
			else if (expr instanceof Parenthesis)
			{
				System.out.println("This is where it is struck");
				fillFilters(qs, curFilter, ((Parenthesis)expr).getExpression());
			}
		}
	}
	
	public IQueryFilter processFilter(Expression expr)
	{
		IQueryFilter retFilter = null;
		// >>>> Logical
		// EqualsTo
		// NotEqualsTo
		// GreaterThan
		// GreaterThanEquals
		// MinorThan - because LessThan would be too easy
		// MinorThanEquals
		// AndExpression
		// OrExpression
		
		if(expr instanceof EqualsTo)
		{
			EqualsTo eExpr = (EqualsTo)expr;
			// there are only three choices may be four ok
			NounMetadata l = getNoun(eExpr.getLeftExpression());
			NounMetadata r = getNoun(eExpr.getRightExpression());
			if(l == null)
				l = new NounMetadata(eExpr.getLeftExpression().toString(), PixelDataType.CONST_STRING);
			if(r == null)
				r = new NounMetadata(eExpr.getRightExpression().toString(), PixelDataType.CONST_STRING);
			retFilter = new SimpleQueryFilter(l, "==", r);
		}
		
		if(expr instanceof NotEqualsTo)
		{
			NotEqualsTo eExpr = (NotEqualsTo)expr;
			// there are only three choices may be four ok
			NounMetadata l = getNoun(eExpr.getLeftExpression());
			NounMetadata r = getNoun(eExpr.getRightExpression());
			if(l == null)
				l = new NounMetadata(eExpr.getLeftExpression().toString(), PixelDataType.CONST_STRING);
			if(r == null)
				r = new NounMetadata(eExpr.getRightExpression().toString(), PixelDataType.CONST_STRING);
			retFilter = new SimpleQueryFilter(l, "!=", r);
		}

		// oh god.. I could say column + something is > something

		if(expr instanceof GreaterThan)
		{
			GreaterThan eExpr = (GreaterThan)expr;
			// there are only three choices may be four ok
			NounMetadata l = getNoun(eExpr.getLeftExpression());
			NounMetadata r = getNoun(eExpr.getRightExpression());
			if(l == null)
				l = new NounMetadata(eExpr.getLeftExpression().toString(), PixelDataType.CONST_STRING);
			if(r == null)
				r = new NounMetadata(eExpr.getRightExpression().toString(), PixelDataType.CONST_STRING);
			retFilter = new SimpleQueryFilter(l, ">", r);
		}

		if(expr instanceof MinorThan)
		{
			MinorThan eExpr = (MinorThan)expr;
			// there are only three choices may be four ok
			NounMetadata l = getNoun(eExpr.getLeftExpression());
			NounMetadata r = getNoun(eExpr.getRightExpression());
			if(l == null)
				l = new NounMetadata(eExpr.getLeftExpression().toString(), PixelDataType.CONST_STRING);
			if(r == null)
				r = new NounMetadata(eExpr.getRightExpression().toString(), PixelDataType.CONST_STRING);
			retFilter = new SimpleQueryFilter(l, "<", r);
		}

		if(expr instanceof GreaterThanEquals)
		{
			GreaterThanEquals eExpr = (GreaterThanEquals)expr;
			// there are only three choices may be four ok
			NounMetadata l = getNoun(eExpr.getLeftExpression());
			NounMetadata r = getNoun(eExpr.getRightExpression());
			if(l == null)
				l = new NounMetadata(eExpr.getLeftExpression().toString(), PixelDataType.CONST_STRING);
			if(r == null)
				r = new NounMetadata(eExpr.getRightExpression().toString(), PixelDataType.CONST_STRING);
			retFilter = new SimpleQueryFilter(l, ">=", r);
		}

		if(expr instanceof MinorThanEquals)
		{
			MinorThanEquals eExpr = (MinorThanEquals)expr;
			// there are only three choices may be four ok
			NounMetadata l = getNoun(eExpr.getLeftExpression());
			NounMetadata r = getNoun(eExpr.getRightExpression());
			if(l == null)
				l = new NounMetadata(eExpr.getLeftExpression().toString(), PixelDataType.CONST_STRING);
			if(r == null)
				r = new NounMetadata(eExpr.getRightExpression().toString(), PixelDataType.CONST_STRING);
			retFilter = new SimpleQueryFilter(l, "<=", r);
		}
		
		// >>> Text
		// LikeExpression
		// AnyComparisonExpression
		// Between
		// AllComparisonExpression
		if(expr instanceof LikeExpression)
		{
			LikeExpression eExpr = (LikeExpression)expr;
			// there are only three choices may be four ok
			NounMetadata l = getNoun(eExpr.getLeftExpression());
			NounMetadata r = getNoun(eExpr.getRightExpression());
			if(l == null)
				l = new NounMetadata(eExpr.getLeftExpression().toString(), PixelDataType.CONST_STRING);
			if(r == null)
				r = new NounMetadata(eExpr.getRightExpression().toString(), PixelDataType.CONST_STRING);
			retFilter = new SimpleQueryFilter(l, "?like", r);
		}
		if(expr instanceof InExpression)
		{
			InExpression eExpr = (InExpression)expr;
			// there are only three choices may be four ok
			NounMetadata l = getNoun(eExpr.getLeftExpression());
			ItemsList list = eExpr.getRightItemsList();
			NounMetadata r = null;
			if(list instanceof ExpressionList)
			{
				List<Expression> el = ((ExpressionList)list).getExpressions();
				List <Object> ol = new Vector<Object>();
				for(int elIndex = 0;elIndex < el.size();elIndex++)
				{
					NounMetadata thisVal = getNoun(el.get(elIndex));
					ol.add(thisVal.getValue());
				}
				r = new NounMetadata(ol, PixelDataType.VECTOR);
			}
			if(l != null && r != null)
			retFilter = new SimpleQueryFilter(l, "?like", r);
		}

		// not going to work the other pieces for now
		
		// >> Math
		// Addition
		// Subtraction
		// Division
		// Multiplication

		// addition etc.. will never be part of filter I dont think
		// I cant say where 2 +3 > 5 ? can I
		// oh god.. I could say column + something is > something
		/*
		if(expr instanceof Addition)
		{
			MinorThanEquals eExpr = (MinorThanEquals)expr;
			// there are only three choices may be four ok
			NounMetadata l = getNoun(eExpr.getLeftExpression());
			NounMetadata r = getNoun(eExpr.getRightExpression());
			retFilter = new SimpleQueryFilter(l, "<=", r);
		}

		*/
		
		return retFilter;
	}
	
	public NounMetadata getNoun(Expression expr)
	{
		
		// Column
		// DoubleValue
		// DateValue
		// LongValue
		// TimestampValue
		// TimeValue
		// StringValue
		// NullValue

		NounMetadata retData = null;
		
		if(expr instanceof LongValue)
		{
			long longValue = ((LongValue) expr).getValue();
			retData = new NounMetadata(longValue, PixelDataType.CONST_DECIMAL);
		}
		if(expr instanceof DoubleValue)
		{
			double longValue = ((DoubleValue) expr).getValue();
			retData = new NounMetadata(longValue, PixelDataType.CONST_DECIMAL);
		}
		if(expr instanceof StringValue)
		{
			String strValue = ((StringValue) expr).getValue();
			retData = new NounMetadata(strValue, PixelDataType.CONST_STRING);
		}
		if(expr instanceof DateValue)
		{
			// need to see about this
			Date dateValue = ((DateValue) expr).getValue();
			retData = new NounMetadata(dateValue+"", PixelDataType.CONST_STRING);
		}
		if(expr instanceof NullValue)
		{
			// need to see about this as well
			String strValue = ((NullValue) expr).toString();
			retData = new NounMetadata(strValue, PixelDataType.CONST_STRING);
		}
		if(expr instanceof Column)
		{
			String colValue = ((Column) expr).getColumnName();
			String tableValue = ((Column)expr).getTable().getName();
			if(tableAlias.containsKey(tableValue))
				tableValue = tableAlias.get(tableValue);
			
			colValue = tableValue + "__" + colValue;
			retData = new NounMetadata(colValue, PixelDataType.COLUMN);
		}
		return retData;
	}
	
	
	public void getAllExpressions(IQueryFilter prevFilter, IQueryFilter curFilter, List <Expression> exprVec)
	{
		// this is going to recurse
		// every single time 
		// when the expression comes in it will make it into
		// left and right.. 
		List <Expression> nextVec = new Vector<Expression>();
		for(int exprIndex = 0;exprIndex < exprVec.size();exprIndex++)
		{
			// I do the processing here
			Expression thisExpr = exprVec.get(exprIndex);
			if(thisExpr instanceof AndExpression) // || thisExpr instanceof OrExpression || thisExpr instanceof Parenthesis)
			{
				AndQueryFilter thisFilter = null;
				
				AndExpression andExpr = (AndExpression)thisExpr;
				// get the left operator
				Expression leftExpression = andExpr.getLeftExpression();
				Expression rightExpression = andExpr.getRightExpression();
				
				if(curFilter instanceof AndQueryFilter)
					// nothing much to do here
					thisFilter = (AndQueryFilter)curFilter;
				else
				{
					thisFilter = new AndQueryFilter();
					// need to add this to the curFilter
					// and add curFilter to this
				}
				
				// just call it back
				// it is always one at a time, there is no sibling, so there is no vector to say
				// it is a full binary tree
				//if(leftExpression)
				
			}
		}
		
		// the operation is divided into three things
		// left op operator and right
		// the operator is typically made as a the root of the node
		// with left and right hanging of it
		// Following are the operator types
		
		// >>> Atomic
		// Column
		// DoubleValue
		// DateValue
		// LongValue
		// TimestampValue
		// TimeValue
		// StringValue
		// NullValue
		
		
		// >>>> Logical
		// EqualsTo
		// NotEqualsTo
		// GreaterThan
		// GreaterThanEquals
		// MinorThan - because LessThan would be too easy
		// MinorThanEquals
		// AndExpression
		// OrExpression
		
		// >>> Text
		// LikeExpression
		// AnyComparisonExpression
		// Between
		// AllComparisonExpression
		
		// >> Math
		// Addition
		// Subtraction
		// Division
		// Multiplication

		// >> Times when you will be in deep shit and need more elaborate thought
		// CaseExpression
		// Parenthesis
		// SubSelect
		
		
		
	}

	
	public static void main(String [] args) throws Exception {
		SqlParser test = new SqlParser();
		String query = "Select * from employee";
		query =  "select distinct c.logicalname ln, (ec.physicalName + 1) ep from "
				+ "concept c, engineconcept ec, engine e inner join sometable s on c.logicalname=s.logical where (ec.localconceptid=c.localconceptid and "
				+ "c.conceptualname in ('val1', 'val2')) or (ec.localconceptid + 5) =1 group by ln order by ln limit 200 offset 50 ";// order by c.logicalname";
		test.processQuery(query);
	}
}

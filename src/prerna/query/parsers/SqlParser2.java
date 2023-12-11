package prerna.query.parsers;

import java.sql.Time;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.Vector;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.GroupByElement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.ParenthesisFromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SetOperation;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.WithItem;
import prerna.query.querystruct.FunctionExpression;
import prerna.query.querystruct.GenExpression;
import prerna.query.querystruct.InGenExpression;
import prerna.query.querystruct.OperationExpression;
import prerna.query.querystruct.OrderByExpression;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.WhenExpression;
import prerna.query.querystruct.filters.IQueryFilter;


/*
 * Select > SelectBody > 
 * 				PlainSelect - Simple one
 * 					- Select - SelectItem - Simple COlumn / Expression 
 * 				SetOperationsList - Union ?
 * 				WithItem - WithItemsList (SelectItem)
 * 					- Select Body (Recursion)
 * 		  > FromItem
 * 				> Table - simple case - sometimes alieas is there sometimes no alias
 * 				> Subselect - this is the case of it is full on paranthesis and output - the paranthesis is established through isUseBrackets / setUseBrackets
 * 					> SelectBody (recursion)
 * 				> TableFunction
 * 				> ParanthesisFromItem
 * 				> SpecialSubselect
 * 				> ValuesList - ColumnNames, ExpressionList
 * 		> Join - On expression, isLeft etc. using Columns
 * 				> FromItem - RightItem (Recursion)
 * 				> Expression - This can be a loop of its own - We have a shit ton of stuff here to deal with
 * 					> SubSelect
 * 					   > Select Body - Recursion
 * 		> Where
 * 				> Expression (Recursion)
 * 					
 * 		
 *		TODO
 *		1. get all the real columns and tables
 *		2. Be able to work with create
 * 		3. Be able to substitute one column for another column
 * 
 * 
 * 
 * 
 */



public class SqlParser2 {

	// to determine type of the expression
//	private static enum EXPR_TYPE {LEFT, RIGHT, INNER};

	GenExpression qs = null;
	GenExpressionWrapper wrapper = new GenExpressionWrapper();
	boolean binary = false;
	boolean column = false;
	public boolean parameterize = true;
	String columnName = null;
	
	boolean processCase = false;
	boolean processAllBinary = false;
	Stack <Boolean> processParam = new Stack<Boolean>();
	

	public SqlParser2() {
		this.wrapper.tableAlias = new Hashtable <String, String>();
		this.wrapper.columnAlias = new Hashtable <String, String>();
		this.wrapper.schema = new Hashtable<String, Set<String>>();
	}

	public GenExpressionWrapper processQuery(String query) throws Exception 
	{

		wrapper = new GenExpressionWrapper();
		//storing the with clause in qs
		List<GenExpression> withqs = new ArrayList<>();
        List<String> withAlias = new ArrayList<>();
		// this is the main query struct
		GenExpression qs = new GenExpression();
		// parse the sql
		Statement stmt = CCJSqlParserUtil.parse(query);
		Select select = ((Select)stmt);
		
		if(select.getSelectBody() instanceof PlainSelect)
		{
			PlainSelect sb = (PlainSelect)select.getSelectBody();
			List<WithItem> withItemList = select.getWithItemsList();
            if (withItemList != null) {
                for (WithItem wi : withItemList) {
                    PlainSelect wbody = (PlainSelect) wi.getSelectBody();
                    String asName = wi.getName();

                    if (wbody instanceof PlainSelect) {
                        GenExpression withstruct = processSelect(null, (PlainSelect) wbody);
                        withqs.add(withstruct);
                        withAlias.add(asName);
                    }
                }
			}		
			qs = processSelect(null, sb);
			qs.setWithFrom(withAlias);
            qs.setWithList(withqs);
		}
		if(select.getSelectBody() instanceof SetOperationList)
		{
			qs = processOperation((SetOperationList)select.getSelectBody());
		}
				
			
		this.qs = qs;
		wrapper.root = qs;
		
		return wrapper;
	}
	
	
	
	public String generateQuery(SelectQueryStruct qs) throws Exception
	{
		String finalQuery = ((GenExpression)qs).printQS(((GenExpression)qs), new StringBuffer()).toString(); 
		
		System.err.println(finalQuery);
		
		// the real test is can I parse it back :)
		//Statement stmt = CCJSqlParserUtil.parse(finalQuery);
		
		System.err.println("Success ");
		return finalQuery;
	}
	
	
	
	public GenExpression processSelect(GenExpression qs, PlainSelect sb)
	{
		//if(sb.toString().contains("UNION"))
		//	System.err.println("Found it.. " + sb);
		wrapper.numSubSelects++; // if it is the first time this will equal to zero
		
		GenExpression thisQs = null;
		//System.err.println("Select " + sb);
		if(qs == null)
		{
			qs = new GenExpression();
			thisQs = qs;
			
		}
		//else
		{
			thisQs = new GenExpression();
			thisQs.parent = qs; // set the parent here
			thisQs.aQuery = sb.toString();
			thisQs.operation = "select";
			thisQs.aliasHash = qs.aliasHash;
			thisQs.randomHash = qs.randomHash;
		}
		
		// calculate distinc
		Distinct dis = sb.getDistinct();
		if(dis != null)
		{
			thisQs.distinct = true;
			/*
			// not sure when this is even used.. but setting at qs level
			List <SelectItem> allSelects = dis.getOnSelectItems();
			System.err.println("Items .. " + allSelects);
			*/
		}		
		FromItem fi = sb.getFromItem();
		
		// can this be null ?
		String alias = "";
		if(fi.getAlias() != null) {
			alias = fi.getAlias().getName();
		}
		List<SelectItem> items = sb.getSelectItems();		

		// this is the simple case
		//if(fi instanceof Table)
		{
			/*Table fromTable = (Table) sb.getFromItem();
			String fromTableName = fromTable.getName();
	
			// if there is no alias
			// we will determine to set the table as the alias
			Alias fromTableAliasObj = fromTable.getAlias();
			if(fromTableAliasObj != null) {
				tableAlias.put(fromTable.getAlias().getName(), fromTable.getName());
			} else {
				tableAlias.put(fromTableName, fromTableName);
			}
			*/
			
			
			if(fi instanceof Table)
			{
				Table fromTable = (Table) sb.getFromItem();
				String fromTableName = fromTable.getName();
				String fromTableAlias = null;
				Alias fromTableAliasObj = fromTable.getAlias();
				if(fromTableAliasObj != null) {
					fromTableAlias = fromTableAliasObj.getName();
					this.wrapper.tableAlias.put(fromTableAlias, fromTableName);
				} else {
					this.wrapper.tableAlias.put(fromTableName, fromTableName);
				}
				thisQs.currentTable = fromTableName;
				thisQs.currentTableAlias = fromTableAlias;
				
				GenExpression fromExpr = new GenExpression();
				fromExpr.setOperation("from");
				fromExpr.setComposite(false);
				fromExpr.aQuery = fi.toString();
				fromExpr.setLeftExpr(fromTableName);
				fromExpr.setLeftAlias(alias);
				thisQs.from = fromExpr;
				
				// tracking tables
				List <GenExpression>selectList = null;
				if(this.wrapper.tableSelect.containsKey(fromTableName)) {
					selectList = this.wrapper.tableSelect.get(fromTableName);
				} else {
					selectList = new Vector<GenExpression>();
				}
				if(!selectList.contains(qs)) {
					selectList.add(qs);
				}
				
				this.wrapper.tableSelect.put(fromTableName, selectList);
			}
			else if(fi instanceof PlainSelect)
			{
				thisQs.currentTable = fi.getAlias().getName();
			}
			else if(fi instanceof SubSelect)
			{
				thisQs.currentTable = fi.getAlias().getName();
			}
			else if(fi instanceof ParenthesisFromItem)
			{
				System.err.println("Got you here");
				thisQs.currentTable = fi.getAlias().getName();
			}

			
			// process the joins into the QS
			fillJoins(thisQs, sb.getJoins());
	
			// now that we have the joins
			// we also have the table aliases we need
			// so we can add the selectors
			fillSelects(thisQs, items);
	
			// fill the filters
			fillFilters(thisQs, null, sb.getWhere());
	
			// fill the groups
			fillGroups(thisQs, sb.getGroupBy());
	
			// fill the order by
			fillOrder(thisQs, sb.getOrderByElements());
	
			// fill the limit
			fillLimitOffset(thisQs, sb.getLimit());
			
			//return thisQs;
		}
		
		// at this point
		// I need to capture this back and then 
		// and then put this back as an alias
		GenExpression substruct = processSelectFromItem(fi, thisQs);
		if(substruct != null)
		{
			substruct.setLeftAlias(alias);
			//substruct.setComposite(true);
			
			thisQs.setComposite(true);
			thisQs.aliasHash.put(alias, substruct);
			thisQs.from = substruct;
		}
		return thisQs;
	}
	
	public GenExpression processSelectFromItem(FromItem fi, GenExpression thisQs)
	{
		if(fi instanceof SubSelect)
		{
			SelectBody sbody = ((SubSelect)fi).getSelectBody();
			if(sbody instanceof PlainSelect)
			{
				GenExpression substruct =  processSelect(thisQs, (PlainSelect)sbody);
				substruct.setComposite(true);
				return substruct;
			}
			else if(sbody instanceof SetOperationList)
			{
				//System.err.println("Et Tu Union ?");
				return processOperation((SetOperationList)sbody);
			}
		}
		else if(fi instanceof SetOperationList)
		{
			//System.err.println("Into the union ? To be handled" + fi);
			return processOperation((SetOperationList)fi);
		}
		else if(fi instanceof Table)
		{
			String fromTableName = "";
			Table fromTable = (Table) fi;
			fromTableName = fromTable.getName();
			String fromTableAlias = null;
			Alias tableAlias = fromTable.getAlias();
			if(tableAlias != null) {
				fromTableAlias = tableAlias.getName();
			}
			thisQs.currentTable = fromTableName;
			thisQs.currentTableAlias = fromTableAlias;
			GenExpression fromExpr = new GenExpression();
			fromExpr.setOperation("from");
			fromExpr.setComposite(false);
			fromExpr.aQuery = fi.toString();
			fromExpr.setLeftExpr(fromTableName);
			//fromExpr.setLeftAlias(alias);
			thisQs.from = fromExpr;
			
			// tracking tables
			List <GenExpression>selectList = null;
			if(this.wrapper.tableSelect.containsKey(fromTableName))
				selectList = this.wrapper.tableSelect.get(fromTableName);
			else
				selectList = new Vector<GenExpression>();
			if(!selectList.contains(qs))
				selectList.add(qs);
			
			
			this.wrapper.tableSelect.put(fromTableName, selectList);
			return fromExpr;
		}

		else if(fi instanceof ParenthesisFromItem)
		{
			//System.err.println(" From Item is " + fi);
			GenExpression gep = processSelectFromItem(((ParenthesisFromItem)fi).getFromItem(), thisQs);
			gep.setComposite(true);
			gep.paranthesis = true;
			return gep;
		}
		return null;

	}
	
	/**
	 * Add the selectors into the query struct
	 * @param qs
	 * @param selects
	 */
	public void fillSelects(SelectQueryStruct qs, List<SelectItem> selects) {
		for(int selectIndex = 0;selectIndex < selects.size();selectIndex++) {
			SelectItem si = selects.get(selectIndex);
			if(si instanceof SelectExpressionItem) {
				SelectExpressionItem sei = (SelectExpressionItem) si;
				Alias seiAlias = sei.getAlias();				
				GenExpression gep = processExpression(qs, sei.getExpression(), null);
				
				if(seiAlias != null) {
					gep.setLeftAlias(seiAlias.getName());
				}
				qs.nselectors.add(gep);
				
				// process for column cleanup
				if(seiAlias != null && this.wrapper.columnSelect.containsKey(seiAlias.getName())) {
					// remove it / add it as the actual one
					// it is already accommodated for some other place
					this.wrapper.columnSelect.remove(seiAlias.getName());					
				}
			} else if(si instanceof AllTableColumns || si instanceof AllColumns) 	{
				GenExpression gep = new GenExpression();
				gep.aQuery = si.toString();
				gep.setLeftExpr(si.toString()); 
				gep.setOperation("opaque");
				qs.nselectors.add(gep);
			}
		}
	}

	/**
	 * Add the joins and store table aliases used
	 * @param qs
	 * @param tableName
	 * @param joins
	 */
	public void fillJoins(GenExpression qs, List <Join> joins) {
		// if there are no joins
		// nothing to do
		if(joins == null || joins.isEmpty()) {
			return;
		}

		// joins are all sitting onproces
		// select.getJoins()
		// each one of which is telling what type of join it is
		// for the case of engineconcept ec and engine e
		// the join seems to say simple is true
		// the last one it says simple is false and it also puts an equation to it
		// each join has a table associated with it
		// sb.joins.get(index).rightitem - table and alias
		// sb.join.get(index).onExpression - tells you the quals to expression

		for(int joinIndex = 0; joinIndex < joins.size(); joinIndex++) 
		{
			Join thisJoin = joins.get(joinIndex);
			
			FromItem fi = thisJoin.getRightItem();

			String rightTableName = null;
			String rightTableAlias = null;
			// if somebody -- need to see if sql grammar can accomodate for stupidity where alias and table are same kind of
			//tableAlias.put(rightTableName, rightTableAlias);
			//tableAlias.put(rightTableAlias, rightTableName);
			
			Expression curJoinExpr = thisJoin.getOnExpression();
			GenExpression expr = processJoinExpression(qs, null, thisJoin);
			qs.joins.add(expr);
			//qs.randomHash.put(expr, expr);
		}
	}
	
	// recurse through the join expressions to get the final
	public GenExpression processJoinExpression(GenExpression qs, GenExpression expr, Join thisJoin) 
	{

		// join is a gen expression
		// join is set with a telescope to true
		// the base join has
		// body - This is the on part of the overall. See the comments below. This is a AST so if I have a = b and c = d then I will have 3 gen expressions. 1. GenExpression (a = b), 2. GenExpression(c = d) 3. 1. AND 2. 
		// the = or AND is the operation
		// left item is the left operand and the right item is the right operand
		// operation - set to join
		// join type = tells you if it is inner etc. 
		// right item is another SQL expression if there is one
		// from is the actual query
		
		// TODO - process using columns
		GenExpression gep = new GenExpression(); // this is the one I am processing for the actual join itself. 
		GenExpression retExpr = processExpression(qs, thisJoin.getOnExpression(), expr); // this processes the on stuff i.e. on a = b. This has the a = b
		gep.telescope = true;
		gep.body = retExpr;
		gep.aQuery = thisJoin.toString();
		gep.parent = qs;
		
		// process the from
		FromItem fi = thisJoin.getRightItem();
		if(fi instanceof ParenthesisFromItem)
			System.err.println("Found the culprint");
		
		GenExpression from2 = new GenExpression();

		String joinType = null;
		// add the join type etc. 
		if(thisJoin.isInner()) {
			joinType = "inner join";
		} else if(thisJoin.isLeft()) {
			joinType = "left outer join";
		} else if(thisJoin.isRight()) {
			joinType = "right outer join";
		} else if(thisJoin.isOuter()) {
			joinType = "outer join";
		} else if(thisJoin.isFull()) {
			joinType = "full join";
		}else if(thisJoin.isCross()) {
			joinType = "cross join";
		}
		if(joinType == null)
			joinType = "JOIN";
		gep.setOperation("join");
		gep.setOn(joinType);
		//qs.joins.add(gep);

		from2 = processJoinFromItem(fi, thisJoin, gep);
		
		//System.err.println("From item is " + fi);
		// need to accomodate for paranthesis from item
		
		from2.parent = gep;
		gep.from = from2;
		
	return gep;

	}
	
	private GenExpression processJoinFromItem(FromItem fi, Join thisJoin, GenExpression gep)
	{
		String rightTableName, rightTableAlias = null;
		GenExpression from2 = new GenExpression();
		if(fi instanceof Table)
		{
			Table rightTable = (Table)fi;
			// add the alias
			rightTableName = rightTable.getName();
			rightTableAlias = rightTableName;
			if(rightTable.getAlias() != null)
				rightTableAlias = rightTable.getAlias().getName();
			
			// turn this into a full from
			from2.setOperation("table");
			from2.setLeftExpr(rightTableName);
			from2.setLeftAlias(rightTableAlias);
			from2.parent = gep;
			return from2;
		}
		else if(fi instanceof SubSelect)
		{
			//System.out.println("Right is" + fi);
			rightTableName = fi.getAlias().getName();
			rightTableAlias = rightTableName;
			String alias = fi.getAlias().getName();
		
			SelectBody sbody = ((SubSelect)fi).getSelectBody();
			if(sbody instanceof PlainSelect)
			{
				from2 =  processSelect(qs, (PlainSelect)sbody);
				from2.operation = "querystruct";
				from2.telescope = true;
				from2.setComposite(true);
				//from2.body = substruct;
				from2.setLeftAlias(alias);
				//qs.joins.add(substruct);
				// add it to the current qs
				// this alias hash 
				//qs.aliasHash.put(alias, substruct);
			}
			// union if it is a set operations list
			else if(sbody instanceof SetOperationList)
			{
				from2 = processOperation((SetOperationList)sbody);
				from2.telescope = true;
				//from2.operation = "union";
				from2.setComposite(true);
				from2.setLeftAlias(alias);
				//from2.body = opQS;
				//gep.randomHash.put("UNION", opQS);
				
				//qs.joins.add(opQS);
			}
			return from2;
		}
		else if(fi instanceof ParenthesisFromItem)
		{
			FromItem innerFromItem = ((ParenthesisFromItem)fi).getFromItem();
			from2 = processJoinFromItem(innerFromItem, thisJoin, gep);
			from2.paranthesis = true;
			from2.setComposite(true);
			//GenExpression processedExpr = processSelect(qs, );
			return from2;
		}
		return from2;
		
	}
	
	public GenExpression processExpression(SelectQueryStruct qs, Expression joinExpr, GenExpression expr)
	{
		// these are either composite relations like and or etc. or simple relations
		if(expr != null && qs != null)
		{
			//expr.parentStruct = (GenExpression)qs;
		}
		if(joinExpr instanceof AndExpression) // || joinExpr instanceof OrExpression)
		{
			GenExpression expr2 = new GenExpression();
			AndExpression aExpr = (AndExpression)joinExpr;
			expr2.setOperation(aExpr.getStringExpression());
			expr2.aQuery = joinExpr.toString();
			
			// this is composite
			expr2.setComposite(true);
			//expr.setExpression(joinExpr.toString());

			Expression left = aExpr.getLeftExpression();
			Expression right = aExpr.getRightExpression();
			expr2.recursive = true;

			wrapper.currentOperator.push("and");
			wrapper.andCount++;
			wrapper.procOrder.put("and" + wrapper.andCount, true);
			wrapper.contextExpression.push(joinExpr.toString());
			
			// process the left and right
			GenExpression leftExpr = processExpression(qs, left,  expr);
			GenExpression rightExpr = processExpression(qs, right,  expr);
			
			expr2.setLeftExpresion(leftExpr);
			expr2.setRightExpresion(rightExpr);
			
			expr2.parent = (GenExpression)qs;
			
			//wrapper.fillParameters();
			//System.out.println("Arbitrary gen expression print" + expr2.printQS(expr2,null));
			
			wrapper.currentOperator.pop();
			wrapper.contextExpression.pop();
			wrapper.andCount--;
			//wrapper.right.pop();
			return expr2;
			//System.err.println("Expression.. " + aExpr.getStringExpression());
		}
		else if(joinExpr instanceof OrExpression) // || joinExpr instanceof OrExpression)
		{
			GenExpression expr2 = new GenExpression();
			OrExpression aExpr = (OrExpression)joinExpr;
			expr2.setOperation(aExpr.getStringExpression());
			expr2.aQuery = joinExpr.toString();
			
			// this is composite
			expr2.setComposite(true);
			//expr.setExpression(joinExpr.toString());

			Expression left = aExpr.getLeftExpression();
			Expression right = aExpr.getRightExpression();
			expr2.recursive = true;

			wrapper.currentOperator.push("or");
			wrapper.orCount++;
			wrapper.procOrder.put("or" + wrapper.andCount, true);
			wrapper.contextExpression.push(joinExpr.toString());
			
			// process the left and right
			GenExpression leftExpr = processExpression(qs, left,  expr);
			GenExpression rightExpr = processExpression(qs, right,  expr);
			
			expr2.setLeftExpresion(leftExpr);
			expr2.setRightExpresion(rightExpr);
			
			expr2.parent = (GenExpression)qs;
			
			wrapper.currentOperator.pop();
			wrapper.contextExpression.pop();
			wrapper.orCount--;
			return expr2;
			//System.err.println("Expression.. " + aExpr.getStringExpression());
		}
		// only the binary expression has 2 sides
		else if(joinExpr instanceof BinaryExpression  
				&& 	(processAllBinary || IQueryFilter.comparatorIsValidSQL(((BinaryExpression) joinExpr).getStringExpression())))
		{
			boolean paramBinary = true;
			
			if(processParam.size() > 0)
			{
				// get the latest and push it back
				paramBinary = processParam.pop();
				processParam.push(paramBinary);
			}

			
			    this.binary = true;
			// do the regular stuff.. I need to accomodate for other pieces but
			//if(!simple) 
			//{
				// this is another fractal that needs to be taken care of
				// so it is like and / or and then below that you have the equal expression etc.
				GenExpression eqExpr = new GenExpression();
				eqExpr.setComposite(true);
				eqExpr.aQuery = joinExpr.toString();
				BinaryExpression joinExpr2 = (BinaryExpression)joinExpr;
				eqExpr.setOperation(joinExpr2.getStringExpression());
				
				// YEAR_ID(left Expression) = 123 (rightExpression)
				String operator = eqExpr.aQuery;
				/*
				// remove left expression
				operator = operator.replace(joinExpr2.getLeftExpression()+"", "");
				operator = operator.replace(joinExpr2.getRightExpression()+"", "");
				operator = operator.trim();
				
				// do need operator here again ?
		
				 */
				String modifier = wrapper.uniqueCounter +"";

				// need ome way to put the or here
				/*
				if(joinExpr2.getStringExpression().equalsIgnoreCase("or"))
				{
					wrapper.currentOperator = joinExpr2.getStringExpression();
				}*/
				if(wrapper.currentOperator.size() > 0)
				{
					operator = wrapper.currentOperator.pop();
					Boolean left = false;
					int count = 0;
					if(operator.equalsIgnoreCase("and")) {
						count = wrapper.andCount;
					} else {
						count = wrapper.orCount;
					}
					count = wrapper.uniqueCounter;
					left = wrapper.procOrder.get(operator + count);
					if(left == null || left) {
						modifier = operator + count + "_left";
						wrapper.procOrder.put(operator + count, false);
						//wrapper.left = true;
					} else {
						modifier = operator + count + "_right";
					}
					wrapper.currentOperator.push(operator);
				}
				
				
				//System.err.println("Expression.. " + joinExpr2.getStringExpression());
				
				String full_from = null;
				String full_To = null;
				
				GenExpression sqs = processExpression(qs,  joinExpr2.getLeftExpression(), eqExpr);
				GenExpression sqs2 = processExpression(qs,  joinExpr2.getRightExpression(), eqExpr);
				
				Object constantValue = null;
				String constantType = null;
				GenExpression exprToTrack = null;
				String tableName = null;
				String aliasName = null;
				
				if(paramBinary) // dont do it for case
				{
					if(((GenExpression)sqs).getOperation().equalsIgnoreCase("column"))
					{
						full_from = ((GenExpression)sqs).getLeftExpr();
						column = true;
						columnName = full_from;
						tableName = ((GenExpression)sqs).tableName;
						aliasName = columnName;
						if(sqs.userTableAlias != null)
							aliasName = sqs.userTableAlias;
	
					}
					else if( (((GenExpression)sqs).getOperation().equalsIgnoreCase("string")) 
							|| (((GenExpression)sqs).getOperation().equalsIgnoreCase("double")) 
							|| (((GenExpression)sqs).getOperation().equalsIgnoreCase("date")) 
							|| (((GenExpression)sqs).getOperation().equalsIgnoreCase("time")) 
							|| (((GenExpression)sqs).getOperation().equalsIgnoreCase("long")) )
					{
						// we got our target
						constantValue = ((GenExpression)sqs).leftItem;
						constantType = ((GenExpression)sqs).getOperation();
						if(columnName != null)
						{
							((GenExpression)sqs).setLeftExpresion("'<" + tableName + "_" + columnName + modifier + eqExpr.getOperation().trim() + ">'");
						}
						exprToTrack = sqs;
						
					}
					// If the operation is function, get the data from the expression
					else if ((((GenExpression) sqs).getOperation().equalsIgnoreCase("function"))) {
						// Casting to FunctionExpression to get the expression data
						FunctionExpression fnsqs = (FunctionExpression) sqs;
						// in case we have function inside function
						while(fnsqs.expressions.size() > 0 && fnsqs.expressions.get(0).getOperation().equals("function")) {
							fnsqs = (FunctionExpression) fnsqs.expressions.get(0);
						}
						if (fnsqs.expressions.size() > 0) {
							// Going with the normal flow
							if (fnsqs.expressions.get(0).getOperation().equalsIgnoreCase("column")) {
								full_from = fnsqs.expressions.get(0).getLeftExpr();
								column = true;
								columnName = full_from;
								tableName = fnsqs.expressions.get(0).tableName;
								aliasName = columnName;
								if (fnsqs.expressions.get(0).userTableAlias != null) {
									aliasName = fnsqs.expressions.get(0).userTableAlias;
								}
							} else if(fnsqs.expressions.get(0).getOperation().equalsIgnoreCase("cast")) {
								GenExpression innerExpression = (GenExpression) fnsqs.expressions.get(0).leftItem;
								full_from = innerExpression.aQuery;
								column = true;
								columnName = innerExpression.getLeftExpr();
								tableName = innerExpression.tableName;
								aliasName = columnName;
								if (fnsqs.expressions.get(0).userTableAlias != null) {
									aliasName = fnsqs.expressions.get(0).userTableAlias;
								}
							} else if ((((GenExpression) fnsqs.expressions.get(0)).getOperation()
									.equalsIgnoreCase("string"))
									|| (((GenExpression) fnsqs.expressions.get(0)).getOperation()
											.equalsIgnoreCase("double"))
									|| (((GenExpression) fnsqs.expressions.get(0)).getOperation()
											.equalsIgnoreCase("date"))
									|| (((GenExpression) fnsqs.expressions.get(0)).getOperation()
											.equalsIgnoreCase("time"))
									|| (((GenExpression) fnsqs.expressions.get(0)).getOperation()
											.equalsIgnoreCase("long"))) {

								constantValue = ((GenExpression) fnsqs.expressions.get(0)).leftItem;
								constantType = ((GenExpression) sqs2).getOperation();
							}
						}
					}
	
					if(((GenExpression)sqs2).getOperation().equalsIgnoreCase("column"))
					{
						// who knows you could be a sadist after all
						full_To = ((GenExpression)sqs2).getLeftExpr();
						column = true;
						columnName = full_To;
						tableName = ((GenExpression)sqs2).tableName;
						aliasName = columnName;
						if(sqs2.userTableAlias != null)
							aliasName = sqs2.userTableAlias;
					}
					else if( (((GenExpression)sqs2).getOperation().equalsIgnoreCase("string")) 
							|| (((GenExpression)sqs2).getOperation().equalsIgnoreCase("double")) 
							|| (((GenExpression)sqs2).getOperation().equalsIgnoreCase("date")) 
							|| (((GenExpression)sqs2).getOperation().equalsIgnoreCase("time")) 
							|| (((GenExpression)sqs2).getOperation().equalsIgnoreCase("long")) )
					{
						// we got our target
						constantValue = ((GenExpression)sqs2).leftItem;
						constantType = ((GenExpression)sqs2).getOperation();
			
						if(columnName != null && parameterize)
						{
							// replace the parameter value so at a later point some one can change it
							// the value is now replaced with table_column_left or right of the and expression followed by operation
							((GenExpression)sqs2).setLeftExpresion("<" + tableName + "_" + columnName + modifier + eqExpr.getOperation().trim() + ">");
						}
						exprToTrack = sqs2;
	
					} 
					// If the operation is function, get the data from the expression
					else if ((((GenExpression) sqs2).getOperation().equalsIgnoreCase("function"))) {
						// Casting to FunctionExpression to get the expression data
						FunctionExpression fnsqs2 = (FunctionExpression) sqs2;
						// in case we have function inside function
						while(fnsqs2.expressions.size() > 0 && fnsqs2.expressions.get(0).getOperation().equals("function")) {
							fnsqs2 = (FunctionExpression) fnsqs2.expressions.get(0);
						}
						if (fnsqs2.expressions.size() > 0) {
							// Going with the normal flow
							if (((GenExpression) fnsqs2.expressions.get(0)).getOperation().equalsIgnoreCase("column")) {
								full_from = ((GenExpression) fnsqs2.expressions.get(0)).getLeftExpr();
								column = true;
								columnName = full_from;
								tableName = ((GenExpression) fnsqs2.expressions.get(0)).tableName;
								aliasName = columnName;
								if (fnsqs2.expressions.get(0).userTableAlias != null) {
									aliasName = ((GenExpression) fnsqs2.expressions.get(0)).userTableAlias;
								}
							} else if(fnsqs2.expressions.get(0).getOperation().equalsIgnoreCase("cast")) {
								GenExpression innerExpression = (GenExpression) fnsqs2.expressions.get(0).leftItem;
								full_from = innerExpression.aQuery;
								column = true;
								columnName = innerExpression.getLeftExpr();
								tableName = innerExpression.tableName;
								aliasName = columnName;
								if (fnsqs2.expressions.get(0).userTableAlias != null) {
									aliasName = fnsqs2.expressions.get(0).userTableAlias;
								}
							}else if ((((GenExpression) fnsqs2.expressions.get(0)).getOperation()
									.equalsIgnoreCase("string"))
									|| (((GenExpression) fnsqs2.expressions.get(0)).getOperation()
											.equalsIgnoreCase("double"))
									|| (((GenExpression) fnsqs2.expressions.get(0)).getOperation()
											.equalsIgnoreCase("date"))
									|| (((GenExpression) fnsqs2.expressions.get(0)).getOperation()
											.equalsIgnoreCase("time"))
									|| (((GenExpression) fnsqs2.expressions.get(0)).getOperation()
											.equalsIgnoreCase("long"))) {

								constantValue = ((GenExpression) fnsqs2.expressions.get(0)).leftItem;
								constantType = ((GenExpression) sqs2).getOperation();
							}
						}
					}

					if(binary && column && tableName != null && constantValue != null)
					{
						String defQuery = "Select q1." + aliasName + " from (" + qs + ") q1";
						this.wrapper.makeParameters(columnName, constantValue, modifier + eqExpr.getOperation().trim(), eqExpr.getOperation().trim(), constantType, exprToTrack, tableName, defQuery);
					}
					
					binary = false;
					column = false;
					columnName = null;
					
				}				
				
//				// keep track of column and table in schema
//				// from 
//				Set<String> columns = new HashSet<String>();
//				if(fromTable != null && this.schema.containsKey(fromTable)) {
//					columns = this.schema.get(fromTable);
//				}
//				columns.add(fromColumn);
//				this.schema.put(fromTable, columns);
//				// to
//				Set<String> columns2 = new HashSet<String>();
//				if(this.schema.containsKey(rightTableName)) {
//					columns2 = this.schema.get(rightTableName);
//				}
//				columns2.add(toColumn);
//				this.schema.put(rightTableName, columns2);
//				
				// need to translate the alias into column name				
				eqExpr.recursive = true;
				eqExpr.setLeftExpresion(sqs);
				eqExpr.setRightExpresion(sqs2);
				eqExpr.setComposite(false);
				eqExpr.setExpression(joinExpr2.toString());
				eqExpr.setLeftExpr(full_from);
				eqExpr.setRightExpr(full_To);
				
				eqExpr.parent = (GenExpression)qs;
				return eqExpr;
		}	
		// need to handle the between join
		else if(joinExpr instanceof SubSelect)
		{
			// need to process this again
			SubSelect ss = (SubSelect)joinExpr;
			SelectBody sb = ss.getSelectBody();
			String alias = null;
			if(ss.getAlias() != null) {
				alias = ss.getAlias().getName();
			}
			
			// this can be something else other than plain select
			if(sb instanceof PlainSelect)
			{
				GenExpression ge = new GenExpression();
				ge.aliasHash = qs.aliasHash;
				ge.randomHash = qs.randomHash;
				ge.setOperation("querystruct");
				ge.telescope = true;
				
				GenExpression sqs = processSelect(ge,  (PlainSelect)ss.getSelectBody());
				ge.body = sqs;
				sqs.parent = (GenExpression)ge;
				if(alias != null) {
					qs.aliasHash.put(alias, sqs);
				}
				return ge;
			}
			else if(sb instanceof SetOperationList)
			{
				//System.err.println("This is probably unin ?" + joinExpr);
				GenExpression gep = processOperation((SetOperationList)sb);
				gep.parent = (GenExpression)qs;
				return gep;
			}
				
		}
		else if(joinExpr instanceof Between)// between // this needs to be handled better
		{
			boolean paramBetween = true;
			
			if(processParam.size() > 0)
			{
				// get the latest and push it back
				paramBetween = processParam.pop();
				processParam.push(paramBetween);
			}
			
			//System.err.println("This is a between expression " + joinExpr);
			GenExpression retExpr = new GenExpression();
			retExpr.setComposite(true);
			retExpr.setOperation("between");
			retExpr.aQuery = joinExpr.toString();
			retExpr.recursive = true;
			String modifier = wrapper.uniqueCounter + "";
			// left and right re set as start and end
			// the actual expression is set up as the body
			
			Between betw = (Between)joinExpr;
			Expression leftExpr = betw.getLeftExpression();
			retExpr.body = processExpression(qs, leftExpr, retExpr);
			Expression start = betw.getBetweenExpressionStart();
			Expression end = betw.getBetweenExpressionEnd();
			//System.err.println("I am missing something here " + joinExpr);
			GenExpression startExpression = processExpression(qs, start, retExpr);
			retExpr.setLeftExpresion(startExpression);
			GenExpression endExpression = processExpression(qs, end, retExpr);
			retExpr.setRightExpresion(endExpression);
			
			retExpr.parent = (GenExpression)qs;
			String tableName = null;
			String aliasName = null;
			
			if(retExpr.body.getOperation().equalsIgnoreCase("column"))
			{
				// who knows you could be a sadist after all
				column = true;
				columnName = retExpr.body.getLeftExpr();
				tableName = retExpr.body.tableName;
				aliasName = columnName;
				if(retExpr.body.userTableAlias != null)
					aliasName = retExpr.body.userTableAlias;
	
			}

			if(paramBetween && tableName != null)
			{
				// process the start value if it is a constant
				if( (startExpression.getOperation().equalsIgnoreCase("string")) 
						|| startExpression.getOperation().equalsIgnoreCase("double") 
						|| startExpression.getOperation().equalsIgnoreCase("date") 
						|| startExpression.getOperation().equalsIgnoreCase("time") 
						|| startExpression.getOperation().equalsIgnoreCase("long") )
				{
					// we got our target
					Object constantValue = startExpression.leftItem;
					String constantType = startExpression.getOperation();
					
					if(columnName != null && parameterize)
					{
						startExpression.setLeftExpresion("'<" + tableName + "_" + columnName + modifier + "between.start" + ">'");
					}
					
					String defQuery = "Select q1." + aliasName + " from (" + qs + ") q1";
					String compositeName = this.wrapper.makeParameters(columnName, constantValue, modifier + "between.start", "between.start",  constantType, startExpression, tableName, defQuery);
					startExpression.setLeftExpresion("'<" + compositeName + ">'");
					System.out.println("Parameterizing  " + columnName + endExpression);
					System.out.println("Query is set to..  " + qs);
	
				}
			
				// process the end value if it is a constant
				if( (endExpression.getOperation().equalsIgnoreCase("string")) 
						|| endExpression.getOperation().equalsIgnoreCase("double") 
						|| endExpression.getOperation().equalsIgnoreCase("date") 
						|| endExpression.getOperation().equalsIgnoreCase("time") 
						|| endExpression.getOperation().equalsIgnoreCase("long") )
				{
					// we got our target
					Object constantValue = endExpression.leftItem;
					String constantType = endExpression.getOperation();
					
					if(columnName != null && parameterize)
					{
						endExpression.setLeftExpresion("'<" + tableName + "_" + columnName  + modifier + "between.end" + ">'");
					}
					
					String defQuery = "Select q1." + aliasName + " from (" + qs + ") q1";
					String compositeName = this.wrapper.makeParameters(columnName, constantValue, modifier + "between.end", "between.end",  constantType, endExpression, tableName, defQuery);
					endExpression.setLeftExpresion("'<" + compositeName + ">'");
					System.out.println("Parameterizing  " + columnName + endExpression);
					System.out.println("Query is set to..  " + qs);
				}
			}				
			binary = false;
			column = false;
			columnName = null;

			return retExpr;
		}
		else if(joinExpr instanceof Column)
		{
			// process the column and return back
			GenExpression retExpr = new GenExpression();
			Column thisCol = (Column)joinExpr;
			retExpr.aQuery = thisCol.toString();
			retExpr.setComposite(false);
			retExpr.setOperation("column");
			String tableName = "";
			String tableAlias = "";
			
			// need to fix the table name
			if(thisCol.getTable() != null) {
				tableName = thisCol.getTable().getFullyQualifiedName();
				retExpr.userTableName = tableName;
				Alias alias = thisCol.getTable().getAlias();
				if(alias != null) {
					tableAlias = alias.getName();
					retExpr.userTableAlias = tableAlias;
				}
			} else {
				tableName = qs.currentTable;
				tableAlias = qs.currentTableAlias;
			}
			// take out __ and put .
			//retExpr.setLeftExpr(tableName + "." + thisCol.getColumnName());
			retExpr.setLeftExpr(thisCol.getColumnName());
			retExpr.tableName = tableName;
			retExpr.tableAlias = tableAlias;
			
			// starts keeping track of the columns
			String columnName = thisCol.getColumnName();
			
			List <GenExpression>selectList = null;
			if(this.wrapper.columnSelect.containsKey(tableName + "." + columnName)) {
				selectList = this.wrapper.columnSelect.get(tableName + "." + columnName);
			} else {
				selectList = new Vector<GenExpression>();
			}
			if(!selectList.contains(qs)) {
				selectList.add((GenExpression) qs);
			}
			this.wrapper.columnSelect.put(tableName + "." + columnName, selectList);
			
			// track based on select too
			List <String> columnList = null;
			if(this.wrapper.selectColumns.containsKey(qs)) {
				columnList = this.wrapper.selectColumns.get(qs);
			} else {
				columnList = new Vector<String>();
			}
			if(!columnList.contains(tableName + "." + columnName)) {
				columnList.add(tableName + "." + columnName);
			}
			
			this.wrapper.selectColumns.put((GenExpression) qs, columnList);
			
			retExpr.parent = (GenExpression)qs;
			return retExpr;
		}
		else if(joinExpr instanceof Function)
		{
			//System.err.println("This is not being handled in expression " + joinExpr);
			Function fexpr = (Function)joinExpr;
			FunctionExpression gep = new FunctionExpression();
			gep.aQuery = fexpr.toString();
			gep.setExpression(fexpr.getName());
			//if(fexpr.toString().contains("coalesce"))
			//	System.err.println("Found a coalesce");
			gep.setOperation("function");
			// going to make this opaque
			gep.setLeftExpr(fexpr.toString());
			gep.distinct = fexpr.isDistinct();
			
			if(!fexpr.isAllColumns())
			{
				List <Expression> el = fexpr.getParameters().getExpressions();
				// will work through expression later
				for(int exprIndex = 0;exprIndex < el.size();exprIndex++)
				{
					GenExpression thisExpression = processExpression(qs, el.get(exprIndex), gep);
					gep.expressions.add(thisExpression);
				}
			}
			else
			{
				GenExpression allColExpression = new GenExpression();
				allColExpression.setOperation("allcol");
				gep.expressions.add(allColExpression);
			}
			// add this to be used later
			wrapper.addFunctionExpression(fexpr.getName(), gep);
			gep.parent = (GenExpression)qs;
			return gep;
			
			//gep.setLeftExpr(fexpr.getP);
		}
		else if(joinExpr instanceof CaseExpression)
		{
			//System.err.println("This is not being handled in expression " + joinExpr);
			CaseExpression cep = (CaseExpression)joinExpr;
			if(!processCase) // so while this idea is good.. this needs to be eventually a stack since you are running recursion.. for now it might work
				processParam.push(false);
			
			WhenExpression wep = new WhenExpression();
			wep.aQuery = joinExpr.toString();
			wep.setOperation("case");
			List <WhenClause> whens = cep.getWhenClauses();
			
			for(int whenIndex = 0;whenIndex < whens.size();whenIndex++)
			{
				WhenClause wc = whens.get(whenIndex);
				Expression we = wc.getWhenExpression();
				// process this expression
				GenExpression when = processExpression(qs, we, wep);

				Expression te = wc.getThenExpression();
				GenExpression then = processExpression(qs, te, wep);
				
				StringBuffer whenBuf = new StringBuffer();
				whenBuf = when.printQS(when, whenBuf);
				StringBuffer thenBuf = new StringBuffer();
				thenBuf = when.printQS(then, thenBuf);
				wep.addWhenThen(whenBuf.toString(), thenBuf.toString());				
				wep.addWhenThenE(when , then);
			}
			// I wonder if we should orient else as well
			// if there is an else - process it
			if(cep.getElseExpression() != null) {
				GenExpression elseE = processExpression(qs, cep.getElseExpression(), wep);
				wep.setElse(cep.getElseExpression().toString());
				wep.setElseE(elseE);
			}
			
			wep.parent = (GenExpression)qs;
			if(!processCase) // so while this idea is good.. this needs to be eventually a stack since you are running recursion.. for now it might work
				processParam.pop();
			return wep;
		}
		else if(joinExpr instanceof StringValue)
		{
			GenExpression gep = new GenExpression();
			gep.aQuery = joinExpr.toString();
			String value = ((StringValue)joinExpr).getValue();
			gep.setOperation("string");
			gep.setExpression("string");
			gep.setLeftExpresion("'" + value + "'");
			gep.setLeftExpr("'" + value + "'");
			gep.parent = (GenExpression)qs;
			return gep;
		}
		else if(joinExpr instanceof LongValue)
		{
			GenExpression gep = new GenExpression();
			gep.aQuery = joinExpr.toString();
			Long value = ((LongValue)joinExpr).getValue();
			gep.setOperation("long");
			gep.setExpression("long");
			gep.setLeftExpresion(value);
			gep.parent = (GenExpression)qs;
			return gep;
		}
		else if(joinExpr instanceof DoubleValue)
		{
			GenExpression gep = new GenExpression();
			gep.aQuery = joinExpr.toString();
			Double value = ((DoubleValue)joinExpr).getValue();
			gep.setOperation("double");
			gep.setExpression("double");
			gep.setLeftExpresion(value);
			gep.parent = (GenExpression)qs;
			return gep;
		}
		else if(joinExpr instanceof DateValue)
		{
			GenExpression gep = new GenExpression();
			gep.aQuery = joinExpr.toString();
			Date value = ((DateValue)joinExpr).getValue();
			gep.setOperation("date");
			gep.setExpression("date");
			gep.setLeftExpresion(value);
			gep.parent = (GenExpression)qs;
			return gep;
		}
		else if(joinExpr instanceof TimestampValue)
		{
			GenExpression gep = new GenExpression();
			gep.aQuery = joinExpr.toString();
			Date value = ((DateValue)joinExpr).getValue();
			gep.setOperation("timestamp");
			gep.setExpression("timestamp");
			gep.setLeftExpresion(value);
			gep.parent = (GenExpression)qs;
			return gep;
		}
		else if(joinExpr instanceof TimeValue)
		{
			GenExpression gep = new GenExpression();
			gep.aQuery = joinExpr.toString();
			Time value = ((TimeValue)joinExpr).getValue();
			gep.setOperation("time");
			gep.setExpression("time");
			gep.setLeftExpresion(value);
			gep.parent = (GenExpression)qs;
			return gep;
		}
		else if(joinExpr instanceof CastExpression)
		{
			CastExpression ce = (CastExpression)joinExpr;		
			GenExpression gep = new GenExpression();
			gep.aQuery = ce.toString();
			//gep.setLeftExpr(ce.toString()); 
			gep.setOperation("cast");
			GenExpression innerExpression = processExpression(qs, ce.getLeftExpression(), null);
			innerExpression.setLeftAlias(ce.getType().toString());	
			//innerExpression.paranthesis = true;
			gep.setLeftExpresion(innerExpression);
			gep.parent = (GenExpression)qs;
			return gep;
		}
		else if(joinExpr instanceof Parenthesis)
		{
			GenExpression gep = new GenExpression();
			gep.setOperation("paranthesis");
			gep.setLeftExpresion("(");
			gep.setExpression(joinExpr.toString());
			
			Expression nextExpr = ((Parenthesis) joinExpr).getExpression();
			gep.telescope = true;
			GenExpression body = processExpression(qs,nextExpr, null);
			gep.body = body;
			gep.setLeftExpresion(body);
			// reset it back
			gep.parent = (GenExpression)qs;
			return gep;
		}
		else if(joinExpr instanceof IsNullExpression)
		{
			IsNullExpression nullExpr = (IsNullExpression)joinExpr;
			GenExpression gep = new GenExpression();
			gep.setOperation("isnull");
			gep.setLeftExpresion(processExpression(qs, nullExpr.getLeftExpression(), expr));
			return gep;
		}
		else if(joinExpr instanceof InExpression)
		{
			boolean paramIn = true;
			
			if(processParam.size() > 0)
			{
				// get the latest and push it back
				paramIn = processParam.pop();
				processParam.push(paramIn);
			}

			
			// need to parameterize this
			InExpression inExpr = (InExpression)joinExpr;
			InGenExpression gep = new InGenExpression();
			gep.setIsNot(inExpr.isNot());
			// left expression is a gen expression
			// rightItemsList is a list of expressions that need to be processed
			gep.setOperation("in");
			String operator = "in";
			
			String modifier = operator + wrapper.uniqueCounter;

			
			String tableName = null;
			if(inExpr.getLeftExpression() != null)
			{
				GenExpression colExpression = processExpression(qs, inExpr.getLeftExpression(), expr);
				gep.setLeftExpresion(colExpression);
				if(colExpression.getOperation().equalsIgnoreCase("column"))
				{
					// who knows you could be a sadist after all
					column = true;
					columnName = colExpression.getLeftExpr();
					tableName = colExpression.tableName;
				}
				
				// If Operation is function, get the column name and table name from the expression 
				if(colExpression.getOperation().equalsIgnoreCase("function"))
				{
					// who knows you could be a sadist after all
					// Casting to FunctionExpression to get the expression data
					FunctionExpression fncolExpression = (FunctionExpression) colExpression;
					column = true;
					columnName = fncolExpression.expressions.get(0).getLeftExpr();
					tableName = fncolExpression.expressions.get(0).tableName;
				}
				
			}
			// sometimes the in can also be a list
			else
			{
				ItemsList litemList = inExpr.getLeftItemsList();
				//System.out.println(itemList);
				if(litemList instanceof ExpressionList)
				{
					ExpressionList el = (ExpressionList)litemList;
					if(el.getExpressions().size() == 1)
					{				
						GenExpression colExpression = processExpression(qs, el.getExpressions().get(0), expr);
						colExpression.paranthesis = true;
						gep.setLeftExpresion(colExpression);
						if(colExpression.getOperation().equalsIgnoreCase("column"))
						{
							// who knows you could be a sadist after all
							column = true;
							columnName = colExpression.getLeftExpr();
							tableName = colExpression.tableName;
						}
					}
				}
				else
				{
					// need to throw an exception here
					System.err.println("Multiple columns in IN is not supported");
				}
			}

			ItemsList itemList = inExpr.getRightItemsList();
			/*
			if(itemList instanceof ExpressionList)
			{
				List <Expression> el = ((ExpressionList)itemList).getExpressions();
				
				for(int itemIndex = 0;itemIndex < el.size();itemIndex++)
				{
					Expression thisExpression = el.get(itemIndex);
					GenExpression ge = processExpression(qs, thisExpression, expr);
					gep.inList.add(ge);
					
					// need to put the parameter here					
				}
				// process the start value if it is a constant
			}*/
			
			if(itemList instanceof SubSelect)
			{
				GenExpression ge = processExpression(qs, (SubSelect)itemList, expr);
				gep.rightItem = ge;
			}
			else
			{
				// do this as an opaque
				GenExpression ge = new GenExpression();
				ge.setOperation("opaque");
				ge.setLeftExpr(itemList.toString());
				// make this an opaque and pump it back
				ge.parent = (GenExpression)qs;
				// we got our target

				gep.inList.add(ge);
				Object constantValue = ge.getLeftExpr();
				// try to inspect and get the value
				String constantType = "string";
				{
					ExpressionList list = (ExpressionList) itemList;
					List<Expression> exprList = list.getExpressions();	
					for(Expression e : exprList) {
						if(e instanceof LongValue) {
							constantType = "long";
							break;
						} else if(joinExpr instanceof DoubleValue) {
							constantType = "long";
							break;
						} else if(joinExpr instanceof TimeValue) {
							constantType = "time";
							break;
						} else if(joinExpr instanceof DateValue) {
							constantType = "date";
							break;
						} else if(joinExpr instanceof TimestampValue) {
							constantType = "timestamp";
							break;
						}
					}
				}
				
				if(columnName != null && paramIn) {
					// removing the quotes for now
					String defQuery = "Select q1." + columnName + " from (" + qs + ") q1";
					this.wrapper.makeParameters(columnName, constantValue, modifier, "in", constantType, ge, tableName, defQuery);
					if(parameterize) {
						ge.setLeftExpr("(<" + tableName + "_" + columnName + modifier + ">)");
					}
				}
			}
				
			//gep.setComposite(composite);
			//gep.setLeftExpresion(processExpression(qs, nullExpr.getLeftExpression(), expr));
			return gep;
		}
		else
		{
			System.err.println("Unhandled expression >>>>> " + joinExpr);
			if(joinExpr == null) {
				return null;
			}
			GenExpression ge = new GenExpression();
			ge.setOperation("opaque");
			ge.setLeftExpr(joinExpr.toString());
			// make this an opaque and pump it back
			ge.parent = (GenExpression)qs;
			
			return ge;
		}
		
		// also need to handle subselect
		return expr;
		
	}

	
	// process operations
	public GenExpression processOperation(SetOperationList sol)
	{
		OperationExpression opExpr = new OperationExpression();
		// recursion follows
		// things it is unioning
		List <SelectBody> solParts = sol.getSelects();
		List <SetOperation> solOps = sol.getOperations();
		
		// now I need to cobble this together
		
		// need to process every select and then tag it with the specific union
		opExpr.setOperation("union");
		opExpr.setComposite(true);
		
		int solIndex = 0;
		for(;solIndex < solOps.size();solIndex++)
		{
			SelectBody sb1 = solParts.get(solIndex);
			
			opExpr.opNames.add(solOps.get(solIndex).toString());
			
			GenExpression sqs1  = null;
			
			if(sb1 instanceof PlainSelect)
			{
				sqs1 = processSelect(null, (PlainSelect)sb1);
			}
			else if(sb1 instanceof SetOperationList)
			{
				sqs1 = processOperation((SetOperationList)sb1);
			}
		
			opExpr.operands.add(sqs1);			
		}
		
		SelectBody lastS = solParts.get(solIndex);
		GenExpression sqs1  = null;
		
		if(lastS instanceof PlainSelect)
		{
			sqs1 = processSelect(null, (PlainSelect)lastS);
		}
		else if(lastS instanceof SetOperationList)
		{
			sqs1 = processOperation((SetOperationList)lastS);
		}
		opExpr.operands.add(sqs1);			
		
		return opExpr;

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

	public void fillFilters(SelectQueryStruct qs, IQueryFilter curFilter, Expression expr) {
		// this is a simple one just go ahead and process it like anything else
		// this should go first.. 
		// if unable to process it is only then we should attempt to create other pieces
		if(expr != null)
		{
			GenExpression fExpr = processExpression(qs, expr, null);
			qs.filter = fExpr;
		}
	}


	/**
	 * Fills in the limit and offset for the query
	 * @param qs
	 * @param limit
	 */
	public void fillLimitOffset(SelectQueryStruct qs, Limit limit) {
		if(limit == null) {
			return;
		}
		// add limit
		if(limit.getRowCount() instanceof LongValue) {
			long limitRow =  ((LongValue)limit.getRowCount()).getValue();
			qs.setLimit(limitRow);
		}

		// add offset
		if(limit.getOffset() instanceof LongValue) {
			long offset =  ((LongValue)limit.getOffset()).getValue();
			qs.setOffSet(offset);
		}
	}

	/**
	 * Add in the order by
	 * @param qs
	 * @param orders
	 */
	public void fillOrder(SelectQueryStruct qs, List <OrderByElement> orders) {
		if(orders == null || orders.isEmpty()) {
			return;
		}

		for(int orderIndex = 0; orderIndex < orders.size(); orderIndex++) {
			
			OrderByElement thisElement = orders.get(orderIndex);
			Expression expr = thisElement.getExpression();
			String sortDir = "ASC";
			if(thisElement.isAscDescPresent() && !thisElement.isAsc()) {
				sortDir = "DESC";
			}

			OrderByExpression obe = new OrderByExpression();
			obe.telescope = true;
			obe.body = processExpression(qs, expr, null);
			if(!sortDir.equalsIgnoreCase("ASC"))
				obe.direction = sortDir;
			qs.norderBy.add(obe);
		}
	}
	
	/**
	 * Add in the group bys
	 * @param qs
	 * @param groupByElement
	 */
	public void fillGroups(SelectQueryStruct qs, GroupByElement groups) {
		if(groups == null) {
			return;
		}
		List<Expression> groupByElement = groups.getGroupByExpressions();
		if(groupByElement == null || groupByElement.isEmpty()) {
			return;
		}
		
		for(int groupIndex = 0; groupIndex < groupByElement.size(); groupIndex++) {
			Expression expr = groupByElement.get(groupIndex);
			GenExpression gep = processExpression(qs, expr, null);
			
			// 
			String tableColumnName = gep.getLeftExpr();
			wrapper.addGroupBy(tableColumnName, (GenExpression)qs);
			
			qs.ngroupBy.add(gep);
		}
	}
	
	public Map<String, List<GenExpression>> getTableColumns(String query)
	{
		Map<String, List<GenExpression>> newTableColumn = null;
		try {
			wrapper = new GenExpressionWrapper();
			// this is the main query struct
			GenExpression qs = new GenExpression();
			// parse the sql
			Statement stmt = CCJSqlParserUtil.parse(query);
			Select select = ((Select)stmt);
			
			if(select.getSelectBody() instanceof PlainSelect)
			{
				PlainSelect sb = (PlainSelect)select.getSelectBody();
				qs = processSelect(null, sb);
			}
			if(select.getSelectBody() instanceof SetOperationList)
			{
				qs = processOperation((SetOperationList)select.getSelectBody());
			}
			Map <Integer, List<GenExpression>> levelSelectors = new HashMap<Integer, List<GenExpression>>();
			List <String> realTables = new ArrayList();
			Map <GenExpression, List<GenExpression>> derivedColumns = new HashMap<GenExpression, List<GenExpression>>();
			Map <String, List<GenExpression>>tableColumns = new HashMap<String, List<GenExpression>>();
			
			Map<String, String> aliases = new HashMap<String, String>();
			
			qs.printLevel2(qs, realTables, 0, null, derivedColumns, levelSelectors, tableColumns, aliases , null, false, true);
					
			newTableColumn = remasterColumns(realTables, tableColumns);
		} catch (JSQLParserException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return newTableColumn;
	}
	
	public Map<Integer, List<GenExpression>> getLevelColumns(String query)
	{
		Map <Integer, List<GenExpression>> levelSelectors = new HashMap<Integer, List<GenExpression>>();
		try {
			wrapper = new GenExpressionWrapper();
			// this is the main query struct
			GenExpression qs = new GenExpression();
			// parse the sql
			Statement stmt = CCJSqlParserUtil.parse(query);
			Select select = ((Select)stmt);
			
			if(select.getSelectBody() instanceof PlainSelect)
			{
				PlainSelect sb = (PlainSelect)select.getSelectBody();
				qs = processSelect(null, sb);
			}
			if(select.getSelectBody() instanceof SetOperationList)
			{
				qs = processOperation((SetOperationList)select.getSelectBody());
			}
			List <String> realTables = new ArrayList();
			Map <GenExpression, List<GenExpression>> derivedColumns = new HashMap<GenExpression, List<GenExpression>>();
			Map <String, List<GenExpression>>tableColumns = new HashMap<String, List<GenExpression>>();
			
			Map<String, String> aliases = new HashMap<String, String>();
			
			qs.printLevel2(qs, realTables, 0, null, derivedColumns, levelSelectors, tableColumns, aliases , null, false, true);
					
		} catch (JSQLParserException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return levelSelectors;
	}

	public Map <String, List<GenExpression>> remasterColumns(List <String> realTables, Map <String, List<GenExpression>> tableColumns)
	{
		Iterator <String> tableKeys = tableColumns.keySet().iterator();
		Map <String, List<GenExpression>> newTableColumn = new HashMap<String, List<GenExpression>>();
		while(tableKeys.hasNext())
		{
			String thisTable = tableKeys.next();
			if(realTables.contains(thisTable))
				newTableColumn.put(thisTable, tableColumns.get(thisTable));
		}
		return newTableColumn;
	}

	public void printRealColumns(Map <String, List<GenExpression>> tableColumns)
	{
		Iterator <String> tableKeys = tableColumns.keySet().iterator();
		while(tableKeys.hasNext())
		{
			String thisTable = tableKeys.next();
			{
				System.err.println("Table >> " + thisTable);
				System.err.println("-----------------------");
				List <GenExpression> columns = tableColumns.get(thisTable);
				for(int colIndex = 0;colIndex < columns.size();colIndex++)
				{
					String thisColumn = columns.get(colIndex).toString();
					//System.err.println("Alias is " + thisColumn);
					System.err.println(thisColumn);
				}
				System.err.println("...............");
			}
			
		}
	}

	
	///////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////


//	public static void main(String [] args) throws Exception {
//		SqlParser2 test = new SqlParser2();
//		String query = "Select * from employee";
//		query =  "select distinct c.logicalname ln, (ec.physicalName + 1) ep from "
//				+ "concept c, engineconcept ec, engine e inner join sometable s on c.logicalname=s.logical where (ec.localconceptid=c.localconceptid and "
//				+ "c.conceptualname in ('val1', 'val2')) or (ec.localconceptid + 5) =1 group by ln order by ln limit 200 offset 50 ";// order by c.logicalname";
//
//		query = "select distinct f.studio, (f.movie_budget - 3) / 2 from f where f.movie_budget * 4 > 10";
//		
//		// thank you so much anthem
//		String query2 = " SELECT	Member_Engagement_Tier AS `Member Engagement Tier`,Site_Service_Name AS `Site Service Name`," + 
//				"		Age_Group_Description AS `Age " + 
//				"Group Description`," + 
//				"		Medical_Member_Paid_PMPM AS `Medical Member Paid PMPM`  " + 
//				"FROM	 (  " + 
//				"	SELECT	MBRSHP.MBR_ENGGMNT_TIER AS `Member_Engagement_Tier`," + 
//				"			CLMS.SITE_SRVC_NM AS `Site_Service_Name`," + 
//				"			MBRSHP.AGE_GRP_DESC AS `Age_Group_Description`," + 
//				"			case " + 
//				"				when sum( MBRSHP.MDCL_MBR_CVRG_CNT ) = 0 then 0 " + 
//				"				else sum(CLMS.MDCL_ACCT_PAID_AMT )/sum( MBRSHP.MDCL_MBR_CVRG_CNT )" + 
//				"			end AS `Medical_Member_Paid_PMPM`," + 
//				"			MBRSHP.TM_PRD_NM AS `Time_Period`  " + 
//				"	FROM	 (  " + 
//				"		SELECT	coalesce(TMBRENGGMNT.ENGGMNT_TIER," + 
//				"				'Not Engaged' ) AS MBR_ENGGMNT_TIER," + 
//				"				DIM_AGE_GRP.AGE_GRP_DESC AS AGE_GRP_DESC," + 
//				"				TM_PRD_FNCTN.TM_PRD_NM AS TM_PRD_NM," + 
//				"				SUM(" + 
//				"				case " + 
//				"					when CII_FACT_MBRSHP.MBR_CVRG_TYPE_CD='001' then CII_FACT_MBRSHP.MBR_CVRG_CNT " + 
//				"					else 0 " + 
//				"				end) AS MDCL_MBR_CVRG_CNT," + 
//				"				CII_ACCT_PRFL.ACCT_ID AS ACCT_ID," + 
//				"				CII_FACT_MBRSHP.MCID AS MCID," + 
//				"				CII_FACT_MBRSHP.ACCT_AGE_GRP_KEY AS ACCT_AGE_GRP_KEY  " + 
//				"		FROM	CII_FACT_MBRSHP " + 
//				"		INNER JOIN (" + 
//				"			SELECT	B.*," + 
//				"					case " + 
//				"						when paidIncurred='Paid' then 111101 " + 
//				"						else B.START_YEAR_MNTH " + 
//				"					end as STRT_SRVC_YEAR_MNTH," + 
//				"					case " + 
//				"						WHEN paidIncurred='Paid' then 888811 " + 
//				"						else B." + 
//				"					END_YEAR_MNTH " + 
//				"					end as " + 
//				"					END_SRVC_YEAR_MNTH," + 
//				"					Case " + 
//				"						when paidIncurred='Paid' then B." + 
//				"					END_YEAR_MNTH " + 
//				"						when paidIncurred='Incurred' " + 
//				"				and 0=0 then 202002 " + 
//				"						else 888811 " + 
//				"					end as " + 
//				"					END_RPTG_PAID_YEAR_MNTH " + 
//				"			FROM	ACIISST_PERIOD_KEY A JOIN     (" + 
//				"				SELECT	LKUP_ID," + 
//				"						YEAR_ID," + 
//				"						 CONCAT(''," + 
//				"						'Paid') AS paidIncurred," + 
//				"						CASE " + 
//				"							WHEN YEAR_ID=1 THEN 'Current Period' " + 
//				"							WHEN YEAR_ID=2 THEN 'Prior Period' " + 
//				"							ELSE 'Prior Period 2' " + 
//				"						END AS TM_PRD_NM," + 
//				"						CASE " + 
//				"							WHEN YEAR_ID=1 THEN 201903 " + 
//				"							WHEN YEAR_ID=2 THEN 201803 " + 
//				"							ELSE 201703 " + 
//				"						END AS START_YEAR_MNTH," + 
//				"						    " + 
//				"						CASE " + 
//				"							WHEN YEAR_ID=1 THEN 202002 " + 
//				"							WHEN YEAR_ID=2 THEN 201902    " + 
//				"							ELSE  201802 " + 
//				"						END AS " + 
//				"						END_YEAR_MNTH " + 
//				"				FROM	ACIISST_PERIOD_KEY) B " + 
//				"				ON A.LKUP_ID = B.LKUP_ID " + 
//				"				AND B.YEAR_ID = A.YEAR_ID " + 
//				"			WHERE	A.LKUP_ID=1 " + 
//				"				AND A.YEAR_ID <= 1) TM_PRD_FNCTN " + 
//				"			ON CII_FACT_MBRSHP.ELGBLTY_CY_MNTH_END_NBR BETWEEN TM_PRD_FNCTN.START_YEAR_MNTH " + 
//				"			and TM_PRD_FNCTN." + 
//				"				END_YEAR_MNTH " + 
//				"		INNER JOIN CII_ACCT_PRFL " + 
//				"			ON CII_FACT_MBRSHP.ACCT_ID = CII_ACCT_PRFL.ACCT_ID " + 
//				"		LEFT	JOIN  ( " + 
//				"			select	a.acct_id," + 
//				"					a.MCID," + 
//				"					a.TM_PRD_NM," + 
//				"					a.ENGGMNT," + 
//				"					b.ENGGMNT_TIER  " + 
//				"			from	 ( " + 
//				"				select	CII_FACT_CP_ENGGMNT.acct_id," + 
//				"						CII_FACT_CP_ENGGMNT.MCID," + 
//				"						TM_PRD_FNCTN.TM_PRD_NM ," + 
//				"								(" + 
//				"						CASE  	" + 
//				"							WHEN MAX(DIM_ENGGMNT.TRDTNL_IND)= 1 THEN 'Traditional'       	" + 
//				"							WHEN MAX(DIM_ENGGMNT.TRDTNL_IND) = 0  	" + 
//				"					AND MAX(DIM_ENGGMNT.ENHNCD_IND) = 1 THEN 'Care Coordination'       	" + 
//				"							WHEN MAX(DIM_ENGGMNT.TRDTNL_IND) = 0   	" + 
//				"					AND MAX(DIM_ENGGMNT.ENHNCD_IND) = 0  	" + 
//				"					AND MAX(DIM_ENGGMNT.EXPNDD_IND)= 1 THEN 'Comprehensive'       	" + 
//				"							ELSE 'Not Engaged'  " + 
//				"						END) AS ENGGMNT 	  " + 
//				"				from	CII_FACT_CP_ENGGMNT  " + 
//				"				inner join DIM_ENGGMNT      	" + 
//				"					on CII_FACT_CP_ENGGMNT.MBR_ENGGMNT_ID = DIM_ENGGMNT.ENGGMNT_ID JOIN ( " + 
//				"					SELECT	B.*," + 
//				"							  case	   	" + 
//				"								when paidIncurred='Paid' then 111101    	" + 
//				"								else B.START_YEAR_MNTH   " + 
//				"							end	as STRT_SRVC_YEAR_MNTH," + 
//				"							 case	   	" + 
//				"								WHEN paidIncurred='Paid' then 888811    	" + 
//				"								else B." + 
//				"							END_YEAR_MNTH   " + 
//				"							end	as " + 
//				"							END_SRVC_YEAR_MNTH," + 
//				"							 Case	   	" + 
//				"								when paidIncurred='Paid' then B." + 
//				"							END_YEAR_MNTH    	" + 
//				"								when paidIncurred='Incurred'    	" + 
//				"						and 0=0 then  202002    	" + 
//				"								else 888811   " + 
//				"							end	as " + 
//				"							END_RPTG_PAID_YEAR_MNTH   " + 
//				"					FROM	ACIISST_PERIOD_KEY A  JOIN (  " + 
//				"						SELECT	LKUP_ID," + 
//				"								YEAR_ID," + 
//				"								CONCAT(''," + 
//				"								'Paid' ) AS paidIncurred," + 
//				"										 " + 
//				"								CASE    	" + 
//				"									WHEN YEAR_ID=1 THEN 'Current Period'    	" + 
//				"									WHEN YEAR_ID=2 THEN 'Prior Period'    	" + 
//				"									ELSE 'Prior Period 2'  " + 
//				"								END	AS TM_PRD_NM," + 
//				"								  CASE	   	" + 
//				"									WHEN YEAR_ID=1 THEN 201903    	" + 
//				"									WHEN YEAR_ID=2 THEN 201803    	" + 
//				"									ELSE  201703   " + 
//				"								END	AS START_YEAR_MNTH," + 
//				"								  CASE	   	" + 
//				"									WHEN YEAR_ID=1 THEN 202002    	" + 
//				"									WHEN YEAR_ID=2 THEN  201902     	" + 
//				"									ELSE 201802   " + 
//				"								END	AS " + 
//				"								END_YEAR_MNTH   " + 
//				"						FROM	ACIISST_PERIOD_KEY) B    	" + 
//				"						ON A.LKUP_ID = B.LKUP_ID    	" + 
//				"						AND B.YEAR_ID = A.YEAR_ID   " + 
//				"					WHERE	A.LKUP_ID=1    	" + 
//				"						AND A.YEAR_ID <= 1  )  TM_PRD_FNCTN    	" + 
//				"					ON CII_FACT_CP_ENGGMNT.ENGGMNT_MNTH_NBR BETWEEN TM_PRD_FNCTN.START_YEAR_MNTH   	" + 
//				"					and TM_PRD_FNCTN." + 
//				"						END_YEAR_MNTH  " + 
//				"				WHERE	CII_FACT_CP_ENGGMNT.ACCT_ID = 'W0016437'  " + 
//				"				group by  CII_FACT_CP_ENGGMNT.acct_id," + 
//				"						CII_FACT_CP_ENGGMNT.MCID ," + 
//				"						TM_PRD_FNCTN.TM_PRD_NM ) a " + 
//				"			inner join ( " + 
//				"				select	 " + 
//				"						CASE	 	" + 
//				"							WHEN DIM_ENGGMNT.TRDTNL_IND= 1 THEN cast('Traditional' as char(20))  	" + 
//				"							WHEN DIM_ENGGMNT.TRDTNL_IND = 0  	" + 
//				"					AND DIM_ENGGMNT.ENHNCD_IND= 1 THEN cast('Care Coordination' as char(20))  	" + 
//				"							WHEN DIM_ENGGMNT.TRDTNL_IND= 0   	" + 
//				"					AND DIM_ENGGMNT.ENHNCD_IND = 0  	" + 
//				"					AND DIM_ENGGMNT.EXPNDD_IND= 1 THEN  cast('Comprehensive' as char(20))  	" + 
//				"							ELSE cast('Not Engaged' as char(20))  " + 
//				"						END	AS ENGGMNT ," + 
//				"						 CASE	 	" + 
//				"							WHEN DIM_ENGGMNT.TRDTNL_IND= 1 THEN cast('Traditional' as char(20))  	" + 
//				"							WHEN DIM_ENGGMNT.TRDTNL_IND = 0  	" + 
//				"					AND DIM_ENGGMNT.ENHNCD_IND= 1 THEN cast('Care Coordination' as char(20))  	" + 
//				"							WHEN DIM_ENGGMNT.TRDTNL_IND= 0   	" + 
//				"					AND DIM_ENGGMNT.ENHNCD_IND = 0  	" + 
//				"					AND DIM_ENGGMNT.EXPNDD_IND= 1 THEN cast('Comprehensive' as char(20))  	" + 
//				"							ELSE cast('Not Engaged' as char(20))  " + 
//				"						END	AS ENGGMNT_TIER                        " + 
//				"				from	DIM_ENGGMNT 					   " + 
//				"				union	all 					   " + 
//				"				select	 " + 
//				"						CASE	 	" + 
//				"							WHEN DIM_ENGGMNT.TRDTNL_IND= 1 THEN cast('Traditional' as char(20))  	" + 
//				"							else cast('Care Coordination' as char(20))  " + 
//				"						end	as ENGGMNT ," + 
//				"						cast( 'Comprehensive' as char(20))  AS ENGGMNT_TIER  " + 
//				"				from	DIM_ENGGMNT  " + 
//				"				where	DIM_ENGGMNT.TRDTNL_IND= 1  	" + 
//				"					or DIM_ENGGMNT.ENHNCD_IND= 1 					   " + 
//				"				union	all 					   " + 
//				"				select	cast('Traditional' as char(20)) as ENGGMNT," + 
//				"						cast('Care Coordination' as char(20))  AS ENGGMNT_TIER  " + 
//				"				from	DIM_ENGGMNT  " + 
//				"				where	DIM_ENGGMNT.TRDTNL_IND= 1 ) b  	" + 
//				"				on a.ENGGMNT = b.ENGGMNT				   )  TMBRENGGMNT   	" + 
//				"			ON CII_FACT_MBRSHP.ACCT_ID = TMBRENGGMNT.ACCT_ID   	" + 
//				"			and CII_FACT_MBRSHP.MCID=TMBRENGGMNT.MCID   	" + 
//				"			and TM_PRD_FNCTN.TM_PRD_NM =  TMBRENGGMNT.TM_PRD_NM " + 
//				"		INNER JOIN DIM_AGE_GRP " + 
//				"			ON CII_FACT_MBRSHP.ACCT_AGE_GRP_KEY = DIM_AGE_GRP.AGE_GRP_KEY  " + 
//				"		WHERE	CII_FACT_MBRSHP.ACCT_ID = 'W0016437'   " + 
//				"		GROUP BY coalesce(TMBRENGGMNT.ENGGMNT_TIER," + 
//				"				'Not Engaged' )," + 
//				"				DIM_AGE_GRP.AGE_GRP_DESC," + 
//				"				TM_PRD_FNCTN.TM_PRD_NM," + 
//				"				CII_ACCT_PRFL.ACCT_ID," + 
//				"				CII_FACT_MBRSHP.MCID," + 
//				"				CII_FACT_MBRSHP.ACCT_AGE_GRP_KEY ) AS MBRSHP " + 
//				"	LEFT OUTER JOIN  (  " + 
//				"		SELECT	DIM_SITE_SRVC.SITE_SRVC_NM AS SITE_SRVC_NM," + 
//				"				SUM(" + 
//				"				case " + 
//				"					when CII_FACT_CLM_LINE.MBR_CVRG_TYPE_CD='001' then CII_FACT_CLM_LINE.ACCT_PAID_AMT " + 
//				"					else 0 " + 
//				"				end ) AS MDCL_ACCT_PAID_AMT," + 
//				"				TM_PRD_FNCTN.TM_PRD_NM AS TM_PRD_NM," + 
//				"				CII_FACT_CLM_LINE.MCID AS MCID," + 
//				"				CII_FACT_CLM_LINE.ACCT_AGE_GRP_KEY AS ACCT_AGE_GRP_KEY," + 
//				"				CII_FACT_CLM_LINE.ACCT_ID AS ACCT_ID  " + 
//				"		FROM	CII_FACT_CLM_LINE " + 
//				"		INNER JOIN (" + 
//				"			SELECT	B.*," + 
//				"					case " + 
//				"						when paidIncurred='Paid' then 111101 " + 
//				"						else B.START_YEAR_MNTH " + 
//				"					end as STRT_SRVC_YEAR_MNTH," + 
//				"					case " + 
//				"						WHEN paidIncurred='Paid' then 888811 " + 
//				"						else B." + 
//				"					END_YEAR_MNTH " + 
//				"					end as " + 
//				"					END_SRVC_YEAR_MNTH," + 
//				"					Case " + 
//				"						when paidIncurred='Paid' then B." + 
//				"					END_YEAR_MNTH " + 
//				"						when paidIncurred='Incurred' " + 
//				"				and 0=0 then 202002 " + 
//				"						else 888811 " + 
//				"					end as " + 
//				"					END_RPTG_PAID_YEAR_MNTH " + 
//				"			FROM	ACIISST_PERIOD_KEY A JOIN     (" + 
//				"				SELECT	LKUP_ID," + 
//				"						YEAR_ID," + 
//				"						 CONCAT(''," + 
//				"						'Paid') AS paidIncurred," + 
//				"						CASE " + 
//				"							WHEN YEAR_ID=1 THEN 'Current Period' " + 
//				"							WHEN YEAR_ID=2 THEN 'Prior Period' " + 
//				"							ELSE 'Prior Period 2' " + 
//				"						END AS TM_PRD_NM," + 
//				"						CASE " + 
//				"							WHEN YEAR_ID=1 THEN 201903 " + 
//				"							WHEN YEAR_ID=2 THEN 201803 " + 
//				"							ELSE 201703 " + 
//				"						END AS START_YEAR_MNTH," + 
//				"						    " + 
//				"						CASE " + 
//				"							WHEN YEAR_ID=1 THEN 202002 " + 
//				"							WHEN YEAR_ID=2 THEN 201902    " + 
//				"							ELSE  201802 " + 
//				"						END AS " + 
//				"						END_YEAR_MNTH " + 
//				"				FROM	ACIISST_PERIOD_KEY) B " + 
//				"				ON A.LKUP_ID = B.LKUP_ID " + 
//				"				AND B.YEAR_ID = A.YEAR_ID " + 
//				"			WHERE	A.LKUP_ID=1 " + 
//				"				AND A.YEAR_ID <= 1) TM_PRD_FNCTN " + 
//				"			ON CII_FACT_CLM_LINE.CLM_SRVC_YEAR_MNTH_NBR BETWEEN TM_PRD_FNCTN.STRT_SRVC_YEAR_MNTH " + 
//				"			and TM_PRD_FNCTN." + 
//				"				END_SRVC_YEAR_MNTH " + 
//				"			AND CII_FACT_CLM_LINE.RPTG_PAID_YEAR_MNTH_NBR BETWEEN TM_PRD_FNCTN.START_YEAR_MNTH " + 
//				"			and TM_PRD_FNCTN." + 
//				"				END_RPTG_PAID_YEAR_MNTH " + 
//				"		INNER JOIN DIM_SITE_SRVC " + 
//				"			ON CII_FACT_CLM_LINE.SITE_SRVC_CD = DIM_SITE_SRVC.SITE_SRVC_CD " + 
//				"			AND CII_FACT_CLM_LINE.MBR_cvrg_type_cd <> 'back_fill'  " + 
//				"		WHERE	CII_FACT_CLM_LINE.ACCT_ID = 'W0016437'   " + 
//				"		GROUP BY DIM_SITE_SRVC.SITE_SRVC_NM," + 
//				"				TM_PRD_FNCTN.TM_PRD_NM," + 
//				"				CII_FACT_CLM_LINE.MCID," + 
//				"				CII_FACT_CLM_LINE.ACCT_AGE_GRP_KEY," + 
//				"				CII_FACT_CLM_LINE.ACCT_ID ) AS CLMS " + 
//				"		ON MBRSHP.MCID=CLMS.MCID " + 
//				"		AND MBRSHP.TM_PRD_NM=CLMS.TM_PRD_NM " + 
//				"		AND MBRSHP.ACCT_AGE_GRP_KEY=CLMS.ACCT_AGE_GRP_KEY " + 
//				"		AND MBRSHP.ACCT_ID=CLMS.ACCT_ID  " + 
//				"	GROUP BY MBRSHP.MBR_ENGGMNT_TIER," + 
//				"			CLMS.SITE_SRVC_NM," + 
//				"			MBRSHP.AGE_GRP_DESC," + 
//				"			MBRSHP.TM_PRD_NM ) AS FNL   " + 
//				"ORDER BY Member_Engagement_Tier," + 
//				"		Site_Service_Name," + 
//				"		Age_Group_Description," + 
//				"		Medical_Member_Paid_PMPM";
//
//		 /*query =  "select distinct c.logicalname ln, (ec.physicalName + 1) ep from "
//		 
//				+ "concept c, engineconcept ec, engine e inner join sometable s on c.logicalname=s.logical where (ec.localconceptid=c.localconceptid and "
//				+ "c.conceptualname in ('val1', 'val2')) or (ec.localconceptid + 5) =1 group by ln order by ln limit 200 offset 50 ";// order by c.logicalname";
//		*/
//		
//		// key things to look at
//		// select expression item and select item
//		// from - seems to be many combinations to from that is interesting
//		// selectbody - it can either be a simple one or a set operation - This is where the union sits. This can be a plain select or something else - I only accomodate for plain select at this point
//		// same with join as well
//		// the select item itself can be a expression
//		
//		// we need a way to parse
//		// but more importantly - we also need a way to express it in pixel
//		
//		// I need the query struct to have to have a full select query struct
//		
//		/*query = "SELECT City FROM Customers\r\n" + 
//				"UNION\r\n" + 
//				"SELECT City FROM Suppliers\r\n" + 
//				"ORDER BY City;";
//		*/
//		String query3 = "Select col as abc from (select mango from t) k";
//
//		String query4 = " SELECT Health_Condition_Category_Description AS \"Health Condition Category Description\",\r\n" + 
//				"		Total_Utilization_Count AS \"Total Utilization Count\"  \r\n" + 
//				"FROM	 (  \r\n" + 
//				"SELECT	CLMS.DIM_DIAG_HLTH_CNDTN_CTGRY_DESC AS Health_Condition_Category_Description,\r\n" + 
//				"		SUM(CLMS.TOTL_UTIL_CNT) AS Total_Utilization_Count,CLMS.TM_PRD_NM AS Time_Period  \r\n" + 
//				"FROM	 (  \r\n" + 
//				"SELECT	DIM_DIAG.HLTH_CNDTN_CTGRY_DESC AS DIM_DIAG_HLTH_CNDTN_CTGRY_DESC,\r\n" + 
//				"		sum(CII_FACT_CLM_LINE.ACCT_ADMT_CNT + CII_FACT_CLM_LINE.ACCT_VST_CNT) AS TOTL_UTIL_CNT,\r\n" + 
//				"		TM_PRD_FNCTN.TM_PRD_NM AS TM_PRD_NM  \r\n" + 
//				"FROM	CII_FACT_CLM_LINE INNER JOIN (\r\n" + 
//				"SELECT	B.*, \r\n" + 
//				"case	\r\n" + 
//				"	when paidIncurred='Paid' then 111101 \r\n" + 
//				"	else B.START_YEAR_MNTH \r\n" + 
//				"end	as STRT_SRVC_YEAR_MNTH,\r\n" + 
//				"case	\r\n" + 
//				"	WHEN paidIncurred='Paid' then 888811 \r\n" + 
//				"	else B.END_YEAR_MNTH \r\n" + 
//				"end	as END_SRVC_YEAR_MNTH,\r\n" + 
//				"Case	\r\n" + 
//				"	when paidIncurred='Paid' then B.END_YEAR_MNTH \r\n" + 
//				"	when paidIncurred='Incurred' \r\n" + 
//				"	and 0=0 then 201908 \r\n" + 
//				"	else 888811 \r\n" + 
//				"end	as END_RPTG_PAID_YEAR_MNTH \r\n" + 
//				"FROM	ACIISST_PERIOD_KEY A JOIN     (\r\n" + 
//				"SELECT	LKUP_ID, YEAR_ID,  CONCAT('','Paid') AS paidIncurred, \r\n" + 
//				"CASE	\r\n" + 
//				"	WHEN YEAR_ID=1 THEN 'Current Period' \r\n" + 
//				"	WHEN YEAR_ID=2 THEN 'Prior Period' \r\n" + 
//				"	ELSE 'Prior Period 2' \r\n" + 
//				"END	AS TM_PRD_NM, \r\n" + 
//				"CASE	\r\n" + 
//				"	WHEN YEAR_ID=1 THEN 201809 \r\n" + 
//				"	WHEN YEAR_ID=2 THEN 201709 \r\n" + 
//				"	ELSE 201609 \r\n" + 
//				"END	AS START_YEAR_MNTH,     \r\n" + 
//				"CASE	\r\n" + 
//				"	WHEN YEAR_ID=1 THEN 201908 \r\n" + 
//				"	WHEN YEAR_ID=2 THEN 201808    \r\n" + 
//				"	ELSE  201708 \r\n" + 
//				"END	AS END_YEAR_MNTH \r\n" + 
//				"FROM	ACIISST_PERIOD_KEY) B \r\n" + 
//				"	ON A.LKUP_ID = B.LKUP_ID \r\n" + 
//				"	AND B.YEAR_ID = A.YEAR_ID \r\n" + 
//				"WHERE	A.LKUP_ID=1 \r\n" + 
//				"	AND A.YEAR_ID <= 1) TM_PRD_FNCTN \r\n" + 
//				"	ON CII_FACT_CLM_LINE.CLM_SRVC_YEAR_MNTH_NBR BETWEEN TM_PRD_FNCTN.STRT_SRVC_YEAR_MNTH \r\n" + 
//				"	and TM_PRD_FNCTN.END_SRVC_YEAR_MNTH \r\n" + 
//				"	AND CII_FACT_CLM_LINE.RPTG_PAID_YEAR_MNTH_NBR BETWEEN TM_PRD_FNCTN.START_YEAR_MNTH \r\n" + 
//				"	and TM_PRD_FNCTN.END_RPTG_PAID_YEAR_MNTH INNER JOIN DIM_DIAG \r\n" + 
//				"	ON CII_FACT_CLM_LINE.RPTG_PRNCPAL_DIAG_KEY = DIM_DIAG.DIAG_KEY  \r\n" + 
//				"WHERE	CII_FACT_CLM_LINE.ACCT_ID = 'W0016437'   \r\n" + 
//				"GROUP BY DIM_DIAG.HLTH_CNDTN_CTGRY_DESC,TM_PRD_FNCTN.TM_PRD_NM ) AS CLMS  \r\n" + 
//				"GROUP BY CLMS.DIM_DIAG_HLTH_CNDTN_CTGRY_DESC,CLMS.TM_PRD_NM ) AS FNL   \r\n" + 
//				"ORDER BY Health_Condition_Category_Description,Total_Utilization_Count";
//		
//		String query5 = "SELECT Time_Period AS \"Time Period\",Health_Condition_Category_Description AS \"Health Condition Category Description\",\r\n" + 
//				"		Total_Utilization_Count AS \"Total Utilization Count\",Paid_Amount AS \"Paid Amount\",\r\n" + 
//				"		Account_Name AS \"Account Name\",CBSA_Name AS \"CBSA Name\",ICD_Diagnosis_Source_Short_Code_Definition_Text AS \"ICD Diagnosis Source Short Code Definition Text\",\r\n" + 
//				"		Paid_PMPM AS \"Paid PMPM\",Paid_HCC_Indicator AS \"Paid HCC Indicator\"  \r\n" + 
//				"FROM	 (  \r\n" + 
//				"SELECT	CLMS.TM_PRD_NM AS Time_Period,CLMS.DIM_DIAG_HLTH_CNDTN_CTGRY_DESC AS Health_Condition_Category_Description,\r\n" + 
//				"		SUM(CLMS.TOTL_UTIL_CNT) AS Total_Utilization_Count,SUM(CLMS.ACCT_PAID_AMT) AS Paid_Amount,\r\n" + 
//				"		CLMS.CII_ACCT_PRFL_ACCT_NM AS Account_Name,CLMS.DIM_CBSA_CBSA_NM AS CBSA_Name,\r\n" + 
//				"		CLMS.DIM_DIAG_SRC_SHRT_CD_DEFN_TXT AS ICD_Diagnosis_Source_Short_Code_Definition_Text,\r\n" + 
//				"		CASE \r\n" + 
//				"	WHEN SUM(MBRSHP.TOTL_CVRG_CNT) = 0 THEN 0 \r\n" + 
//				"	ELSE SUM(CAST (CLMS.ACCT_PAID_AMT as DECIMAL(18,6))) / SUM(CAST (MBRSHP.TOTL_CVRG_CNT as DECIMAL(18,\r\n" + 
//				"		6))) \r\n" + 
//				"END	AS Paid_PMPM,CLMS.PAID_HCC_IND AS Paid_HCC_Indicator  \r\n" + 
//				"FROM	 (  \r\n" + 
//				"SELECT	TM_PRD_FNCTN.TM_PRD_NM AS TM_PRD_NM,DIM_DIAG.HLTH_CNDTN_CTGRY_DESC AS DIM_DIAG_HLTH_CNDTN_CTGRY_DESC,\r\n" + 
//				"		sum(cii_fact_clm_line.ACCT_ADMT_CNT +cii_fact_clm_line.ACCT_VST_CNT) AS TOTL_UTIL_CNT,\r\n" + 
//				"		SUM(CII_FACT_CLM_LINE.ACCT_PAID_AMT) AS ACCT_PAID_AMT,CII_ACCT_PRFL.ACCT_NM AS CII_ACCT_PRFL_ACCT_NM,\r\n" + 
//				"		DIM_CBSA.CBSA_NM AS DIM_CBSA_CBSA_NM,DIM_DIAG.SRC_SHRT_CD_DEFN_TXT AS DIM_DIAG_SRC_SHRT_CD_DEFN_TXT,\r\n" + 
//				"		COALESCE(HCC_CHK.PAID_HCC_IND,'N') AS PAID_HCC_IND,CII_FACT_CLM_LINE.CBSA_ID AS CBSA_ID,\r\n" + 
//				"		CII_FACT_CLM_LINE.ACCT_ID AS ACCT_ID  \r\n" + 
//				"FROM	CII_FACT_CLM_LINE INNER JOIN (\r\n" + 
//				"SELECT	B.*, \r\n" + 
//				"case	\r\n" + 
//				"	when paidIncurred='Paid' then 111101 \r\n" + 
//				"	else B.START_YEAR_MNTH \r\n" + 
//				"end	as STRT_SRVC_YEAR_MNTH,\r\n" + 
//				"case	\r\n" + 
//				"	WHEN paidIncurred='Paid' then 888811 \r\n" + 
//				"	else B.END_YEAR_MNTH \r\n" + 
//				"end	as END_SRVC_YEAR_MNTH,\r\n" + 
//				"Case	\r\n" + 
//				"	when paidIncurred='Paid' then B.END_YEAR_MNTH \r\n" + 
//				"	when paidIncurred='Incurred' \r\n" + 
//				"	and 0=0 then 201908 \r\n" + 
//				"	else 888811 \r\n" + 
//				"end	as END_RPTG_PAID_YEAR_MNTH \r\n" + 
//				"FROM	ACIISST_PERIOD_KEY A JOIN     (\r\n" + 
//				"SELECT	LKUP_ID, YEAR_ID,  CONCAT('','Paid') AS paidIncurred, \r\n" + 
//				"CASE	\r\n" + 
//				"	WHEN YEAR_ID=1 THEN 'Current Period' \r\n" + 
//				"	WHEN YEAR_ID=2 THEN 'Prior Period' \r\n" + 
//				"	ELSE 'Prior Period 2' \r\n" + 
//				"END	AS TM_PRD_NM, \r\n" + 
//				"CASE	\r\n" + 
//				"	WHEN YEAR_ID=1 THEN 201809 \r\n" + 
//				"	WHEN YEAR_ID=2 THEN 201709 \r\n" + 
//				"	ELSE 201609 \r\n" + 
//				"END	AS START_YEAR_MNTH,     \r\n" + 
//				"CASE	\r\n" + 
//				"	WHEN YEAR_ID=1 THEN 201908 \r\n" + 
//				"	WHEN YEAR_ID=2 THEN 201808    \r\n" + 
//				"	ELSE  201708 \r\n" + 
//				"END	AS END_YEAR_MNTH \r\n" + 
//				"FROM	ACIISST_PERIOD_KEY) B \r\n" + 
//				"	ON A.LKUP_ID = B.LKUP_ID \r\n" + 
//				"	AND B.YEAR_ID = A.YEAR_ID \r\n" + 
//				"WHERE	A.LKUP_ID=1 \r\n" + 
//				"	AND A.YEAR_ID <= 3) TM_PRD_FNCTN \r\n" + 
//				"	ON CII_FACT_CLM_LINE.CLM_SRVC_YEAR_MNTH_NBR BETWEEN TM_PRD_FNCTN.STRT_SRVC_YEAR_MNTH \r\n" + 
//				"	and TM_PRD_FNCTN.END_SRVC_YEAR_MNTH \r\n" + 
//				"	AND CII_FACT_CLM_LINE.RPTG_PAID_YEAR_MNTH_NBR BETWEEN TM_PRD_FNCTN.START_YEAR_MNTH \r\n" + 
//				"	and TM_PRD_FNCTN.END_RPTG_PAID_YEAR_MNTH INNER JOIN DIM_DIAG \r\n" + 
//				"	ON CII_FACT_CLM_LINE.RPTG_PRNCPAL_DIAG_KEY = DIM_DIAG.DIAG_KEY INNER JOIN CII_ACCT_PRFL \r\n" + 
//				"	ON CII_FACT_CLM_LINE.ACCT_ID = CII_ACCT_PRFL.ACCT_ID INNER JOIN DIM_CBSA \r\n" + 
//				"	ON CII_FACT_CLM_LINE.CBSA_ID = DIM_CBSA.CBSA_ID LEFT JOIN (\r\n" + 
//				"select TM_PRD_FNCTN.TM_PRD_NM, cii_fact_clm_line.MCID,  \r\n" + 
//				"CASE \r\n" + 
//				"	WHEN  CONCAT('','Paid') = 'Paid' THEN \r\n" + 
//				"CASE	\r\n" + 
//				"	WHEN SUM(cii_fact_clm_line.ACCT_PAID_AMT) >= 100000  THEN 'Y' \r\n" + 
//				"	ELSE 'N' \r\n" + 
//				"END\r\n" + 
//				"	ELSE \r\n" + 
//				"CASE	\r\n" + 
//				"	WHEN SUM(cii_fact_clm_line.ACCT_ALWD_AMT) >= 100000  THEN 'Y' \r\n" + 
//				"	ELSE 'N' \r\n" + 
//				"END\r\n" + 
//				"END AS PAID_HCC_IND\r\n" + 
//				"from cii_fact_clm_line \r\n" + 
//				"JOIN (\r\n" + 
//				"SELECT	B.*, \r\n" + 
//				"case \r\n" + 
//				" when paidIncurred='Paid' then 111101 \r\n" + 
//				" else B.START_YEAR_MNTH \r\n" + 
//				"end as STRT_SRVC_YEAR_MNTH,\r\n" + 
//				"case \r\n" + 
//				" WHEN paidIncurred='Paid' then 888811 \r\n" + 
//				" else B.END_YEAR_MNTH \r\n" + 
//				"end as END_SRVC_YEAR_MNTH,\r\n" + 
//				"Case \r\n" + 
//				" when paidIncurred='Paid' then B.END_YEAR_MNTH \r\n" + 
//				" when paidIncurred='Incurred' \r\n" + 
//				" and 0=0 then 201908 \r\n" + 
//				" else 888811 \r\n" + 
//				"end as END_RPTG_PAID_YEAR_MNTH \r\n" + 
//				"FROM ACIISST_PERIOD_KEY A JOIN (\r\n" + 
//				"SELECT LKUP_ID, YEAR_ID, CONCAT('','Paid') AS paidIncurred, \r\n" + 
//				"CASE \r\n" + 
//				" WHEN YEAR_ID=1 THEN 'Current Period' \r\n" + 
//				" WHEN YEAR_ID=2 THEN 'Prior Period' \r\n" + 
//				" ELSE 'Prior Period 2'\r\n" + 
//				"END AS TM_PRD_NM, \r\n" + 
//				"CASE \r\n" + 
//				" WHEN YEAR_ID=1 THEN 201809 \r\n" + 
//				" WHEN YEAR_ID=2 THEN 201709 \r\n" + 
//				" ELSE 201609 \r\n" + 
//				"END AS START_YEAR_MNTH, \r\n" + 
//				"CASE \r\n" + 
//				" WHEN YEAR_ID=1 THEN 201908 \r\n" + 
//				" WHEN YEAR_ID=2 THEN 201808  \r\n" + 
//				" ELSE  201708 \r\n" + 
//				"END AS END_YEAR_MNTH \r\n" + 
//				"FROM ACIISST_PERIOD_KEY) B \r\n" + 
//				" ON A.LKUP_ID = B.LKUP_ID \r\n" + 
//				" AND B.YEAR_ID = A.YEAR_ID \r\n" + 
//				"WHERE A.LKUP_ID=1 \r\n" + 
//				" AND A.YEAR_ID <= 3)  TM_PRD_FNCTN \r\n" + 
//				" ON CLM_SRVC_YEAR_MNTH_NBR BETWEEN TM_PRD_FNCTN.STRT_SRVC_YEAR_MNTH \r\n" + 
//				"	and TM_PRD_FNCTN.END_SRVC_YEAR_MNTH\r\n" + 
//				"	AND RPTG_PAID_YEAR_MNTH_NBR between TM_PRD_FNCTN.START_YEAR_MNTH \r\n" + 
//				"	and TM_PRD_FNCTN.END_RPTG_PAID_YEAR_MNTH\r\n" + 
//				"WHERE cii_fact_clm_line.ACCT_ID = 'W0016437'\r\n" + 
//				"	AND cii_fact_clm_line.MBR_CVRG_TYPE_CD IN ('001','102')\r\n" + 
//				"group by TM_PRD_FNCTN.TM_PRD_NM, cii_fact_clm_line.MCID\r\n" + 
//				")HCC_CHK \r\n" + 
//				"	ON HCC_CHK.MCID=CII_FACT_CLM_LINE.MCID \r\n" + 
//				"	and HCC_CHK.TM_PRD_NM=TM_PRD_FNCTN.TM_PRD_NM  \r\n" + 
//				"WHERE	CII_FACT_CLM_LINE.ACCT_ID = 'W0016437'   \r\n" + 
//				"GROUP BY TM_PRD_FNCTN.TM_PRD_NM,DIM_DIAG.HLTH_CNDTN_CTGRY_DESC,\r\n" + 
//				"		CII_ACCT_PRFL.ACCT_NM,DIM_CBSA.CBSA_NM,DIM_DIAG.SRC_SHRT_CD_DEFN_TXT,\r\n" + 
//				"		COALESCE(HCC_CHK.PAID_HCC_IND,'N'),CII_FACT_CLM_LINE.CBSA_ID,\r\n" + 
//				"		CII_FACT_CLM_LINE.ACCT_ID ) AS CLMS LEFT OUTER JOIN  (  \r\n" + 
//				"SELECT	TM_PRD_FNCTN.TM_PRD_NM AS TM_PRD_NM,SUM(\r\n" + 
//				"CASE	\r\n" + 
//				"	WHEN TMBRSHP.MIN_CVRG_PRTY_NBR IS NULL THEN 0 \r\n" + 
//				"	ELSE CII_FACT_MBRSHP.MBR_CVRG_CNT \r\n" + 
//				"END) AS TOTL_CVRG_CNT,CII_FACT_MBRSHP.CBSA_ID AS CBSA_ID,CII_FACT_MBRSHP.ACCT_ID AS ACCT_ID  \r\n" + 
//				"FROM	CII_FACT_MBRSHP INNER JOIN (\r\n" + 
//				"SELECT	B.*, \r\n" + 
//				"case	\r\n" + 
//				"	when paidIncurred='Paid' then 111101 \r\n" + 
//				"	else B.START_YEAR_MNTH \r\n" + 
//				"end	as STRT_SRVC_YEAR_MNTH,\r\n" + 
//				"case	\r\n" + 
//				"	WHEN paidIncurred='Paid' then 888811 \r\n" + 
//				"	else B.END_YEAR_MNTH \r\n" + 
//				"end	as END_SRVC_YEAR_MNTH,\r\n" + 
//				"Case	\r\n" + 
//				"	when paidIncurred='Paid' then B.END_YEAR_MNTH \r\n" + 
//				"	when paidIncurred='Incurred' \r\n" + 
//				"	and 0=0 then 201908 \r\n" + 
//				"	else 888811 \r\n" + 
//				"end	as END_RPTG_PAID_YEAR_MNTH \r\n" + 
//				"FROM	ACIISST_PERIOD_KEY A JOIN     (\r\n" + 
//				"SELECT	LKUP_ID, YEAR_ID,  CONCAT('','Paid') AS paidIncurred, \r\n" + 
//				"CASE	\r\n" + 
//				"	WHEN YEAR_ID=1 THEN 'Current Period' \r\n" + 
//				"	WHEN YEAR_ID=2 THEN 'Prior Period' \r\n" + 
//				"	ELSE 'Prior Period 2' \r\n" + 
//				"END	AS TM_PRD_NM, \r\n" + 
//				"CASE	\r\n" + 
//				"	WHEN YEAR_ID=1 THEN 201809 \r\n" + 
//				"	WHEN YEAR_ID=2 THEN 201709 \r\n" + 
//				"	ELSE 201609 \r\n" + 
//				"END	AS START_YEAR_MNTH,     \r\n" + 
//				"CASE	\r\n" + 
//				"	WHEN YEAR_ID=1 THEN 201908 \r\n" + 
//				"	WHEN YEAR_ID=2 THEN 201808    \r\n" + 
//				"	ELSE  201708 \r\n" + 
//				"END	AS END_YEAR_MNTH \r\n" + 
//				"FROM	ACIISST_PERIOD_KEY) B \r\n" + 
//				"	ON A.LKUP_ID = B.LKUP_ID \r\n" + 
//				"	AND B.YEAR_ID = A.YEAR_ID \r\n" + 
//				"WHERE	A.LKUP_ID=1 \r\n" + 
//				"	AND A.YEAR_ID <= 3) TM_PRD_FNCTN \r\n" + 
//				"	ON CII_FACT_MBRSHP.ELGBLTY_CY_MNTH_END_NBR BETWEEN TM_PRD_FNCTN.START_YEAR_MNTH \r\n" + 
//				"	and TM_PRD_FNCTN.END_YEAR_MNTH LEFT JOIN (\r\n" + 
//				"select acct_id as FRC_JN,ELGBLTY_CY_MNTH_END_NBR, MCID,  MIN(CVRG_PRTY_NBR) as MIN_CVRG_PRTY_NBR,\r\n" + 
//				"		TM_PRD_FNCTN.TM_PRD_NM\r\n" + 
//				"from cii_fact_mbrshp \r\n" + 
//				"JOIN (\r\n" + 
//				"SELECT	B.*, \r\n" + 
//				"case \r\n" + 
//				" when paidIncurred='Paid' then 111101 \r\n" + 
//				" else B.START_YEAR_MNTH \r\n" + 
//				"end as STRT_SRVC_YEAR_MNTH,\r\n" + 
//				"case \r\n" + 
//				" WHEN paidIncurred='Paid' then 888811 \r\n" + 
//				" else B.END_YEAR_MNTH \r\n" + 
//				"end as END_SRVC_YEAR_MNTH,\r\n" + 
//				"Case \r\n" + 
//				" when paidIncurred='Paid' then B.END_YEAR_MNTH \r\n" + 
//				" when paidIncurred='Incurred' \r\n" + 
//				" and 0=0 then 201908 \r\n" + 
//				" else 888811 \r\n" + 
//				"end as END_RPTG_PAID_YEAR_MNTH \r\n" + 
//				"FROM ACIISST_PERIOD_KEY A JOIN (\r\n" + 
//				"SELECT LKUP_ID, YEAR_ID, CONCAT('','Paid') AS paidIncurred, \r\n" + 
//				"CASE \r\n" + 
//				" WHEN YEAR_ID=1 THEN 'Current Period' \r\n" + 
//				" WHEN YEAR_ID=2 THEN 'Prior Period' \r\n" + 
//				" ELSE 'Prior Period 2'\r\n" + 
//				"END AS TM_PRD_NM, \r\n" + 
//				"CASE \r\n" + 
//				" WHEN YEAR_ID=1 THEN 201809 \r\n" + 
//				" WHEN YEAR_ID=2 THEN 201709 \r\n" + 
//				" ELSE 201609 \r\n" + 
//				"END AS START_YEAR_MNTH, \r\n" + 
//				"CASE \r\n" + 
//				" WHEN YEAR_ID=1 THEN 201908 \r\n" + 
//				" WHEN YEAR_ID=2 THEN 201808  \r\n" + 
//				" ELSE  201708 \r\n" + 
//				"END AS END_YEAR_MNTH \r\n" + 
//				"FROM ACIISST_PERIOD_KEY) B \r\n" + 
//				" ON A.LKUP_ID = B.LKUP_ID \r\n" + 
//				" AND B.YEAR_ID = A.YEAR_ID \r\n" + 
//				"WHERE A.LKUP_ID=1 \r\n" + 
//				" AND A.YEAR_ID <= 3) \r\n" + 
//				" TM_PRD_FNCTN \r\n" + 
//				"	ON CII_FACT_MBRSHP.ELGBLTY_CY_MNTH_END_NBR BETWEEN TM_PRD_FNCTN.START_YEAR_MNTH \r\n" + 
//				"	and TM_PRD_FNCTN.END_YEAR_MNTH\r\n" + 
//				"\r\n" + 
//				"WHERE CII_FACT_MBRSHP.ACCT_ID = 'W0016437'\r\n" + 
//				"GROUP BY acct_id, ELGBLTY_CY_MNTH_END_NBR, MCID,TM_PRD_NM\r\n" + 
//				")  TMBRSHP \r\n" + 
//				"	ON CII_FACT_MBRSHP.CVRG_PRTY_NBR = TMBRSHP.MIN_CVRG_PRTY_NBR \r\n" + 
//				"	and CII_FACT_MBRSHP.MCID=TMBRSHP.MCID \r\n" + 
//				"	and TMBRSHP.TM_PRD_NM=TM_PRD_FNCTN.TM_PRD_NM\r\n" + 
//				"	and TMBRSHP.ELGBLTY_CY_MNTH_END_NBR=CII_FACT_MBRSHP.ELGBLTY_CY_MNTH_END_NBR  \r\n" + 
//				"WHERE	CII_FACT_MBRSHP.ACCT_ID = 'W0016437'   \r\n" + 
//				"GROUP BY TM_PRD_FNCTN.TM_PRD_NM,CII_FACT_MBRSHP.CBSA_ID,CII_FACT_MBRSHP.ACCT_ID ) AS MBRSHP \r\n" + 
//				"	ON CLMS.CBSA_ID=MBRSHP.CBSA_ID \r\n" + 
//				"	AND CLMS.ACCT_ID=MBRSHP.ACCT_ID \r\n" + 
//				"	AND CLMS.TM_PRD_NM=MBRSHP.TM_PRD_NM  \r\n" + 
//				"GROUP BY CLMS.TM_PRD_NM,CLMS.DIM_DIAG_HLTH_CNDTN_CTGRY_DESC,CLMS.CII_ACCT_PRFL_ACCT_NM,\r\n" + 
//				"		CLMS.DIM_CBSA_CBSA_NM,CLMS.DIM_DIAG_SRC_SHRT_CD_DEFN_TXT,CLMS.PAID_HCC_IND ) AS FNL   \r\n" + 
//				"ORDER BY Time_Period,Health_Condition_Category_Description,Total_Utilization_Count,\r\n" + 
//				"		Paid_Amount,Account_Name,CBSA_Name,ICD_Diagnosis_Source_Short_Code_Definition_Text,\r\n" + 
//				"		Paid_PMPM,Paid_HCC_Indicator";
//		
//		String query6 = "\r\n" + 
//				" SELECT Time_Period AS \"Time_Period\",Health_Condition_Category_Description AS \"Health_Condition_Category_Description\",\r\n" + 
//				"		Total_Utilization_Count AS \"Total_Utilization_Count\",Paid_Amount AS \"Paid_Amount\",\r\n" + 
//				"		Account_Name AS \"Account_Name\",CBSA_Name AS \"CBSA_Name\",ICD_Diagnosis_Source_Short_Code_Definition_Text AS \"ICD_Diagnosis_Source_Short_Code_Definition_Text \" ,\r\n" + 
//				"		Paid_PMPM AS \"Paid_PMPM\", Paid_HCC_Indicator AS \"Paid_HCC_Indicator\",\r\n" + 
//				"		Age_Group_Description AS \"Age_Group_Description\"  \r\n" + 
//				"FROM	 (  \r\n" + 
//				"SELECT	CLMS.TM_PRD_NM AS Time_Period,CLMS.DIM_DIAG_HLTH_CNDTN_CTGRY_DESC AS Health_Condition_Category_Description,\r\n" + 
//				"		SUM(CLMS.TOTL_UTIL_CNT) AS Total_Utilization_Count,SUM(CLMS.ACCT_PAID_AMT) AS Paid_Amount,\r\n" + 
//				"		CLMS.CII_ACCT_PRFL_ACCT_NM AS Account_Name,CLMS.DIM_CBSA_CBSA_NM AS CBSA_Name,\r\n" + 
//				"		CLMS.DIM_DIAG_SRC_SHRT_CD_DEFN_TXT AS ICD_Diagnosis_Source_Short_Code_Definition_Text,\r\n" + 
//				"		CASE \r\n" + 
//				"	WHEN SUM(MBRSHP.TOTL_CVRG_CNT) = 0 THEN 0 \r\n" + 
//				"	ELSE SUM(CAST (CLMS.ACCT_PAID_AMT as DECIMAL(18,6))) / SUM(CAST (MBRSHP.TOTL_CVRG_CNT as DECIMAL(18,\r\n" + 
//				"		6))) \r\n" + 
//				"END	AS Paid_PMPM,CLMS.PAID_HCC_IND AS Paid_HCC_Indicator,\r\n" + 
//				"		CLMS.DIM_AGE_GRP_AGE_GRP_DESC AS Age_Group_Description  \r\n" + 
//				"FROM	 (  \r\n" + 
//				"SELECT	TM_PRD_FNCTN.TM_PRD_NM AS TM_PRD_NM,DIM_DIAG.HLTH_CNDTN_CTGRY_DESC AS DIM_DIAG_HLTH_CNDTN_CTGRY_DESC,\r\n" + 
//				"		sum(cii_fact_clm_line.ACCT_ADMT_CNT +cii_fact_clm_line.ACCT_VST_CNT) AS TOTL_UTIL_CNT,\r\n" + 
//				"		SUM(CII_FACT_CLM_LINE.ACCT_PAID_AMT) AS ACCT_PAID_AMT,CII_ACCT_PRFL.ACCT_NM AS CII_ACCT_PRFL_ACCT_NM,\r\n" + 
//				"		DIM_CBSA.CBSA_NM AS DIM_CBSA_CBSA_NM,DIM_DIAG.SRC_SHRT_CD_DEFN_TXT AS DIM_DIAG_SRC_SHRT_CD_DEFN_TXT,\r\n" + 
//				"		COALESCE(HCC_CHK.PAID_HCC_IND,'N') AS PAID_HCC_IND,\r\n" + 
//				"		DIM_AGE_GRP.AGE_GRP_DESC AS DIM_AGE_GRP_AGE_GRP_DESC,CII_FACT_CLM_LINE.CBSA_ID AS CBSA_ID,\r\n" + 
//				"		CII_FACT_CLM_LINE.ACCT_ID AS ACCT_ID,CII_FACT_CLM_LINE.ACCT_AGE_GRP_KEY AS ACCT_AGE_GRP_KEY  \r\n" + 
//				"FROM	CII_FACT_CLM_LINE INNER JOIN (\r\n" + 
//				"SELECT	B.*, \r\n" + 
//				"case	\r\n" + 
//				"	when paidIncurred='Paid' then 111101 \r\n" + 
//				"	else B.START_YEAR_MNTH \r\n" + 
//				"end	as STRT_SRVC_YEAR_MNTH,\r\n" + 
//				"case	\r\n" + 
//				"	WHEN paidIncurred='Paid' then 888811 \r\n" + 
//				"	else B.END_YEAR_MNTH \r\n" + 
//				"end	as END_SRVC_YEAR_MNTH,\r\n" + 
//				"Case	\r\n" + 
//				"	when paidIncurred='Paid' then B.END_YEAR_MNTH \r\n" + 
//				"	when paidIncurred='Incurred' \r\n" + 
//				"	and 0=0 then 201908 \r\n" + 
//				"	else 888811 \r\n" + 
//				"end	as END_RPTG_PAID_YEAR_MNTH \r\n" + 
//				"FROM	ACIISST_PERIOD_KEY A JOIN     (\r\n" + 
//				"SELECT	LKUP_ID, YEAR_ID,  CONCAT('','Paid') AS paidIncurred, \r\n" + 
//				"CASE	\r\n" + 
//				"	WHEN YEAR_ID=1 THEN 'Current Period' \r\n" + 
//				"	WHEN YEAR_ID=2 THEN 'Prior Period' \r\n" + 
//				"	ELSE 'Prior Period 2' \r\n" + 
//				"END	AS TM_PRD_NM, \r\n" + 
//				"CASE	\r\n" + 
//				"	WHEN YEAR_ID=1 THEN 201809 \r\n" + 
//				"	WHEN YEAR_ID=2 THEN 201709 \r\n" + 
//				"	ELSE 201609 \r\n" + 
//				"END	AS START_YEAR_MNTH,     \r\n" + 
//				"CASE	\r\n" + 
//				"	WHEN YEAR_ID=1 THEN 201908 \r\n" + 
//				"	WHEN YEAR_ID=2 THEN 201808    \r\n" + 
//				"	ELSE  201708 \r\n" + 
//				"END	AS END_YEAR_MNTH \r\n" + 
//				"FROM	ACIISST_PERIOD_KEY) B \r\n" + 
//				"	ON A.LKUP_ID = B.LKUP_ID \r\n" + 
//				"	AND B.YEAR_ID = A.YEAR_ID \r\n" + 
//				"WHERE	A.LKUP_ID=1 \r\n" + 
//				"	AND A.YEAR_ID <= 3) TM_PRD_FNCTN \r\n" + 
//				"	ON CII_FACT_CLM_LINE.CLM_SRVC_YEAR_MNTH_NBR BETWEEN TM_PRD_FNCTN.STRT_SRVC_YEAR_MNTH \r\n" + 
//				"	and TM_PRD_FNCTN.END_SRVC_YEAR_MNTH \r\n" + 
//				"	AND CII_FACT_CLM_LINE.RPTG_PAID_YEAR_MNTH_NBR BETWEEN TM_PRD_FNCTN.START_YEAR_MNTH \r\n" + 
//				"	and TM_PRD_FNCTN.END_RPTG_PAID_YEAR_MNTH INNER JOIN DIM_DIAG \r\n" + 
//				"	ON CII_FACT_CLM_LINE.RPTG_PRNCPAL_DIAG_KEY = DIM_DIAG.DIAG_KEY INNER JOIN CII_ACCT_PRFL \r\n" + 
//				"	ON CII_FACT_CLM_LINE.ACCT_ID = CII_ACCT_PRFL.ACCT_ID INNER JOIN DIM_CBSA \r\n" + 
//				"	ON CII_FACT_CLM_LINE.CBSA_ID = DIM_CBSA.CBSA_ID LEFT JOIN (\r\n" + 
//				"select TM_PRD_FNCTN.TM_PRD_NM, cii_fact_clm_line.MCID,  \r\n" + 
//				"CASE \r\n" + 
//				"	WHEN  CONCAT('','Paid') = 'Paid' THEN \r\n" + 
//				"CASE	\r\n" + 
//				"	WHEN SUM(cii_fact_clm_line.ACCT_PAID_AMT) >= 100000  THEN 'Y' \r\n" + 
//				"	ELSE 'N' \r\n" + 
//				"END\r\n" + 
//				"	ELSE \r\n" + 
//				"CASE	\r\n" + 
//				"	WHEN SUM(cii_fact_clm_line.ACCT_ALWD_AMT) >= 100000  THEN 'Y' \r\n" + 
//				"	ELSE 'N' \r\n" + 
//				"END\r\n" + 
//				"END AS PAID_HCC_IND\r\n" + 
//				"from cii_fact_clm_line \r\n" + 
//				"JOIN (\r\n" + 
//				"SELECT	B.*, \r\n" + 
//				"case \r\n" + 
//				" when paidIncurred='Paid' then 111101 \r\n" + 
//				" else B.START_YEAR_MNTH \r\n" + 
//				"end as STRT_SRVC_YEAR_MNTH,\r\n" + 
//				"case \r\n" + 
//				" WHEN paidIncurred='Paid' then 888811 \r\n" + 
//				" else B.END_YEAR_MNTH \r\n" + 
//				"end as END_SRVC_YEAR_MNTH,\r\n" + 
//				"Case \r\n" + 
//				" when paidIncurred='Paid' then B.END_YEAR_MNTH \r\n" + 
//				" when paidIncurred='Incurred' \r\n" + 
//				" and 0=0 then 201908 \r\n" + 
//				" else 888811 \r\n" + 
//				"end as END_RPTG_PAID_YEAR_MNTH \r\n" + 
//				"FROM ACIISST_PERIOD_KEY A JOIN (\r\n" + 
//				"SELECT LKUP_ID, YEAR_ID, CONCAT('','Paid') AS paidIncurred, \r\n" + 
//				"CASE \r\n" + 
//				" WHEN YEAR_ID=1 THEN 'Current Period' \r\n" + 
//				" WHEN YEAR_ID=2 THEN 'Prior Period' \r\n" + 
//				" ELSE 'Prior Period 2'\r\n" + 
//				"END AS TM_PRD_NM, \r\n" + 
//				"CASE \r\n" + 
//				" WHEN YEAR_ID=1 THEN 201809 \r\n" + 
//				" WHEN YEAR_ID=2 THEN 201709 \r\n" + 
//				" ELSE 201609 \r\n" + 
//				"END AS START_YEAR_MNTH, \r\n" + 
//				"CASE \r\n" + 
//				" WHEN YEAR_ID=1 THEN 201908 \r\n" + 
//				" WHEN YEAR_ID=2 THEN 201808  \r\n" + 
//				" ELSE  201708 \r\n" + 
//				"END AS END_YEAR_MNTH \r\n" + 
//				"FROM ACIISST_PERIOD_KEY) B \r\n" + 
//				" ON A.LKUP_ID = B.LKUP_ID \r\n" + 
//				" AND B.YEAR_ID = A.YEAR_ID \r\n" + 
//				"WHERE A.LKUP_ID=1 \r\n" + 
//				" AND A.YEAR_ID <= 3)  TM_PRD_FNCTN \r\n" + 
//				" ON CLM_SRVC_YEAR_MNTH_NBR BETWEEN TM_PRD_FNCTN.STRT_SRVC_YEAR_MNTH \r\n" + 
//				"	and TM_PRD_FNCTN.END_SRVC_YEAR_MNTH\r\n" + 
//				"	AND RPTG_PAID_YEAR_MNTH_NBR between TM_PRD_FNCTN.START_YEAR_MNTH \r\n" + 
//				"	and TM_PRD_FNCTN.END_RPTG_PAID_YEAR_MNTH\r\n" + 
//				"WHERE cii_fact_clm_line.ACCT_ID = 'W0016437'\r\n" + 
//				"	AND cii_fact_clm_line.MBR_CVRG_TYPE_CD IN ('001','102')\r\n" + 
//				"group by TM_PRD_FNCTN.TM_PRD_NM, cii_fact_clm_line.MCID\r\n" + 
//				")HCC_CHK \r\n" + 
//				"	ON HCC_CHK.MCID=CII_FACT_CLM_LINE.MCID \r\n" + 
//				"	and HCC_CHK.TM_PRD_NM=TM_PRD_FNCTN.TM_PRD_NM \r\n" + 
//				"	INNER JOIN DIM_AGE_GRP \r\n" + 
//				"	ON CII_FACT_CLM_LINE.ACCT_AGE_GRP_KEY = DIM_AGE_GRP.AGE_GRP_KEY  \r\n" + 
//				"WHERE	CII_FACT_CLM_LINE.ACCT_ID = 'W0016437'   \r\n" + 
//				"GROUP BY TM_PRD_FNCTN.TM_PRD_NM,DIM_DIAG.HLTH_CNDTN_CTGRY_DESC,\r\n" + 
//				"		CII_ACCT_PRFL.ACCT_NM,DIM_CBSA.CBSA_NM,DIM_DIAG.SRC_SHRT_CD_DEFN_TXT,\r\n" + 
//				"		COALESCE(HCC_CHK.PAID_HCC_IND,'N'),\r\n" + 
//				"			DIM_AGE_GRP.AGE_GRP_DESC,CII_FACT_CLM_LINE.CBSA_ID,CII_FACT_CLM_LINE.ACCT_ID,\r\n" + 
//				"		CII_FACT_CLM_LINE.ACCT_AGE_GRP_KEY ) AS CLMS LEFT OUTER JOIN  (  \r\n" + 
//				"SELECT	TM_PRD_FNCTN.TM_PRD_NM AS TM_PRD_NM,SUM(\r\n" + 
//				"CASE	\r\n" + 
//				"	WHEN TMBRSHP.MIN_CVRG_PRTY_NBR IS NULL THEN 0 \r\n" + 
//				"	ELSE CII_FACT_MBRSHP.MBR_CVRG_CNT \r\n" + 
//				"END) AS TOTL_CVRG_CNT,CII_FACT_MBRSHP.CBSA_ID AS CBSA_ID,CII_FACT_MBRSHP.ACCT_ID AS ACCT_ID,\r\n" + 
//				"		CII_FACT_MBRSHP.ACCT_AGE_GRP_KEY AS ACCT_AGE_GRP_KEY  \r\n" + 
//				"FROM	CII_FACT_MBRSHP INNER JOIN (\r\n" + 
//				"SELECT	B.*, \r\n" + 
//				"case	\r\n" + 
//				"	when paidIncurred='Paid' then 111101 \r\n" + 
//				"	else B.START_YEAR_MNTH \r\n" + 
//				"end	as STRT_SRVC_YEAR_MNTH,\r\n" + 
//				"case	\r\n" + 
//				"	WHEN paidIncurred='Paid' then 888811 \r\n" + 
//				"	else B.END_YEAR_MNTH \r\n" + 
//				"end	as END_SRVC_YEAR_MNTH,\r\n" + 
//				"Case	\r\n" + 
//				"	when paidIncurred='Paid' then B.END_YEAR_MNTH \r\n" + 
//				"	when paidIncurred='Incurred' \r\n" + 
//				"	and 0=0 then 201908 \r\n" + 
//				"	else 888811 \r\n" + 
//				"end	as END_RPTG_PAID_YEAR_MNTH \r\n" + 
//				"FROM	ACIISST_PERIOD_KEY A JOIN     (\r\n" + 
//				"SELECT	LKUP_ID, YEAR_ID,  CONCAT('','Paid') AS paidIncurred, \r\n" + 
//				"CASE	\r\n" + 
//				"	WHEN YEAR_ID=1 THEN 'Current Period' \r\n" + 
//				"	WHEN YEAR_ID=2 THEN 'Prior Period' \r\n" + 
//				"	ELSE 'Prior Period 2' \r\n" + 
//				"END	AS TM_PRD_NM, \r\n" + 
//				"CASE	\r\n" + 
//				"	WHEN YEAR_ID=1 THEN 201809 \r\n" + 
//				"	WHEN YEAR_ID=2 THEN 201709 \r\n" + 
//				"	ELSE 201609 \r\n" + 
//				"END	AS START_YEAR_MNTH,     \r\n" + 
//				"CASE	\r\n" + 
//				"	WHEN YEAR_ID=1 THEN 201908 \r\n" + 
//				"	WHEN YEAR_ID=2 THEN 201808    \r\n" + 
//				"	ELSE  201708 \r\n" + 
//				"END	AS END_YEAR_MNTH \r\n" + 
//				"FROM	ACIISST_PERIOD_KEY) B \r\n" + 
//				"	ON A.LKUP_ID = B.LKUP_ID \r\n" + 
//				"	AND B.YEAR_ID = A.YEAR_ID \r\n" + 
//				"WHERE	A.LKUP_ID=1 \r\n" + 
//				"	AND A.YEAR_ID <= 3) TM_PRD_FNCTN \r\n" + 
//				"	ON CII_FACT_MBRSHP.ELGBLTY_CY_MNTH_END_NBR BETWEEN TM_PRD_FNCTN.START_YEAR_MNTH \r\n" + 
//				"	and TM_PRD_FNCTN.END_YEAR_MNTH LEFT JOIN (\r\n" + 
//				"select acct_id as FRC_JN,ELGBLTY_CY_MNTH_END_NBR, MCID,  MIN(CVRG_PRTY_NBR) as MIN_CVRG_PRTY_NBR,\r\n" + 
//				"		TM_PRD_FNCTN.TM_PRD_NM\r\n" + 
//				"from cii_fact_mbrshp \r\n" + 
//				"JOIN (\r\n" + 
//				"SELECT	B.*, \r\n" + 
//				"case \r\n" + 
//				" when paidIncurred='Paid' then 111101 \r\n" + 
//				" else B.START_YEAR_MNTH \r\n" + 
//				"end as STRT_SRVC_YEAR_MNTH,\r\n" + 
//				"case \r\n" + 
//				" WHEN paidIncurred='Paid' then 888811 \r\n" + 
//				" else B.END_YEAR_MNTH \r\n" + 
//				"end as END_SRVC_YEAR_MNTH,\r\n" + 
//				"Case \r\n" + 
//				" when paidIncurred='Paid' then B.END_YEAR_MNTH \r\n" + 
//				" when paidIncurred='Incurred' \r\n" + 
//				" and 0=0 then 201908 \r\n" + 
//				" else 888811 \r\n" + 
//				"end as END_RPTG_PAID_YEAR_MNTH \r\n" + 
//				"FROM ACIISST_PERIOD_KEY A JOIN (\r\n" + 
//				"SELECT LKUP_ID, YEAR_ID, CONCAT('','Paid') AS paidIncurred, \r\n" + 
//				"CASE \r\n" + 
//				" WHEN YEAR_ID=1 THEN 'Current Period' \r\n" + 
//				" WHEN YEAR_ID=2 THEN 'Prior Period' \r\n" + 
//				" ELSE 'Prior Period 2'\r\n" + 
//				"END AS TM_PRD_NM, \r\n" + 
//				"CASE \r\n" + 
//				" WHEN YEAR_ID=1 THEN 201809 \r\n" + 
//				" WHEN YEAR_ID=2 THEN 201709 \r\n" + 
//				" ELSE 201609 \r\n" + 
//				"END AS START_YEAR_MNTH, \r\n" + 
//				"CASE \r\n" + 
//				" WHEN YEAR_ID=1 THEN 201908 \r\n" + 
//				" WHEN YEAR_ID=2 THEN 201808  \r\n" + 
//				" ELSE  201708 \r\n" + 
//				"END AS END_YEAR_MNTH \r\n" + 
//				"FROM ACIISST_PERIOD_KEY) B \r\n" + 
//				" ON A.LKUP_ID = B.LKUP_ID \r\n" + 
//				" AND B.YEAR_ID = A.YEAR_ID \r\n" + 
//				"WHERE A.LKUP_ID=1 \r\n" + 
//				" AND A.YEAR_ID <= 3) \r\n" + 
//				" TM_PRD_FNCTN \r\n" + 
//				"	ON CII_FACT_MBRSHP.ELGBLTY_CY_MNTH_END_NBR BETWEEN TM_PRD_FNCTN.START_YEAR_MNTH \r\n" + 
//				"	and TM_PRD_FNCTN.END_YEAR_MNTH\r\n" + 
//				"\r\n" + 
//				"WHERE CII_FACT_MBRSHP.ACCT_ID = 'W0016437'\r\n" + 
//				"GROUP BY acct_id, ELGBLTY_CY_MNTH_END_NBR, MCID,TM_PRD_NM\r\n" + 
//				")  TMBRSHP \r\n" + 
//				"	ON CII_FACT_MBRSHP.CVRG_PRTY_NBR = TMBRSHP.MIN_CVRG_PRTY_NBR \r\n" + 
//				"	and CII_FACT_MBRSHP.MCID=TMBRSHP.MCID \r\n" + 
//				"	and TMBRSHP.TM_PRD_NM=TM_PRD_FNCTN.TM_PRD_NM\r\n" + 
//				"	and TMBRSHP.ELGBLTY_CY_MNTH_END_NBR=CII_FACT_MBRSHP.ELGBLTY_CY_MNTH_END_NBR  \r\n" + 
//				"WHERE	CII_FACT_MBRSHP.ACCT_ID = 'W0016437'   \r\n" + 
//				"GROUP BY TM_PRD_FNCTN.TM_PRD_NM,CII_FACT_MBRSHP.CBSA_ID,CII_FACT_MBRSHP.ACCT_ID,\r\n" + 
//				"		CII_FACT_MBRSHP.ACCT_AGE_GRP_KEY ) AS MBRSHP \r\n" + 
//				"	ON CLMS.CBSA_ID=MBRSHP.CBSA_ID \r\n" + 
//				"	AND CLMS.ACCT_ID=MBRSHP.ACCT_ID \r\n" + 
//				"	AND CLMS.TM_PRD_NM=MBRSHP.TM_PRD_NM \r\n" + 
//				"	AND CLMS.ACCT_AGE_GRP_KEY=MBRSHP.ACCT_AGE_GRP_KEY  \r\n" + 
//				"GROUP BY CLMS.TM_PRD_NM,CLMS.DIM_DIAG_HLTH_CNDTN_CTGRY_DESC,CLMS.CII_ACCT_PRFL_ACCT_NM,\r\n" + 
//				"		CLMS.DIM_CBSA_CBSA_NM,CLMS.DIM_DIAG_SRC_SHRT_CD_DEFN_TXT,CLMS.PAID_HCC_IND,\r\n" + 
//				"		CLMS.DIM_AGE_GRP_AGE_GRP_DESC ) AS FNL\r\n" + 
//				"	";
//		String query7 = "SELECT Time_Period AS \"Time Period\",Health_Condition_Category_Description AS \"Health Condition Category Description\",\r\n" + 
//				"		Total_Utilization_Count AS \"Total Utilization Count\",Paid_Amount AS \"Paid Amount\",\r\n" + 
//				"		Account_Name AS \"Account Name\",CBSA_Name AS \"CBSA Name\",ICD_Diagnosis_Source_Short_Code_Definition_Text AS \"ICD Diagnosis Source Short Code Definition Text\",\r\n" + 
//				"		Paid_PMPM AS \"Paid PMPM\"  \r\n" + 
//				"FROM	 (  \r\n" + 
//				"SELECT	CLMS.TM_PRD_NM AS Time_Period,CLMS.DIM_DIAG_HLTH_CNDTN_CTGRY_DESC AS Health_Condition_Category_Description,\r\n" + 
//				"		SUM(CLMS.TOTL_UTIL_CNT) AS Total_Utilization_Count,SUM(CLMS.ACCT_PAID_AMT) AS Paid_Amount,\r\n" + 
//				"		CLMS.CII_ACCT_PRFL_ACCT_NM AS Account_Name,CLMS.DIM_CBSA_CBSA_NM AS CBSA_Name,\r\n" + 
//				"		CLMS.DIM_DIAG_SRC_SHRT_CD_DEFN_TXT AS ICD_Diagnosis_Source_Short_Code_Definition_Text,\r\n" + 
//				"		CASE \r\n" + 
//				"	WHEN SUM(MBRSHP.TOTL_CVRG_CNT) = 0 THEN 0 \r\n" + 
//				"	ELSE SUM(CAST (CLMS.ACCT_PAID_AMT as DECIMAL(18,6))) / SUM(CAST (MBRSHP.TOTL_CVRG_CNT as DECIMAL(18,\r\n" + 
//				"		6))) \r\n" + 
//				"END	AS Paid_PMPM\r\n" + 
//				"FROM	 (  \r\n" + 
//				"SELECT	TM_PRD_FNCTN.TM_PRD_NM AS TM_PRD_NM,DIM_DIAG.HLTH_CNDTN_CTGRY_DESC AS DIM_DIAG_HLTH_CNDTN_CTGRY_DESC,\r\n" + 
//				"		sum(cii_fact_clm_line.ACCT_ADMT_CNT +cii_fact_clm_line.ACCT_VST_CNT) AS TOTL_UTIL_CNT,\r\n" + 
//				"		SUM(CII_FACT_CLM_LINE.ACCT_PAID_AMT) AS ACCT_PAID_AMT,CII_ACCT_PRFL.ACCT_NM AS CII_ACCT_PRFL_ACCT_NM,\r\n" + 
//				"		DIM_CBSA.CBSA_NM AS DIM_CBSA_CBSA_NM,DIM_DIAG.SRC_SHRT_CD_DEFN_TXT AS DIM_DIAG_SRC_SHRT_CD_DEFN_TXT,\r\n" + 
//				"		CII_FACT_CLM_LINE.CBSA_ID AS CBSA_ID,CII_FACT_CLM_LINE.ACCT_ID AS ACCT_ID  \r\n" + 
//				"FROM	CII_FACT_CLM_LINE INNER JOIN (\r\n" + 
//				"SELECT	B.*, \r\n" + 
//				"case	\r\n" + 
//				"	when paidIncurred='Paid' then 111101 \r\n" + 
//				"	else B.START_YEAR_MNTH \r\n" + 
//				"end	as STRT_SRVC_YEAR_MNTH,\r\n" + 
//				"case	\r\n" + 
//				"	WHEN paidIncurred='Paid' then 888811 \r\n" + 
//				"	else B.END_YEAR_MNTH \r\n" + 
//				"end	as END_SRVC_YEAR_MNTH,\r\n" + 
//				"Case	\r\n" + 
//				"	when paidIncurred='Paid' then B.END_YEAR_MNTH \r\n" + 
//				"	when paidIncurred='Incurred' \r\n" + 
//				"	and 0=0 then 201908 \r\n" + 
//				"	else 888811 \r\n" + 
//				"end	as END_RPTG_PAID_YEAR_MNTH \r\n" + 
//				"FROM	ACIISST_PERIOD_KEY A JOIN     (\r\n" + 
//				"SELECT	LKUP_ID, YEAR_ID,  CONCAT('','Paid') AS paidIncurred, \r\n" + 
//				"CASE	\r\n" + 
//				"	WHEN YEAR_ID=1 THEN 'Current Period' \r\n" + 
//				"	WHEN YEAR_ID=2 THEN 'Prior Period' \r\n" + 
//				"	ELSE 'Prior Period 2' \r\n" + 
//				"END	AS TM_PRD_NM, \r\n" + 
//				"CASE	\r\n" + 
//				"	WHEN YEAR_ID=1 THEN 201809 \r\n" + 
//				"	WHEN YEAR_ID=2 THEN 201709 \r\n" + 
//				"	ELSE 201609 \r\n" + 
//				"END	AS START_YEAR_MNTH,     \r\n" + 
//				"CASE	\r\n" + 
//				"	WHEN YEAR_ID=1 THEN 201908 \r\n" + 
//				"	WHEN YEAR_ID=2 THEN 201808    \r\n" + 
//				"	ELSE  201708 \r\n" + 
//				"END	AS END_YEAR_MNTH \r\n" + 
//				"FROM	ACIISST_PERIOD_KEY) B \r\n" + 
//				"	ON A.LKUP_ID = B.LKUP_ID \r\n" + 
//				"	AND B.YEAR_ID = A.YEAR_ID \r\n" + 
//				"WHERE	A.LKUP_ID=1 \r\n" + 
//				"	AND A.YEAR_ID <= 3) TM_PRD_FNCTN \r\n" + 
//				"	ON CII_FACT_CLM_LINE.CLM_SRVC_YEAR_MNTH_NBR BETWEEN TM_PRD_FNCTN.STRT_SRVC_YEAR_MNTH \r\n" + 
//				"	and TM_PRD_FNCTN.END_SRVC_YEAR_MNTH \r\n" + 
//				"	AND CII_FACT_CLM_LINE.RPTG_PAID_YEAR_MNTH_NBR BETWEEN TM_PRD_FNCTN.START_YEAR_MNTH \r\n" + 
//				"	and TM_PRD_FNCTN.END_RPTG_PAID_YEAR_MNTH INNER JOIN DIM_DIAG \r\n" + 
//				"	ON CII_FACT_CLM_LINE.RPTG_PRNCPAL_DIAG_KEY = DIM_DIAG.DIAG_KEY INNER JOIN CII_ACCT_PRFL \r\n" + 
//				"	ON CII_FACT_CLM_LINE.ACCT_ID = CII_ACCT_PRFL.ACCT_ID INNER JOIN DIM_CBSA \r\n" + 
//				"	ON CII_FACT_CLM_LINE.CBSA_ID = DIM_CBSA.CBSA_ID  \r\n" + 
//				"WHERE	CII_FACT_CLM_LINE.ACCT_ID = 'W0016437'   \r\n" + 
//				"GROUP BY TM_PRD_FNCTN.TM_PRD_NM,DIM_DIAG.HLTH_CNDTN_CTGRY_DESC,\r\n" + 
//				"		CII_ACCT_PRFL.ACCT_NM,DIM_CBSA.CBSA_NM,DIM_DIAG.SRC_SHRT_CD_DEFN_TXT,\r\n" + 
//				"		CII_FACT_CLM_LINE.CBSA_ID,CII_FACT_CLM_LINE.ACCT_ID ) AS CLMS LEFT OUTER JOIN  (  \r\n" + 
//				"SELECT	TM_PRD_FNCTN.TM_PRD_NM AS TM_PRD_NM,SUM(\r\n" + 
//				"CASE	\r\n" + 
//				"	WHEN TMBRSHP.MIN_CVRG_PRTY_NBR IS NULL THEN 0 \r\n" + 
//				"	ELSE CII_FACT_MBRSHP.MBR_CVRG_CNT \r\n" + 
//				"END) AS TOTL_CVRG_CNT,CII_FACT_MBRSHP.CBSA_ID AS CBSA_ID,CII_FACT_MBRSHP.ACCT_ID AS ACCT_ID  \r\n" + 
//				"FROM	CII_FACT_MBRSHP INNER JOIN (\r\n" + 
//				"SELECT	B.*, \r\n" + 
//				"case	\r\n" + 
//				"	when paidIncurred='Paid' then 111101 \r\n" + 
//				"	else B.START_YEAR_MNTH \r\n" + 
//				"end	as STRT_SRVC_YEAR_MNTH,\r\n" + 
//				"case	\r\n" + 
//				"	WHEN paidIncurred='Paid' then 888811 \r\n" + 
//				"	else B.END_YEAR_MNTH \r\n" + 
//				"end	as END_SRVC_YEAR_MNTH,\r\n" + 
//				"Case	\r\n" + 
//				"	when paidIncurred='Paid' then B.END_YEAR_MNTH \r\n" + 
//				"	when paidIncurred='Incurred' \r\n" + 
//				"	and 0=0 then 201908 \r\n" + 
//				"	else 888811 \r\n" + 
//				"end	as END_RPTG_PAID_YEAR_MNTH \r\n" + 
//				"FROM	ACIISST_PERIOD_KEY A JOIN     (\r\n" + 
//				"SELECT	LKUP_ID, YEAR_ID,  CONCAT('','Paid') AS paidIncurred, \r\n" + 
//				"CASE	\r\n" + 
//				"	WHEN YEAR_ID=1 THEN 'Current Period' \r\n" + 
//				"	WHEN YEAR_ID=2 THEN 'Prior Period' \r\n" + 
//				"	ELSE 'Prior Period 2' \r\n" + 
//				"END	AS TM_PRD_NM, \r\n" + 
//				"CASE	\r\n" + 
//				"	WHEN YEAR_ID=1 THEN 201809 \r\n" + 
//				"	WHEN YEAR_ID=2 THEN 201709 \r\n" + 
//				"	ELSE 201609 \r\n" + 
//				"END	AS START_YEAR_MNTH,     \r\n" + 
//				"CASE	\r\n" + 
//				"	WHEN YEAR_ID=1 THEN 201908 \r\n" + 
//				"	WHEN YEAR_ID=2 THEN 201808    \r\n" + 
//				"	ELSE  201708 \r\n" + 
//				"END	AS END_YEAR_MNTH \r\n" + 
//				"FROM	ACIISST_PERIOD_KEY) B \r\n" + 
//				"	ON A.LKUP_ID = B.LKUP_ID \r\n" + 
//				"	AND B.YEAR_ID = A.YEAR_ID \r\n" + 
//				"WHERE	A.LKUP_ID=1 \r\n" + 
//				"	AND A.YEAR_ID <= 3) TM_PRD_FNCTN \r\n" + 
//				"	ON CII_FACT_MBRSHP.ELGBLTY_CY_MNTH_END_NBR BETWEEN TM_PRD_FNCTN.START_YEAR_MNTH \r\n" + 
//				"	and TM_PRD_FNCTN.END_YEAR_MNTH LEFT JOIN (\r\n" + 
//				"select acct_id as FRC_JN,ELGBLTY_CY_MNTH_END_NBR, MCID,  MIN(CVRG_PRTY_NBR) as MIN_CVRG_PRTY_NBR,\r\n" + 
//				"		TM_PRD_FNCTN.TM_PRD_NM\r\n" + 
//				"from cii_fact_mbrshp \r\n" + 
//				"JOIN (\r\n" + 
//				"SELECT	B.*, \r\n" + 
//				"case \r\n" + 
//				" when paidIncurred='Paid' then 111101 \r\n" + 
//				" else B.START_YEAR_MNTH \r\n" + 
//				"end as STRT_SRVC_YEAR_MNTH,\r\n" + 
//				"case \r\n" + 
//				" WHEN paidIncurred='Paid' then 888811 \r\n" + 
//				" else B.END_YEAR_MNTH \r\n" + 
//				"end as END_SRVC_YEAR_MNTH,\r\n" + 
//				"Case \r\n" + 
//				" when paidIncurred='Paid' then B.END_YEAR_MNTH \r\n" + 
//				" when paidIncurred='Incurred' \r\n" + 
//				" and 0=0 then 201908 \r\n" + 
//				" else 888811 \r\n" + 
//				"end as END_RPTG_PAID_YEAR_MNTH \r\n" + 
//				"FROM ACIISST_PERIOD_KEY A JOIN (\r\n" + 
//				"SELECT LKUP_ID, YEAR_ID, CONCAT('','Paid') AS paidIncurred, \r\n" + 
//				"CASE \r\n" + 
//				" WHEN YEAR_ID=1 THEN 'Current Period' \r\n" + 
//				" WHEN YEAR_ID=2 THEN 'Prior Period' \r\n" + 
//				" ELSE 'Prior Period 2'\r\n" + 
//				"END AS TM_PRD_NM, \r\n" + 
//				"CASE \r\n" + 
//				" WHEN YEAR_ID=1 THEN 201809 \r\n" + 
//				" WHEN YEAR_ID=2 THEN 201709 \r\n" + 
//				" ELSE 201609 \r\n" + 
//				"END AS START_YEAR_MNTH, \r\n" + 
//				"CASE \r\n" + 
//				" WHEN YEAR_ID=1 THEN 201908 \r\n" + 
//				" WHEN YEAR_ID=2 THEN 201808  \r\n" + 
//				" ELSE  201708 \r\n" + 
//				"END AS END_YEAR_MNTH \r\n" + 
//				"FROM ACIISST_PERIOD_KEY) B \r\n" + 
//				" ON A.LKUP_ID = B.LKUP_ID \r\n" + 
//				" AND B.YEAR_ID = A.YEAR_ID \r\n" + 
//				"WHERE A.LKUP_ID=1 \r\n" + 
//				" AND A.YEAR_ID <= 3) \r\n" + 
//				" TM_PRD_FNCTN \r\n" + 
//				"	ON CII_FACT_MBRSHP.ELGBLTY_CY_MNTH_END_NBR BETWEEN 20 \r\n" + 
//				"	and 30\r\n" + 
//				"\r\n" + 
//				"WHERE CII_FACT_MBRSHP.ACCT_ID = 'W0016437'\r\n" + 
//				"GROUP BY acct_id, ELGBLTY_CY_MNTH_END_NBR, MCID,TM_PRD_NM\r\n" + 
//				")  TMBRSHP \r\n" + 
//				"	ON CII_FACT_MBRSHP.CVRG_PRTY_NBR = TMBRSHP.MIN_CVRG_PRTY_NBR \r\n" + 
//				"	and CII_FACT_MBRSHP.MCID=TMBRSHP.MCID \r\n" + 
//				"	and TMBRSHP.TM_PRD_NM=TM_PRD_FNCTN.TM_PRD_NM\r\n" + 
//				"	and TMBRSHP.ELGBLTY_CY_MNTH_END_NBR=CII_FACT_MBRSHP.ELGBLTY_CY_MNTH_END_NBR  \r\n" + 
//				"WHERE	CII_FACT_MBRSHP.ACCT_ID = 'W0016437'   \r\n" + 
//				"GROUP BY TM_PRD_FNCTN.TM_PRD_NM,CII_FACT_MBRSHP.CBSA_ID,CII_FACT_MBRSHP.ACCT_ID ) AS MBRSHP \r\n" + 
//				"	ON CLMS.CBSA_ID=MBRSHP.CBSA_ID \r\n" + 
//				"	AND CLMS.ACCT_ID=MBRSHP.ACCT_ID \r\n" + 
//				"	AND CLMS.TM_PRD_NM=MBRSHP.TM_PRD_NM  \r\n" + 
//				"GROUP BY CLMS.TM_PRD_NM,CLMS.DIM_DIAG_HLTH_CNDTN_CTGRY_DESC,CLMS.CII_ACCT_PRFL_ACCT_NM,\r\n" + 
//				"		CLMS.DIM_CBSA_CBSA_NM,CLMS.DIM_DIAG_SRC_SHRT_CD_DEFN_TXT ) AS FNL   \r\n" + 
//				"ORDER BY Time_Period,Health_Condition_Category_Description,Total_Utilization_Count,\r\n" + 
//				"		Paid_Amount,Account_Name,CBSA_Name,ICD_Diagnosis_Source_Short_Code_Definition_Text,\r\n" + 
//				"		Paid_PMPM";
//		
//		String query8 = "Select a.abc, b.b1,  coalesce(a.a1, 'abcd'), a.a1 + 2 from a as a, (b) as b, (c) as c on a.id = b.id";
//		
//		String query9 = "SELECT DISTINCT Title.Title AS \"Genre\" , CASE WHEN TMBRSHP.MIN_CVRG_PRTY_NBR IS NULL THEN 0 ELSE CII_FACT_MBRSHP.MBR_CVRG_CNT END AS mango, sum(MovieBudget), ( CAST(( CAST(Title.MovieBudget  AS DECIMAL) * CAST(2 AS DECIMAL) )  AS DECIMAL) + CAST(Title.RevenueDomestic AS DECIMAL) ) AS \"Der_value\" FROM Title Title ";
//
//		String query10 = "SELECT DISTINCT Title.Title AS \"Genre\" , CASE WHEN TMBRSHP.MIN_CVRG_PRTY_NBR in ('a', 'b') THEN 0 WHEN TMBRSHP.MIN_CVRG_PRTY_NBR=30 THEN 21 ELSE CII_FACT_MBRSHP.MBR_CVRG_CNT END AS mango, sum(MovieBudget), ( CAST(( CAST(Title.MovieBudget  AS DECIMAL) * CAST(2 AS DECIMAL) )  AS DECIMAL) + CAST(Title.RevenueDomestic AS DECIMAL) ) AS \"Der_value\" FROM Title Title WHERE Genre in ('abc', 'y') or Genre=20";
//		
//		String query11 = "SELECT DISTINCT Title.MovieBudget AS \"MovieBudget\" , Title.RevenueDomestic AS \"RevenueDomestic\" , Title.RevenueInternational AS \"RevenueInternational\" , Title.RottenTomatoesAudience AS \"RottenTomatoesAudience\" , Title.RottenTomatoesCritics AS \"RottenTomatoesCritics\" , Title.Title AS \"Title\" FROM Title Title  WHERE (Title.MovieBudget) IN ( 1000000 , 1700000 ) \r\n";
//
//		String query12 = "SELECT DISTINCT customquery.\"MovieBudget\" AS \"MovieBudget\" , customquery.\"RevenueDomestic\" AS \"RevenueDomestic\" , customquery.\"RevenueInternational\" AS \"RevenueInternational\" , customquery.\"RottenTomatoesAudience\" AS \"RottenTomatoesAudience\" , customquery.\"RottenTomatoesCritics\" AS \"RottenTomatoesCritics\" , customquery.\"Title\" AS \"Title\" FROM (SELECT  avqLBU.\"MovieBudget\", avqLBU.\"RevenueDomestic\", avqLBU.\"RevenueInternational\", avqLBU.\"RottenTomatoesAudience\", avqLBU.\"RottenTomatoesCritics\", avqLBU.\"Title\"  FROM ( SELECT  Title.MovieBudget as \"MovieBudget\", Title.RevenueDomestic as \"RevenueDomestic\", Title.RevenueInternational as \"RevenueInternational\", Title.RottenTomatoesAudience as \"RottenTomatoesAudience\", Title.RottenTomatoesCritics as \"RottenTomatoesCritics\", Title.Title as \"Title\"  FROM Title as Title  WHERE (Title.MovieBudget)  IN  (1000000, 1700000)  UNION ALL  SELECT  Title.MovieBudget as \"MovieBudget\", Title.RevenueDomestic as \"RevenueDomestic\", Title.RevenueInternational as \"RevenueInternational\", Title.RottenTomatoesAudience as \"RottenTomatoesAudience\", Title.RottenTomatoesCritics as \"RottenTomatoesCritics\", Title.Title as \"Title\"  FROM Title as Title  WHERE (Title.MovieBudget)  IN  (500000, 800000)) AS avqLBU ) AS customquery\r\n";
//		
//		// testing queries for smart measure
//		
//		// harness 1 - multi level no join
//		// all with alias
//		String query13 = "Select B1 as A1, A.A2, A.A3 as A3 from "
//				+ "("
//				+ "Select B.B1, B.B2 as A2, B3 as A3 from "
//				+ "("
//				+ "Select C1 as B1, C2 as B2, C3 as B3 from C"
//				+ " where B3='Roof' ) B"
//				+ ") A where A3='poof'"
//				+ ";";
//		
//		// without alias
//		String query14 = "Select B1, A.A2, A.A3 as A3 from "
//				+ "("
//				+ "Select B.B1, B.B2 as A2, B3 as A3 from "
//				+ "("
//				+ "Select C1 as B1, C2 as B2, C3 as B3 from C"
//				+ ") B"
//				+ ") A"
//				+ ";";
//		
//		// without alias
//		// not in projection, but only in group by
//		String query15 = "Select A.A2, A.A3 as A3 from "
//				+ "("
//				+ "Select B.B1, B.B2 as A2, B3 as A3 from "
//				+ "("
//				+ "Select C1 as B1, C2 as B2, C3 as B3 from C"
//				+ ") B"
//				+ ") A group by B1"
//				+ ";";
//
//
//		// joins
//		String query16 = "Select A.A2, A.A3 as A3, detok(E.E1), E.E2 from "
//				+ "("
//				+ "Select B.B1, detok(B.B2) as A2, B3 as A3 from "
//				+ "("
//				+ "Select C1 as B1, C2 as B2, C3 as B3 from C "
//				+ "inner join ("
//				+ "Select D1, D2, detok(D3) as D3 from D where D3='Roof') D on "
//				+ "C.C1 = D.D1"
//				+ ") B"
//				+ ") A inner join ("
//				+ "Select E1, E2, E3 from E) E on B1=E.E1"
//				+ " where (A3 = 'Fun' and A3 = 'Bun') or A3 = 'Gun';";
//		
//		// A3 - Age Less Than
//		// Second A3 - Age Less than
//		
//		
//		String query17 = "Select a,b, c from tabla where 1 = 0 and (a = 'a' or c = 'q')";
//
//		String query18 = "Select A.A2, A.A3 as A3, detok(E.E1), E.E2 from "
//				+ "("
//				+ "Select B.B1, detok(B.B2) as A2, B3 as A3 from "
//				+ "("
//				+ "Select C1 as B1, C2 as B2, C3 as B3 from C "
//				+ "inner join ("
//				+ "Select D1, D2, detok(D3) as D3 from D where D3='Roof') D on "
//				+ "C.C1 = D.D1"
//				+ ") B"
//				+ ") A inner join ("
//				+ "Select E1, E2, E3 from E) E on B1=E.E1"
//				+ " where (A3 = 'Fun' or A3 = 'Bun') or A2 = 'Gun';";
//
//		
//		String query19 = "Select Nominated, Genre, Studio, Title, MovieBudget, RevenueDomestic, RevenueInternational from Movie2 where Studio = 'WB' and (MovieBudget > 10000000 or MovieBudget > 20000000)";
//		// there are a couple of scenarios to model for join
//		// I have only one join easy for me to drop this - what happens to remaining stuff ?
//		// I have 2 joins 
//		String query20 = "Select Nominated, Genre, Studio, Title, MovieBudget, RevenueDomestic, RevenueInternational from Movie2 where MovieBudget = 1000 and Studio ='WB' and RevenueDomestic in (1,2,3,4)";
//
//		String query21 = "Select d123.a, b, c from d as d123 where a = NULL";
//		
//		String query22 = "SELECT Account_ID AS \"Account ID\",Master_Consumer_ID AS \"Master Consumer ID\",Member_Coverage_Type_Description AS \"Member Coverage Type Description\",Member_Gender_Code AS \"Member Gender Code\",Reporting_Member_Relationship_Description AS \"Reporting Member Relationship Description\",Age_Group_Description AS \"Age Group Description\",Age_In_Years AS \"Age In Years\",Contract_Type_Code AS \"Contract Type Code\",Eligibility_Year_Month_Ending_Number AS \"Eligibility Year Month Ending Number\",State_Code AS \"State Code\",CBSA_Name AS \"CBSA Name\",Member_PCP_Indicator AS \"Member PCP Indicator\",Subscriber_ID AS \"Subscriber ID\",Continuous_Enrollment_for_1_Period AS \"Continuous Enrollment for 1 Period\",Member_Birth_Date AS \"Member Birth Date\",Account_Name AS \"Account Name\",Time_Period_Start AS \"Time Period Start\",Time_Period_End AS \"Time Period End\",Non_Utilizer_Indicator AS \"Non Utilizer Indicator\",Member_Coverage_Count AS \"Member Coverage Count\"  FROM  (  SELECT MBRSHP.ACCT_ID AS Account_ID,MBRSHP.MCID AS Master_Consumer_ID,MBRSHP.MBR_CVRG_TYPE_DESC AS Member_Coverage_Type_Description,MBRSHP.MBR_GNDR_CD AS Member_Gender_Code,MBRSHP.RPTG_MBR_RLTNSHP_DESC AS Reporting_Member_Relationship_Description,MBRSHP.AGE_GRP_DESC AS Age_Group_Description,MBRSHP.AGE_IN_YRS_NBR AS Age_In_Years,MBRSHP.CNTRCT_TYPE_CD AS Contract_Type_Code,MBRSHP.ELGBLTY_CY_MNTH_END_NBR AS Eligibility_Year_Month_Ending_Number,MBRSHP.ST_CD AS State_Code,MBRSHP.CBSA_NM AS CBSA_Name,MBRSHP.PCP_IND AS Member_PCP_Indicator,MBRSHP.FMBRSHP_SBSCRBR_ID AS Subscriber_ID,MBRSHP.CNTNUS_ENRLMNT_1_PRD_CD AS Continuous_Enrollment_for_1_Period,MBRSHP.MBR_BRTH_DT AS Member_Birth_Date,MBRSHP.ACCT_NM AS Account_Name,MBRSHP.TIME_PRD_STRT_NBR AS Time_Period_Start,MBRSHP.TIME_PRD_END_NBR AS Time_Period_End,MBRSHP.Non_Utilizer_Ind AS Non_Utilizer_Indicator,SUM(MBRSHP.SUM_MBR_CVRG_CNT) AS Member_Coverage_Count,MBRSHP.TM_PRD_NM AS Time_Period  FROM  (  SELECT CII_ACCT_PRFL.ACCT_ID AS ACCT_ID,CII_FACT_MBRSHP.MCID AS MCID,DIM_MBR_CVRG_TYPE.MBR_CVRG_TYPE_DESC AS MBR_CVRG_TYPE_DESC,CII_FACT_MBRSHP.MBR_GNDR_CD AS MBR_GNDR_CD,DIM_RPTG_MBR_RLTNSHP.RPTG_MBR_RLTNSHP_DESC AS RPTG_MBR_RLTNSHP_DESC,case when DIM_AGE_GRP.AGE_GRP_DESC='1-17' then '1 through 17' else  DIM_AGE_GRP.AGE_GRP_DESC end AS AGE_GRP_DESC,CII_FACT_MBRSHP.AGE_IN_YRS_NBR AS AGE_IN_YRS_NBR,CII_FACT_MBRSHP.CNTRCT_TYPE_CD AS CNTRCT_TYPE_CD,CII_FACT_MBRSHP.ELGBLTY_CY_MNTH_END_NBR AS ELGBLTY_CY_MNTH_END_NBR,CII_FACT_MBRSHP.ST_CD AS ST_CD,DIM_CBSA.CBSA_NM AS CBSA_NM,CII_FACT_MBRSHP.PCP_IND AS PCP_IND,CII_FACT_MBRSHP.SBSCRBR_ID AS FMBRSHP_SBSCRBR_ID,CE.CNTNUS_ENRLMNT_1_PRD_CD AS CNTNUS_ENRLMNT_1_PRD_CD,DIM_MCID.MBR_BRTH_DT AS MBR_BRTH_DT,CII_ACCT_PRFL.ACCT_NM AS ACCT_NM,CASE WHEN TM_PRD_FNCTN.TM_PRD_NM= 'Current Period'  THEN 201910 WHEN TM_PRD_FNCTN.TM_PRD_NM= 'Prior Period' THEN 201810 WHEN TM_PRD_FNCTN.TM_PRD_NM=  'Prior Period 2'  THEN 201801 end AS TIME_PRD_STRT_NBR,CASE WHEN TM_PRD_FNCTN.TM_PRD_NM= 'Current Period'  THEN 202009 WHEN TM_PRD_FNCTN.TM_PRD_NM= 'Prior Period' THEN 201909 WHEN TM_PRD_FNCTN.TM_PRD_NM=  'Prior Period 2'  THEN 201809 end AS TIME_PRD_END_NBR,UT.Non_Utilizer_Ind AS Non_Utilizer_Ind,SUM(MBR_CVRG_CNT) AS SUM_MBR_CVRG_CNT,TM_PRD_FNCTN.TM_PRD_NM AS TM_PRD_NM  FROM CII_FACT_MBRSHP INNER JOIN (SELECT YEAR_CD_NM as TM_PRD_NM,\r\n" + 
//				"            STRT_MNTH_NBR as START_YEAR_MNTH,\r\n" + 
//				"            END_MNTH_NBR as END_YEAR_MNTH,\r\n" + 
//				"            case\r\n" + 
//				"                        when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"                        and INCRD_PAID_CD = 'PAID' then 111101\r\n" + 
//				"                        when TM_PRD_TYPE_CD = 'Custom' then STRT_MNTH_NBR\r\n" + 
//				"                        else SRVC_STRT_MNTH_NBR\r\n" + 
//				"            end as STRT_SRVC_YEAR_MNTH,\r\n" + 
//				"            case\r\n" + 
//				"                        when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"                        and INCRD_PAID_CD = 'PAID' then 888812\r\n" + 
//				"                        when TM_PRD_TYPE_CD = 'Custom' then END_MNTH_NBR\r\n" + 
//				"                        else SRVC_END_MNTH_NBR\r\n" + 
//				"            end as END_SRVC_YEAR_MNTH,\r\n" + 
//				"            case\r\n" + 
//				"                        when TM_PRD_TYPE_CD <> 'Custom' then PAID_END_MNTH_NBR\r\n" + 
//				"                        when INCRD_PAID_CD = 'PAID' then END_MNTH_NBR\r\n" + 
//				"                        when INCRD_PAID_CD = 'INC2' then 888811\r\n" + 
//				"                        when INCRD_PAID_CD = 'INC3' then dm.YEAR_MNTH_NBR\r\n" + 
//				"                        when INCRD_PAID_CD = 'INC1' then PAID_END_MNTH_NBR\r\n" + 
//				"            end as END_RPTG_PAID_YEAR_MNTH\r\n" + 
//				"from\r\n" + 
//				"            DIM_TM_PRD_ADHC dtp\r\n" + 
//				"cross join DIM_MNTH dm\r\n" + 
//				"            where (dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"            and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"            and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"            --    AND DIM_TM_PRD_ADHC.TM_3061_RULE_IND = 'N'  \r\n" + 
//				"            and dtp.YEAR_ID <= 1\r\n" + 
//				"            --In ('Current','Prior','Prior 2')  \r\n" + 
//				"            and dtp.CSTM_STRT_CURNT_MNTH_NBR = 201910\r\n" + 
//				"            and dtp.CSTM_END_CURNT_MNTH_NBR = 202009\r\n" + 
//				"            and dm.YEAR_MNTH_NBR = 202009\r\n" + 
//				")\r\n" + 
//				"            or (dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"            and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"            and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"            --AND DIM_TM_PRD_ADHC.TM_3061_RULE_IND = 'N'\r\n" + 
//				"            and dtp.YEAR_ID <= 1\r\n" + 
//				"            --In ('Current','Prior','Prior 2')  \r\n" + 
//				"            and dtp.CSTM_STRT_PRIOR_MNTH_NBR = 201810\r\n" + 
//				"            and dtp.CSTM_END_PRIOR_MNTH_NBR = 201909\r\n" + 
//				"            and dm.YEAR_MNTH_NBR = 202009)\r\n" + 
//				"            or (dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"            and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"            and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"            --AND DIM_TM_PRD_ADHC.TM_3061_RULE_IND = 'N'\r\n" + 
//				"            and dtp.YEAR_ID <= 1\r\n" + 
//				"            --In ('Current','Prior','Prior 2') \r\n" + 
//				"            and dtp.CSTM_STRT_PRIOR_2_MNTH_NBR = 201801\r\n" + 
//				"            and dtp.CSTM_END_PRIOR_2_MNTH_NBR = 201809\r\n" + 
//				"            and dm.YEAR_MNTH_NBR = 202009) ) TM_PRD_FNCTN ON CII_FACT_MBRSHP.ELGBLTY_CY_MNTH_END_NBR BETWEEN TM_PRD_FNCTN.START_YEAR_MNTH and TM_PRD_FNCTN.END_YEAR_MNTH INNER JOIN ( Select\r\n" + 
//				"mbrshp.ACCT_ID,\r\n" + 
//				"mbrshp.MCID,\r\n" + 
//				"mbrshp.mbr_cvrg_type_cd,\r\n" + 
//				"mbrshp.tm_prd_nm,\r\n" + 
//				"Case \r\n" + 
//				"            when (mbrshp.mcid = clms.mcid \r\n" + 
//				"            and mbrshp.mbr_cvrg_type_cd = clms.mbr_cvrg_type_cd) then 'N' \r\n" + 
//				"            Else 'Y' \r\n" + 
//				"End      as Non_Utilizer_Ind\r\n" + 
//				"from\r\n" + 
//				"(\r\n" + 
//				"Select\r\n" + 
//				"fact.ACCT_ID,\r\n" + 
//				"MCID,\r\n" + 
//				"MBR_CVRG_TYPE_CD,\r\n" + 
//				"  TM_PRD_NM\r\n" + 
//				"  from\r\n" + 
//				"cii_fact_mbrshp fact\r\n" + 
//				"\r\n" + 
//				"JOIN (             \r\n" + 
//				"SELECT    YEAR_CD_NM as TM_PRD_NM,\r\n" + 
//				"            STRT_MNTH_NBR as START_YEAR_MNTH,\r\n" + 
//				"            END_MNTH_NBR as END_YEAR_MNTH,\r\n" + 
//				"            case\r\n" + 
//				"                        when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"                        and INCRD_PAID_CD = 'PAID' then 111101\r\n" + 
//				"                        when TM_PRD_TYPE_CD = 'Custom' then STRT_MNTH_NBR\r\n" + 
//				"                        else SRVC_STRT_MNTH_NBR\r\n" + 
//				"            end as STRT_SRVC_YEAR_MNTH,\r\n" + 
//				"            case\r\n" + 
//				"                        when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"                        and INCRD_PAID_CD = 'PAID' then 888812\r\n" + 
//				"                        when TM_PRD_TYPE_CD = 'Custom' then END_MNTH_NBR\r\n" + 
//				"                        else SRVC_END_MNTH_NBR\r\n" + 
//				"            end as END_SRVC_YEAR_MNTH,\r\n" + 
//				"            case\r\n" + 
//				"                        when TM_PRD_TYPE_CD <> 'Custom' then PAID_END_MNTH_NBR\r\n" + 
//				"                        when INCRD_PAID_CD = 'PAID' then END_MNTH_NBR\r\n" + 
//				"                        when INCRD_PAID_CD = 'INC2' then 888811\r\n" + 
//				"                        when INCRD_PAID_CD = 'INC3' then dm.YEAR_MNTH_NBR\r\n" + 
//				"                        when INCRD_PAID_CD = 'INC1' then PAID_END_MNTH_NBR\r\n" + 
//				"            end as END_RPTG_PAID_YEAR_MNTH\r\n" + 
//				"from\r\n" + 
//				"            DIM_TM_PRD_ADHC dtp\r\n" + 
//				"cross join DIM_MNTH dm\r\n" + 
//				"            where (dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"            and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"            and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"            --    AND DIM_TM_PRD_ADHC.TM_3061_RULE_IND = 'N'  \r\n" + 
//				"            and dtp.YEAR_ID <= 1\r\n" + 
//				"            --In ('Current','Prior','Prior 2')  \r\n" + 
//				"            and dtp.CSTM_STRT_CURNT_MNTH_NBR = 201910\r\n" + 
//				"            and dtp.CSTM_END_CURNT_MNTH_NBR = 202009\r\n" + 
//				"            and dm.YEAR_MNTH_NBR = 202009\r\n" + 
//				")\r\n" + 
//				"            or (dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"            and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"            and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"            --AND DIM_TM_PRD_ADHC.TM_3061_RULE_IND = 'N'\r\n" + 
//				"            and dtp.YEAR_ID <= 1\r\n" + 
//				"            --In ('Current','Prior','Prior 2')  \r\n" + 
//				"            and dtp.CSTM_STRT_PRIOR_MNTH_NBR = 201810\r\n" + 
//				"            and dtp.CSTM_END_PRIOR_MNTH_NBR = 201909\r\n" + 
//				"            and dm.YEAR_MNTH_NBR = 202009)\r\n" + 
//				"            or (dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"            and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"            and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"            --AND DIM_TM_PRD_ADHC.TM_3061_RULE_IND = 'N'\r\n" + 
//				"            and dtp.YEAR_ID <= 1\r\n" + 
//				"            --In ('Current','Prior','Prior 2') \r\n" + 
//				"            and dtp.CSTM_STRT_PRIOR_2_MNTH_NBR = 201801\r\n" + 
//				"            and dtp.CSTM_END_PRIOR_2_MNTH_NBR = 201809\r\n" + 
//				"            and dm.YEAR_MNTH_NBR = 202009)) TM_PRD_FNCTN                 \r\n" + 
//				"    ON fact.ELGBLTY_CY_MNTH_END_NBR BETWEEN TM_PRD_FNCTN.START_YEAR_MNTH                \r\n" + 
//				"    and TM_PRD_FNCTN.END_YEAR_MNTH  \r\n" + 
//				"            INNER JOIN (Select ACCT_ID,SGMNTN_DIM_KEY from ACIISST_SGMNTN_BRDG where ACIISST_USER_ID= 16200068 and ACCT_ID in ('W0004156') and SRC_FLTR_ID= '2c64ebe7-d64b-48ee-99cc-d66eb263819d' and FLTR_SRC_NM= 'User Session')SGMNTN on fact.SGMNTN_DIM_KEY = SGMNTN.SGMNTN_DIM_KEY and fact.ACCT_ID=SGMNTN.ACCT_ID  \r\n" + 
//				"WHERE      fact.ACCT_ID = 'W0004156'                  \r\n" + 
//				"GROUP BY  fact.acct_id,  fact.MCID, TM_PRD_FNCTN.TM_PRD_NM, fact.MBR_CVRG_TYPE_CD\r\n" + 
//				") mbrshp\r\n" + 
//				"\r\n" + 
//				"left outer join\r\n" + 
//				"(\r\n" + 
//				"Select\r\n" + 
//				"clm.ACCT_ID,\r\n" + 
//				"MCID,\r\n" + 
//				"MBR_CVRG_TYPE_CD,\r\n" + 
//				"TM_PRD_NM\r\n" + 
//				"\r\n" + 
//				"from\r\n" + 
//				"cii_fact_clm_line clm\r\n" + 
//				"JOIN (             \r\n" + 
//				"SELECT     YEAR_CD_NM as TM_PRD_NM,\r\n" + 
//				"            STRT_MNTH_NBR as START_YEAR_MNTH,\r\n" + 
//				"            END_MNTH_NBR as END_YEAR_MNTH,\r\n" + 
//				"            case\r\n" + 
//				"                        when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"                        and INCRD_PAID_CD = 'PAID' then 111101\r\n" + 
//				"                        when TM_PRD_TYPE_CD = 'Custom' then STRT_MNTH_NBR\r\n" + 
//				"                        else SRVC_STRT_MNTH_NBR\r\n" + 
//				"            end as STRT_SRVC_YEAR_MNTH,\r\n" + 
//				"            case\r\n" + 
//				"                        when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"                        and INCRD_PAID_CD = 'PAID' then 888812\r\n" + 
//				"                        when TM_PRD_TYPE_CD = 'Custom' then END_MNTH_NBR\r\n" + 
//				"                        else SRVC_END_MNTH_NBR\r\n" + 
//				"            end as END_SRVC_YEAR_MNTH,\r\n" + 
//				"            case\r\n" + 
//				"                        when TM_PRD_TYPE_CD <> 'Custom' then PAID_END_MNTH_NBR\r\n" + 
//				"                        when INCRD_PAID_CD = 'PAID' then END_MNTH_NBR\r\n" + 
//				"                        when INCRD_PAID_CD = 'INC2' then 888811\r\n" + 
//				"                        when INCRD_PAID_CD = 'INC3' then dm.YEAR_MNTH_NBR\r\n" + 
//				"                        when INCRD_PAID_CD = 'INC1' then PAID_END_MNTH_NBR\r\n" + 
//				"            end as END_RPTG_PAID_YEAR_MNTH\r\n" + 
//				"from\r\n" + 
//				"            DIM_TM_PRD_ADHC dtp\r\n" + 
//				"cross join DIM_MNTH dm\r\n" + 
//				"            where (dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"            and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"            and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"            --    AND DIM_TM_PRD_ADHC.TM_3061_RULE_IND = 'N'  \r\n" + 
//				"            and dtp.YEAR_ID <= 1\r\n" + 
//				"            --In ('Current','Prior','Prior 2')  \r\n" + 
//				"            and dtp.CSTM_STRT_CURNT_MNTH_NBR = 201910\r\n" + 
//				"            and dtp.CSTM_END_CURNT_MNTH_NBR = 202009\r\n" + 
//				"            and dm.YEAR_MNTH_NBR = 202009\r\n" + 
//				")\r\n" + 
//				"            or (dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"            and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"            and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"            --AND DIM_TM_PRD_ADHC.TM_3061_RULE_IND = 'N'\r\n" + 
//				"            and dtp.YEAR_ID <= 1\r\n" + 
//				"            --In ('Current','Prior','Prior 2')  \r\n" + 
//				"            and dtp.CSTM_STRT_PRIOR_MNTH_NBR = 201810\r\n" + 
//				"            and dtp.CSTM_END_PRIOR_MNTH_NBR = 201909\r\n" + 
//				"            and dm.YEAR_MNTH_NBR = 202009)\r\n" + 
//				"            or (dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"            and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"            and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"            --AND DIM_TM_PRD_ADHC.TM_3061_RULE_IND = 'N'\r\n" + 
//				"            and dtp.YEAR_ID <= 1\r\n" + 
//				"            --In ('Current','Prior','Prior 2') \r\n" + 
//				"            and dtp.CSTM_STRT_PRIOR_2_MNTH_NBR = 201801\r\n" + 
//				"            and dtp.CSTM_END_PRIOR_2_MNTH_NBR = 201809\r\n" + 
//				"            and dm.YEAR_MNTH_NBR = 202009)) TM_PRD_FNCTN                 \r\n" + 
//				"    ON clm.CLM_SRVC_YEAR_MNTH_NBR BETWEEN TM_PRD_FNCTN.START_YEAR_MNTH   \r\n" + 
//				"            and TM_PRD_FNCTN.END_YEAR_MNTH  \r\n" + 
//				"            INNER JOIN (Select ACCT_ID,SGMNTN_DIM_KEY from ACIISST_SGMNTN_BRDG where ACIISST_USER_ID= 16200068 and ACCT_ID in ('W0004156') and SRC_FLTR_ID= '2c64ebe7-d64b-48ee-99cc-d66eb263819d' and FLTR_SRC_NM= 'User Session')SGMNTN on clm.SGMNTN_DIM_KEY = SGMNTN.SGMNTN_DIM_KEY and clm.ACCT_ID=SGMNTN.ACCT_ID\r\n" + 
//				"WHERE      clm.ACCT_ID =  'W0004156'                        \r\n" + 
//				"GROUP BY  clm.acct_id,  clm.MCID, TM_PRD_FNCTN.TM_PRD_NM, clm.MBR_CVRG_TYPE_CD\r\n" + 
//				") clms\r\n" + 
//				"\r\n" + 
//				"            on\r\n" + 
//				"mbrshp.acct_id = clms.acct_id \r\n" + 
//				"            and mbrshp.mcid=clms.mcid  \r\n" + 
//				"            and mbrshp.TM_PRD_NM = clms.tm_prd_nm \r\n" + 
//				"            and mbrshp.mbr_cvrg_type_cd  = clms.mbr_cvrg_type_cd  ) UT\r\n" + 
//				"\r\n" + 
//				"            ON TM_PRD_FNCTN.TM_PRD_NM = UT.TM_PRD_NM  \r\n" + 
//				"            AND  CII_FACT_MBRSHP.ACCT_ID =UT.ACCT_ID  \r\n" + 
//				"            AND  CII_FACT_MBRSHP.MCID =UT.MCID  \r\n" + 
//				"            and CII_FACT_MBRSHP.MBR_CVRG_TYPE_CD = UT.MBR_CVRG_TYPE_CD INNER JOIN CII_ACCT_PRFL ON CII_FACT_MBRSHP.ACCT_ID = CII_ACCT_PRFL.ACCT_ID INNER JOIN DIM_MBR_CVRG_TYPE ON CII_FACT_MBRSHP.MBR_CVRG_TYPE_CD = DIM_MBR_CVRG_TYPE.MBR_CVRG_TYPE_CD INNER JOIN DIM_RPTG_MBR_RLTNSHP ON CII_FACT_MBRSHP.RPTG_MBR_RLTNSHP_CD = DIM_RPTG_MBR_RLTNSHP.RPTG_MBR_RLTNSHP_CD INNER JOIN DIM_AGE_GRP ON CII_FACT_MBRSHP.ACCT_AGE_GRP_KEY = DIM_AGE_GRP.AGE_GRP_KEY INNER JOIN DIM_CBSA ON CII_FACT_MBRSHP.CBSA_ID = DIM_CBSA.CBSA_ID INNER  JOIN ( \r\n" + 
//				"            select     m.TM_PRD_NM,  m.acct_id, m.mcid, m. MBR_CVRG_TYPE_CD ,\r\n" + 
//				"   case\r\n" + 
//				"    when m.mnths = b.mnths\r\n" + 
//				"    and m.MBR_CVRG_TYPE_CD <> 'back_fill' then 'Continuous'\r\n" + 
//				"          when strt_mnth = start_prd\r\n" + 
//				"    and end_mnth<>end_prd\r\n" + 
//				"    and m.MBR_CVRG_TYPE_CD <> 'back_fill' then 'Termed'\r\n" + 
//				"          when strt_mnth <> start_prd\r\n" + 
//				"    and end_mnth=end_prd\r\n" + 
//				"    and m.MBR_CVRG_TYPE_CD <> 'back_fill'then 'Added'\r\n" + 
//				"          when m.MBR_CVRG_TYPE_CD = 'back_fill' Then 'NA'\r\n" + 
//				"          else 'Other'\r\n" + 
//				"    end as CNTNUS_ENRLMNT_1_PRD_CD\r\n" + 
//				"            from     (\r\n" + 
//				"                        select   fact .acct_id,\r\n" + 
//				"                                                fact .MCID,\r\n" + 
//				"                                                TM_PRD_FNCTN.TM_PRD_NM,\r\n" + 
//				"                                                fact.MBR_CVRG_TYPE_CD ,\r\n" + 
//				"                                                count (distinct fact .ELGBLTY_CY_MNTH_END_NBR) mnths ,\r\n" + 
//				"                                                min(fact .ELGBLTY_CY_MNTH_END_NBR) strt_mnth ,\r\n" + 
//				"                                                max(fact .ELGBLTY_CY_MNTH_END_NBR) \r\n" + 
//				"                                                end_mnth \r\n" + 
//				"                        from     cii_fact_mbrshp fact  JOIN (              \r\n" + 
//				"                                    SELECT                YEAR_CD_NM as TM_PRD_NM,\r\n" + 
//				"            STRT_MNTH_NBR as START_YEAR_MNTH,\r\n" + 
//				"            END_MNTH_NBR as END_YEAR_MNTH,\r\n" + 
//				"            case\r\n" + 
//				"                        when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"                        and INCRD_PAID_CD = 'PAID' then 111101\r\n" + 
//				"                        when TM_PRD_TYPE_CD = 'Custom' then STRT_MNTH_NBR\r\n" + 
//				"                        else SRVC_STRT_MNTH_NBR\r\n" + 
//				"            end as STRT_SRVC_YEAR_MNTH,\r\n" + 
//				"            case\r\n" + 
//				"                        when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"                        and INCRD_PAID_CD = 'PAID' then 888812\r\n" + 
//				"                        when TM_PRD_TYPE_CD = 'Custom' then END_MNTH_NBR\r\n" + 
//				"                        else SRVC_END_MNTH_NBR\r\n" + 
//				"            end as END_SRVC_YEAR_MNTH,\r\n" + 
//				"            case\r\n" + 
//				"                        when TM_PRD_TYPE_CD <> 'Custom' then PAID_END_MNTH_NBR\r\n" + 
//				"                        when INCRD_PAID_CD = 'PAID' then END_MNTH_NBR\r\n" + 
//				"                        when INCRD_PAID_CD = 'INC2' then 888811\r\n" + 
//				"                        when INCRD_PAID_CD = 'INC3' then dm.YEAR_MNTH_NBR\r\n" + 
//				"                        when INCRD_PAID_CD = 'INC1' then PAID_END_MNTH_NBR\r\n" + 
//				"            end as END_RPTG_PAID_YEAR_MNTH\r\n" + 
//				"from\r\n" + 
//				"            DIM_TM_PRD_ADHC dtp\r\n" + 
//				"cross join DIM_MNTH dm\r\n" + 
//				"            where (dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"            and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"            and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"            --    AND DIM_TM_PRD_ADHC.TM_3061_RULE_IND = 'N'  \r\n" + 
//				"            and dtp.YEAR_ID <= 1\r\n" + 
//				"            --In ('Current','Prior','Prior 2')  \r\n" + 
//				"            and dtp.CSTM_STRT_CURNT_MNTH_NBR = 201910\r\n" + 
//				"            and dtp.CSTM_END_CURNT_MNTH_NBR = 202009\r\n" + 
//				"            and dm.YEAR_MNTH_NBR = 202009\r\n" + 
//				")\r\n" + 
//				"            or (dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"            and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"            and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"            --AND DIM_TM_PRD_ADHC.TM_3061_RULE_IND = 'N'\r\n" + 
//				"            and dtp.YEAR_ID <= 1\r\n" + 
//				"            --In ('Current','Prior','Prior 2')  \r\n" + 
//				"            and dtp.CSTM_STRT_PRIOR_MNTH_NBR = 201810\r\n" + 
//				"            and dtp.CSTM_END_PRIOR_MNTH_NBR = 201909\r\n" + 
//				"            and dm.YEAR_MNTH_NBR = 202009)\r\n" + 
//				"            or (dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"            and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"            and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"            --AND DIM_TM_PRD_ADHC.TM_3061_RULE_IND = 'N'\r\n" + 
//				"            and dtp.YEAR_ID <= 1\r\n" + 
//				"            --In ('Current','Prior','Prior 2') \r\n" + 
//				"            and dtp.CSTM_STRT_PRIOR_2_MNTH_NBR = 201801\r\n" + 
//				"            and dtp.CSTM_END_PRIOR_2_MNTH_NBR = 201809\r\n" + 
//				"            and dm.YEAR_MNTH_NBR = 202009) ) TM_PRD_FNCTN                      \r\n" + 
//				"                                    ON fact .ELGBLTY_CY_MNTH_END_NBR BETWEEN TM_PRD_FNCTN.START_YEAR_MNTH  \r\n" + 
//				"                                    and TM_PRD_FNCTN. \r\n" + 
//				"                                                END_YEAR_MNTH INNER JOIN (Select ACCT_ID,SGMNTN_DIM_KEY from ACIISST_SGMNTN_BRDG where ACIISST_USER_ID= 16200068 and ACCT_ID in ('W0004156') and SRC_FLTR_ID= '2c64ebe7-d64b-48ee-99cc-d66eb263819d' and FLTR_SRC_NM= 'User Session')SGMNTN on fact.SGMNTN_DIM_KEY = SGMNTN.SGMNTN_DIM_KEY and fact.ACCT_ID=SGMNTN.ACCT_ID   \r\n" + 
//				"                        WHERE            fact .ACCT_ID = 'W0004156'                         \r\n" + 
//				"                        GROUP BY  fact .acct_id,\r\n" + 
//				"                                                fact .MCID,\r\n" + 
//				"                                                TM_PRD_FNCTN.TM_PRD_NM,\r\n" + 
//				"                                                fact .MBR_CVRG_TYPE_CD ) m \r\n" + 
//				"            inner join (\r\n" + 
//				"                        select   \r\n" + 
//				"                                                case \r\n" + 
//				"                                                            when YEAR_ID= 1 then 'Current Period'  \r\n" + 
//				"                                                            when YEAR_ID= 2 then 'Prior Period' \r\n" + 
//				"                                                            when YEAR_ID= 3 then 'Prior Period 2'   \r\n" + 
//				"                                                            ELSE    'FOO' \r\n" + 
//				"                                                END as TM_PRD_NM,\r\n" + 
//				"                                                max(e_abs) - max(s_abs)+ 1 mnths,\r\n" + 
//				"                                                max(s_prd) as start_prd,\r\n" + 
//				"                                                max(e_prd)  as \r\n" + 
//				"                                                end_prd \r\n" + 
//				"                        from        ( \r\n" + 
//				"                                    select        \r\n" + 
//				"                                                            CASE \r\n" + 
//				"                                                                        WHEN YEAR_MNTH_NBR =  201910  \r\n" + 
//				"                                                or YEAR_MNTH_NBR = 202009 then 1 \r\n" + 
//				"                                                                        WHEN YEAR_MNTH_NBR = 201810  \r\n" + 
//				"                                                or YEAR_MNTH_NBR =  201909   then 2   \r\n" + 
//				"                                                                        WHEN  YEAR_MNTH_NBR = 201801  \r\n" + 
//				"                                                or  YEAR_MNTH_NBR = 201809 then  3 \r\n" + 
//				"                                                                        ELSE  4 \r\n" + 
//				"                                                            END  as YEAR_ID,\r\n" + 
//				"                                                            CASE \r\n" + 
//				"                                                                        WHEN YEAR_MNTH_NBR =  201910  \r\n" + 
//				"                                                or  YEAR_MNTH_NBR = 201810  \r\n" + 
//				"                                                or YEAR_MNTH_NBR = 201801  then ABS_YEAR_MNTH_NBR \r\n" + 
//				"                                                                        else null \r\n" + 
//				"                                                            end as s_abs ,\r\n" + 
//				"                                                            CASE \r\n" + 
//				"                                                                        WHEN YEAR_MNTH_NBR =  201910  \r\n" + 
//				"                                                or YEAR_MNTH_NBR = 201810  \r\n" + 
//				"                                                or YEAR_MNTH_NBR = 201801  then YEAR_MNTH_NBR \r\n" + 
//				"                                                                        else null \r\n" + 
//				"                                                            end as s_prd ,\r\n" + 
//				"                                                                             \r\n" + 
//				"                                                            CASE  \r\n" + 
//				"                                                                        WHEN YEAR_MNTH_NBR = 202009  \r\n" + 
//				"                                                or YEAR_MNTH_NBR =  201909  \r\n" + 
//				"                                                or YEAR_MNTH_NBR = 201809  then ABS_YEAR_MNTH_NBR \r\n" + 
//				"                                                                        else null \r\n" + 
//				"                                                            end as e_abs,\r\n" + 
//				"                                                            CASE \r\n" + 
//				"                                                                        WHEN YEAR_MNTH_NBR = 202009  \r\n" + 
//				"                                                or YEAR_MNTH_NBR =  201909  \r\n" + 
//				"                                                or YEAR_MNTH_NBR = 201809  then YEAR_MNTH_NBR \r\n" + 
//				"                                                                        else null \r\n" + 
//				"                                                            end as e_prd                 \r\n" + 
//				"                                    from     dim_mnth   \r\n" + 
//				"                                    where    \r\n" + 
//				"                                                            CASE \r\n" + 
//				"                                                                        WHEN YEAR_MNTH_NBR =  201910  \r\n" + 
//				"                                                or YEAR_MNTH_NBR = 202009 then 1               \r\n" + 
//				"                                                                        WHEN YEAR_MNTH_NBR = 201810  \r\n" + 
//				"                                                or YEAR_MNTH_NBR =  201909   then 2 \r\n" + 
//				"                                                                        WHEN  YEAR_MNTH_NBR = 201801  \r\n" + 
//				"                                                or  YEAR_MNTH_NBR = 201809 then  3 \r\n" + 
//				"                                                                        ELSE 0 \r\n" + 
//				"                                                            END  > 0 ) a  \r\n" + 
//				"                        group by \r\n" + 
//				"                                                case \r\n" + 
//				"                                                            when YEAR_ID= 1 then 'Current Period' \r\n" + 
//				"                                                            when YEAR_ID= 2 then 'Prior Period'  \r\n" + 
//				"                                                            when YEAR_ID= 3 then 'Prior Period 2'   \r\n" + 
//				"                                                            ELSE    'FOO' \r\n" + 
//				"                                                END) b     \r\n" + 
//				"                        on  m.TM_PRD_NM = b.TM_PRD_NM) CE              \r\n" + 
//				"            ON TM_PRD_FNCTN.TM_PRD_NM = CE.TM_PRD_NM      \r\n" + 
//				"            AND  CII_FACT_MBRSHP.ACCT_ID =CE.ACCT_ID     \r\n" + 
//				"            AND  CII_FACT_MBRSHP.MCID =CE.MCID     \r\n" + 
//				"            and CII_FACT_MBRSHP.MBR_CVRG_TYPE_CD = CE.MBR_CVRG_TYPE_CD INNER JOIN DIM_MCID ON CII_FACT_MBRSHP.MCID = DIM_MCID.MCID AND CII_FACT_MBRSHP.ACCT_ID = DIM_MCID.ACCT_ID INNER JOIN (Select ACCT_ID,SGMNTN_DIM_KEY from ACIISST_SGMNTN_BRDG where ACIISST_USER_ID= 16200068 and ACCT_ID in ('W0004156') and SRC_FLTR_ID= '2c64ebe7-d64b-48ee-99cc-d66eb263819d' and FLTR_SRC_NM= 'User Session')SGMNTN on CII_FACT_MBRSHP.SGMNTN_DIM_KEY = SGMNTN.SGMNTN_DIM_KEY and CII_FACT_MBRSHP.ACCT_ID=SGMNTN.ACCT_ID WHERE CII_FACT_MBRSHP.ACCT_ID in ('W0004156')   GROUP BY CII_ACCT_PRFL.ACCT_ID,CII_FACT_MBRSHP.MCID,DIM_MBR_CVRG_TYPE.MBR_CVRG_TYPE_DESC,CII_FACT_MBRSHP.MBR_GNDR_CD,DIM_RPTG_MBR_RLTNSHP.RPTG_MBR_RLTNSHP_DESC,case when DIM_AGE_GRP.AGE_GRP_DESC='1-17' then '1 through 17' else  DIM_AGE_GRP.AGE_GRP_DESC end,CII_FACT_MBRSHP.AGE_IN_YRS_NBR,CII_FACT_MBRSHP.CNTRCT_TYPE_CD,CII_FACT_MBRSHP.ELGBLTY_CY_MNTH_END_NBR,CII_FACT_MBRSHP.ST_CD,DIM_CBSA.CBSA_NM,CII_FACT_MBRSHP.PCP_IND,CII_FACT_MBRSHP.SBSCRBR_ID,CE.CNTNUS_ENRLMNT_1_PRD_CD,DIM_MCID.MBR_BRTH_DT,CII_ACCT_PRFL.ACCT_NM,CASE WHEN TM_PRD_FNCTN.TM_PRD_NM= 'Current Period'  THEN 201910 WHEN TM_PRD_FNCTN.TM_PRD_NM= 'Prior Period' THEN 201810 WHEN TM_PRD_FNCTN.TM_PRD_NM=  'Prior Period 2'  THEN 201801 end,CASE WHEN TM_PRD_FNCTN.TM_PRD_NM= 'Current Period'  THEN 202009 WHEN TM_PRD_FNCTN.TM_PRD_NM= 'Prior Period' THEN 201909 WHEN TM_PRD_FNCTN.TM_PRD_NM=  'Prior Period 2'  THEN 201809 end,UT.Non_Utilizer_Ind,TM_PRD_FNCTN.TM_PRD_NM ) AS MBRSHP  GROUP BY MBRSHP.ACCT_ID,MBRSHP.MCID,MBRSHP.MBR_CVRG_TYPE_DESC,MBRSHP.MBR_GNDR_CD,MBRSHP.RPTG_MBR_RLTNSHP_DESC,MBRSHP.AGE_GRP_DESC,MBRSHP.AGE_IN_YRS_NBR,MBRSHP.CNTRCT_TYPE_CD,MBRSHP.ELGBLTY_CY_MNTH_END_NBR,MBRSHP.ST_CD,MBRSHP.CBSA_NM,MBRSHP.PCP_IND,MBRSHP.FMBRSHP_SBSCRBR_ID,MBRSHP.CNTNUS_ENRLMNT_1_PRD_CD,MBRSHP.MBR_BRTH_DT,MBRSHP.ACCT_NM,MBRSHP.TIME_PRD_STRT_NBR,MBRSHP.TIME_PRD_END_NBR,MBRSHP.Non_Utilizer_Ind,MBRSHP.TM_PRD_NM ) AS FNL   ORDER BY Account_ID,Master_Consumer_ID,Member_Coverage_Type_Description,Member_Gender_Code,Reporting_Member_Relationship_Description,Age_Group_Description,Age_In_Years,Contract_Type_Code,Eligibility_Year_Month_Ending_Number,State_Code,CBSA_Name,Member_PCP_Indicator,Subscriber_ID,Continuous_Enrollment_for_1_Period,Member_Birth_Date,Account_Name,Time_Period_Start,Time_Period_End,Non_Utilizer_Indicator,Member_Coverage_Count\r\n";
//		
//		String query23 = "SELECT Account_ID AS \"Account ID\",Master_Consumer_ID AS \"Master Consumer ID\",Member_Coverage_Type_Description AS \"Member Coverage Type Description\",Member_Gender_Code AS \"Member Gender Code\",Reporting_Member_Relationship_Description AS \"Reporting Member Relationship Description\",Age_Group_Description AS \"Age Group Description\",Age_In_Years AS \"Age In Years\",Contract_Type_Code AS \"Contract Type Code\",Eligibility_Year_Month_Ending_Number AS \"Eligibility Year Month Ending Number\",State_Code AS \"State Code\",CBSA_Name AS \"CBSA Name\",Member_PCP_Indicator AS \"Member PCP Indicator\",Subscriber_ID AS \"Subscriber ID\",Continuous_Enrollment_for_1_Period AS \"Continuous Enrollment for 1 Period\",Member_Birth_Date AS \"Member Birth Date\",Account_Name AS \"Account Name\",Time_Period_Start AS \"Time Period Start\",Time_Period_End AS \"Time Period End\",Non_Utilizer_Indicator AS \"Non Utilizer Indicator\",Member_Coverage_Count AS \"Member Coverage Count\"  FROM  (  SELECT MBRSHP.ACCT_ID AS Account_ID,MBRSHP.MCID AS Master_Consumer_ID,MBRSHP.MBR_CVRG_TYPE_DESC AS Member_Coverage_Type_Description,MBRSHP.MBR_GNDR_CD AS Member_Gender_Code,MBRSHP.RPTG_MBR_RLTNSHP_DESC AS Reporting_Member_Relationship_Description,MBRSHP.AGE_GRP_DESC AS Age_Group_Description,MBRSHP.AGE_IN_YRS_NBR AS Age_In_Years,MBRSHP.CNTRCT_TYPE_CD AS Contract_Type_Code,MBRSHP.ELGBLTY_CY_MNTH_END_NBR AS Eligibility_Year_Month_Ending_Number,MBRSHP.ST_CD AS State_Code,MBRSHP.CBSA_NM AS CBSA_Name,MBRSHP.PCP_IND AS Member_PCP_Indicator,MBRSHP.FMBRSHP_SBSCRBR_ID AS Subscriber_ID,MBRSHP.CNTNUS_ENRLMNT_1_PRD_CD AS Continuous_Enrollment_for_1_Period,MBRSHP.MBR_BRTH_DT AS Member_Birth_Date,MBRSHP.ACCT_NM AS Account_Name,MBRSHP.TIME_PRD_STRT_NBR AS Time_Period_Start,MBRSHP.TIME_PRD_END_NBR AS Time_Period_End,MBRSHP.Non_Utilizer_Ind AS Non_Utilizer_Indicator,SUM(MBRSHP.SUM_MBR_CVRG_CNT) AS Member_Coverage_Count,MBRSHP.TM_PRD_NM AS Time_Period  FROM  (  SELECT CII_ACCT_PRFL.ACCT_ID AS ACCT_ID,CII_FACT_MBRSHP.MCID AS MCID,DIM_MBR_CVRG_TYPE.MBR_CVRG_TYPE_DESC AS MBR_CVRG_TYPE_DESC,CII_FACT_MBRSHP.MBR_GNDR_CD AS MBR_GNDR_CD,DIM_RPTG_MBR_RLTNSHP.RPTG_MBR_RLTNSHP_DESC AS RPTG_MBR_RLTNSHP_DESC,case when DIM_AGE_GRP.AGE_GRP_DESC='1-17' then '1 through 17' else  DIM_AGE_GRP.AGE_GRP_DESC end AS AGE_GRP_DESC,CII_FACT_MBRSHP.AGE_IN_YRS_NBR AS AGE_IN_YRS_NBR,CII_FACT_MBRSHP.CNTRCT_TYPE_CD AS CNTRCT_TYPE_CD,CII_FACT_MBRSHP.ELGBLTY_CY_MNTH_END_NBR AS ELGBLTY_CY_MNTH_END_NBR,CII_FACT_MBRSHP.ST_CD AS ST_CD,DIM_CBSA.CBSA_NM AS CBSA_NM,CII_FACT_MBRSHP.PCP_IND AS PCP_IND,CII_FACT_MBRSHP.SBSCRBR_ID AS FMBRSHP_SBSCRBR_ID,CE.CNTNUS_ENRLMNT_1_PRD_CD AS CNTNUS_ENRLMNT_1_PRD_CD,DIM_MCID.MBR_BRTH_DT AS MBR_BRTH_DT,CII_ACCT_PRFL.ACCT_NM AS ACCT_NM,CASE WHEN TM_PRD_FNCTN.TM_PRD_NM= 'Current Period'  THEN 201910 WHEN TM_PRD_FNCTN.TM_PRD_NM= 'Prior Period' THEN 201810 WHEN TM_PRD_FNCTN.TM_PRD_NM=  'Prior Period 2'  THEN 201801 end AS TIME_PRD_STRT_NBR,CASE WHEN TM_PRD_FNCTN.TM_PRD_NM= 'Current Period'  THEN 202009 WHEN TM_PRD_FNCTN.TM_PRD_NM= 'Prior Period' THEN 201909 WHEN TM_PRD_FNCTN.TM_PRD_NM=  'Prior Period 2'  THEN 201809 end AS TIME_PRD_END_NBR,UT.Non_Utilizer_Ind AS Non_Utilizer_Ind,SUM(MBR_CVRG_CNT) AS SUM_MBR_CVRG_CNT,TM_PRD_FNCTN.TM_PRD_NM AS TM_PRD_NM  FROM CII_FACT_MBRSHP INNER JOIN (SELECT YEAR_CD_NM as TM_PRD_NM,\r\n" + 
//				"                STRT_MNTH_NBR as START_YEAR_MNTH,\r\n" + 
//				"                END_MNTH_NBR as END_YEAR_MNTH,\r\n" + 
//				"                case\r\n" + 
//				"                                when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"                                and INCRD_PAID_CD = 'PAID' then 111101\r\n" + 
//				"                                when TM_PRD_TYPE_CD = 'Custom' then STRT_MNTH_NBR\r\n" + 
//				"                                else SRVC_STRT_MNTH_NBR\r\n" + 
//				"                end as STRT_SRVC_YEAR_MNTH,\r\n" + 
//				"                case\r\n" + 
//				"                                when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"                                and INCRD_PAID_CD = 'PAID' then 888812\r\n" + 
//				"                                when TM_PRD_TYPE_CD = 'Custom' then END_MNTH_NBR\r\n" + 
//				"                                else SRVC_END_MNTH_NBR\r\n" + 
//				"                end as END_SRVC_YEAR_MNTH,\r\n" + 
//				"                case\r\n" + 
//				"                                when TM_PRD_TYPE_CD <> 'Custom' then PAID_END_MNTH_NBR\r\n" + 
//				"                                when INCRD_PAID_CD = 'PAID' then END_MNTH_NBR\r\n" + 
//				"                                when INCRD_PAID_CD = 'INC2' then 888811\r\n" + 
//				"                                when INCRD_PAID_CD = 'INC3' then dm.YEAR_MNTH_NBR\r\n" + 
//				"                                when INCRD_PAID_CD = 'INC1' then PAID_END_MNTH_NBR\r\n" + 
//				"                end as END_RPTG_PAID_YEAR_MNTH\r\n" + 
//				"from\r\n" + 
//				"                DIM_TM_PRD_ADHC dtp\r\n" + 
//				"cross join DIM_MNTH dm\r\n" + 
//				"                where (dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"                and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"                and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"                --    AND DIM_TM_PRD_ADHC.TM_3061_RULE_IND = 'N'  \r\n" + 
//				"                and dtp.YEAR_ID <= 1\r\n" + 
//				"                --In ('Current','Prior','Prior 2')  \r\n" + 
//				"                and dtp.CSTM_STRT_CURNT_MNTH_NBR = 201910\r\n" + 
//				"                and dtp.CSTM_END_CURNT_MNTH_NBR = 202009\r\n" + 
//				"                and dm.YEAR_MNTH_NBR = 202009\r\n" + 
//				")\r\n" + 
//				"                or (dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"                and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"                and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"                --AND DIM_TM_PRD_ADHC.TM_3061_RULE_IND = 'N'\r\n" + 
//				"                and dtp.YEAR_ID <= 1\r\n" + 
//				"                --In ('Current','Prior','Prior 2')  \r\n" + 
//				"                and dtp.CSTM_STRT_PRIOR_MNTH_NBR = 201810\r\n" + 
//				"                and dtp.CSTM_END_PRIOR_MNTH_NBR = 201909\r\n" + 
//				"                and dm.YEAR_MNTH_NBR = 202009)\r\n" + 
//				"                or (dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"                and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"                and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"                --AND DIM_TM_PRD_ADHC.TM_3061_RULE_IND = 'N'\r\n" + 
//				"                and dtp.YEAR_ID <= 1\r\n" + 
//				"                --In ('Current','Prior','Prior 2') \r\n" + 
//				"                and dtp.CSTM_STRT_PRIOR_2_MNTH_NBR = 201801\r\n" + 
//				"                and dtp.CSTM_END_PRIOR_2_MNTH_NBR = 201809\r\n" + 
//				"                and dm.YEAR_MNTH_NBR = 202009) ) TM_PRD_FNCTN ON CII_FACT_MBRSHP.ELGBLTY_CY_MNTH_END_NBR BETWEEN TM_PRD_FNCTN.START_YEAR_MNTH and TM_PRD_FNCTN.END_YEAR_MNTH INNER JOIN ( Select\r\n" + 
//				"mbrshp.ACCT_ID,\r\n" + 
//				"mbrshp.MCID,\r\n" + 
//				"mbrshp.mbr_cvrg_type_cd,\r\n" + 
//				"mbrshp.tm_prd_nm,\r\n" + 
//				"Case \r\n" + 
//				"                when (mbrshp.mcid = clms.mcid \r\n" + 
//				"                and mbrshp.mbr_cvrg_type_cd = clms.mbr_cvrg_type_cd) then 'N' \r\n" + 
//				"                Else 'Y' \r\n" + 
//				"End        as Non_Utilizer_Ind\r\n" + 
//				"from\r\n" + 
//				"(\r\n" + 
//				"Select\r\n" + 
//				"fact.ACCT_ID,\r\n" + 
//				"MCID,\r\n" + 
//				"MBR_CVRG_TYPE_CD,\r\n" + 
//				"  TM_PRD_NM\r\n" + 
//				"  from\r\n" + 
//				"cii_fact_mbrshp fact\r\n" + 
//				"\r\n" + 
//				"JOIN (             \r\n" + 
//				"SELECT    YEAR_CD_NM as TM_PRD_NM,\r\n" + 
//				"                STRT_MNTH_NBR as START_YEAR_MNTH,\r\n" + 
//				"                END_MNTH_NBR as END_YEAR_MNTH,\r\n" + 
//				"                case\r\n" + 
//				"                                when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"                                and INCRD_PAID_CD = 'PAID' then 111101\r\n" + 
//				"                                when TM_PRD_TYPE_CD = 'Custom' then STRT_MNTH_NBR\r\n" + 
//				"                                else SRVC_STRT_MNTH_NBR\r\n" + 
//				"                end as STRT_SRVC_YEAR_MNTH,\r\n" + 
//				"                case\r\n" + 
//				"                                when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"                                and INCRD_PAID_CD = 'PAID' then 888812\r\n" + 
//				"                                when TM_PRD_TYPE_CD = 'Custom' then END_MNTH_NBR\r\n" + 
//				"                                else SRVC_END_MNTH_NBR\r\n" + 
//				"                end as END_SRVC_YEAR_MNTH,\r\n" + 
//				"                case\r\n" + 
//				"                                when TM_PRD_TYPE_CD <> 'Custom' then PAID_END_MNTH_NBR\r\n" + 
//				"                                when INCRD_PAID_CD = 'PAID' then END_MNTH_NBR\r\n" + 
//				"                                when INCRD_PAID_CD = 'INC2' then 888811\r\n" + 
//				"                                when INCRD_PAID_CD = 'INC3' then dm.YEAR_MNTH_NBR\r\n" + 
//				"                                when INCRD_PAID_CD = 'INC1' then PAID_END_MNTH_NBR\r\n" + 
//				"                end as END_RPTG_PAID_YEAR_MNTH\r\n" + 
//				"from\r\n" + 
//				"                DIM_TM_PRD_ADHC dtp\r\n" + 
//				"cross join DIM_MNTH dm\r\n" + 
//				"                where (dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"                and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"                and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"                --    AND DIM_TM_PRD_ADHC.TM_3061_RULE_IND = 'N'  \r\n" + 
//				"                and dtp.YEAR_ID <= 1\r\n" + 
//				"                --In ('Current','Prior','Prior 2')  \r\n" + 
//				"                and dtp.CSTM_STRT_CURNT_MNTH_NBR = 201910\r\n" + 
//				"                and dtp.CSTM_END_CURNT_MNTH_NBR = 202009\r\n" + 
//				"                and dm.YEAR_MNTH_NBR = 202009\r\n" + 
//				")\r\n" + 
//				"                or (dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"                and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"                and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"                --AND DIM_TM_PRD_ADHC.TM_3061_RULE_IND = 'N'\r\n" + 
//				"                and dtp.YEAR_ID <= 1\r\n" + 
//				"                --In ('Current','Prior','Prior 2')  \r\n" + 
//				"                and dtp.CSTM_STRT_PRIOR_MNTH_NBR = 201810\r\n" + 
//				"                and dtp.CSTM_END_PRIOR_MNTH_NBR = 201909\r\n" + 
//				"                and dm.YEAR_MNTH_NBR = 202009)\r\n" + 
//				"                or (dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"                and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"                and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"                --AND DIM_TM_PRD_ADHC.TM_3061_RULE_IND = 'N'\r\n" + 
//				"                and dtp.YEAR_ID <= 1\r\n" + 
//				"                --In ('Current','Prior','Prior 2') \r\n" + 
//				"                and dtp.CSTM_STRT_PRIOR_2_MNTH_NBR = 201801\r\n" + 
//				"                and dtp.CSTM_END_PRIOR_2_MNTH_NBR = 201809\r\n" + 
//				"                and dm.YEAR_MNTH_NBR = 202009)) TM_PRD_FNCTN                 \r\n" + 
//				"    ON fact.ELGBLTY_CY_MNTH_END_NBR BETWEEN TM_PRD_FNCTN.START_YEAR_MNTH                \r\n" + 
//				"    and TM_PRD_FNCTN.END_YEAR_MNTH  \r\n" + 
//				"                INNER JOIN (Select ACCT_ID,SGMNTN_DIM_KEY, SGMNTN_NM, SRC_FLTR_ID from ACIISST_SGMNTN_BRDG where ACCT_ID in ('W0016437') and SRC_FLTR_ID in ('4733bea4-a055-4641-b834-2c6818e13f45'))SGMNTN on fact.SGMNTN_DIM_KEY = SGMNTN.SGMNTN_DIM_KEY and fact.ACCT_ID=SGMNTN.ACCT_ID  \r\n" + 
//				"WHERE      fact.ACCT_ID = 'W0016437'                  \r\n" + 
//				"GROUP BY  fact.acct_id,  fact.MCID, TM_PRD_FNCTN.TM_PRD_NM, fact.MBR_CVRG_TYPE_CD\r\n" + 
//				") mbrshp\r\n" + 
//				"\r\n" + 
//				"left outer join\r\n" + 
//				"(\r\n" + 
//				"Select\r\n" + 
//				"clm.ACCT_ID,\r\n" + 
//				"MCID,\r\n" + 
//				"MBR_CVRG_TYPE_CD,\r\n" + 
//				"TM_PRD_NM\r\n" + 
//				"\r\n" + 
//				"from\r\n" + 
//				"cii_fact_clm_line clm\r\n" + 
//				"JOIN (             \r\n" + 
//				"SELECT     YEAR_CD_NM as TM_PRD_NM,\r\n" + 
//				"                STRT_MNTH_NBR as START_YEAR_MNTH,\r\n" + 
//				"                END_MNTH_NBR as END_YEAR_MNTH,\r\n" + 
//				"                case\r\n" + 
//				"                                when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"                                and INCRD_PAID_CD = 'PAID' then 111101\r\n" + 
//				"                                when TM_PRD_TYPE_CD = 'Custom' then STRT_MNTH_NBR\r\n" + 
//				"                                else SRVC_STRT_MNTH_NBR\r\n" + 
//				"                end as STRT_SRVC_YEAR_MNTH,\r\n" + 
//				"                case\r\n" + 
//				"                                when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"                                and INCRD_PAID_CD = 'PAID' then 888812\r\n" + 
//				"                                when TM_PRD_TYPE_CD = 'Custom' then END_MNTH_NBR\r\n" + 
//				"                                else SRVC_END_MNTH_NBR\r\n" + 
//				"                end as END_SRVC_YEAR_MNTH,\r\n" + 
//				"                case\r\n" + 
//				"                                when TM_PRD_TYPE_CD <> 'Custom' then PAID_END_MNTH_NBR\r\n" + 
//				"                                when INCRD_PAID_CD = 'PAID' then END_MNTH_NBR\r\n" + 
//				"                                when INCRD_PAID_CD = 'INC2' then 888811\r\n" + 
//				"                                when INCRD_PAID_CD = 'INC3' then dm.YEAR_MNTH_NBR\r\n" + 
//				"                                when INCRD_PAID_CD = 'INC1' then PAID_END_MNTH_NBR\r\n" + 
//				"                end as END_RPTG_PAID_YEAR_MNTH\r\n" + 
//				"from\r\n" + 
//				"                DIM_TM_PRD_ADHC dtp\r\n" + 
//				"cross join DIM_MNTH dm\r\n" + 
//				"                where (dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"                and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"                and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"                --    AND DIM_TM_PRD_ADHC.TM_3061_RULE_IND = 'N'  \r\n" + 
//				"                and dtp.YEAR_ID <= 1\r\n" + 
//				"                --In ('Current','Prior','Prior 2')  \r\n" + 
//				"                and dtp.CSTM_STRT_CURNT_MNTH_NBR = 201910\r\n" + 
//				"                and dtp.CSTM_END_CURNT_MNTH_NBR = 202009\r\n" + 
//				"                and dm.YEAR_MNTH_NBR = 202009\r\n" + 
//				")\r\n" + 
//				"                or (dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"                and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"                and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"                --AND DIM_TM_PRD_ADHC.TM_3061_RULE_IND = 'N'\r\n" + 
//				"                and dtp.YEAR_ID <= 1\r\n" + 
//				"                --In ('Current','Prior','Prior 2')  \r\n" + 
//				"                and dtp.CSTM_STRT_PRIOR_MNTH_NBR = 201810\r\n" + 
//				"                and dtp.CSTM_END_PRIOR_MNTH_NBR = 201909\r\n" + 
//				"                and dm.YEAR_MNTH_NBR = 202009)\r\n" + 
//				"                or (dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"                and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"                and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"                --AND DIM_TM_PRD_ADHC.TM_3061_RULE_IND = 'N'\r\n" + 
//				"                and dtp.YEAR_ID <= 1\r\n" + 
//				"                --In ('Current','Prior','Prior 2') \r\n" + 
//				"                and dtp.CSTM_STRT_PRIOR_2_MNTH_NBR = 201801\r\n" + 
//				"                and dtp.CSTM_END_PRIOR_2_MNTH_NBR = 201809\r\n" + 
//				"                and dm.YEAR_MNTH_NBR = 202009)) TM_PRD_FNCTN                 \r\n" + 
//				"    ON clm.CLM_SRVC_YEAR_MNTH_NBR BETWEEN TM_PRD_FNCTN.START_YEAR_MNTH   \r\n" + 
//				"                and TM_PRD_FNCTN.END_YEAR_MNTH  \r\n" + 
//				"                INNER JOIN (Select ACCT_ID,SGMNTN_DIM_KEY, SGMNTN_NM, SRC_FLTR_ID from ACIISST_SGMNTN_BRDG where ACCT_ID in ('W0016437') and SRC_FLTR_ID in ('4733bea4-a055-4641-b834-2c6818e13f45'))SGMNTN on clm.SGMNTN_DIM_KEY = SGMNTN.SGMNTN_DIM_KEY and clm.ACCT_ID=SGMNTN.ACCT_ID\r\n" + 
//				"WHERE      clm.ACCT_ID =  'W0016437'                        \r\n" + 
//				"GROUP BY  clm.acct_id,  clm.MCID, TM_PRD_FNCTN.TM_PRD_NM, clm.MBR_CVRG_TYPE_CD\r\n" + 
//				") clms\r\n" + 
//				"\r\n" + 
//				"                on\r\n" + 
//				"mbrshp.acct_id = clms.acct_id \r\n" + 
//				"                and mbrshp.mcid=clms.mcid  \r\n" + 
//				"                and mbrshp.TM_PRD_NM = clms.tm_prd_nm \r\n" + 
//				"                and mbrshp.mbr_cvrg_type_cd  = clms.mbr_cvrg_type_cd  ) UT\r\n" + 
//				"\r\n" + 
//				"                ON TM_PRD_FNCTN.TM_PRD_NM = UT.TM_PRD_NM  \r\n" + 
//				"                AND  CII_FACT_MBRSHP.ACCT_ID =UT.ACCT_ID  \r\n" + 
//				"                AND  CII_FACT_MBRSHP.MCID =UT.MCID  \r\n" + 
//				"                and CII_FACT_MBRSHP.MBR_CVRG_TYPE_CD = UT.MBR_CVRG_TYPE_CD INNER JOIN CII_ACCT_PRFL ON CII_FACT_MBRSHP.ACCT_ID = CII_ACCT_PRFL.ACCT_ID INNER JOIN DIM_MBR_CVRG_TYPE ON CII_FACT_MBRSHP.MBR_CVRG_TYPE_CD = DIM_MBR_CVRG_TYPE.MBR_CVRG_TYPE_CD INNER JOIN DIM_RPTG_MBR_RLTNSHP ON CII_FACT_MBRSHP.RPTG_MBR_RLTNSHP_CD = DIM_RPTG_MBR_RLTNSHP.RPTG_MBR_RLTNSHP_CD INNER JOIN DIM_AGE_GRP ON CII_FACT_MBRSHP.ACCT_AGE_GRP_KEY = DIM_AGE_GRP.AGE_GRP_KEY INNER JOIN DIM_CBSA ON CII_FACT_MBRSHP.CBSA_ID = DIM_CBSA.CBSA_ID INNER  JOIN ( \r\n" + 
//				"                select      m.TM_PRD_NM,  m.acct_id, m.mcid, m. MBR_CVRG_TYPE_CD ,\r\n" + 
//				"   case\r\n" + 
//				"    when m.mnths = b.mnths\r\n" + 
//				"    and m.MBR_CVRG_TYPE_CD <> 'back_fill' then 'Continuous'\r\n" + 
//				"          when strt_mnth = start_prd\r\n" + 
//				"    and end_mnth<>end_prd\r\n" + 
//				"    and m.MBR_CVRG_TYPE_CD <> 'back_fill' then 'Termed'\r\n" + 
//				"          when strt_mnth <> start_prd\r\n" + 
//				"    and end_mnth=end_prd\r\n" + 
//				"    and m.MBR_CVRG_TYPE_CD <> 'back_fill'then 'Added'\r\n" + 
//				"          when m.MBR_CVRG_TYPE_CD = 'back_fill' Then 'NA'\r\n" + 
//				"          else 'Other'\r\n" + 
//				"    end as CNTNUS_ENRLMNT_1_PRD_CD\r\n" + 
//				"                from      (\r\n" + 
//				"                                select    fact .acct_id,\r\n" + 
//				"                                                                fact .MCID,\r\n" + 
//				"                                                                TM_PRD_FNCTN.TM_PRD_NM,\r\n" + 
//				"                                                                fact.MBR_CVRG_TYPE_CD ,\r\n" + 
//				"                                                                count (distinct fact .ELGBLTY_CY_MNTH_END_NBR) mnths ,\r\n" + 
//				"                                                                min(fact .ELGBLTY_CY_MNTH_END_NBR) strt_mnth ,\r\n" + 
//				"                                                                max(fact .ELGBLTY_CY_MNTH_END_NBR) \r\n" + 
//				"                                                                end_mnth \r\n" + 
//				"                                from      cii_fact_mbrshp fact  JOIN (              \r\n" + 
//				"                                                SELECT      YEAR_CD_NM as TM_PRD_NM,\r\n" + 
//				"                STRT_MNTH_NBR as START_YEAR_MNTH,\r\n" + 
//				"                END_MNTH_NBR as END_YEAR_MNTH,\r\n" + 
//				"                case\r\n" + 
//				"                                when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"                                and INCRD_PAID_CD = 'PAID' then 111101\r\n" + 
//				"                                when TM_PRD_TYPE_CD = 'Custom' then STRT_MNTH_NBR\r\n" + 
//				"                                else SRVC_STRT_MNTH_NBR\r\n" + 
//				"                end as STRT_SRVC_YEAR_MNTH,\r\n" + 
//				"                case\r\n" + 
//				"                                when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"                                and INCRD_PAID_CD = 'PAID' then 888812\r\n" + 
//				"                                when TM_PRD_TYPE_CD = 'Custom' then END_MNTH_NBR\r\n" + 
//				"                                else SRVC_END_MNTH_NBR\r\n" + 
//				"                end as END_SRVC_YEAR_MNTH,\r\n" + 
//				"                case\r\n" + 
//				"                                when TM_PRD_TYPE_CD <> 'Custom' then PAID_END_MNTH_NBR\r\n" + 
//				"                                when INCRD_PAID_CD = 'PAID' then END_MNTH_NBR\r\n" + 
//				"                                when INCRD_PAID_CD = 'INC2' then 888811\r\n" + 
//				"                                when INCRD_PAID_CD = 'INC3' then dm.YEAR_MNTH_NBR\r\n" + 
//				"                                when INCRD_PAID_CD = 'INC1' then PAID_END_MNTH_NBR\r\n" + 
//				"                end as END_RPTG_PAID_YEAR_MNTH\r\n" + 
//				"from\r\n" + 
//				"                DIM_TM_PRD_ADHC dtp\r\n" + 
//				"cross join DIM_MNTH dm\r\n" + 
//				"                where (dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"                and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"                and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"                --    AND DIM_TM_PRD_ADHC.TM_3061_RULE_IND = 'N'  \r\n" + 
//				"                and dtp.YEAR_ID <= 1\r\n" + 
//				"                --In ('Current','Prior','Prior 2')  \r\n" + 
//				"                and dtp.CSTM_STRT_CURNT_MNTH_NBR = 201910\r\n" + 
//				"                and dtp.CSTM_END_CURNT_MNTH_NBR = 202009\r\n" + 
//				"                and dm.YEAR_MNTH_NBR = 202009\r\n" + 
//				")\r\n" + 
//				"                or (dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"                and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"                and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"                --AND DIM_TM_PRD_ADHC.TM_3061_RULE_IND = 'N'\r\n" + 
//				"                and dtp.YEAR_ID <= 1\r\n" + 
//				"                --In ('Current','Prior','Prior 2')  \r\n" + 
//				"                and dtp.CSTM_STRT_PRIOR_MNTH_NBR = 201810\r\n" + 
//				"                and dtp.CSTM_END_PRIOR_MNTH_NBR = 201909\r\n" + 
//				"                and dm.YEAR_MNTH_NBR = 202009)\r\n" + 
//				"                or (dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"                and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"                and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"                --AND DIM_TM_PRD_ADHC.TM_3061_RULE_IND = 'N'\r\n" + 
//				"                and dtp.YEAR_ID <= 1\r\n" + 
//				"                --In ('Current','Prior','Prior 2') \r\n" + 
//				"                and dtp.CSTM_STRT_PRIOR_2_MNTH_NBR = 201801\r\n" + 
//				"                and dtp.CSTM_END_PRIOR_2_MNTH_NBR = 201809\r\n" + 
//				"                and dm.YEAR_MNTH_NBR = 202009) ) TM_PRD_FNCTN                      \r\n" + 
//				"                                                ON fact .ELGBLTY_CY_MNTH_END_NBR BETWEEN TM_PRD_FNCTN.START_YEAR_MNTH  \r\n" + 
//				"                                                and TM_PRD_FNCTN. \r\n" + 
//				"                                                                END_YEAR_MNTH INNER JOIN (Select ACCT_ID,SGMNTN_DIM_KEY, SGMNTN_NM, SRC_FLTR_ID from ACIISST_SGMNTN_BRDG where ACCT_ID in ('W0016437') and SRC_FLTR_ID in ('4733bea4-a055-4641-b834-2c6818e13f45'))SGMNTN on fact.SGMNTN_DIM_KEY = SGMNTN.SGMNTN_DIM_KEY and fact.ACCT_ID=SGMNTN.ACCT_ID   \r\n" + 
//				"                                WHERE fact .ACCT_ID = 'W0016437'                         \r\n" + 
//				"                                GROUP BY  fact .acct_id,\r\n" + 
//				"                                                                fact .MCID,\r\n" + 
//				"                                                                TM_PRD_FNCTN.TM_PRD_NM,\r\n" + 
//				"                                                                fact .MBR_CVRG_TYPE_CD ) m \r\n" + 
//				"                inner join (\r\n" + 
//				"                                select    \r\n" + 
//				"                                                                case \r\n" + 
//				"                                                                                when YEAR_ID= 1 then 'Current Period'  \r\n" + 
//				"                                                                                when YEAR_ID= 2 then 'Prior Period' \r\n" + 
//				"                                                                                when YEAR_ID= 3 then 'Prior Period 2'   \r\n" + 
//				"                                                                                ELSE    'FOO' \r\n" + 
//				"                                                                END as TM_PRD_NM,\r\n" + 
//				"                                                                max(e_abs) - max(s_abs)+ 1 mnths,\r\n" + 
//				"                                                                max(s_prd) as start_prd,\r\n" + 
//				"                                                                max(e_prd)  as \r\n" + 
//				"                                                                end_prd \r\n" + 
//				"                                from         ( \r\n" + 
//				"                                                select         \r\n" + 
//				"                                                                                CASE \r\n" + 
//				"                                                                                                WHEN YEAR_MNTH_NBR =  201910  \r\n" + 
//				"                                                                or YEAR_MNTH_NBR = 202009 then 1 \r\n" + 
//				"                                                                                                WHEN YEAR_MNTH_NBR = 201810  \r\n" + 
//				"                                                                or YEAR_MNTH_NBR =  201909   then 2   \r\n" + 
//				"                                                                                                WHEN  YEAR_MNTH_NBR = 201801  \r\n" + 
//				"                                                                or  YEAR_MNTH_NBR = 201809 then  3 \r\n" + 
//				"                                                                                                ELSE  4 \r\n" + 
//				"                                                                                END  as YEAR_ID,\r\n" + 
//				"                                                                                CASE \r\n" + 
//				"                                                                                                WHEN YEAR_MNTH_NBR =  201910  \r\n" + 
//				"                                                                or  YEAR_MNTH_NBR = 201810  \r\n" + 
//				"                                                                or YEAR_MNTH_NBR = 201801  then ABS_YEAR_MNTH_NBR \r\n" + 
//				"                                                                                                else null \r\n" + 
//				"                                                                                end as s_abs ,\r\n" + 
//				"                                                                                CASE \r\n" + 
//				"                                                                                                WHEN YEAR_MNTH_NBR =  201910  \r\n" + 
//				"                                                                or YEAR_MNTH_NBR = 201810  \r\n" + 
//				"                                                                or YEAR_MNTH_NBR = 201801  then YEAR_MNTH_NBR \r\n" + 
//				"                                                                                                else null \r\n" + 
//				"                                                                                end as s_prd ,\r\n" + 
//				"                                                                                                 \r\n" + 
//				"                                                                                CASE  \r\n" + 
//				"                                                                                                WHEN YEAR_MNTH_NBR = 202009  \r\n" + 
//				"                                                                or YEAR_MNTH_NBR =  201909  \r\n" + 
//				"                                                                or YEAR_MNTH_NBR = 201809  then ABS_YEAR_MNTH_NBR \r\n" + 
//				"                                                                                                else null \r\n" + 
//				"                                                                                end as e_abs,\r\n" + 
//				"                                                                                CASE \r\n" + 
//				"                                                                                                WHEN YEAR_MNTH_NBR = 202009  \r\n" + 
//				"                                                                or YEAR_MNTH_NBR =  201909  \r\n" + 
//				"                                                                or YEAR_MNTH_NBR = 201809  then YEAR_MNTH_NBR \r\n" + 
//				"                                                                                                else null \r\n" + 
//				"                                                                                end as e_prd                 \r\n" + 
//				"                                                from      dim_mnth   \r\n" + 
//				"                                                where   \r\n" + 
//				"                                                                                CASE \r\n" + 
//				"                                                                                                WHEN YEAR_MNTH_NBR =  201910  \r\n" + 
//				"                                                                or YEAR_MNTH_NBR = 202009 then 1               \r\n" + 
//				"                                                                                                WHEN YEAR_MNTH_NBR = 201810  \r\n" + 
//				"                                                                or YEAR_MNTH_NBR =  201909   then 2 \r\n" + 
//				"                                                                                                WHEN  YEAR_MNTH_NBR = 201801  \r\n" + 
//				"                                                                or  YEAR_MNTH_NBR = 201809 then  3 \r\n" + 
//				"                                                                                                ELSE 0 \r\n" + 
//				"                                                                                END  > 0 ) a  \r\n" + 
//				"                                group by \r\n" + 
//				"                                                                case \r\n" + 
//				"                                                                                when YEAR_ID= 1 then 'Current Period' \r\n" + 
//				"                                                                                when YEAR_ID= 2 then 'Prior Period'  \r\n" + 
//				"                                                                                when YEAR_ID= 3 then 'Prior Period 2'   \r\n" + 
//				"                                                                                ELSE    'FOO' \r\n" + 
//				"                                                                END) b     \r\n" + 
//				"                                on  m.TM_PRD_NM = b.TM_PRD_NM) CE              \r\n" + 
//				"                ON TM_PRD_FNCTN.TM_PRD_NM = CE.TM_PRD_NM      \r\n" + 
//				"                AND  CII_FACT_MBRSHP.ACCT_ID =CE.ACCT_ID     \r\n" + 
//				"                AND  CII_FACT_MBRSHP.MCID =CE.MCID     \r\n" + 
//				"                and CII_FACT_MBRSHP.MBR_CVRG_TYPE_CD = CE.MBR_CVRG_TYPE_CD INNER JOIN DIM_MCID ON CII_FACT_MBRSHP.MCID = DIM_MCID.MCID AND CII_FACT_MBRSHP.ACCT_ID = DIM_MCID.ACCT_ID INNER JOIN (Select ACCT_ID,SGMNTN_DIM_KEY, SGMNTN_NM, SRC_FLTR_ID from ACIISST_SGMNTN_BRDG where ACCT_ID in ('W0016437') and SRC_FLTR_ID in ('4733bea4-a055-4641-b834-2c6818e13f45'))SGMNTN on CII_FACT_MBRSHP.SGMNTN_DIM_KEY = SGMNTN.SGMNTN_DIM_KEY and CII_FACT_MBRSHP.ACCT_ID=SGMNTN.ACCT_ID WHERE CII_FACT_MBRSHP.ACCT_ID in ('W0016437')   GROUP BY CII_ACCT_PRFL.ACCT_ID,CII_FACT_MBRSHP.MCID,DIM_MBR_CVRG_TYPE.MBR_CVRG_TYPE_DESC,CII_FACT_MBRSHP.MBR_GNDR_CD,DIM_RPTG_MBR_RLTNSHP.RPTG_MBR_RLTNSHP_DESC,case when DIM_AGE_GRP.AGE_GRP_DESC='1-17' then '1 through 17' else  DIM_AGE_GRP.AGE_GRP_DESC end,CII_FACT_MBRSHP.AGE_IN_YRS_NBR,CII_FACT_MBRSHP.CNTRCT_TYPE_CD,CII_FACT_MBRSHP.ELGBLTY_CY_MNTH_END_NBR,CII_FACT_MBRSHP.ST_CD,DIM_CBSA.CBSA_NM,CII_FACT_MBRSHP.PCP_IND,CII_FACT_MBRSHP.SBSCRBR_ID,CE.CNTNUS_ENRLMNT_1_PRD_CD,DIM_MCID.MBR_BRTH_DT,CII_ACCT_PRFL.ACCT_NM,CASE WHEN TM_PRD_FNCTN.TM_PRD_NM= 'Current Period'  THEN 201910 WHEN TM_PRD_FNCTN.TM_PRD_NM= 'Prior Period' THEN 201810 WHEN TM_PRD_FNCTN.TM_PRD_NM=  'Prior Period 2'  THEN 201801 end,CASE WHEN TM_PRD_FNCTN.TM_PRD_NM= 'Current Period'  THEN 202009 WHEN TM_PRD_FNCTN.TM_PRD_NM= 'Prior Period' THEN 201909 WHEN TM_PRD_FNCTN.TM_PRD_NM=  'Prior Period 2'  THEN 201809 end,UT.Non_Utilizer_Ind,TM_PRD_FNCTN.TM_PRD_NM ) AS MBRSHP  GROUP BY MBRSHP.ACCT_ID,MBRSHP.MCID,MBRSHP.MBR_CVRG_TYPE_DESC,MBRSHP.MBR_GNDR_CD,MBRSHP.RPTG_MBR_RLTNSHP_DESC,MBRSHP.AGE_GRP_DESC,MBRSHP.AGE_IN_YRS_NBR,MBRSHP.CNTRCT_TYPE_CD,MBRSHP.ELGBLTY_CY_MNTH_END_NBR,MBRSHP.ST_CD,MBRSHP.CBSA_NM,MBRSHP.PCP_IND,MBRSHP.FMBRSHP_SBSCRBR_ID,MBRSHP.CNTNUS_ENRLMNT_1_PRD_CD,MBRSHP.MBR_BRTH_DT,MBRSHP.ACCT_NM,MBRSHP.TIME_PRD_STRT_NBR,MBRSHP.TIME_PRD_END_NBR,MBRSHP.Non_Utilizer_Ind,MBRSHP.TM_PRD_NM ) AS FNL   ORDER BY Account_ID,Master_Consumer_ID,Member_Coverage_Type_Description,Member_Gender_Code,Reporting_Member_Relationship_Description,Age_Group_Description,Age_In_Years,Contract_Type_Code,Eligibility_Year_Month_Ending_Number,State_Code,CBSA_Name,Member_PCP_Indicator,Subscriber_ID,Continuous_Enrollment_for_1_Period,Member_Birth_Date,Account_Name,Time_Period_Start,Time_Period_End,Non_Utilizer_Indicator,Member_Coverage_Count";
//		
//		String query30 = "SELECT Account_Name AS \"Account Name\",Gaps_Closed AS \"Gaps Closed\",Member_Engaged_Indicator AS \"Member Engaged Indicator\",Member_Engagement_Tier AS \"Member Engagement Tier\",Time_Period AS \"Time Period\",Earned_Incentive_Indicator AS \"Earned Incentive Indicator\"  FROM  (  SELECT GAPSVNGS.ACCT_NM AS Account_Name,SUM(GAPSVNGS.GPS_CLSD) AS Gaps_Closed,GAPSVNGS.MBR_ENGGD_IND AS Member_Engaged_Indicator,GAPSVNGS.MBR_ENGGMNT_TIER AS Member_Engagement_Tier,GAPSVNGS.TM_PRD_NM AS Time_Period,GAPSVNGS.EARND_INCNTV_CD AS Earned_Incentive_Indicator  FROM  (  SELECT CII_ACCT_PRFL.ACCT_NM AS ACCT_NM,SUM( CASE WHEN CII_FACT_GAP_SVNGS.CMPLNC_YEAR_MNTH_YEAR_NBR <= CASE WHEN substr(cast(TM_PRD_FNCTN.END_YEAR_MNTH  as varchar(6)) ,5,2) = '01' THEN TM_PRD_FNCTN.END_YEAR_MNTH   - 89 ELSE TM_PRD_FNCTN.END_YEAR_MNTH   - 1 END  THEN CII_FACT_GAP_SVNGS.CLSUR_CNT ELSE 0 END) AS GPS_CLSD,coalesce( TMBRENGGMNT2.MBR_ENGGD_IND,'N') AS MBR_ENGGD_IND,coalesce(TMBRENGGMNT.ENGGMNT_TIER, 'Not Engaged') AS MBR_ENGGMNT_TIER,TM_PRD_FNCTN.TM_PRD_NM AS TM_PRD_NM,INCNTV_MBRSHP.EARND_INCNTV_CD AS EARND_INCNTV_CD  FROM CII_FACT_GAP_SVNGS INNER JOIN (\r\n" + 
//				"Select YEAR_CD_NM as TM_PRD_NM,\r\n" + 
//				"STRT_MNTH_NBR as START_YEAR_MNTH,\r\n" + 
//				"END_MNTH_NBR as END_YEAR_MNTH,\r\n" + 
//				"case\r\n" + 
//				"when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"and INCRD_PAID_CD = 'PAID' then 111101\r\n" + 
//				"when TM_PRD_TYPE_CD = 'Custom' then STRT_MNTH_NBR\r\n" + 
//				"else SRVC_STRT_MNTH_NBR\r\n" + 
//				"end as STRT_SRVC_YEAR_MNTH,\r\n" + 
//				"case\r\n" + 
//				"when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"and INCRD_PAID_CD = 'PAID' then 888812\r\n" + 
//				"when TM_PRD_TYPE_CD = 'Custom' then END_MNTH_NBR\r\n" + 
//				"else SRVC_END_MNTH_NBR\r\n" + 
//				"end as END_SRVC_YEAR_MNTH,\r\n" + 
//				"case\r\n" + 
//				"when TM_PRD_TYPE_CD <> 'Custom' then PAID_END_MNTH_NBR\r\n" + 
//				"when INCRD_PAID_CD = 'PAID' then END_MNTH_NBR\r\n" + 
//				"when INCRD_PAID_CD = 'INC2' then 888811\r\n" + 
//				"when INCRD_PAID_CD = 'INC3' then dm.YEAR_MNTH_NBR\r\n" + 
//				"when INCRD_PAID_CD = 'INC1' then PAID_END_MNTH_NBR\r\n" + 
//				"end as END_RPTG_PAID_YEAR_MNTH\r\n" + 
//				"from\r\n" + 
//				"DIM_TM_PRD_ADHC dtp\r\n" + 
//				"join DIM_MNTH dm on 1=1\r\n" + 
//				"where   dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"and dtp.YEAR_ID <= 3\r\n" + 
//				"and dm.YEAR_MNTH_NBR = 202011\r\n" + 
//				"and (\r\n" + 
//				"(dtp.CSTM_STRT_CURNT_MNTH_NBR = 201912 and dtp.CSTM_END_CURNT_MNTH_NBR = 202011)\r\n" + 
//				"or (dtp.CSTM_STRT_PRIOR_MNTH_NBR = 201812 and dtp.CSTM_END_PRIOR_MNTH_NBR = 201911)\r\n" + 
//				"or (dtp.CSTM_STRT_PRIOR_2_MNTH_NBR = 201801 and dtp.CSTM_END_PRIOR_2_MNTH_NBR = 201811) )) TM_PRD_FNCTN ON CII_FACT_GAP_SVNGS.RLSET_RUN_YEAR_MNTH_NBR BETWEEN CASE WHEN substr(cast(TM_PRD_FNCTN.START_YEAR_MNTH as varchar(6)) ,5,2) <= '06' THEN TM_PRD_FNCTN.START_YEAR_MNTH  - 94 ELSE TM_PRD_FNCTN.START_YEAR_MNTH  - 6 END and CASE WHEN substr(cast(TM_PRD_FNCTN.END_YEAR_MNTH as varchar(6)) ,5,2) <= '06' THEN TM_PRD_FNCTN.END_YEAR_MNTH  - 94 ELSE TM_PRD_FNCTN.END_YEAR_MNTH  - 6 END INNER JOIN (    SELECT MBRSHP.TM_PRD_NM AS Time_Period,MBRSHP.DIM_MBR_CVRG_TYPE_MBR_CVRG_TYPE_DESC AS Member_Coverage_Type_Description,   MBRSHP.CII_FACT_MBRSHP_MCID AS Master_Consumer_ID  FROM (       SELECT TM_PRD_FNCTN.TM_PRD_NM AS TM_PRD_NM,       DIM_MBR_CVRG_TYPE.MBR_CVRG_TYPE_DESC AS DIM_MBR_CVRG_TYPE_MBR_CVRG_TYPE_DESC,       CII_FACT_MBRSHP.MCID AS CII_FACT_MBRSHP_MCID      FROM CII_FACT_MBRSHP     INNER JOIN (       \r\n" + 
//				"Select YEAR_CD_NM as TM_PRD_NM,\r\n" + 
//				"STRT_MNTH_NBR as START_YEAR_MNTH,\r\n" + 
//				"END_MNTH_NBR as END_YEAR_MNTH,\r\n" + 
//				"case\r\n" + 
//				"when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"and INCRD_PAID_CD = 'PAID' then 111101\r\n" + 
//				"when TM_PRD_TYPE_CD = 'Custom' then STRT_MNTH_NBR\r\n" + 
//				"else SRVC_STRT_MNTH_NBR\r\n" + 
//				"end as STRT_SRVC_YEAR_MNTH,\r\n" + 
//				"case\r\n" + 
//				"when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"and INCRD_PAID_CD = 'PAID' then 888812\r\n" + 
//				"when TM_PRD_TYPE_CD = 'Custom' then END_MNTH_NBR\r\n" + 
//				"else SRVC_END_MNTH_NBR\r\n" + 
//				"end as END_SRVC_YEAR_MNTH,\r\n" + 
//				"case\r\n" + 
//				"when TM_PRD_TYPE_CD <> 'Custom' then PAID_END_MNTH_NBR\r\n" + 
//				"when INCRD_PAID_CD = 'PAID' then END_MNTH_NBR\r\n" + 
//				"when INCRD_PAID_CD = 'INC2' then 888811\r\n" + 
//				"when INCRD_PAID_CD = 'INC3' then dm.YEAR_MNTH_NBR\r\n" + 
//				"when INCRD_PAID_CD = 'INC1' then PAID_END_MNTH_NBR\r\n" + 
//				"end as END_RPTG_PAID_YEAR_MNTH\r\n" + 
//				"from\r\n" + 
//				"DIM_TM_PRD_ADHC dtp\r\n" + 
//				"join DIM_MNTH dm on 1=1\r\n" + 
//				"where   dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"and dtp.YEAR_ID <= 3\r\n" + 
//				"and dm.YEAR_MNTH_NBR = 202011\r\n" + 
//				"and (\r\n" + 
//				"(dtp.CSTM_STRT_CURNT_MNTH_NBR = 201912 and dtp.CSTM_END_CURNT_MNTH_NBR = 202011)\r\n" + 
//				"or (dtp.CSTM_STRT_PRIOR_MNTH_NBR = 201812 and dtp.CSTM_END_PRIOR_MNTH_NBR = 201911)\r\n" + 
//				"or (dtp.CSTM_STRT_PRIOR_2_MNTH_NBR = 201801 and dtp.CSTM_END_PRIOR_2_MNTH_NBR = 201811) )) TM_PRD_FNCTN      ON CII_FACT_MBRSHP.ELGBLTY_CY_MNTH_END_NBR BETWEEN TM_PRD_FNCTN.START_YEAR_MNTH       and TM_PRD_FNCTN.      END_YEAR_MNTH      INNER JOIN DIM_MBR_CVRG_TYPE       ON CII_FACT_MBRSHP.MBR_CVRG_TYPE_CD = DIM_MBR_CVRG_TYPE.MBR_CVRG_TYPE_CD  INNER JOIN (Select ACCT_ID,SGMNTN_DIM_KEY, SGMNTN_NM, SRC_FLTR_ID from ACIISST_SGMNTN_BRDG where ACCT_ID in ('W0016437') and SRC_FLTR_ID in ('d97ef60a-2503-41d2-acac-2c91c904e9e0'))SGMNTN on CII_FACT_MBRSHP.SGMNTN_DIM_KEY = SGMNTN.SGMNTN_DIM_KEY and CII_FACT_MBRSHP.ACCT_ID=SGMNTN.ACCT_ID    WHERE CII_FACT_MBRSHP.ACCT_ID IN ('W0016437')       AND DIM_MBR_CVRG_TYPE.MBR_CVRG_TYPE_DESC = 'Medical'      GROUP BY TM_PRD_FNCTN.TM_PRD_NM,       DIM_MBR_CVRG_TYPE.MBR_CVRG_TYPE_DESC,       CII_FACT_MBRSHP.MCID ) AS MBRSHP    GROUP BY MBRSHP.TM_PRD_NM,    MBRSHP.DIM_MBR_CVRG_TYPE_MBR_CVRG_TYPE_DESC,    CII_FACT_MBRSHP_MCID ) AS MDCL_MBRSHP  on (TM_PRD_FNCTN.TM_PRD_NM = MDCL_MBRSHP.Time_Period  and CII_FACT_GAP_SVNGS.MCID = MDCL_MBRSHP.Master_Consumer_ID) INNER JOIN  (SELECT MAX(CII_FACT_MBRSHP.EARND_INCNTV_CD) EARND_INCNTV_CD, CII_FACT_MBRSHP.MCID, CII_FACT_MBRSHP.ACCT_ID, TM_PRD_NM FROM CII_FACT_MBRSHP \r\n" + 
//				"JOIN\r\n" + 
//				"(SELECT YEAR_CD_NM as TM_PRD_NM,\r\n" + 
//				"STRT_MNTH_NBR as START_YEAR_MNTH,\r\n" + 
//				"END_MNTH_NBR as END_YEAR_MNTH,\r\n" + 
//				"case\r\n" + 
//				"  when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"  and INCRD_PAID_CD = 'PAID' then 111101\r\n" + 
//				"  when TM_PRD_TYPE_CD = 'Custom' then STRT_MNTH_NBR\r\n" + 
//				"  else SRVC_STRT_MNTH_NBR\r\n" + 
//				"end as STRT_SRVC_YEAR_MNTH,\r\n" + 
//				"case\r\n" + 
//				"  when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"  and INCRD_PAID_CD = 'PAID' then 888812\r\n" + 
//				"  when TM_PRD_TYPE_CD = 'Custom' then END_MNTH_NBR\r\n" + 
//				"  else SRVC_END_MNTH_NBR\r\n" + 
//				"end as END_SRVC_YEAR_MNTH,\r\n" + 
//				"case\r\n" + 
//				"  when TM_PRD_TYPE_CD <> 'Custom' then PAID_END_MNTH_NBR\r\n" + 
//				"  when INCRD_PAID_CD = 'PAID' then END_MNTH_NBR\r\n" + 
//				"  when INCRD_PAID_CD = 'INC2' then 888811\r\n" + 
//				"  when INCRD_PAID_CD = 'INC3' then dm.YEAR_MNTH_NBR\r\n" + 
//				"  when INCRD_PAID_CD = 'INC1' then PAID_END_MNTH_NBR\r\n" + 
//				"end as END_RPTG_PAID_YEAR_MNTH\r\n" + 
//				"from\r\n" + 
//				"DIM_TM_PRD_ADHC dtp\r\n" + 
//				"join DIM_MNTH dm on 1=1\r\n" + 
//				"where   dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"and dtp.YEAR_ID <= 3\r\n" + 
//				"and dm.YEAR_MNTH_NBR = 202011\r\n" + 
//				"and (\r\n" + 
//				"(dtp.CSTM_STRT_CURNT_MNTH_NBR = 201912 and dtp.CSTM_END_CURNT_MNTH_NBR = 202011)\r\n" + 
//				"or (dtp.CSTM_STRT_PRIOR_MNTH_NBR = 201812 and dtp.CSTM_END_PRIOR_MNTH_NBR = 201911)\r\n" + 
//				"or (dtp.CSTM_STRT_PRIOR_2_MNTH_NBR = 201801 and dtp.CSTM_END_PRIOR_2_MNTH_NBR = 201811) )   )  TM_PRD_FNCTN   \r\n" + 
//				" ON CII_FACT_MBRSHP.ELGBLTY_CY_MNTH_END_NBR BETWEEN TM_PRD_FNCTN.START_YEAR_MNTH  and TM_PRD_FNCTN.END_YEAR_MNTH WHERE CII_FACT_MBRSHP.ACCT_ID IN ('W0016437')  and ELGBL_INCNTV_IND='Y'\r\n" + 
//				"group by CII_FACT_MBRSHP.MCID, CII_FACT_MBRSHP.ACCT_ID, TM_PRD_NM)\r\n" + 
//				"INCNTV_MBRSHP \r\n" + 
//				"ON CII_FACT_GAP_SVNGS.MCID=INCNTV_MBRSHP.MCID AND TM_PRD_FNCTN.TM_PRD_NM=INCNTV_MBRSHP.TM_PRD_NM AND CII_FACT_GAP_SVNGS.ACCT_ID=INCNTV_MBRSHP.ACCT_ID INNER JOIN CII_ACCT_PRFL ON CII_FACT_GAP_SVNGS.ACCT_ID = CII_ACCT_PRFL.ACCT_ID LEFT JOIN  (   \r\n" + 
//				"select CII_FACT_CP_ENGGMNT.acct_id, CII_FACT_CP_ENGGMNT.MCID, TM_PRD_FNCTN.TM_PRD_NM ,\r\n" + 
//				"  (  \r\n" + 
//				"CASE     \r\n" + 
//				" WHEN MAX(DIM_ENGGMNT.TRDTNL_IND)= 1 THEN 'Traditional'          \r\n" + 
//				" WHEN MAX(DIM_ENGGMNT.TRDTNL_IND) = 0     \r\n" + 
//				" AND MAX(DIM_ENGGMNT.ENHNCD_IND) = 1 THEN 'Care Coordination'          \r\n" + 
//				" WHEN MAX(DIM_ENGGMNT.TRDTNL_IND) = 0      \r\n" + 
//				" AND MAX(DIM_ENGGMNT.ENHNCD_IND) = 0     \r\n" + 
//				" AND MAX(DIM_ENGGMNT.EXPNDD_IND)= 1 THEN 'Comprehensive'          \r\n" + 
//				" ELSE 'Not Engaged'    \r\n" + 
//				"END) AS MBR_ENGGMNT_LVL  , \r\n" + 
//				"case  \r\n" + 
//				" when MAX(DIM_ENGGMNT.EXPNDD_IND)= 0 then 'N'  \r\n" + 
//				" else 'Y'  \r\n" + 
//				"end as MBR_ENGGD_IND , MAX(DIM_ENGGMNT.ENHNCD_IND) as isEnhanced ,\r\n" + 
//				"  MAX(DIM_ENGGMNT.EXPNDD_IND) as isExpanded , MAX(DIM_ENGGMNT.TRDTNL_IND) as isTraditional  \r\n" + 
//				"from CII_FACT_CP_ENGGMNT   inner join DIM_ENGGMNT         \r\n" + 
//				" on CII_FACT_CP_ENGGMNT.MBR_ENGGMNT_ID = DIM_ENGGMNT.ENGGMNT_ID JOIN (   \r\n" + 
//				"SELECT YEAR_CD_NM as TM_PRD_NM,\r\n" + 
//				"\r\n" + 
//				"STRT_MNTH_NBR as START_YEAR_MNTH,\r\n" + 
//				"\r\n" + 
//				"END_MNTH_NBR as END_YEAR_MNTH,\r\n" + 
//				"\r\n" + 
//				"case\r\n" + 
//				"\r\n" + 
//				"  when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"\r\n" + 
//				"  and INCRD_PAID_CD = 'PAID' then 111101\r\n" + 
//				"\r\n" + 
//				"  when TM_PRD_TYPE_CD = 'Custom' then STRT_MNTH_NBR\r\n" + 
//				"\r\n" + 
//				"  else SRVC_STRT_MNTH_NBR\r\n" + 
//				"\r\n" + 
//				"end as STRT_SRVC_YEAR_MNTH,\r\n" + 
//				"\r\n" + 
//				"case\r\n" + 
//				"\r\n" + 
//				"  when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"\r\n" + 
//				"  and INCRD_PAID_CD = 'PAID' then 888812\r\n" + 
//				"\r\n" + 
//				"  when TM_PRD_TYPE_CD = 'Custom' then END_MNTH_NBR\r\n" + 
//				"\r\n" + 
//				"  else SRVC_END_MNTH_NBR\r\n" + 
//				"\r\n" + 
//				"end as END_SRVC_YEAR_MNTH,\r\n" + 
//				"\r\n" + 
//				"case\r\n" + 
//				"\r\n" + 
//				"  when TM_PRD_TYPE_CD <> 'Custom' then PAID_END_MNTH_NBR\r\n" + 
//				"\r\n" + 
//				"  when INCRD_PAID_CD = 'PAID' then END_MNTH_NBR\r\n" + 
//				"\r\n" + 
//				"  when INCRD_PAID_CD = 'INC2' then 888811\r\n" + 
//				"\r\n" + 
//				"  when INCRD_PAID_CD = 'INC3' then dm.YEAR_MNTH_NBR\r\n" + 
//				"\r\n" + 
//				"  when INCRD_PAID_CD = 'INC1' then PAID_END_MNTH_NBR\r\n" + 
//				"\r\n" + 
//				"end as END_RPTG_PAID_YEAR_MNTH\r\n" + 
//				"\r\n" + 
//				"from\r\n" + 
//				"\r\n" + 
//				"DIM_TM_PRD_ADHC dtp\r\n" + 
//				"\r\n" + 
//				"join DIM_MNTH dm \r\n" + 
//				" on 1=1\r\n" + 
//				"\r\n" + 
//				"where dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"\r\n" + 
//				"and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"\r\n" + 
//				"and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"\r\n" + 
//				"and dtp.YEAR_ID <= 3\r\n" + 
//				"\r\n" + 
//				"and dm.YEAR_MNTH_NBR = 202011\r\n" + 
//				"\r\n" + 
//				"and (\r\n" + 
//				"\r\n" + 
//				"(dtp.CSTM_STRT_CURNT_MNTH_NBR = 201912 \r\n" + 
//				" and dtp.CSTM_END_CURNT_MNTH_NBR = 202011)\r\n" + 
//				"\r\n" + 
//				"or (dtp.CSTM_STRT_PRIOR_MNTH_NBR = 201812 \r\n" + 
//				" and dtp.CSTM_END_PRIOR_MNTH_NBR = 201911)\r\n" + 
//				"\r\n" + 
//				"or (dtp.CSTM_STRT_PRIOR_2_MNTH_NBR = 201801 \r\n" + 
//				" and dtp.CSTM_END_PRIOR_2_MNTH_NBR = 201811) )   )  TM_PRD_FNCTN       \r\n" + 
//				" ON CII_FACT_CP_ENGGMNT.ENGGMNT_MNTH_NBR BETWEEN TM_PRD_FNCTN.START_YEAR_MNTH      \r\n" + 
//				" and TM_PRD_FNCTN. END_YEAR_MNTH   \r\n" + 
//				"WHERE CII_FACT_CP_ENGGMNT.ACCT_ID IN ('W0016437')    \r\n" + 
//				"group by  CII_FACT_CP_ENGGMNT.acct_id, CII_FACT_CP_ENGGMNT.MCID ,\r\n" + 
//				"  TM_PRD_FNCTN.TM_PRD_NM )   TMBRENGGMNT2      \r\n" + 
//				" ON CII_FACT_GAP_SVNGS.ACCT_ID = TMBRENGGMNT2.ACCT_ID      \r\n" + 
//				" and CII_FACT_GAP_SVNGS.MCID=TMBRENGGMNT2.MCID      \r\n" + 
//				" and TM_PRD_FNCTN.TM_PRD_NM =  TMBRENGGMNT2.TM_PRD_NM LEFT    JOIN  (   \r\n" + 
//				"select    a.acct_id, a.MCID,a.TM_PRD_NM, a.ENGGMNT, b.ENGGMNT_TIER    \r\n" + 
//				"from     (   \r\n" + 
//				"select    CII_FACT_CP_ENGGMNT.acct_id, CII_FACT_CP_ENGGMNT.MCID,\r\n" + 
//				"  TM_PRD_FNCTN.TM_PRD_NM ,                 (  \r\n" + 
//				"CASE               \r\n" + 
//				" WHEN MAX(DIM_ENGGMNT.TRDTNL_IND)= 1 THEN 'Traditional'                 \r\n" + 
//				" WHEN MAX(DIM_ENGGMNT.TRDTNL_IND) = 0            \r\n" + 
//				" AND MAX(DIM_ENGGMNT.ENHNCD_IND) = 1 THEN 'Care Coordination'                 \r\n" + 
//				" WHEN MAX(DIM_ENGGMNT.TRDTNL_IND) = 0             \r\n" + 
//				" AND MAX(DIM_ENGGMNT.ENHNCD_IND) = 0            \r\n" + 
//				" AND MAX(DIM_ENGGMNT.EXPNDD_IND)= 1 THEN 'Comprehensive'                 \r\n" + 
//				" ELSE 'Not Engaged'    \r\n" + 
//				"END) AS ENGGMNT         \r\n" + 
//				"from    CII_FACT_CP_ENGGMNT  inner join DIM_ENGGMNT                \r\n" + 
//				" on CII_FACT_CP_ENGGMNT.MBR_ENGGMNT_ID = DIM_ENGGMNT.ENGGMNT_ID JOIN (   \r\n" + 
//				"SELECT    YEAR_CD_NM as TM_PRD_NM,\r\n" + 
//				"\r\n" + 
//				"STRT_MNTH_NBR as START_YEAR_MNTH,\r\n" + 
//				"\r\n" + 
//				"END_MNTH_NBR as END_YEAR_MNTH,\r\n" + 
//				"\r\n" + 
//				"case\r\n" + 
//				"\r\n" + 
//				"  when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"\r\n" + 
//				" and INCRD_PAID_CD = 'PAID' then 111101\r\n" + 
//				"\r\n" + 
//				"  when TM_PRD_TYPE_CD = 'Custom' then STRT_MNTH_NBR\r\n" + 
//				"\r\n" + 
//				"  else SRVC_STRT_MNTH_NBR\r\n" + 
//				"\r\n" + 
//				"end as STRT_SRVC_YEAR_MNTH,\r\n" + 
//				"\r\n" + 
//				"case\r\n" + 
//				"\r\n" + 
//				"  when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"\r\n" + 
//				"  and INCRD_PAID_CD = 'PAID' then 888812\r\n" + 
//				"\r\n" + 
//				"  when TM_PRD_TYPE_CD = 'Custom' then END_MNTH_NBR\r\n" + 
//				"\r\n" + 
//				"  else SRVC_END_MNTH_NBR\r\n" + 
//				"\r\n" + 
//				"end as END_SRVC_YEAR_MNTH,\r\n" + 
//				"\r\n" + 
//				"case\r\n" + 
//				"\r\n" + 
//				"  when TM_PRD_TYPE_CD <> 'Custom' then PAID_END_MNTH_NBR\r\n" + 
//				"\r\n" + 
//				"  when INCRD_PAID_CD = 'PAID' then END_MNTH_NBR\r\n" + 
//				"\r\n" + 
//				"  when INCRD_PAID_CD = 'INC2' then 888811\r\n" + 
//				"\r\n" + 
//				"  when INCRD_PAID_CD = 'INC3' then dm.YEAR_MNTH_NBR\r\n" + 
//				"\r\n" + 
//				"  when INCRD_PAID_CD = 'INC1' then PAID_END_MNTH_NBR\r\n" + 
//				"\r\n" + 
//				"end as END_RPTG_PAID_YEAR_MNTH\r\n" + 
//				"\r\n" + 
//				"from\r\n" + 
//				"\r\n" + 
//				"DIM_TM_PRD_ADHC dtp\r\n" + 
//				"\r\n" + 
//				"join DIM_MNTH dm \r\n" + 
//				" on 1=1\r\n" + 
//				"\r\n" + 
//				"where dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"\r\n" + 
//				"and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"\r\n" + 
//				"and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"\r\n" + 
//				"and dtp.YEAR_ID <= 3\r\n" + 
//				"\r\n" + 
//				"and dm.YEAR_MNTH_NBR = 202011\r\n" + 
//				"\r\n" + 
//				"and (\r\n" + 
//				"\r\n" + 
//				"(dtp.CSTM_STRT_CURNT_MNTH_NBR = 201912 \r\n" + 
//				" and dtp.CSTM_END_CURNT_MNTH_NBR = 202011)\r\n" + 
//				"\r\n" + 
//				"or (dtp.CSTM_STRT_PRIOR_MNTH_NBR = 201812 \r\n" + 
//				" and dtp.CSTM_END_PRIOR_MNTH_NBR = 201911)\r\n" + 
//				"\r\n" + 
//				"or (dtp.CSTM_STRT_PRIOR_2_MNTH_NBR = 201801 \r\n" + 
//				" and dtp.CSTM_END_PRIOR_2_MNTH_NBR = 201811) )   )  TM_PRD_FNCTN              \r\n" + 
//				" ON CII_FACT_CP_ENGGMNT.ENGGMNT_MNTH_NBR BETWEEN TM_PRD_FNCTN.START_YEAR_MNTH             \r\n" + 
//				" and TM_PRD_FNCTN. END_YEAR_MNTH    \r\n" + 
//				"WHERE    CII_FACT_CP_ENGGMNT.ACCT_ID IN ('W0016437')    \r\n" + 
//				"group by  CII_FACT_CP_ENGGMNT.acct_id, CII_FACT_CP_ENGGMNT.MCID ,\r\n" + 
//				"          TM_PRD_FNCTN.TM_PRD_NM ) a  inner join (   \r\n" + 
//				"select       \r\n" + 
//				"CASE               \r\n" + 
//				" WHEN DIM_ENGGMNT.TRDTNL_IND= 1 THEN cast('Traditional' as char(20))            \r\n" + 
//				" WHEN DIM_ENGGMNT.TRDTNL_IND = 0            \r\n" + 
//				" AND DIM_ENGGMNT.ENHNCD_IND= 1 THEN cast('Care Coordination' as char(20))            \r\n" + 
//				" WHEN DIM_ENGGMNT.TRDTNL_IND= 0             \r\n" + 
//				" AND DIM_ENGGMNT.ENHNCD_IND = 0            \r\n" + 
//				" AND DIM_ENGGMNT.EXPNDD_IND= 1 THEN  cast('Comprehensive' as char(20))            \r\n" + 
//				" ELSE cast('Not Engaged' as char(20))    \r\n" + 
//				"END    AS ENGGMNT ,   \r\n" + 
//				"CASE               \r\n" + 
//				" WHEN DIM_ENGGMNT.TRDTNL_IND= 1 THEN cast('Traditional' as char(20))            \r\n" + 
//				" WHEN DIM_ENGGMNT.TRDTNL_IND = 0            \r\n" + 
//				" AND DIM_ENGGMNT.ENHNCD_IND= 1 THEN cast('Care Coordination' as char(20))            \r\n" + 
//				" WHEN DIM_ENGGMNT.TRDTNL_IND= 0             \r\n" + 
//				" AND DIM_ENGGMNT.ENHNCD_IND = 0            \r\n" + 
//				" AND DIM_ENGGMNT.EXPNDD_IND= 1 THEN cast('Comprehensive' as char(20))            \r\n" + 
//				" ELSE cast('Not Engaged' as char(20))    \r\n" + 
//				"END    AS ENGGMNT_TIER                          \r\n" + 
//				"from    DIM_ENGGMNT                          \r\n" + 
//				"union    all                          \r\n" + 
//				"select       \r\n" + 
//				"CASE               \r\n" + 
//				" WHEN DIM_ENGGMNT.TRDTNL_IND= 1 THEN cast('Traditional' as char(20))            \r\n" + 
//				" else cast('Care Coordination' as char(20))    \r\n" + 
//				"end    as ENGGMNT , cast( 'Comprehensive' as char(20))  AS ENGGMNT_TIER    \r\n" + 
//				"from    DIM_ENGGMNT    \r\n" + 
//				"where    DIM_ENGGMNT.TRDTNL_IND= 1            \r\n" + 
//				" or DIM_ENGGMNT.ENHNCD_IND= 1                          \r\n" + 
//				"union    all                          \r\n" + 
//				"select    cast('Traditional' as char(20)) as ENGGMNT, cast('Care Coordination' as char(20))  AS ENGGMNT_TIER    \r\n" + 
//				"from    DIM_ENGGMNT    \r\n" + 
//				"where    DIM_ENGGMNT.TRDTNL_IND= 1 ) b            \r\n" + 
//				" on a.ENGGMNT = b.ENGGMNT                   )  TMBRENGGMNT             \r\n" + 
//				" ON CII_FACT_GAP_SVNGS.ACCT_ID = TMBRENGGMNT.ACCT_ID             \r\n" + 
//				" and CII_FACT_GAP_SVNGS.MCID=TMBRENGGMNT.MCID             \r\n" + 
//				" and TM_PRD_FNCTN.TM_PRD_NM =  TMBRENGGMNT.TM_PRD_NM INNER JOIN (Select ACCT_ID,SGMNTN_DIM_KEY, SGMNTN_NM, SRC_FLTR_ID from ACIISST_SGMNTN_BRDG where ACCT_ID in ('W0016437') and SRC_FLTR_ID in ('d97ef60a-2503-41d2-acac-2c91c904e9e0'))SGMNTN on CII_FACT_GAP_SVNGS.SGMNTN_DIM_KEY = SGMNTN.SGMNTN_DIM_KEY and CII_FACT_GAP_SVNGS.ACCT_ID=SGMNTN.ACCT_ID WHERE CII_FACT_GAP_SVNGS.ACCT_ID in ('W0016437')   GROUP BY CII_ACCT_PRFL.ACCT_NM,coalesce( TMBRENGGMNT2.MBR_ENGGD_IND,'N'),coalesce(TMBRENGGMNT.ENGGMNT_TIER, 'Not Engaged'),TM_PRD_FNCTN.TM_PRD_NM,INCNTV_MBRSHP.EARND_INCNTV_CD ) AS GAPSVNGS  GROUP BY GAPSVNGS.ACCT_NM,GAPSVNGS.MBR_ENGGD_IND,GAPSVNGS.MBR_ENGGMNT_TIER,GAPSVNGS.TM_PRD_NM,GAPSVNGS.EARND_INCNTV_CD ) AS FNL   ORDER BY Account_Name,Gaps_Closed,Member_Engaged_Indicator,Member_Engagement_Tier,Time_Period,Earned_Incentive_Indicator\r\n" + 
//				"";
//		
//		String query31 = "SELECT Account_Name AS \"Account Name\",Gaps_Closed AS \"Gaps Closed\",Member_Engaged_Indicator AS \"Member Engaged Indicator\",Member_Engagement_Tier AS \"Member Engagement Tier\",Time_Period AS \"Time Period\",Earned_Incentive_Indicator AS \"Earned Incentive Indicator\"  FROM  (  SELECT GAPSVNGS.ACCT_NM AS Account_Name,SUM(GAPSVNGS.GPS_CLSD) AS Gaps_Closed,GAPSVNGS.MBR_ENGGD_IND AS Member_Engaged_Indicator,GAPSVNGS.MBR_ENGGMNT_TIER AS Member_Engagement_Tier,GAPSVNGS.TM_PRD_NM AS Time_Period,GAPSVNGS.EARND_INCNTV_CD AS Earned_Incentive_Indicator  FROM  (  SELECT CII_ACCT_PRFL.ACCT_NM AS ACCT_NM,SUM( CASE WHEN CII_FACT_GAP_SVNGS.CMPLNC_YEAR_MNTH_YEAR_NBR <= CASE WHEN substr(cast(TM_PRD_FNCTN.END_YEAR_MNTH  as varchar(6)) ,5,2) = '01' THEN TM_PRD_FNCTN.END_YEAR_MNTH   - 89 ELSE TM_PRD_FNCTN.END_YEAR_MNTH   - 1 END  THEN CII_FACT_GAP_SVNGS.CLSUR_CNT ELSE 0 END) AS GPS_CLSD,coalesce( TMBRENGGMNT2.MBR_ENGGD_IND,'N') AS MBR_ENGGD_IND,coalesce(TMBRENGGMNT.ENGGMNT_TIER, 'Not Engaged') AS MBR_ENGGMNT_TIER,TM_PRD_FNCTN.TM_PRD_NM AS TM_PRD_NM,INCNTV_MBRSHP.EARND_INCNTV_CD AS EARND_INCNTV_CD  FROM CII_FACT_GAP_SVNGS INNER JOIN (\r\n" + 
//				"Select YEAR_CD_NM as TM_PRD_NM,\r\n" + 
//				"STRT_MNTH_NBR as START_YEAR_MNTH,\r\n" + 
//				"END_MNTH_NBR as END_YEAR_MNTH,\r\n" + 
//				"case\r\n" + 
//				"when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"and INCRD_PAID_CD = 'PAID' then 111101\r\n" + 
//				"when TM_PRD_TYPE_CD = 'Custom' then STRT_MNTH_NBR\r\n" + 
//				"else SRVC_STRT_MNTH_NBR\r\n" + 
//				"end as STRT_SRVC_YEAR_MNTH,\r\n" + 
//				"case\r\n" + 
//				"when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"and INCRD_PAID_CD = 'PAID' then 888812\r\n" + 
//				"when TM_PRD_TYPE_CD = 'Custom' then END_MNTH_NBR\r\n" + 
//				"else SRVC_END_MNTH_NBR\r\n" + 
//				"end as END_SRVC_YEAR_MNTH,\r\n" + 
//				"case\r\n" + 
//				"when TM_PRD_TYPE_CD <> 'Custom' then PAID_END_MNTH_NBR\r\n" + 
//				"when INCRD_PAID_CD = 'PAID' then END_MNTH_NBR\r\n" + 
//				"when INCRD_PAID_CD = 'INC2' then 888811\r\n" + 
//				"when INCRD_PAID_CD = 'INC3' then dm.YEAR_MNTH_NBR\r\n" + 
//				"when INCRD_PAID_CD = 'INC1' then PAID_END_MNTH_NBR\r\n" + 
//				"end as END_RPTG_PAID_YEAR_MNTH\r\n" + 
//				"from\r\n" + 
//				"DIM_TM_PRD_ADHC dtp\r\n" + 
//				"join DIM_MNTH dm on 1=1\r\n" + 
//				"where   dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"and dtp.YEAR_ID <= 3\r\n" + 
//				"and dm.YEAR_MNTH_NBR = 202011\r\n" + 
//				"and (\r\n" + 
//				"(dtp.CSTM_STRT_CURNT_MNTH_NBR = 201912 and dtp.CSTM_END_CURNT_MNTH_NBR = 202011)\r\n" + 
//				"or (dtp.CSTM_STRT_PRIOR_MNTH_NBR = 201812 and dtp.CSTM_END_PRIOR_MNTH_NBR = 201911)\r\n" + 
//				"or (dtp.CSTM_STRT_PRIOR_2_MNTH_NBR = 201801 and dtp.CSTM_END_PRIOR_2_MNTH_NBR = 201811) )) TM_PRD_FNCTN ON CII_FACT_GAP_SVNGS.RLSET_RUN_YEAR_MNTH_NBR BETWEEN CASE WHEN substr(cast(TM_PRD_FNCTN.START_YEAR_MNTH as varchar(6)) ,5,2) <= '06' THEN TM_PRD_FNCTN.START_YEAR_MNTH  - 94 ELSE TM_PRD_FNCTN.START_YEAR_MNTH  - 6 END and CASE WHEN substr(cast(TM_PRD_FNCTN.END_YEAR_MNTH as varchar(6)) ,5,2) <= '06' THEN TM_PRD_FNCTN.END_YEAR_MNTH  - 94 ELSE TM_PRD_FNCTN.END_YEAR_MNTH  - 6 END INNER JOIN (    SELECT MBRSHP.TM_PRD_NM AS Time_Period,MBRSHP.DIM_MBR_CVRG_TYPE_MBR_CVRG_TYPE_DESC AS Member_Coverage_Type_Description,   MBRSHP.CII_FACT_MBRSHP_MCID AS Master_Consumer_ID  FROM (       SELECT TM_PRD_FNCTN.TM_PRD_NM AS TM_PRD_NM,       DIM_MBR_CVRG_TYPE.MBR_CVRG_TYPE_DESC AS DIM_MBR_CVRG_TYPE_MBR_CVRG_TYPE_DESC,       CII_FACT_MBRSHP.MCID AS CII_FACT_MBRSHP_MCID      FROM CII_FACT_MBRSHP     INNER JOIN (       \r\n" + 
//				"Select YEAR_CD_NM as TM_PRD_NM,\r\n" + 
//				"STRT_MNTH_NBR as START_YEAR_MNTH,\r\n" + 
//				"END_MNTH_NBR as END_YEAR_MNTH,\r\n" + 
//				"case\r\n" + 
//				"when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"and INCRD_PAID_CD = 'PAID' then 111101\r\n" + 
//				"when TM_PRD_TYPE_CD = 'Custom' then STRT_MNTH_NBR\r\n" + 
//				"else SRVC_STRT_MNTH_NBR\r\n" + 
//				"end as STRT_SRVC_YEAR_MNTH,\r\n" + 
//				"case\r\n" + 
//				"when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"and INCRD_PAID_CD = 'PAID' then 888812\r\n" + 
//				"when TM_PRD_TYPE_CD = 'Custom' then END_MNTH_NBR\r\n" + 
//				"else SRVC_END_MNTH_NBR\r\n" + 
//				"end as END_SRVC_YEAR_MNTH,\r\n" + 
//				"case\r\n" + 
//				"when TM_PRD_TYPE_CD <> 'Custom' then PAID_END_MNTH_NBR\r\n" + 
//				"when INCRD_PAID_CD = 'PAID' then END_MNTH_NBR\r\n" + 
//				"when INCRD_PAID_CD = 'INC2' then 888811\r\n" + 
//				"when INCRD_PAID_CD = 'INC3' then dm.YEAR_MNTH_NBR\r\n" + 
//				"when INCRD_PAID_CD = 'INC1' then PAID_END_MNTH_NBR\r\n" + 
//				"end as END_RPTG_PAID_YEAR_MNTH\r\n" + 
//				"from\r\n" + 
//				"DIM_TM_PRD_ADHC dtp\r\n" + 
//				"join DIM_MNTH dm on 1=1\r\n" + 
//				"where   dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"and dtp.YEAR_ID <= 3\r\n" + 
//				"and dm.YEAR_MNTH_NBR = 202011\r\n" + 
//				"and (\r\n" + 
//				"(dtp.CSTM_STRT_CURNT_MNTH_NBR = 201912 and dtp.CSTM_END_CURNT_MNTH_NBR = 202011)\r\n" + 
//				"or (dtp.CSTM_STRT_PRIOR_MNTH_NBR = 201812 and dtp.CSTM_END_PRIOR_MNTH_NBR = 201911)\r\n" + 
//				"or (dtp.CSTM_STRT_PRIOR_2_MNTH_NBR = 201801 and dtp.CSTM_END_PRIOR_2_MNTH_NBR = 201811) )) TM_PRD_FNCTN      ON CII_FACT_MBRSHP.ELGBLTY_CY_MNTH_END_NBR BETWEEN TM_PRD_FNCTN.START_YEAR_MNTH       and TM_PRD_FNCTN.      END_YEAR_MNTH      INNER JOIN DIM_MBR_CVRG_TYPE       ON CII_FACT_MBRSHP.MBR_CVRG_TYPE_CD = DIM_MBR_CVRG_TYPE.MBR_CVRG_TYPE_CD  INNER JOIN (Select ACCT_ID,SGMNTN_DIM_KEY, SGMNTN_NM, SRC_FLTR_ID from ACIISST_SGMNTN_BRDG where ACCT_ID in ('W0016437') and SRC_FLTR_ID in ('d97ef60a-2503-41d2-acac-2c91c904e9e0'))SGMNTN on CII_FACT_MBRSHP.SGMNTN_DIM_KEY = SGMNTN.SGMNTN_DIM_KEY and CII_FACT_MBRSHP.ACCT_ID=SGMNTN.ACCT_ID    WHERE CII_FACT_MBRSHP.ACCT_ID IN ('W0016437')       AND DIM_MBR_CVRG_TYPE.MBR_CVRG_TYPE_DESC = 'Medical'      GROUP BY TM_PRD_FNCTN.TM_PRD_NM,       DIM_MBR_CVRG_TYPE.MBR_CVRG_TYPE_DESC,       CII_FACT_MBRSHP.MCID ) AS MBRSHP    GROUP BY MBRSHP.TM_PRD_NM,    MBRSHP.DIM_MBR_CVRG_TYPE_MBR_CVRG_TYPE_DESC,    CII_FACT_MBRSHP_MCID ) AS MDCL_MBRSHP  on (TM_PRD_FNCTN.TM_PRD_NM = MDCL_MBRSHP.Time_Period  and CII_FACT_GAP_SVNGS.MCID = MDCL_MBRSHP.Master_Consumer_ID) INNER JOIN  (SELECT MAX(CII_FACT_MBRSHP.EARND_INCNTV_CD) EARND_INCNTV_CD, CII_FACT_MBRSHP.MCID, CII_FACT_MBRSHP.ACCT_ID, TM_PRD_NM FROM CII_FACT_MBRSHP \r\n" + 
//				"JOIN\r\n" + 
//				"(SELECT YEAR_CD_NM as TM_PRD_NM,\r\n" + 
//				"STRT_MNTH_NBR as START_YEAR_MNTH,\r\n" + 
//				"END_MNTH_NBR as END_YEAR_MNTH,\r\n" + 
//				"case\r\n" + 
//				"  when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"  and INCRD_PAID_CD = 'PAID' then 111101\r\n" + 
//				"  when TM_PRD_TYPE_CD = 'Custom' then STRT_MNTH_NBR\r\n" + 
//				"  else SRVC_STRT_MNTH_NBR\r\n" + 
//				"end as STRT_SRVC_YEAR_MNTH,\r\n" + 
//				"case\r\n" + 
//				"  when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"  and INCRD_PAID_CD = 'PAID' then 888812\r\n" + 
//				"  when TM_PRD_TYPE_CD = 'Custom' then END_MNTH_NBR\r\n" + 
//				"  else SRVC_END_MNTH_NBR\r\n" + 
//				"end as END_SRVC_YEAR_MNTH,\r\n" + 
//				"case\r\n" + 
//				"  when TM_PRD_TYPE_CD <> 'Custom' then PAID_END_MNTH_NBR\r\n" + 
//				"  when INCRD_PAID_CD = 'PAID' then END_MNTH_NBR\r\n" + 
//				"  when INCRD_PAID_CD = 'INC2' then 888811\r\n" + 
//				"  when INCRD_PAID_CD = 'INC3' then dm.YEAR_MNTH_NBR\r\n" + 
//				"  when INCRD_PAID_CD = 'INC1' then PAID_END_MNTH_NBR\r\n" + 
//				"end as END_RPTG_PAID_YEAR_MNTH\r\n" + 
//				"from\r\n" + 
//				"DIM_TM_PRD_ADHC dtp\r\n" + 
//				"join DIM_MNTH dm on 1=1\r\n" + 
//				"where   dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"and dtp.YEAR_ID <= 3\r\n" + 
//				"and dm.YEAR_MNTH_NBR = 202011\r\n" + 
//				"and (\r\n" + 
//				"(dtp.CSTM_STRT_CURNT_MNTH_NBR = 201912 and dtp.CSTM_END_CURNT_MNTH_NBR = 202011)\r\n" + 
//				"or (dtp.CSTM_STRT_PRIOR_MNTH_NBR = 201812 and dtp.CSTM_END_PRIOR_MNTH_NBR = 201911)\r\n" + 
//				"or (dtp.CSTM_STRT_PRIOR_2_MNTH_NBR = 201801 and dtp.CSTM_END_PRIOR_2_MNTH_NBR = 201811) )   )  TM_PRD_FNCTN   \r\n" + 
//				" ON CII_FACT_MBRSHP.ELGBLTY_CY_MNTH_END_NBR BETWEEN TM_PRD_FNCTN.START_YEAR_MNTH  and TM_PRD_FNCTN.END_YEAR_MNTH WHERE CII_FACT_MBRSHP.ACCT_ID IN ('W0016437')  and ELGBL_INCNTV_IND='Y'\r\n" + 
//				"group by CII_FACT_MBRSHP.MCID, CII_FACT_MBRSHP.ACCT_ID, TM_PRD_NM)\r\n" + 
//				"INCNTV_MBRSHP \r\n" + 
//				"ON CII_FACT_GAP_SVNGS.MCID=INCNTV_MBRSHP.MCID AND TM_PRD_FNCTN.TM_PRD_NM=INCNTV_MBRSHP.TM_PRD_NM AND CII_FACT_GAP_SVNGS.ACCT_ID=INCNTV_MBRSHP.ACCT_ID INNER JOIN CII_ACCT_PRFL ON CII_FACT_GAP_SVNGS.ACCT_ID = CII_ACCT_PRFL.ACCT_ID LEFT JOIN  (   \r\n" + 
//				"select CII_FACT_CP_ENGGMNT.acct_id, CII_FACT_CP_ENGGMNT.MCID, TM_PRD_FNCTN.TM_PRD_NM ,\r\n" + 
//				"  (  \r\n" + 
//				"CASE     \r\n" + 
//				" WHEN MAX(DIM_ENGGMNT.TRDTNL_IND)= 1 THEN 'Traditional'          \r\n" + 
//				" WHEN MAX(DIM_ENGGMNT.TRDTNL_IND) = 0     \r\n" + 
//				" AND MAX(DIM_ENGGMNT.ENHNCD_IND) = 1 THEN 'Care Coordination'          \r\n" + 
//				" WHEN MAX(DIM_ENGGMNT.TRDTNL_IND) = 0      \r\n" + 
//				" AND MAX(DIM_ENGGMNT.ENHNCD_IND) = 0     \r\n" + 
//				" AND MAX(DIM_ENGGMNT.EXPNDD_IND)= 1 THEN 'Comprehensive'          \r\n" + 
//				" ELSE 'Not Engaged'    \r\n" + 
//				"END) AS MBR_ENGGMNT_LVL  , \r\n" + 
//				"case  \r\n" + 
//				" when MAX(DIM_ENGGMNT.EXPNDD_IND)= 0 then 'N'  \r\n" + 
//				" else 'Y'  \r\n" + 
//				"end as MBR_ENGGD_IND , MAX(DIM_ENGGMNT.ENHNCD_IND) as isEnhanced ,\r\n" + 
//				"  MAX(DIM_ENGGMNT.EXPNDD_IND) as isExpanded , MAX(DIM_ENGGMNT.TRDTNL_IND) as isTraditional  \r\n" + 
//				"from CII_FACT_CP_ENGGMNT   inner join DIM_ENGGMNT         \r\n" + 
//				" on CII_FACT_CP_ENGGMNT.MBR_ENGGMNT_ID = DIM_ENGGMNT.ENGGMNT_ID JOIN (   \r\n" + 
//				"SELECT YEAR_CD_NM as TM_PRD_NM,\r\n" + 
//				"\r\n" + 
//				"STRT_MNTH_NBR as START_YEAR_MNTH,\r\n" + 
//				"\r\n" + 
//				"END_MNTH_NBR as END_YEAR_MNTH,\r\n" + 
//				"\r\n" + 
//				"case\r\n" + 
//				"\r\n" + 
//				"  when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"\r\n" + 
//				"  and INCRD_PAID_CD = 'PAID' then 111101\r\n" + 
//				"\r\n" + 
//				"  when TM_PRD_TYPE_CD = 'Custom' then STRT_MNTH_NBR\r\n" + 
//				"\r\n" + 
//				"  else SRVC_STRT_MNTH_NBR\r\n" + 
//				"\r\n" + 
//				"end as STRT_SRVC_YEAR_MNTH,\r\n" + 
//				"\r\n" + 
//				"case\r\n" + 
//				"\r\n" + 
//				"  when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"\r\n" + 
//				"  and INCRD_PAID_CD = 'PAID' then 888812\r\n" + 
//				"\r\n" + 
//				"  when TM_PRD_TYPE_CD = 'Custom' then END_MNTH_NBR\r\n" + 
//				"\r\n" + 
//				"  else SRVC_END_MNTH_NBR\r\n" + 
//				"\r\n" + 
//				"end as END_SRVC_YEAR_MNTH,\r\n" + 
//				"\r\n" + 
//				"case\r\n" + 
//				"\r\n" + 
//				"  when TM_PRD_TYPE_CD <> 'Custom' then PAID_END_MNTH_NBR\r\n" + 
//				"\r\n" + 
//				"  when INCRD_PAID_CD = 'PAID' then END_MNTH_NBR\r\n" + 
//				"\r\n" + 
//				"  when INCRD_PAID_CD = 'INC2' then 888811\r\n" + 
//				"\r\n" + 
//				"  when INCRD_PAID_CD = 'INC3' then dm.YEAR_MNTH_NBR\r\n" + 
//				"\r\n" + 
//				"  when INCRD_PAID_CD = 'INC1' then PAID_END_MNTH_NBR\r\n" + 
//				"\r\n" + 
//				"end as END_RPTG_PAID_YEAR_MNTH\r\n" + 
//				"\r\n" + 
//				"from\r\n" + 
//				"\r\n" + 
//				"DIM_TM_PRD_ADHC dtp\r\n" + 
//				"\r\n" + 
//				"join DIM_MNTH dm \r\n" + 
//				" on 1=1\r\n" + 
//				"\r\n" + 
//				"where dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"\r\n" + 
//				"and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"\r\n" + 
//				"and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"\r\n" + 
//				"and dtp.YEAR_ID <= 3\r\n" + 
//				"\r\n" + 
//				"and dm.YEAR_MNTH_NBR = 202011\r\n" + 
//				"\r\n" + 
//				"and (\r\n" + 
//				"\r\n" + 
//				"(dtp.CSTM_STRT_CURNT_MNTH_NBR = 201912 \r\n" + 
//				" and dtp.CSTM_END_CURNT_MNTH_NBR = 202011)\r\n" + 
//				"\r\n" + 
//				"or (dtp.CSTM_STRT_PRIOR_MNTH_NBR = 201812 \r\n" + 
//				" and dtp.CSTM_END_PRIOR_MNTH_NBR = 201911)\r\n" + 
//				"\r\n" + 
//				"or (dtp.CSTM_STRT_PRIOR_2_MNTH_NBR = 201801 \r\n" + 
//				" and dtp.CSTM_END_PRIOR_2_MNTH_NBR = 201811) )   )  TM_PRD_FNCTN       \r\n" + 
//				" ON CII_FACT_CP_ENGGMNT.ENGGMNT_MNTH_NBR BETWEEN TM_PRD_FNCTN.START_YEAR_MNTH      \r\n" + 
//				" and TM_PRD_FNCTN. END_YEAR_MNTH   \r\n" + 
//				"WHERE CII_FACT_CP_ENGGMNT.ACCT_ID IN ('W0016437')    \r\n" + 
//				"group by  CII_FACT_CP_ENGGMNT.acct_id, CII_FACT_CP_ENGGMNT.MCID ,\r\n" + 
//				"  TM_PRD_FNCTN.TM_PRD_NM )   TMBRENGGMNT2      \r\n" + 
//				" ON CII_FACT_GAP_SVNGS.ACCT_ID = TMBRENGGMNT2.ACCT_ID      \r\n" + 
//				" and CII_FACT_GAP_SVNGS.MCID=TMBRENGGMNT2.MCID      \r\n" + 
//				" and TM_PRD_FNCTN.TM_PRD_NM =  TMBRENGGMNT2.TM_PRD_NM LEFT    JOIN  (   \r\n" + 
//				"select    a.acct_id, a.MCID,a.TM_PRD_NM, a.ENGGMNT, b.ENGGMNT_TIER    \r\n" + 
//				"from     (   \r\n" + 
//				"select    CII_FACT_CP_ENGGMNT.acct_id, CII_FACT_CP_ENGGMNT.MCID,\r\n" + 
//				"  TM_PRD_FNCTN.TM_PRD_NM ,                 (  \r\n" + 
//				"CASE               \r\n" + 
//				" WHEN MAX(DIM_ENGGMNT.TRDTNL_IND)= 1 THEN 'Traditional'                 \r\n" + 
//				" WHEN MAX(DIM_ENGGMNT.TRDTNL_IND) = 0            \r\n" + 
//				" AND MAX(DIM_ENGGMNT.ENHNCD_IND) = 1 THEN 'Care Coordination'                 \r\n" + 
//				" WHEN MAX(DIM_ENGGMNT.TRDTNL_IND) = 0             \r\n" + 
//				" AND MAX(DIM_ENGGMNT.ENHNCD_IND) = 0            \r\n" + 
//				" AND MAX(DIM_ENGGMNT.EXPNDD_IND)= 1 THEN 'Comprehensive'                 \r\n" + 
//				" ELSE 'Not Engaged'    \r\n" + 
//				"END) AS ENGGMNT         \r\n" + 
//				"from    CII_FACT_CP_ENGGMNT  inner join DIM_ENGGMNT                \r\n" + 
//				" on CII_FACT_CP_ENGGMNT.MBR_ENGGMNT_ID = DIM_ENGGMNT.ENGGMNT_ID JOIN (   \r\n" + 
//				"SELECT    YEAR_CD_NM as TM_PRD_NM,\r\n" + 
//				"\r\n" + 
//				"STRT_MNTH_NBR as START_YEAR_MNTH,\r\n" + 
//				"\r\n" + 
//				"END_MNTH_NBR as END_YEAR_MNTH,\r\n" + 
//				"\r\n" + 
//				"case\r\n" + 
//				"\r\n" + 
//				"  when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"\r\n" + 
//				" and INCRD_PAID_CD = 'PAID' then 111101\r\n" + 
//				"\r\n" + 
//				"  when TM_PRD_TYPE_CD = 'Custom' then STRT_MNTH_NBR\r\n" + 
//				"\r\n" + 
//				"  else SRVC_STRT_MNTH_NBR\r\n" + 
//				"\r\n" + 
//				"end as STRT_SRVC_YEAR_MNTH,\r\n" + 
//				"\r\n" + 
//				"case\r\n" + 
//				"\r\n" + 
//				"  when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"\r\n" + 
//				"  and INCRD_PAID_CD = 'PAID' then 888812\r\n" + 
//				"\r\n" + 
//				"  when TM_PRD_TYPE_CD = 'Custom' then END_MNTH_NBR\r\n" + 
//				"\r\n" + 
//				"  else SRVC_END_MNTH_NBR\r\n" + 
//				"\r\n" + 
//				"end as END_SRVC_YEAR_MNTH,\r\n" + 
//				"\r\n" + 
//				"case\r\n" + 
//				"\r\n" + 
//				"  when TM_PRD_TYPE_CD <> 'Custom' then PAID_END_MNTH_NBR\r\n" + 
//				"\r\n" + 
//				"  when INCRD_PAID_CD = 'PAID' then END_MNTH_NBR\r\n" + 
//				"\r\n" + 
//				"  when INCRD_PAID_CD = 'INC2' then 888811\r\n" + 
//				"\r\n" + 
//				"  when INCRD_PAID_CD = 'INC3' then dm.YEAR_MNTH_NBR\r\n" + 
//				"\r\n" + 
//				"  when INCRD_PAID_CD = 'INC1' then PAID_END_MNTH_NBR\r\n" + 
//				"\r\n" + 
//				"end as END_RPTG_PAID_YEAR_MNTH\r\n" + 
//				"\r\n" + 
//				"from\r\n" + 
//				"\r\n" + 
//				"DIM_TM_PRD_ADHC dtp\r\n" + 
//				"\r\n" + 
//				"join DIM_MNTH dm \r\n" + 
//				" on 1=1\r\n" + 
//				"\r\n" + 
//				"where dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"\r\n" + 
//				"and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"\r\n" + 
//				"and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"\r\n" + 
//				"and dtp.YEAR_ID <= 3\r\n" + 
//				"\r\n" + 
//				"and dm.YEAR_MNTH_NBR = 202011\r\n" + 
//				"\r\n" + 
//				"and (\r\n" + 
//				"\r\n" + 
//				"(dtp.CSTM_STRT_CURNT_MNTH_NBR = 201912 \r\n" + 
//				" and dtp.CSTM_END_CURNT_MNTH_NBR = 202011)\r\n" + 
//				"\r\n" + 
//				"or (dtp.CSTM_STRT_PRIOR_MNTH_NBR = 201812 \r\n" + 
//				" and dtp.CSTM_END_PRIOR_MNTH_NBR = 201911)\r\n" + 
//				"\r\n" + 
//				"or (dtp.CSTM_STRT_PRIOR_2_MNTH_NBR = 201801 \r\n" + 
//				" and dtp.CSTM_END_PRIOR_2_MNTH_NBR = 201811) )   )  TM_PRD_FNCTN              \r\n" + 
//				" ON CII_FACT_CP_ENGGMNT.ENGGMNT_MNTH_NBR BETWEEN TM_PRD_FNCTN.START_YEAR_MNTH             \r\n" + 
//				" and TM_PRD_FNCTN. END_YEAR_MNTH    \r\n" + 
//				"WHERE    CII_FACT_CP_ENGGMNT.ACCT_ID IN ('W0016437')    \r\n" + 
//				"group by  CII_FACT_CP_ENGGMNT.acct_id, CII_FACT_CP_ENGGMNT.MCID ,\r\n" + 
//				"          TM_PRD_FNCTN.TM_PRD_NM ) a  inner join (   \r\n" + 
//				"select       \r\n" + 
//				"CASE               \r\n" + 
//				" WHEN DIM_ENGGMNT.TRDTNL_IND= 1 THEN cast('Traditional' as char(20))            \r\n" + 
//				" WHEN DIM_ENGGMNT.TRDTNL_IND = 0            \r\n" + 
//				" AND DIM_ENGGMNT.ENHNCD_IND= 1 THEN cast('Care Coordination' as char(20))            \r\n" + 
//				" WHEN DIM_ENGGMNT.TRDTNL_IND= 0             \r\n" + 
//				" AND DIM_ENGGMNT.ENHNCD_IND = 0            \r\n" + 
//				" AND DIM_ENGGMNT.EXPNDD_IND= 1 THEN  cast('Comprehensive' as char(20))            \r\n" + 
//				" ELSE cast('Not Engaged' as char(20))    \r\n" + 
//				"END    AS ENGGMNT ,   \r\n" + 
//				"CASE               \r\n" + 
//				" WHEN DIM_ENGGMNT.TRDTNL_IND= 1 THEN cast('Traditional' as char(20))            \r\n" + 
//				" WHEN DIM_ENGGMNT.TRDTNL_IND = 0            \r\n" + 
//				" AND DIM_ENGGMNT.ENHNCD_IND= 1 THEN cast('Care Coordination' as char(20))            \r\n" + 
//				" WHEN DIM_ENGGMNT.TRDTNL_IND= 0             \r\n" + 
//				" AND DIM_ENGGMNT.ENHNCD_IND = 0            \r\n" + 
//				" AND DIM_ENGGMNT.EXPNDD_IND= 1 THEN cast('Comprehensive' as char(20))            \r\n" + 
//				" ELSE cast('Not Engaged' as char(20))    \r\n" + 
//				"END    AS ENGGMNT_TIER                          \r\n" + 
//				"from    DIM_ENGGMNT                          \r\n" + 
//				"union    all                          \r\n" + 
//				"select       \r\n" + 
//				"CASE               \r\n" + 
//				" WHEN DIM_ENGGMNT.TRDTNL_IND= 1 THEN cast('Traditional' as char(20))            \r\n" + 
//				" else cast('Care Coordination' as char(20))    \r\n" + 
//				"end    as ENGGMNT , cast( 'Comprehensive' as char(20))  AS ENGGMNT_TIER    \r\n" + 
//				"from    DIM_ENGGMNT    \r\n" + 
//				"where    DIM_ENGGMNT.TRDTNL_IND= 1            \r\n" + 
//				" or DIM_ENGGMNT.ENHNCD_IND= 1                          \r\n" + 
//				"union    all                          \r\n" + 
//				"select    cast('Traditional' as char(20)) as ENGGMNT, cast('Care Coordination' as char(20))  AS ENGGMNT_TIER    \r\n" + 
//				"from    DIM_ENGGMNT    \r\n" + 
//				"where    DIM_ENGGMNT.TRDTNL_IND= 1 ) b            \r\n" + 
//				" on a.ENGGMNT = b.ENGGMNT                   )  TMBRENGGMNT             \r\n" + 
//				" ON CII_FACT_GAP_SVNGS.ACCT_ID = TMBRENGGMNT.ACCT_ID             \r\n" + 
//				" and CII_FACT_GAP_SVNGS.MCID=TMBRENGGMNT.MCID             \r\n" + 
//				" and TM_PRD_FNCTN.TM_PRD_NM =  TMBRENGGMNT.TM_PRD_NM INNER JOIN (Select ACCT_ID,SGMNTN_DIM_KEY, SGMNTN_NM, SRC_FLTR_ID from ACIISST_SGMNTN_BRDG where ACCT_ID in ('W0016437') and SRC_FLTR_ID in ('d97ef60a-2503-41d2-acac-2c91c904e9e0'))SGMNTN on CII_FACT_GAP_SVNGS.SGMNTN_DIM_KEY = SGMNTN.SGMNTN_DIM_KEY and CII_FACT_GAP_SVNGS.ACCT_ID=SGMNTN.ACCT_ID WHERE CII_FACT_GAP_SVNGS.ACCT_ID in ('W0016437')   GROUP BY CII_ACCT_PRFL.ACCT_NM,coalesce( TMBRENGGMNT2.MBR_ENGGD_IND,'N'),coalesce(TMBRENGGMNT.ENGGMNT_TIER, 'Not Engaged'),TM_PRD_FNCTN.TM_PRD_NM,INCNTV_MBRSHP.EARND_INCNTV_CD ) AS GAPSVNGS  GROUP BY GAPSVNGS.ACCT_NM,GAPSVNGS.MBR_ENGGD_IND,GAPSVNGS.MBR_ENGGMNT_TIER,GAPSVNGS.TM_PRD_NM,GAPSVNGS.EARND_INCNTV_CD ) AS FNL   ORDER BY Account_Name,Gaps_Closed,Member_Engaged_Indicator,Member_Engagement_Tier,Time_Period,Earned_Incentive_Indicator\r\n" + 
//				"";
//		
//		String query24 = "Select a,b,c from d where (age > 20 and age < 40) or (age > 40 and age < 70)";
//		
//		String query25 = "Select  count(distinct a), c from b";
//		String query26 = "Select  distinct a, max(distinct c) from b";
//		
//		String query27 = "select cast('Traditional' as char(20)) as abc from table123";
//		String query28 = "select '123' as abc from table123";
//		
//		String query29 = "select substr(cast(TM_PRD_FNCTN.START_YEAR_MNTH as varchar(6)) ,5,2) from abc";
//		
//		String query32 = "select stupid(count(*)) from mango";
//		
//		String query33 = "select count(*) from ( SELECT AS \"Account ID\",AS \"Account Name\",AS \"Time Period\",AS \"Member Coverage Type Description\",AS \"Eligibility Year Month Ending Number\",AS \"Member Coverage Count\",CASE WHEN sum(CAST(AS decimal (22,6))) over(partition by )=0 then 0.00 else SUM(CAST(AS decimal (22,6))) over(partition by )/ sum(CAST(AS decimal (22,6))) over(partition by ) *100 end AS \"Member Coverage Count - Total %: Member Coverage Type Description,Reporting Member Relationship Description,Eligibility Year Month Ending Number,,Eligibility Calendar Month Year  Name\",AS \"Reporting Member Relationship Description\",AS \"Eligibility Calendar Month Year  Name\", AS \"Contract Type Description\"  FROM  (  SELECT MBRSHP.ACCT_ID AS MBRSHP.ACCT_NM AS MBRSHP.TM_PRD_NM AS MBRSHP.MBR_CVRG_TYPE_DESC AS MBRSHP.ELGBLTY_CY_MNTH_END_NBR AS SUM(MBRSHP.SUM_MBR_CVRG_CNT) AS MBRSHP.RPTG_MBR_RLTNSHP_DESC AS MBRSHP.CLNDR_MNTH_YEAR_NM AS MBRSHP.CNTRCT_TYPE_DESC AS   FROM  (  SELECT CII_ACCT_PRFL.ACCT_ID AS ACCT_ID,CII_ACCT_PRFL.ACCT_NM AS ACCT_NM,TM_PRD_FNCTN.TM_PRD_NM AS TM_PRD_NM,DIM_MBR_CVRG_TYPE.MBR_CVRG_TYPE_DESC AS MBR_CVRG_TYPE_DESC,CII_FACT_MBRSHP.ELGBLTY_CY_MNTH_END_NBR AS ELGBLTY_CY_MNTH_END_NBR,SUM(MBR_CVRG_CNT) AS SUM_MBR_CVRG_CNT,DIM_RPTG_MBR_RLTNSHP.RPTG_MBR_RLTNSHP_DESC AS RPTG_MBR_RLTNSHP_DESC,DIM_MNTH.CLNDR_MNTH_YEAR_NM AS CLNDR_MNTH_YEAR_NM,DIM_CNTRCT_TYPE.CNTRCT_TYPE_DESC AS CNTRCT_TYPE_DESC  FROM CII_FACT_MBRSHP INNER JOIN (SELECT YEAR_CD_NM as TM_PRD_NM,\r\n" + 
//				"	STRT_MNTH_NBR as START_YEAR_MNTH,\r\n" + 
//				"	END_MNTH_NBR as END_YEAR_MNTH,\r\n" + 
//				"	case\r\n" + 
//				"		when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"		and INCRD_PAID_CD = 'PAID' then 111101\r\n" + 
//				"		when TM_PRD_TYPE_CD = 'Custom' then STRT_MNTH_NBR\r\n" + 
//				"		else SRVC_STRT_MNTH_NBR\r\n" + 
//				"	end as STRT_SRVC_YEAR_MNTH,\r\n" + 
//				"	case\r\n" + 
//				"		when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"		and INCRD_PAID_CD = 'PAID' then 888812\r\n" + 
//				"		when TM_PRD_TYPE_CD = 'Custom' then END_MNTH_NBR\r\n" + 
//				"		else SRVC_END_MNTH_NBR\r\n" + 
//				"	end as END_SRVC_YEAR_MNTH,\r\n" + 
//				"	case\r\n" + 
//				"		when TM_PRD_TYPE_CD <> 'Custom' then PAID_END_MNTH_NBR\r\n" + 
//				"		when INCRD_PAID_CD = 'PAID' then END_MNTH_NBR\r\n" + 
//				"		when INCRD_PAID_CD = 'INC2' then 888811\r\n" + 
//				"		when INCRD_PAID_CD = 'INC3' then dm.YEAR_MNTH_NBR\r\n" + 
//				"		when INCRD_PAID_CD = 'INC1' then PAID_END_MNTH_NBR\r\n" + 
//				"	end as END_RPTG_PAID_YEAR_MNTH\r\n" + 
//				"from\r\n" + 
//				"	DIM_TM_PRD_ADHC dtp\r\n" + 
//				"join DIM_MNTH dm on 1=1\r\n" + 
//				"where	dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"and dtp.YEAR_ID <= 3\r\n" + 
//				"and dm.YEAR_MNTH_NBR = 202102\r\n" + 
//				"and (\r\n" + 
//				"(dtp.CSTM_STRT_CURNT_MNTH_NBR = 202003 and dtp.CSTM_END_CURNT_MNTH_NBR = 202102)\r\n" + 
//				"or (dtp.CSTM_STRT_PRIOR_MNTH_NBR = 201903 and dtp.CSTM_END_PRIOR_MNTH_NBR = 202002)\r\n" + 
//				"or (dtp.CSTM_STRT_PRIOR_2_MNTH_NBR = 201803 and dtp.CSTM_END_PRIOR_2_MNTH_NBR = 201902) ) ) TM_PRD_FNCTN ON CII_FACT_MBRSHP.ELGBLTY_CY_MNTH_END_NBR BETWEEN TM_PRD_FNCTN.START_YEAR_MNTH and TM_PRD_FNCTN.END_YEAR_MNTH INNER JOIN DIM_MBR_CVRG_TYPE ON CII_FACT_MBRSHP.MBR_CVRG_TYPE_CD = DIM_MBR_CVRG_TYPE.MBR_CVRG_TYPE_CD INNER JOIN DIM_RPTG_MBR_RLTNSHP ON CII_FACT_MBRSHP.RPTG_MBR_RLTNSHP_CD = DIM_RPTG_MBR_RLTNSHP.RPTG_MBR_RLTNSHP_CD INNER JOIN DIM_MNTH ON CII_FACT_MBRSHP.ELGBLTY_CY_MNTH_END_NBR = DIM_MNTH.YEAR_MNTH_NBR INNER JOIN DIM_CNTRCT_TYPE ON CII_FACT_MBRSHP.CNTRCT_TYPE_CD = DIM_CNTRCT_TYPE.CNTRCT_TYPE_CD INNER JOIN CII_ACCT_PRFL ON CII_FACT_MBRSHP.ACCT_ID = CII_ACCT_PRFL.ACCT_ID INNER JOIN (Select ACCT_ID,SGMNTN_DIM_KEY, SGMNTN_NM, SRC_FLTR_ID from ACIISST_SGMNTN_BRDG where ACCT_ID in ('W0003618') and SRC_FLTR_ID in ('195d1b33-4e3c-4ebc-b51f-acfb59d288a1'))SGMNTN on CII_FACT_MBRSHP.SGMNTN_DIM_KEY = SGMNTN.SGMNTN_DIM_KEY and CII_FACT_MBRSHP.ACCT_ID=SGMNTN.ACCT_ID WHERE CII_FACT_MBRSHP.ACCT_ID in ('W0003618')   GROUP BY CII_ACCT_PRFL.ACCT_ID,CII_ACCT_PRFL.ACCT_NM,TM_PRD_FNCTN.TM_PRD_NM,DIM_MBR_CVRG_TYPE.MBR_CVRG_TYPE_DESC,CII_FACT_MBRSHP.ELGBLTY_CY_MNTH_END_NBR,DIM_RPTG_MBR_RLTNSHP.RPTG_MBR_RLTNSHP_DESC,DIM_MNTH.CLNDR_MNTH_YEAR_NM,DIM_CNTRCT_TYPE.CNTRCT_TYPE_DESC ) AS MBRSHP  GROUP BY MBRSHP.ACCT_ID,MBRSHP.ACCT_NM,MBRSHP.TM_PRD_NM,MBRSHP.MBR_CVRG_TYPE_DESC,MBRSHP.ELGBLTY_CY_MNTH_END_NBR,MBRSHP.RPTG_MBR_RLTNSHP_DESC,MBRSHP.CLNDR_MNTH_YEAR_NM,MBRSHP.CNTRCT_TYPE_DESC ) AS FNL    ) t";
//		
//		String query34 = "SELECT actor_name, title, gender " + 
//				" FROM actor" + 
//				" WHERE gender > 'm' and title in (SELECT title" + 
//				"                FROM mv" + 
//				"                WHERE director = 'Steven Spielberg' AND revenue_domestic > budget)"
//				+ "and actor_name in ('brad', 'chad', 'mad')" ;
//		
//		test.parameterize = true;
//		//test.processCase = true;
//		//Map<String, List<GenExpression>> tableColumns = test.getTableColumns(query4);
//		//test.printRealColumns(tableColumns);
//		//test.printOutput(wrapper.root);
//
//		
//		GenExpressionWrapper wrapper = test.processQuery(query34);
//		test.generateQuery(wrapper.root);
//		GenExpression root = wrapper.root;
//		
//		// now that the sql is parameterized
//		// every <> block can then be used to understand what are the values inside of it
//		// the main index is primarily paramStringToParamStruct
//		// which then tags into gen expression
//		// you can set any of this and then invoke fill parameters which will fill the query and send result back as a query
//		String paramName = "actor_genderand0_left>";
//		Object paramValue = wrapper.getCurrentValueOfParam(paramName);
//
//		String defaultQuery = wrapper.getQueryForParam(paramName);		
//		System.err.println(paramName + "<>" + paramValue);
//		System.err.println("Query for param " + defaultQuery);
//		wrapper.setCurrentValueOfParam(paramName, "'monkeshwaran'");
//		wrapper.fillParameters();
//		test.generateQuery(wrapper.root);
//		
//		
//		List <ParamStructDetails> plist = new Vector<ParamStructDetails>();
//		ParamStructDetails pStruct = new ParamStructDetails();
//		pStruct.setColumnName("MovieBudget");
//		pStruct.setTableName("Movie2");
//		pStruct.setuOperator("or1_left>");
//		pStruct = new ParamStructDetails();
//		pStruct.setColumnName("RevenueDomestic");
//		pStruct.setTableName("Movie2");
//		pStruct.setuOperator("in");
//		plist.add(pStruct);
//		String finalQuery = GenExpressionWrapper.transformQueryWithParams(query20, plist);
//		System.err.println(" transformed query ..  " + finalQuery);
//		
//		// YEAR_ID
//		wrapper.replaceColumn("gender", "'fractor'");
//		//wrapper.replaceColumn("YER_ID", "123456");
//
//		wrapper.fillParameters();
//		test.generateQuery(wrapper.root);
//
//		// works with Query 2
//		//Object [] output = root.printLineage(root, "Member Engagement Tier", null ,null, null, 0);
//		
//		// with alias
//		//Object [] output = root.printLineage(root, "A1", null ,null, null, 0);
//		
//		// without alias
//		//Object [] output = root.printLineage(root, "B1", null ,null, null, 0);
//
//		//Object [] output = root.printLineage(root, "B1", null ,null, null, 0);
//
//		System.out.println("Physical Name for B3 >> " + wrapper.getPhysicalColumnName(root, "B3"));
//		System.out.println("Physical Name for A3 >> " + wrapper.getPhysicalColumnName(root, "A3"));
//		
//		System.out.println("Detok List >> " + wrapper.getColumnsForFunction("detok"));
//		
//		wrapper.neutralizeSelector(root, "A3", true);
//		System.err.println("After removing it");
//		test.generateQuery(wrapper.root);
//		
//		wrapper.neutralizeSelector(root, "A3", false);
//		System.err.println("After adding it");
//		test.generateQuery(wrapper.root);
//		
//
//		wrapper.neutralizeFunction(root, "detok", true);
//		System.err.println("After removing Function");
//		test.generateQuery(wrapper.root);
//
//		wrapper.neutralizeFunction(root, "detok", false);
//		System.err.println("After Adding Function");
//		test.generateQuery(wrapper.root);
//		
//		System.out.println("Adding detok to existing function");
//		wrapper.addFunctionToSelector(root, "A2", "detok");
//		test.generateQuery(wrapper.root);
//
//		// get the param map and modify the value
//		// =CII_FACT_MBRSHP.ACCT_ID
//		/*
//		ParamStruct replaceStruct = test.operatorTableColumnParamIndex.get("=CII_FACT_MBRSHP.ACCT_ID");
//		replaceStruct.setCurrentValue("DA_REPLACEMENTS");
//		List <ParamStruct> paramList = new Vector<ParamStruct>();
//		paramList.add(replaceStruct);
//		*/
//		wrapper.replaceColumn("A2", "'Helloworld'");
//		//wrapper.replaceColumn("A3", "'Helloworld'");
//		wrapper.replaceTableColumnOperator("A.A3or1.right=", "'Mango boy'");
//		wrapper.replaceColumn("YEAR_ID", "123");
//		//wrapper.replaceColumn("YER_ID", "123456");
//
//		wrapper.fillParameters();
//		test.generateQuery(wrapper.root);
//
////		String mkquery = "select abc from table xyz";
////		GenExpressionWrapper wrapper = test.processQuery(mkquery);
////		System.out.println(GenExpression.printQS(wrapper.root, null) + "");
//		
//		/*
//		Map <String, GenExpression> filterMap = new HashMap<String, GenExpression>();
//		
//		// remove the groupby on cii_fact_mbrshp.acct_id
//		List <String> columnsToRemove = new Vector<String>();
//		columnsToRemove.add("cii_fact_mbrshp.ELGBLTY_CY_MNTH_END_NBR");
//
//		// parameterize something else
//		GenExpression ge = new GenExpression();
//		ge.setOperation("Opaque");
//		ge.setLeftExpr("  Account = 'PK Imagined'  ");
//		//ACIISST_PERIOD_KEY
//		//filterMap.put("CII_FACT_CLM_LINE", ge);
//		filterMap.put("CII_FACT_MBRSHP.ACCT_ID", ge);
//		
//		//test.addRowFilter(filterMap);
//		test.appendParameter(columnsToRemove, filterMap);
//		test.printOutput(qs);
//		*/
//	}
}


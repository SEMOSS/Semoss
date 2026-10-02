/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.query.parsers;

import java.sql.Time;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.GroupByElement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.ParenthesedFromItem;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SetOperation;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.WithItem;
import prerna.query.querystruct.FunctionExpression;
import prerna.query.querystruct.GenExpression;
import prerna.query.querystruct.InGenExpression;
import prerna.query.querystruct.OperationExpression;
import prerna.query.querystruct.OrderByExpression;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.WhenExpression;
import prerna.query.querystruct.filters.IQueryFilter;

/**
 * Walks a JSQLParser AST and rebuilds it as a tree of {@link GenExpression}
 * nodes, which can be printed back out as SQL, inspected for the tables and
 * columns a query touches, or rewritten to swap constants for named parameters.
 * <p>
 * The traversal mirrors the shape of the JSQLParser AST:
 * 
 * <pre>
 * Select
 *   PlainSelect        a single SELECT: select items, FROM, joins, WHERE, GROUP BY, ORDER BY, LIMIT
 *   SetOperationList   a UNION / INTERSECT / EXCEPT chain of nested Selects
 *   Values             a literal VALUES list
 *   WithItem           a CTE, each wrapping a ParenthesedSelect
 *
 * FromItem
 *   Table              a plain table reference, with or without an alias
 *   ParenthesedSelect  a parenthesized subquery, recursing back into Select
 *   ParenthesedFromItem  parenthesized joins, recursing back into FromItem
 *   TableFunction
 *
 * Join
 *   getRightItem()     a FromItem, recursing
 *   getOnExpressions() the ON predicates, recursing into Expression
 *
 * Expression           conditionals, arithmetic, functions, CASE, IN, BETWEEN,
 *                      literals, columns, and nested ParenthesedSelects
 * </pre>
 *
 * Each node is recorded on a shared {@link GenExpressionWrapper}, which
 * accumulates the table and column aliases, the per-table and per-select column
 * usage, and the parameters discovered along the way.
 * <p>
 * TODO:
 * <ol>
 * <li>resolve every selector back to a real column and table</li>
 * <li>support CREATE statements</li>
 * <li>support substituting one column for another</li>
 * </ol>
 */
public class SqlParser {

	GenExpression qs = null;
	GenExpressionWrapper wrapper = new GenExpressionWrapper();
	boolean binary = false;
	boolean column = false;
	public boolean parameterize = true;
	String columnName = null;

	boolean processCase = false;
	boolean processAllBinary = false;
	Stack<Boolean> processParam = new Stack<Boolean>();
	private static final Logger classLogger = LogManager.getLogger(SqlParser.class);

	public SqlParser() {
		this.wrapper.tableAlias = new HashMap<String, String>();
		this.wrapper.columnAlias = new HashMap<String, String>();
		this.wrapper.schema = new HashMap<String, Set<String>>();
	}

	/**
	 * Parse a SELECT and return the wrapper holding the resulting expression tree
	 * along with the aliases, column usage, and parameters found in it.
	 *
	 * @param query the SQL to parse
	 * @return the populated wrapper, whose root is the top level expression
	 * @throws Exception if the SQL cannot be parsed
	 */
	public GenExpressionWrapper processQuery(String query) throws Exception {

		wrapper = new GenExpressionWrapper();
		// the WITH clauses are collected separately and hung off the root
		List<GenExpression> withqs = new ArrayList<>();
		List<String> withAlias = new ArrayList<>();
		// this is the main query struct
		GenExpression qs = new GenExpression();
		// parse the sql
		Statement stmt = CCJSqlParserUtil.parse(query);
		Select select = ((Select) stmt);

		if (select instanceof PlainSelect) {
			PlainSelect sb = (PlainSelect) select;
			List<WithItem<?>> withItemList = select.getWithItemsList();
			if (withItemList != null) {
				for (WithItem<?> wi : withItemList) {
					ParenthesedSelect wparen = wi.getSelect();
					Select wbody = wparen == null ? null : wparen.getSelect();
					String asName = wi.getAliasName();

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
		if (select instanceof SetOperationList) {
			qs = processOperation((SetOperationList) select);
		}

		this.qs = qs;
		wrapper.root = qs;

		return wrapper;
	}

	/**
	 * Print an expression tree back out as SQL.
	 *
	 * @param qs the root expression, which must be a {@link GenExpression}
	 * @return the rendered SQL
	 * @throws Exception if the tree cannot be rendered
	 */
	public String generateQuery(SelectQueryStruct qs) throws Exception {
		String finalQuery = GenExpression.printQS((GenExpression) qs, new StringBuffer()).toString();
		classLogger.debug("Generated query {}", finalQuery);
		return finalQuery;
	}

	/**
	 * Convert one PlainSelect into an expression tree, recursing through its FROM
	 * item, joins, filters, groupings, ordering, and limit.
	 *
	 * @param qs the parent expression, or null for the outermost select
	 * @param sb the select to convert
	 * @return the expression built for this select
	 */
	public GenExpression processSelect(GenExpression qs, PlainSelect sb) {
		wrapper.numSubSelects++; // if it is the first time this will equal to zero

		GenExpression thisQs = null;
		if (qs == null) {
			qs = new GenExpression();
			thisQs = qs;
		}
		// the parent is reused for alias and hash bookkeeping even when it was just
		// created above, so this always builds a fresh expression for the select
		{
			thisQs = new GenExpression();
			thisQs.parent = qs; // set the parent here
			thisQs.aQuery = sb.toString();
			thisQs.operation = "select";
			thisQs.aliasHash = qs.aliasHash;
			thisQs.randomHash = qs.randomHash;
		}

		// DISTINCT ON is not modelled, only whether the select is distinct at all
		Distinct dis = sb.getDistinct();
		if (dis != null) {
			thisQs.distinct = true;
		}
		FromItem fi = sb.getFromItem();

		String alias = "";
		if (fi.getAlias() != null) {
			alias = fi.getAlias().getName();
		}
		List<SelectItem<?>> items = sb.getSelectItems();

		{
			if (fi instanceof Table) {
				Table fromTable = (Table) sb.getFromItem();
				String fromTableName = fromTable.getName();
				String fromTableAlias = null;
				Alias fromTableAliasObj = fromTable.getAlias();
				if (fromTableAliasObj != null) {
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
				List<GenExpression> selectList = null;
				if (this.wrapper.tableSelect.containsKey(fromTableName)) {
					selectList = this.wrapper.tableSelect.get(fromTableName);
				} else {
					selectList = new ArrayList<GenExpression>();
				}
				if (!selectList.contains(thisQs)) {
					selectList.add(thisQs);
				}

				this.wrapper.tableSelect.put(fromTableName, selectList);
			} else if (fi instanceof PlainSelect) {
				thisQs.currentTable = fi.getAlias().getName();
			} else if (fi instanceof ParenthesedSelect) {
				thisQs.currentTable = fi.getAlias().getName();
			} else if (fi instanceof ParenthesedFromItem) {
				thisQs.currentTable = fi.getAlias().getName();
			}

			// the joins come first because they register the table aliases that the
			// selectors, filters, groupings, and ordering below resolve against
			fillJoins(thisQs, sb.getJoins());
			fillSelects(thisQs, items);
			fillFilters(thisQs, null, sb.getWhere());
			fillGroups(thisQs, sb.getGroupBy());
			fillOrder(thisQs, sb.getOrderByElements());
			fillLimitOffset(thisQs, sb.getLimit());
		}

		// a subquery in the FROM is captured as its own expression and registered
		// under the alias it was given
		GenExpression substruct = processSelectFromItem(fi, thisQs);
		if (substruct != null) {
			substruct.setLeftAlias(alias);
			thisQs.setComposite(true);
			thisQs.aliasHash.put(alias, substruct);
			thisQs.from = substruct;
		}
		return thisQs;
	}

	/**
	 * Convert the FROM item of a select into an expression. A plain table becomes a
	 * leaf "from" node; a subquery or set operation recurses back into the select
	 * handling.
	 *
	 * @param fi     the FROM item to convert
	 * @param thisQs the expression for the select that owns this FROM item
	 * @return the expression for the FROM item, or null if the item is a shape this
	 *         parser does not model
	 */
	public GenExpression processSelectFromItem(FromItem fi, GenExpression thisQs) {
		if (fi instanceof ParenthesedSelect) {
			Select sbody = ((ParenthesedSelect) fi).getSelect();
			if (sbody instanceof PlainSelect) {
				GenExpression substruct = processSelect(thisQs, (PlainSelect) sbody);
				substruct.setComposite(true);
				return substruct;
			} else if (sbody instanceof SetOperationList) {
				return processOperation((SetOperationList) sbody);
			}
		} else if (fi instanceof SetOperationList) {
			return processOperation((SetOperationList) fi);
		} else if (fi instanceof Table) {
			String fromTableName = "";
			Table fromTable = (Table) fi;
			fromTableName = fromTable.getName();
			String fromTableAlias = null;
			Alias tableAlias = fromTable.getAlias();
			if (tableAlias != null) {
				fromTableAlias = tableAlias.getName();
			}
			thisQs.currentTable = fromTableName;
			thisQs.currentTableAlias = fromTableAlias;
			GenExpression fromExpr = new GenExpression();
			fromExpr.setOperation("from");
			fromExpr.setComposite(false);
			fromExpr.aQuery = fi.toString();
			fromExpr.setLeftExpr(fromTableName);
			thisQs.from = fromExpr;

			// tracking tables
			List<GenExpression> selectList = null;
			if (this.wrapper.tableSelect.containsKey(fromTableName)) {
				selectList = this.wrapper.tableSelect.get(fromTableName);
			} else {
				selectList = new ArrayList<GenExpression>();
			}
			if (!selectList.contains(thisQs)) {
				selectList.add(thisQs);
			}

			this.wrapper.tableSelect.put(fromTableName, selectList);
			return fromExpr;
		}

		else if (fi instanceof ParenthesedFromItem) {
			GenExpression gep = processSelectFromItem(((ParenthesedFromItem) fi).getFromItem(), thisQs);
			gep.setComposite(true);
			gep.paranthesis = true;
			return gep;
		}
		return null;

	}

	/**
	 * Add the selectors into the query struct. A select item whose alias shadows a
	 * column already recorded on the wrapper replaces that entry, so the alias does
	 * not get reported as a column of its own.
	 *
	 * @param qs      the expression for the select
	 * @param selects the select items to convert
	 */
	public void fillSelects(SelectQueryStruct qs, List<SelectItem<?>> selects) {
		for (int selectIndex = 0; selectIndex < selects.size(); selectIndex++) {
			SelectItem<?> si = selects.get(selectIndex);
			Expression expr = si.getExpression();
			// AllTableColumns extends AllColumns, so both t.* and * land on this check
			if (expr instanceof AllColumns) {
				GenExpression gep = new GenExpression();
				gep.aQuery = si.toString();
				gep.setLeftExpr(si.toString());
				gep.setOperation("opaque");
				qs.nselectors.add(gep);
			} else {
				Alias seiAlias = si.getAlias();
				GenExpression gep = processExpression(qs, expr, null);

				if (seiAlias != null) {
					gep.setLeftAlias(seiAlias.getName());
				}
				qs.nselectors.add(gep);

				// process for column cleanup
				if (seiAlias != null && this.wrapper.columnSelect.containsKey(seiAlias.getName())) {
					// remove it / add it as the actual one
					// it is already accommodated for some other place
					this.wrapper.columnSelect.remove(seiAlias.getName());
				}
			}
		}
	}

	/**
	 * Convert each join on a select into an expression and hang it off the select.
	 *
	 * @param qs    the expression for the select that owns these joins
	 * @param joins the joins to convert, may be null or empty
	 */
	public void fillJoins(GenExpression qs, List<Join> joins) {
		if (joins == null || joins.isEmpty()) {
			return;
		}

		for (int joinIndex = 0; joinIndex < joins.size(); joinIndex++) {
			Join thisJoin = joins.get(joinIndex);
			qs.joins.add(processJoinExpression(qs, null, thisJoin));
		}
	}

	/**
	 * Build the expression for a single join. The ON predicate becomes the body of
	 * the join node, the join keyword becomes its "on" text, and the right hand
	 * FROM item becomes its "from", recursing for nested joins and subqueries.
	 *
	 * @param qs       the expression for the select that owns the join
	 * @param expr     the enclosing expression to attach the ON predicate under, if
	 *                 any
	 * @param thisJoin the join to convert
	 * @return the expression for the join
	 */
	// TODO - process USING columns
	public GenExpression processJoinExpression(GenExpression qs, GenExpression expr, Join thisJoin) {
		GenExpression gep = new GenExpression();
		// a join can carry several ON expressions, and cross joins carry none at all
		Collection<Expression> onExpressions = thisJoin.getOnExpressions();
		Expression onExpression = onExpressions.isEmpty() ? null : onExpressions.iterator().next();
		GenExpression retExpr = processExpression(qs, onExpression, expr);
		gep.telescope = true;
		gep.body = retExpr;
		gep.aQuery = thisJoin.toString();
		gep.parent = qs;

		FromItem fi = thisJoin.getRightItem();
		GenExpression from2 = new GenExpression();

		String joinType = null;
		if (thisJoin.isInner()) {
			joinType = "inner join";
		} else if (thisJoin.isLeft()) {
			joinType = "left outer join";
		} else if (thisJoin.isRight()) {
			joinType = "right outer join";
		} else if (thisJoin.isOuter()) {
			joinType = "outer join";
		} else if (thisJoin.isFull()) {
			joinType = "full join";
		} else if (thisJoin.isCross()) {
			joinType = "cross join";
		}
		if (joinType == null) {
			joinType = "JOIN";
		}
		gep.setOperation("join");
		gep.setOn(joinType);

		from2 = processJoinFromItem(fi, thisJoin, gep);
		from2.parent = gep;
		gep.from = from2;

		return gep;
	}

	/**
	 * Convert the right hand side of a join into an expression, recursing for
	 * subqueries, set operations, and parenthesized joins.
	 *
	 * @param fi       the right hand FROM item of the join
	 * @param thisJoin the join being processed
	 * @param gep      the expression for the join, used as the parent
	 * @return the expression for the FROM item, empty if the item is a shape this
	 *         parser does not model
	 */
	private GenExpression processJoinFromItem(FromItem fi, Join thisJoin, GenExpression gep) {
		String rightTableName, rightTableAlias = null;
		GenExpression from2 = new GenExpression();
		if (fi instanceof Table) {
			Table rightTable = (Table) fi;
			rightTableName = rightTable.getName();
			rightTableAlias = rightTableName;
			if (rightTable.getAlias() != null) {
				rightTableAlias = rightTable.getAlias().getName();
			}
			// register the alias so a column qualified by it resolves to the real table,
			// the same way the FROM table's alias does
			this.wrapper.tableAlias.put(rightTableAlias, rightTableName);

			// turn this into a full from
			from2.setOperation("table");
			from2.setLeftExpr(rightTableName);
			from2.setLeftAlias(rightTableAlias);
			from2.parent = gep;
			return from2;
		} else if (fi instanceof ParenthesedSelect) {
			rightTableName = fi.getAlias().getName();
			rightTableAlias = rightTableName;
			String alias = fi.getAlias().getName();

			Select sbody = ((ParenthesedSelect) fi).getSelect();
			if (sbody instanceof PlainSelect) {
				from2 = processSelect(qs, (PlainSelect) sbody);
				from2.operation = "querystruct";
				from2.telescope = true;
				from2.setComposite(true);
				from2.setLeftAlias(alias);
			}
			// union if it is a set operations list
			else if (sbody instanceof SetOperationList) {
				from2 = processOperation((SetOperationList) sbody);
				from2.telescope = true;
				from2.setComposite(true);
				from2.setLeftAlias(alias);
			}
			return from2;
		} else if (fi instanceof ParenthesedFromItem) {
			FromItem innerFromItem = ((ParenthesedFromItem) fi).getFromItem();
			from2 = processJoinFromItem(innerFromItem, thisJoin, gep);
			from2.paranthesis = true;
			from2.setComposite(true);
			return from2;
		}
		return from2;
	}

	/**
	 * Convert a single SQL expression into an expression node, recursing into its
	 * operands. Constants that sit opposite a column are also registered as
	 * parameters on the wrapper, and replaced with a parameter placeholder when
	 * {@link #parameterize} is set.
	 *
	 * @param qs       the expression for the select the expression belongs to
	 * @param joinExpr the SQL expression to convert, may be null
	 * @param expr     the enclosing expression, may be null at the top of a tree
	 * @return the expression node, or the enclosing expression when the input is a
	 *         shape with nothing to add
	 */
	public GenExpression processExpression(SelectQueryStruct qs, Expression joinExpr, GenExpression expr) {
		// these are either composite relations like and or etc. or simple relations
		if (joinExpr instanceof AndExpression) {
			GenExpression expr2 = new GenExpression();
			AndExpression aExpr = (AndExpression) joinExpr;
			expr2.setOperation(aExpr.getStringExpression());
			expr2.aQuery = joinExpr.toString();

			// this is composite
			expr2.setComposite(true);

			Expression left = aExpr.getLeftExpression();
			Expression right = aExpr.getRightExpression();
			expr2.recursive = true;

			// the operator stack lets the operands below name their parameters after the
			// branch of the tree they sit on
			wrapper.currentOperator.push("and");
			wrapper.andCount++;
			wrapper.procOrder.put("and" + wrapper.andCount, true);
			wrapper.contextExpression.push(joinExpr.toString());

			// process the left and right
			GenExpression leftExpr = processExpression(qs, left, expr);
			GenExpression rightExpr = processExpression(qs, right, expr);

			expr2.setLeftExpresion(leftExpr);
			expr2.setRightExpresion(rightExpr);

			expr2.parent = (GenExpression) qs;

			wrapper.currentOperator.pop();
			wrapper.contextExpression.pop();
			wrapper.andCount--;
			return expr2;
		} else if (joinExpr instanceof OrExpression) {
			GenExpression expr2 = new GenExpression();
			OrExpression aExpr = (OrExpression) joinExpr;
			expr2.setOperation(aExpr.getStringExpression());
			expr2.aQuery = joinExpr.toString();

			// this is composite
			expr2.setComposite(true);

			Expression left = aExpr.getLeftExpression();
			Expression right = aExpr.getRightExpression();
			expr2.recursive = true;

			wrapper.currentOperator.push("or");
			wrapper.orCount++;
			wrapper.procOrder.put("or" + wrapper.andCount, true);
			wrapper.contextExpression.push(joinExpr.toString());

			// process the left and right
			GenExpression leftExpr = processExpression(qs, left, expr);
			GenExpression rightExpr = processExpression(qs, right, expr);

			expr2.setLeftExpresion(leftExpr);
			expr2.setRightExpresion(rightExpr);

			expr2.parent = (GenExpression) qs;

			wrapper.currentOperator.pop();
			wrapper.contextExpression.pop();
			wrapper.orCount--;
			return expr2;
		}
		// only the binary expression has 2 sides
		else if (joinExpr instanceof BinaryExpression && (processAllBinary
				|| IQueryFilter.comparatorIsValidSQL(((BinaryExpression) joinExpr).getStringExpression()))) {
			boolean paramBinary = true;

			if (processParam.size() > 0) {
				// get the latest and push it back
				paramBinary = processParam.pop();
				processParam.push(paramBinary);
			}

			this.binary = true;
			// this is another fractal that needs to be taken care of
			// so it is like and / or and then below that you have the equal expression etc.
			GenExpression eqExpr = new GenExpression();
			eqExpr.setComposite(true);
			eqExpr.aQuery = joinExpr.toString();
			BinaryExpression joinExpr2 = (BinaryExpression) joinExpr;
			eqExpr.setOperation(joinExpr2.getStringExpression());

			// e.g. YEAR_ID (left expression) = 123 (right expression)
			String operator = eqExpr.aQuery;
			String modifier = wrapper.uniqueCounter + "";

			// name the parameter after the enclosing AND / OR and the side of it this
			// comparison sits on, so the two halves of a range get distinct names
			if (wrapper.currentOperator.size() > 0) {
				operator = wrapper.currentOperator.pop();
				Boolean left = false;
				int count = 0;
				if (operator.equalsIgnoreCase("and")) {
					count = wrapper.andCount;
				} else {
					count = wrapper.orCount;
				}
				count = wrapper.uniqueCounter;
				left = wrapper.procOrder.get(operator + count);
				if (left == null || left) {
					modifier = operator + count + "_left";
					wrapper.procOrder.put(operator + count, false);
				} else {
					modifier = operator + count + "_right";
				}
				wrapper.currentOperator.push(operator);
			}

			String full_from = null;
			String full_To = null;

			GenExpression sqs = processExpression(qs, joinExpr2.getLeftExpression(), eqExpr);
			GenExpression sqs2 = processExpression(qs, joinExpr2.getRightExpression(), eqExpr);

			Object constantValue = null;
			String constantType = null;
			GenExpression exprToTrack = null;
			String tableName = null;
			String aliasName = null;

			// skipped inside a CASE, where the branches are rendered verbatim
			if (paramBinary) {
				if (sqs.getOperation().equalsIgnoreCase("column")) {
					full_from = sqs.getLeftExpr();
					column = true;
					columnName = full_from;
					tableName = sqs.tableName;
					aliasName = columnName;
					if (sqs.userTableAlias != null) {
						aliasName = sqs.userTableAlias;
					}

				} else if ((sqs.getOperation().equalsIgnoreCase("string"))
						|| (sqs.getOperation().equalsIgnoreCase("double"))
						|| (sqs.getOperation().equalsIgnoreCase("date"))
						|| (sqs.getOperation().equalsIgnoreCase("time"))
						|| (sqs.getOperation().equalsIgnoreCase("long"))) {
					// we got our target
					constantValue = sqs.leftItem;
					constantType = sqs.getOperation();
					if (columnName != null) {
						sqs.setLeftExpresion(
								"'<" + tableName + "_" + columnName + modifier + eqExpr.getOperation().trim() + ">'");
					}
					exprToTrack = sqs;

				}
				// If the operation is function, get the data from the expression
				else if ((sqs.getOperation().equalsIgnoreCase("function"))) {
					// Casting to FunctionExpression to get the expression data
					FunctionExpression fnsqs = (FunctionExpression) sqs;
					// in case we have function inside function
					while (fnsqs.expressions.size() > 0 && fnsqs.expressions.get(0).getOperation().equals("function")) {
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
						} else if (fnsqs.expressions.get(0).getOperation().equalsIgnoreCase("cast")) {
							GenExpression innerExpression = (GenExpression) fnsqs.expressions.get(0).leftItem;
							full_from = innerExpression.aQuery;
							column = true;
							columnName = innerExpression.getLeftExpr();
							tableName = innerExpression.tableName;
							aliasName = columnName;
							if (fnsqs.expressions.get(0).userTableAlias != null) {
								aliasName = fnsqs.expressions.get(0).userTableAlias;
							}
						} else if ((fnsqs.expressions.get(0).getOperation().equalsIgnoreCase("string"))
								|| (fnsqs.expressions.get(0).getOperation().equalsIgnoreCase("double"))
								|| (fnsqs.expressions.get(0).getOperation().equalsIgnoreCase("date"))
								|| (fnsqs.expressions.get(0).getOperation().equalsIgnoreCase("time"))
								|| (fnsqs.expressions.get(0).getOperation().equalsIgnoreCase("long"))) {

							constantValue = fnsqs.expressions.get(0).leftItem;
							constantType = sqs2.getOperation();
						}
					}
				}

				// the column can just as easily be on the right hand side
				if (sqs2.getOperation().equalsIgnoreCase("column")) {
					full_To = sqs2.getLeftExpr();
					column = true;
					columnName = full_To;
					tableName = sqs2.tableName;
					aliasName = columnName;
					if (sqs2.userTableAlias != null) {
						aliasName = sqs2.userTableAlias;
					}
				} else if ((sqs2.getOperation().equalsIgnoreCase("string"))
						|| (sqs2.getOperation().equalsIgnoreCase("double"))
						|| (sqs2.getOperation().equalsIgnoreCase("date"))
						|| (sqs2.getOperation().equalsIgnoreCase("time"))
						|| (sqs2.getOperation().equalsIgnoreCase("long"))) {
					// we got our target
					constantValue = sqs2.leftItem;
					constantType = sqs2.getOperation();

					if (columnName != null && parameterize) {
						// replace the parameter value so at a later point some one can change it
						// the value is now replaced with table_column_left or right of the and
						// expression followed by operation
						sqs2.setLeftExpresion(
								"<" + tableName + "_" + columnName + modifier + eqExpr.getOperation().trim() + ">");
					}
					exprToTrack = sqs2;

				}
				// If the operation is function, get the data from the expression
				else if ((sqs2.getOperation().equalsIgnoreCase("function"))) {
					// Casting to FunctionExpression to get the expression data
					FunctionExpression fnsqs2 = (FunctionExpression) sqs2;
					// in case we have function inside function
					while (fnsqs2.expressions.size() > 0
							&& fnsqs2.expressions.get(0).getOperation().equals("function")) {
						fnsqs2 = (FunctionExpression) fnsqs2.expressions.get(0);
					}
					if (fnsqs2.expressions.size() > 0) {
						// Going with the normal flow
						if (fnsqs2.expressions.get(0).getOperation().equalsIgnoreCase("column")) {
							full_from = fnsqs2.expressions.get(0).getLeftExpr();
							column = true;
							columnName = full_from;
							tableName = fnsqs2.expressions.get(0).tableName;
							aliasName = columnName;
							if (fnsqs2.expressions.get(0).userTableAlias != null) {
								aliasName = fnsqs2.expressions.get(0).userTableAlias;
							}
						} else if (fnsqs2.expressions.get(0).getOperation().equalsIgnoreCase("cast")) {
							GenExpression innerExpression = (GenExpression) fnsqs2.expressions.get(0).leftItem;
							full_from = innerExpression.aQuery;
							column = true;
							columnName = innerExpression.getLeftExpr();
							tableName = innerExpression.tableName;
							aliasName = columnName;
							if (fnsqs2.expressions.get(0).userTableAlias != null) {
								aliasName = fnsqs2.expressions.get(0).userTableAlias;
							}
						} else if ((fnsqs2.expressions.get(0).getOperation().equalsIgnoreCase("string"))
								|| (fnsqs2.expressions.get(0).getOperation().equalsIgnoreCase("double"))
								|| (fnsqs2.expressions.get(0).getOperation().equalsIgnoreCase("date"))
								|| (fnsqs2.expressions.get(0).getOperation().equalsIgnoreCase("time"))
								|| (fnsqs2.expressions.get(0).getOperation().equalsIgnoreCase("long"))) {

							constantValue = fnsqs2.expressions.get(0).leftItem;
							constantType = sqs2.getOperation();
						}
					}
				}

				if (binary && column && tableName != null && constantValue != null) {
					String defQuery = "Select q1." + aliasName + " from (" + qs + ") q1";
					this.wrapper.makeParameters(columnName, constantValue, modifier + eqExpr.getOperation().trim(),
							eqExpr.getOperation().trim(), constantType, exprToTrack, tableName, defQuery);
				}

				binary = false;
				column = false;
				columnName = null;

			}

			eqExpr.recursive = true;
			eqExpr.setLeftExpresion(sqs);
			eqExpr.setRightExpresion(sqs2);
			eqExpr.setComposite(false);
			eqExpr.setExpression(joinExpr2.toString());
			eqExpr.setLeftExpr(full_from);
			eqExpr.setRightExpr(full_To);

			eqExpr.parent = (GenExpression) qs;
			return eqExpr;
		}
		// a subquery used as an expression, e.g. the right hand side of an IN
		else if (joinExpr instanceof ParenthesedSelect) {
			ParenthesedSelect ss = (ParenthesedSelect) joinExpr;
			Select sb = ss.getSelect();
			String alias = null;
			if (ss.getAlias() != null) {
				alias = ss.getAlias().getName();
			}

			// this can be something else other than plain select
			if (sb instanceof PlainSelect) {
				GenExpression ge = new GenExpression();
				ge.aliasHash = qs.aliasHash;
				ge.randomHash = qs.randomHash;
				ge.setOperation("querystruct");
				ge.telescope = true;

				GenExpression sqs = processSelect(ge, (PlainSelect) sb);
				ge.body = sqs;
				sqs.parent = ge;
				if (alias != null) {
					qs.aliasHash.put(alias, sqs);
				}
				return ge;
			} else if (sb instanceof SetOperationList) {
				GenExpression gep = processOperation((SetOperationList) sb);
				gep.parent = (GenExpression) qs;
				return gep;
			}

		} else if (joinExpr instanceof Between) {
			boolean paramBetween = true;

			if (processParam.size() > 0) {
				// get the latest and push it back
				paramBetween = processParam.pop();
				processParam.push(paramBetween);
			}

			GenExpression retExpr = new GenExpression();
			retExpr.setComposite(true);
			retExpr.setOperation("between");
			retExpr.aQuery = joinExpr.toString();
			retExpr.recursive = true;
			String modifier = wrapper.uniqueCounter + "";
			// the column under test becomes the body, and the bounds become the left and
			// right expressions

			Between betw = (Between) joinExpr;
			Expression leftExpr = betw.getLeftExpression();
			retExpr.body = processExpression(qs, leftExpr, retExpr);
			Expression start = betw.getBetweenExpressionStart();
			Expression end = betw.getBetweenExpressionEnd();
			GenExpression startExpression = processExpression(qs, start, retExpr);
			retExpr.setLeftExpresion(startExpression);
			GenExpression endExpression = processExpression(qs, end, retExpr);
			retExpr.setRightExpresion(endExpression);

			retExpr.parent = (GenExpression) qs;
			String tableName = null;
			String aliasName = null;

			if (retExpr.body.getOperation().equalsIgnoreCase("column")) {
				column = true;
				columnName = retExpr.body.getLeftExpr();
				tableName = retExpr.body.tableName;
				aliasName = columnName;
				if (retExpr.body.userTableAlias != null) {
					aliasName = retExpr.body.userTableAlias;
				}

			}

			if (paramBetween && tableName != null) {
				// process the start value if it is a constant
				if ((startExpression.getOperation().equalsIgnoreCase("string"))
						|| startExpression.getOperation().equalsIgnoreCase("double")
						|| startExpression.getOperation().equalsIgnoreCase("date")
						|| startExpression.getOperation().equalsIgnoreCase("time")
						|| startExpression.getOperation().equalsIgnoreCase("long")) {
					// we got our target
					Object constantValue = startExpression.leftItem;
					String constantType = startExpression.getOperation();

					if (columnName != null && parameterize) {
						startExpression.setLeftExpresion(
								"'<" + tableName + "_" + columnName + modifier + "between.start" + ">'");
					}

					String defQuery = "Select q1." + aliasName + " from (" + qs + ") q1";
					String compositeName = this.wrapper.makeParameters(columnName, constantValue,
							modifier + "between.start", "between.start", constantType, startExpression, tableName,
							defQuery);
					startExpression.setLeftExpresion("'<" + compositeName + ">'");
					classLogger.debug("Parameterized {} in query {}", columnName, qs);

				}

				// process the end value if it is a constant
				if ((endExpression.getOperation().equalsIgnoreCase("string"))
						|| endExpression.getOperation().equalsIgnoreCase("double")
						|| endExpression.getOperation().equalsIgnoreCase("date")
						|| endExpression.getOperation().equalsIgnoreCase("time")
						|| endExpression.getOperation().equalsIgnoreCase("long")) {
					// we got our target
					Object constantValue = endExpression.leftItem;
					String constantType = endExpression.getOperation();

					if (columnName != null && parameterize) {
						endExpression.setLeftExpresion(
								"'<" + tableName + "_" + columnName + modifier + "between.end" + ">'");
					}

					String defQuery = "Select q1." + aliasName + " from (" + qs + ") q1";
					String compositeName = this.wrapper.makeParameters(columnName, constantValue,
							modifier + "between.end", "between.end", constantType, endExpression, tableName, defQuery);
					endExpression.setLeftExpresion("'<" + compositeName + ">'");
					classLogger.debug("Parameterized {} in query {}", columnName, qs);
				}
			}
			binary = false;
			column = false;
			columnName = null;

			return retExpr;
		} else if (joinExpr instanceof Column) {
			// process the column and return back
			GenExpression retExpr = new GenExpression();
			Column thisCol = (Column) joinExpr;
			retExpr.aQuery = thisCol.toString();
			retExpr.setComposite(false);
			retExpr.setOperation("column");
			String tableName = "";
			String tableAlias = "";

			// an unqualified column belongs to whatever table the enclosing select is on
			if (thisCol.getTable() != null) {
				tableName = thisCol.getTable().getFullyQualifiedName();
				retExpr.userTableName = tableName;
				Alias alias = thisCol.getTable().getAlias();
				if (alias != null) {
					tableAlias = alias.getName();
					retExpr.userTableAlias = tableAlias;
				}
			} else {
				tableName = qs.currentTable;
				tableAlias = qs.currentTableAlias;
			}
			retExpr.setLeftExpr(thisCol.getColumnName());
			retExpr.tableName = tableName;
			retExpr.tableAlias = tableAlias;

			// starts keeping track of the columns
			String columnName = thisCol.getColumnName();

			List<GenExpression> selectList = null;
			if (this.wrapper.columnSelect.containsKey(tableName + "." + columnName)) {
				selectList = this.wrapper.columnSelect.get(tableName + "." + columnName);
			} else {
				selectList = new ArrayList<GenExpression>();
			}
			if (!selectList.contains(qs)) {
				selectList.add((GenExpression) qs);
			}
			this.wrapper.columnSelect.put(tableName + "." + columnName, selectList);

			// track based on select too
			List<String> columnList = null;
			if (this.wrapper.selectColumns.containsKey(qs)) {
				columnList = this.wrapper.selectColumns.get(qs);
			} else {
				columnList = new ArrayList<String>();
			}
			if (!columnList.contains(tableName + "." + columnName)) {
				columnList.add(tableName + "." + columnName);
			}

			this.wrapper.selectColumns.put((GenExpression) qs, columnList);

			retExpr.parent = (GenExpression) qs;
			return retExpr;
		} else if (joinExpr instanceof Function) {
			Function fexpr = (Function) joinExpr;
			FunctionExpression gep = new FunctionExpression();
			gep.aQuery = fexpr.toString();
			gep.setExpression(fexpr.getName());
			gep.setOperation("function");
			// the whole call is kept verbatim as well, so it can be rendered back out
			// without reassembling it from the arguments
			gep.setLeftExpr(fexpr.toString());
			gep.distinct = fexpr.isDistinct();

			if (!fexpr.isAllColumns()) {
				List<? extends Expression> el = fexpr.getParameters();
				for (int exprIndex = 0; exprIndex < el.size(); exprIndex++) {
					GenExpression thisExpression = processExpression(qs, el.get(exprIndex), gep);
					gep.expressions.add(thisExpression);
				}
			} else {
				GenExpression allColExpression = new GenExpression();
				allColExpression.setOperation("allcol");
				gep.expressions.add(allColExpression);
			}
			// add this to be used later
			wrapper.addFunctionExpression(fexpr.getName(), gep);
			gep.parent = (GenExpression) qs;
			return gep;
		} else if (joinExpr instanceof CaseExpression) {
			CaseExpression cep = (CaseExpression) joinExpr;
			// suppress parameterization for everything under the CASE, since its branches
			// are rendered verbatim. TODO this should be a stack to survive nested cases
			if (!processCase) {
				processParam.push(false);
			}

			WhenExpression wep = new WhenExpression();
			wep.aQuery = joinExpr.toString();
			wep.setOperation("case");
			List<WhenClause> whens = cep.getWhenClauses();

			for (int whenIndex = 0; whenIndex < whens.size(); whenIndex++) {
				WhenClause wc = whens.get(whenIndex);
				Expression we = wc.getWhenExpression();
				// process this expression
				GenExpression when = processExpression(qs, we, wep);

				Expression te = wc.getThenExpression();
				GenExpression then = processExpression(qs, te, wep);

				StringBuffer whenBuf = GenExpression.printQS(when, new StringBuffer());
				StringBuffer thenBuf = GenExpression.printQS(then, new StringBuffer());
				wep.addWhenThen(whenBuf.toString(), thenBuf.toString());
				wep.addWhenThenE(when, then);
			}
			// if there is an else - process it
			if (cep.getElseExpression() != null) {
				GenExpression elseE = processExpression(qs, cep.getElseExpression(), wep);
				wep.setElse(cep.getElseExpression().toString());
				wep.setElseE(elseE);
			}

			wep.parent = (GenExpression) qs;
			if (!processCase) {
				processParam.pop();
			}
			return wep;
		} else if (joinExpr instanceof StringValue) {
			GenExpression gep = new GenExpression();
			gep.aQuery = joinExpr.toString();
			String value = ((StringValue) joinExpr).getValue();
			gep.setOperation("string");
			gep.setExpression("string");
			gep.setLeftExpresion("'" + value + "'");
			gep.setLeftExpr("'" + value + "'");
			gep.parent = (GenExpression) qs;
			return gep;
		} else if (joinExpr instanceof LongValue) {
			GenExpression gep = new GenExpression();
			gep.aQuery = joinExpr.toString();
			Long value = ((LongValue) joinExpr).getValue();
			gep.setOperation("long");
			gep.setExpression("long");
			gep.setLeftExpresion(value);
			gep.parent = (GenExpression) qs;
			return gep;
		} else if (joinExpr instanceof DoubleValue) {
			GenExpression gep = new GenExpression();
			gep.aQuery = joinExpr.toString();
			Double value = ((DoubleValue) joinExpr).getValue();
			gep.setOperation("double");
			gep.setExpression("double");
			gep.setLeftExpresion(value);
			gep.parent = (GenExpression) qs;
			return gep;
		} else if (joinExpr instanceof DateValue) {
			GenExpression gep = new GenExpression();
			gep.aQuery = joinExpr.toString();
			Date value = ((DateValue) joinExpr).getValue();
			gep.setOperation("date");
			gep.setExpression("date");
			gep.setLeftExpresion(value);
			gep.parent = (GenExpression) qs;
			return gep;
		} else if (joinExpr instanceof TimestampValue) {
			GenExpression gep = new GenExpression();
			gep.aQuery = joinExpr.toString();
			Date value = ((DateValue) joinExpr).getValue();
			gep.setOperation("timestamp");
			gep.setExpression("timestamp");
			gep.setLeftExpresion(value);
			gep.parent = (GenExpression) qs;
			return gep;
		} else if (joinExpr instanceof TimeValue) {
			GenExpression gep = new GenExpression();
			gep.aQuery = joinExpr.toString();
			Time value = ((TimeValue) joinExpr).getValue();
			gep.setOperation("time");
			gep.setExpression("time");
			gep.setLeftExpresion(value);
			gep.parent = (GenExpression) qs;
			return gep;
		} else if (joinExpr instanceof CastExpression) {
			CastExpression ce = (CastExpression) joinExpr;
			GenExpression gep = new GenExpression();
			gep.aQuery = ce.toString();
			gep.setOperation("cast");
			// the target type rides along as the alias of the expression being cast
			GenExpression innerExpression = processExpression(qs, ce.getLeftExpression(), null);
			innerExpression.setLeftAlias(ce.getColDataType().toString());
			gep.setLeftExpresion(innerExpression);
			gep.parent = (GenExpression) qs;
			return gep;
		}
		// a single parenthesized expression, e.g. WHERE (a = 1 AND b = 2). Lists with
		// more than one element fall through to the opaque handling below
		else if (joinExpr instanceof ParenthesedExpressionList
				&& ((ParenthesedExpressionList<?>) joinExpr).size() == 1) {
			GenExpression gep = new GenExpression();
			gep.setOperation("paranthesis");
			gep.setExpression(joinExpr.toString());

			Expression nextExpr = ((ParenthesedExpressionList<?>) joinExpr).get(0);
			gep.telescope = true;
			GenExpression body = processExpression(qs, nextExpr, null);
			gep.body = body;
			gep.setLeftExpresion(body);
			gep.parent = (GenExpression) qs;
			return gep;
		} else if (joinExpr instanceof IsNullExpression) {
			IsNullExpression nullExpr = (IsNullExpression) joinExpr;
			GenExpression gep = new GenExpression();
			gep.setOperation("isnull");
			gep.setLeftExpresion(processExpression(qs, nullExpr.getLeftExpression(), expr));
			return gep;
		} else if (joinExpr instanceof InExpression) {
			boolean paramIn = true;

			if (processParam.size() > 0) {
				// get the latest and push it back
				paramIn = processParam.pop();
				processParam.push(paramIn);
			}

			InExpression inExpr = (InExpression) joinExpr;
			InGenExpression gep = new InGenExpression();
			gep.setIsNot(inExpr.isNot());
			// the left side is a single expression; the right side is either a subquery
			// or a value list that gets parameterized as a whole
			gep.setOperation("in");
			String operator = "in";

			String modifier = operator + wrapper.uniqueCounter;

			String tableName = null;
			Expression leftExpression = inExpr.getLeftExpression();
			// sometimes the in can also be a list, which the parser hands back as a
			// parenthesed expression list rather than a dedicated items list
			if (leftExpression instanceof ExpressionList) {
				ExpressionList<?> el = (ExpressionList<?>) leftExpression;
				if (el.size() == 1) {
					GenExpression colExpression = processExpression(qs, el.get(0), expr);
					colExpression.paranthesis = true;
					gep.setLeftExpresion(colExpression);
					if (colExpression.getOperation().equalsIgnoreCase("column")) {
						column = true;
						columnName = colExpression.getLeftExpr();
						tableName = colExpression.tableName;
					}
				} else {
					// TODO this should raise rather than warn
					classLogger.warn("Multiple columns in IN is not supported, leaving {} unparameterized",
							leftExpression);
				}
			} else if (leftExpression != null) {
				GenExpression colExpression = processExpression(qs, leftExpression, expr);
				gep.setLeftExpresion(colExpression);
				if (colExpression.getOperation().equalsIgnoreCase("column")) {
					column = true;
					columnName = colExpression.getLeftExpr();
					tableName = colExpression.tableName;
				}

				// If Operation is function, get the column name and table name from the
				// expression
				if (colExpression.getOperation().equalsIgnoreCase("function")) {
					// Casting to FunctionExpression to get the expression data
					FunctionExpression fncolExpression = (FunctionExpression) colExpression;
					column = true;
					columnName = fncolExpression.expressions.get(0).getLeftExpr();
					tableName = fncolExpression.expressions.get(0).tableName;
				}

			}

			Expression itemList = inExpr.getRightExpression();
			if (itemList instanceof ParenthesedSelect) {
				GenExpression ge = processExpression(qs, itemList, expr);
				gep.rightItem = ge;
			} else {
				// the value list is kept whole as an opaque node, so the parameter it
				// produces stands in for the entire list rather than one element
				GenExpression ge = new GenExpression();
				ge.setOperation("opaque");
				ge.setLeftExpr(itemList.toString());
				ge.parent = (GenExpression) qs;

				gep.inList.add(ge);
				Object constantValue = ge.getLeftExpr();
				// the list is typed off its first recognizable literal
				String constantType = "string";
				{
					ExpressionList<?> list = (ExpressionList<?>) itemList;
					for (Expression e : list) {
						if (e instanceof LongValue) {
							constantType = "long";
							break;
						} else if (joinExpr instanceof DoubleValue) {
							constantType = "long";
							break;
						} else if (joinExpr instanceof TimeValue) {
							constantType = "time";
							break;
						} else if (joinExpr instanceof DateValue) {
							constantType = "date";
							break;
						} else if (joinExpr instanceof TimestampValue) {
							constantType = "timestamp";
							break;
						}
					}
				}

				if (columnName != null && paramIn) {
					// removing the quotes for now
					String defQuery = "Select q1." + columnName + " from (" + qs + ") q1";
					this.wrapper.makeParameters(columnName, constantValue, modifier, "in", constantType, ge, tableName,
							defQuery);
					if (parameterize) {
						ge.setLeftExpr("(<" + tableName + "_" + columnName + modifier + ">)");
					}
				}
			}

			return gep;
		} else {
			// anything this parser does not model is carried through verbatim so the
			// query can still be rendered back out unchanged
			classLogger.debug("Unhandled expression, carrying it through verbatim: {}", joinExpr);
			if (joinExpr == null) {
				return null;
			}
			GenExpression ge = new GenExpression();
			ge.setOperation("opaque");
			ge.setLeftExpr(joinExpr.toString());
			ge.parent = (GenExpression) qs;

			return ge;
		}

		return expr;
	}

	/**
	 * Convert a UNION / INTERSECT / EXCEPT chain into a single expression whose
	 * operands are the individual selects and whose opNames are the set operators
	 * between them. There is always one more operand than operator, so the last
	 * select is handled after the loop.
	 *
	 * @param sol the set operation chain to convert
	 * @return the expression for the chain
	 */
	public GenExpression processOperation(SetOperationList sol) {
		OperationExpression opExpr = new OperationExpression();
		List<Select> solParts = sol.getSelects();
		List<SetOperation> solOps = sol.getOperations();

		opExpr.setOperation("union");
		opExpr.setComposite(true);

		int solIndex = 0;
		for (; solIndex < solOps.size(); solIndex++) {
			Select sb1 = solParts.get(solIndex);

			opExpr.opNames.add(solOps.get(solIndex).toString());

			GenExpression sqs1 = null;

			if (sb1 instanceof PlainSelect) {
				sqs1 = processSelect(null, (PlainSelect) sb1);
			} else if (sb1 instanceof SetOperationList) {
				sqs1 = processOperation((SetOperationList) sb1);
			}

			opExpr.operands.add(sqs1);
		}

		// the trailing select, which has no operator after it
		Select lastS = solParts.get(solIndex);
		GenExpression sqs1 = null;

		if (lastS instanceof PlainSelect) {
			sqs1 = processSelect(null, (PlainSelect) lastS);
		} else if (lastS instanceof SetOperationList) {
			sqs1 = processOperation((SetOperationList) lastS);
		}
		opExpr.operands.add(sqs1);

		return opExpr;
	}

	/**
	 * Set the WHERE clause of the select. The whole predicate is kept as one
	 * expression tree rather than being flattened into a list of filters, so
	 * arbitrary nesting of AND, OR, and parentheses survives a round trip.
	 *
	 * @param qs        the expression for the select
	 * @param curFilter unused, kept for signature compatibility with the callers
	 * @param expr      the WHERE predicate, may be null
	 */
	public void fillFilters(SelectQueryStruct qs, IQueryFilter curFilter, Expression expr) {
		if (expr != null) {
			GenExpression fExpr = processExpression(qs, expr, null);
			qs.filter = fExpr;
		}
	}

	/**
	 * Fills in the limit and offset for the query. Anything that is not a plain
	 * numeric literal is ignored.
	 *
	 * @param qs    the expression for the select
	 * @param limit the LIMIT clause, may be null
	 */
	public void fillLimitOffset(SelectQueryStruct qs, Limit limit) {
		if (limit == null) {
			return;
		}
		// add limit
		if (limit.getRowCount() instanceof LongValue) {
			long limitRow = ((LongValue) limit.getRowCount()).getValue();
			qs.setLimit(limitRow);
		}

		// add offset
		if (limit.getOffset() instanceof LongValue) {
			long offset = ((LongValue) limit.getOffset()).getValue();
			qs.setOffSet(offset);
		}
	}

	/**
	 * Add in the order by. ASC is the default, so only DESC is recorded.
	 *
	 * @param qs     the expression for the select
	 * @param orders the ORDER BY elements, may be null or empty
	 */
	public void fillOrder(SelectQueryStruct qs, List<OrderByElement> orders) {
		if (orders == null || orders.isEmpty()) {
			return;
		}

		for (int orderIndex = 0; orderIndex < orders.size(); orderIndex++) {

			OrderByElement thisElement = orders.get(orderIndex);
			Expression expr = thisElement.getExpression();
			String sortDir = "ASC";
			if (thisElement.isAscDescPresent() && !thisElement.isAsc()) {
				sortDir = "DESC";
			}

			OrderByExpression obe = new OrderByExpression();
			obe.telescope = true;
			obe.body = processExpression(qs, expr, null);
			if (!sortDir.equalsIgnoreCase("ASC")) {
				obe.direction = sortDir;
			}
			qs.norderBy.add(obe);
		}
	}

	/**
	 * Add in the group bys, also recording each grouped column on the wrapper.
	 *
	 * @param qs     the expression for the select
	 * @param groups the GROUP BY clause, may be null
	 */
	public void fillGroups(SelectQueryStruct qs, GroupByElement groups) {
		if (groups == null) {
			return;
		}
		ExpressionList<?> groupByElement = groups.getGroupByExpressionList();
		if (groupByElement == null || groupByElement.isEmpty()) {
			return;
		}

		for (int groupIndex = 0; groupIndex < groupByElement.size(); groupIndex++) {
			Expression expr = groupByElement.get(groupIndex);
			GenExpression gep = processExpression(qs, expr, null);

			String tableColumnName = gep.getLeftExpr();
			wrapper.addGroupBy(tableColumnName, (GenExpression) qs);

			qs.ngroupBy.add(gep);
		}
	}

	/**
	 * Parse a query and report which columns each real, physical table contributes.
	 * Derived tables and subquery aliases are dropped, so only names that resolve
	 * to something in the database survive.
	 *
	 * @param query the SQL to parse
	 * @return table name to the columns used from it, or null if parsing failed
	 */
	public Map<String, List<GenExpression>> getTableColumns(String query) {
		Map<String, List<GenExpression>> newTableColumn = null;
		try {
			wrapper = new GenExpressionWrapper();
			GenExpression qs = new GenExpression();
			Statement stmt = CCJSqlParserUtil.parse(query);
			Select select = ((Select) stmt);

			if (select instanceof PlainSelect) {
				PlainSelect sb = (PlainSelect) select;
				qs = processSelect(null, sb);
			}
			if (select instanceof SetOperationList) {
				qs = processOperation((SetOperationList) select);
			}
			Map<Integer, List<GenExpression>> levelSelectors = new HashMap<Integer, List<GenExpression>>();
			List<String> realTables = new ArrayList<String>();
			Map<GenExpression, List<GenExpression>> derivedColumns = new HashMap<GenExpression, List<GenExpression>>();
			Map<String, List<GenExpression>> tableColumns = new HashMap<String, List<GenExpression>>();

			Map<String, String> aliases = new HashMap<String, String>();

			// walking the tree fills realTables and tableColumns as a side effect
			GenExpression.printLevel2(qs, realTables, 0, null, derivedColumns, levelSelectors, tableColumns, aliases,
					null, false, true);

			newTableColumn = remasterColumns(realTables, tableColumns);
		} catch (JSQLParserException e) {
			classLogger.error("Failed to parse the SQL query {}", query, e);
		}

		return newTableColumn;
	}

	/**
	 * Parse a query and report the selectors found at each level of nesting, where
	 * level 0 is the outermost select.
	 *
	 * @param query the SQL to parse
	 * @return nesting level to the selectors at that level, empty if parsing failed
	 */
	public Map<Integer, List<GenExpression>> getLevelColumns(String query) {
		Map<Integer, List<GenExpression>> levelSelectors = new HashMap<Integer, List<GenExpression>>();
		try {
			wrapper = new GenExpressionWrapper();
			GenExpression qs = new GenExpression();
			Statement stmt = CCJSqlParserUtil.parse(query);
			Select select = ((Select) stmt);

			if (select instanceof PlainSelect) {
				PlainSelect sb = (PlainSelect) select;
				qs = processSelect(null, sb);
			}
			if (select instanceof SetOperationList) {
				qs = processOperation((SetOperationList) select);
			}
			List<String> realTables = new ArrayList<String>();
			Map<GenExpression, List<GenExpression>> derivedColumns = new HashMap<GenExpression, List<GenExpression>>();
			Map<String, List<GenExpression>> tableColumns = new HashMap<String, List<GenExpression>>();

			Map<String, String> aliases = new HashMap<String, String>();

			// walking the tree fills levelSelectors as a side effect
			GenExpression.printLevel2(qs, realTables, 0, null, derivedColumns, levelSelectors, tableColumns, aliases,
					null, false, true);

		} catch (JSQLParserException e) {
			classLogger.error("Failed to parse the SQL query {}", query, e);
		}

		return levelSelectors;
	}

	/**
	 * Narrow a table to column map down to the tables that are real rather than
	 * derived.
	 *
	 * @param realTables   the names that resolve to physical tables
	 * @param tableColumns every name seen, mapped to its columns
	 * @return the entries of tableColumns whose key is in realTables
	 */
	public Map<String, List<GenExpression>> remasterColumns(List<String> realTables,
			Map<String, List<GenExpression>> tableColumns) {
		Iterator<String> tableKeys = tableColumns.keySet().iterator();
		Map<String, List<GenExpression>> newTableColumn = new HashMap<String, List<GenExpression>>();
		while (tableKeys.hasNext()) {
			String thisTable = tableKeys.next();
			if (realTables.contains(thisTable)) {
				newTableColumn.put(thisTable, tableColumns.get(thisTable));
			}
		}
		return newTableColumn;
	}

	/**
	 * Log a table to column map, for debugging.
	 *
	 * @param tableColumns the map to log
	 */
	public void printRealColumns(Map<String, List<GenExpression>> tableColumns) {
		Iterator<String> tableKeys = tableColumns.keySet().iterator();
		while (tableKeys.hasNext()) {
			String thisTable = tableKeys.next();
			classLogger.info("Table {} uses columns {}", thisTable, tableColumns.get(thisTable));
		}
	}
}

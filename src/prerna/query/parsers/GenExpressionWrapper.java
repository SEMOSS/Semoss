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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import prerna.query.parsers.ParamStructDetails.LEVEL;
import prerna.query.parsers.ParamStructDetails.QUOTE;
import prerna.query.querystruct.FunctionExpression;
import prerna.query.querystruct.GenExpression;
import prerna.query.querystruct.OperationExpression;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.PixelDataType;

/**
 * Everything {@link SqlParser} learns about a query while it walks the AST,
 * gathered in one place: the expression tree itself plus the indexes needed to
 * find and rewrite parts of it afterwards.
 * <p>
 * Parameters are indexed in a chain of four widening keys, so a caller can
 * replace a value at whatever scope they mean:
 *
 * <pre>
 * columnTableIndex             ACCTID            -&gt; every table that has an ACCTID
 * columnTableOperatorIndex     CLMS_ACCTID       -&gt; every comparison against that one column
 * operatorTableColumnParamIndex CLMS_ACCTIDand0_left= -&gt; the single parameter for one comparison
 * paramToExpressionMap         that parameter    -&gt; the expression nodes carrying its value
 * </pre>
 *
 * So {@link #replaceColumn(String, Object)} fans out across every table and
 * operator, while {@link #replaceTableColumnOperator(String, Object)} touches
 * exactly one comparison.
 */
public class GenExpressionWrapper {

	private static final Logger classLogger = LogManager.getLogger(GenExpressionWrapper.class);

	// keep table alias, this is {alias => table name}
	public Map<String, String> tableAlias = null;
	// keep column alias
	public Map<String, String> columnAlias = null;
	// used to keep track of every table and column set used
	public Map<String, Set<String>> schema = null;

	// keeps track of column to the selects that use it
	public Map<String, List<GenExpression>> columnSelect = new HashMap<>();
	// keep track of table to the selects that use it
	public Map<String, List<GenExpression>> tableSelect = new HashMap<>();
	// the inverse of columnSelect, select to the columns it uses
	public Map<GenExpression, List<String>> selectColumns = new HashMap<>();

	// groupby hash
	public Map<String, GenExpression> groupByHash = new HashMap<>();
	public Map<String, GenExpression> joinHash = new HashMap<>();

	// the top of the expression tree for the parsed query
	public GenExpression root = null;

	// the widest level, e.g. ACCTID, keyed by column and valued by the
	// columnTableOperatorIndex keys it covers
	public Map<String, List<String>> columnTableIndex = new HashMap<>();
	// one level narrower, e.g. CLMS_ACCTID, keyed by table and column and valued by
	// the operatorTableColumnParamIndex keys it covers
	public Map<String, List<String>> columnTableOperatorIndex = new HashMap<>();
	// the narrowest level, e.g. CLMS_ACCTIDand0_left=, one entry per comparison and
	// valued by the parameter that stands in for it
	public Map<String, ParamStructDetails> operatorTableColumnParamIndex = new HashMap<>();

	// each parameter to the expression nodes whose value it fills
	public Map<ParamStructDetails, List<GenExpression>> paramToExpressionMap = new HashMap<>();

	// the same parameters, keyed by their placeholder text
	public Map<String, ParamStructDetails> paramStringToParamMap = new HashMap<>();

	// tracks where the parser currently is in the AND / OR tree, so a parameter can
	// be named after the branch it sits on
	public Stack<String> currentOperator = new Stack<String>();
	public Stack<String> contextExpression = new Stack<String>();
	public Map<String, Boolean> procOrder = new HashMap<String, Boolean>();
	int andCount = 0;
	int orCount = 0;
	int uniqueCounter = 0;

	// how many subselects are there
	public int numSubSelects = -1;

	// keeps function name to the expressions that call it
	public Map<String, List<GenExpression>> functionExpressionMapper = new HashMap<String, List<GenExpression>>();

	public GenExpressionWrapper() {
		tableAlias = new HashMap<String, String>();
		columnAlias = new HashMap<String, String>();
		schema = new HashMap<String, Set<String>>();
	}

	/**
	 * Render the whole expression tree back out as SQL.
	 *
	 * @return the rendered query
	 * @throws Exception if the tree cannot be rendered
	 */
	public String printOutput() throws Exception {
		return GenExpression.printQS(root, new StringBuffer()).toString();
	}

	/**
	 * Record that a column is grouped on.
	 *
	 * @param columnName the grouped column
	 * @param expr       the expression for the select that groups on it
	 */
	public void addGroupBy(String columnName, GenExpression expr) {
		groupByHash.put(columnName, expr);
	}

	/**
	 * Record that a column is joined on.
	 *
	 * @param columnName the joined column
	 * @param expr       the expression for the join
	 */
	public void addJoin(String columnName, GenExpression expr) {
		joinHash.put(columnName, expr);
	}

	/**
	 * AND an extra filter into the WHERE clause of every select that reads from the
	 * given table, or set it as the WHERE clause outright when the select has none.
	 *
	 * @param filterValues table name to the filter expression to apply to it
	 */
	public void addRowFilter(Map<String, GenExpression> filterValues) {
		// I still need to account for if the user already comes with this

		Iterator<String> cols = filterValues.keySet().iterator();
		while (cols.hasNext()) {
			// get the col
			String thisCol = cols.next();
			GenExpression userFilter = filterValues.get(thisCol);

			// see if a select with that table exists
			if (tableSelect.containsKey(thisCol)) {
				// get the expression and see what is the where clause
				List<GenExpression> selects = tableSelect.get(thisCol);
				for (int selectIndex = 0; selectIndex < selects.size(); selectIndex++) {
					SelectQueryStruct select = selects.get(selectIndex);
					GenExpression filter = select.filter;

					classLogger.debug("Filter is set to {}", filter);
					if (filter != null) {
						GenExpression thisFilter = new GenExpression();
						thisFilter.setOperation(" AND ");
						filter.paranthesis = true;
						thisFilter.setLeftExpresion(filter);
						// forcing a random opaque one
						userFilter.paranthesis = true;
						thisFilter.setRightExpresion(userFilter);
						// replace with the new filter
						select.filter = thisFilter;
					}
					// add a new filter otherwise
					else {
						select.filter = userFilter;
					}

				}
			}
		}
	}

	/**
	 * Drop some columns from every select and parameterize others.
	 * <p>
	 * Columns are matched by alias or name across all selects, so a name that is
	 * aliased differently in two places will be treated as one column.
	 *
	 * @param columnsToRemove columns to drop from the selectors and groupings
	 * @param paramValues     column name to the expression to parameterize it with
	 */
	public void appendParameter(List<String> columnsToRemove, Map<String, GenExpression> paramValues) {
		// first is the remove
		for (int colIndex = 0; colIndex < columnsToRemove.size(); colIndex++) {
			Iterator<GenExpression> allSelects = selectColumns.keySet().iterator();

			while (allSelects.hasNext()) {
				GenExpression thisSelect = allSelects.next();
				thisSelect.removeSelect(columnsToRemove.get(colIndex));
				thisSelect.removeGroup(columnsToRemove.get(colIndex));
			}
		}

		// second is add
		Iterator<String> cols = paramValues.keySet().iterator();
		while (cols.hasNext()) {
			// get the col
			String thisCol = cols.next();
			GenExpression userFilter = paramValues.get(thisCol);

			// see if a select with that table exists
			if (columnSelect.containsKey(thisCol)) {
				// get the expression and see what is the where clause
				List<GenExpression> selects = columnSelect.get(thisCol);
				for (int selectIndex = 0; selectIndex < selects.size(); selectIndex++) {
					SelectQueryStruct select = selects.get(selectIndex);
					select.parameterizeColumn(thisCol, userFilter);
				}
			}
		}
	}

	/**
	 * Set a new value everywhere the column is compared against a constant,
	 * regardless of which table it came from or which operator was used.
	 *
	 * @param columnName the column to replace values for
	 * @param value      the new value
	 */
	public void replaceColumn(String columnName, Object value) {
		List<String> tableColumn = columnTableIndex.get(columnName);
		if (tableColumn != null) {
			for (int index = 0; index < tableColumn.size(); index++) {
				replaceTableColumn(tableColumn.get(index), value);
			}
		}
	}

	/**
	 * Set a new value everywhere one table's column is compared against a constant,
	 * regardless of which operator was used.
	 *
	 * @param id    a {@link #columnTableOperatorIndex} key, e.g. CLMS_ACCTID
	 * @param value the new value
	 */
	public void replaceTableColumn(String id, Object value) {
		List<String> tableColumnOperator = columnTableOperatorIndex.get(id);
		if (tableColumnOperator != null) {
			for (int index = 0; index < tableColumnOperator.size(); index++) {
				replaceTableColumnOperator(tableColumnOperator.get(index), value);
			}
		}
	}

	/**
	 * Set a new value for one specific comparison.
	 *
	 * @param id    an {@link #operatorTableColumnParamIndex} key, e.g.
	 *              CLMS_ACCTIDand0_left=
	 * @param value the new value
	 */
	public void replaceTableColumnOperator(String id, Object value) {
		if (operatorTableColumnParamIndex.containsKey(id)) {
			ParamStructDetails tableColumnOperatorParam = operatorTableColumnParamIndex.get(id);
			tableColumnOperatorParam.setCurrentValue(value);
		}
	}

	/**
	 * Push each parameter's current value back into the expression nodes it stands
	 * for, so the tree renders with values rather than placeholders.
	 */
	public void fillParameters() {
		Iterator<ParamStructDetails> paramIterator = paramToExpressionMap.keySet().iterator();
		while (paramIterator.hasNext()) {
			ParamStructDetails daStruct = paramIterator.next();
			fillExpressionsWithValue(paramToExpressionMap.get(daStruct), String.valueOf(daStruct.getCurrentValue()));
		}
	}

	/**
	 * Write one rendered value into every expression that carries a parameter. An
	 * opaque node holds its text directly, everything else holds it as the left
	 * expression.
	 *
	 * @param exprs      the expressions to fill, may be null
	 * @param finalValue the text to write
	 */
	private static void fillExpressionsWithValue(List<GenExpression> exprs, String finalValue) {
		if (exprs == null) {
			return;
		}
		for (int exprIndex = 0; exprIndex < exprs.size(); exprIndex++) {
			GenExpression thisExpression = exprs.get(exprIndex);
			if (!thisExpression.operation.equalsIgnoreCase("opaque")) {
				thisExpression.setLeftExpresion(finalValue);
			} else {
				thisExpression.setLeftExpr(finalValue);
			}
		}
	}

	/**
	 * Replace the parsed parameters with the names the user gave them, so the
	 * rendered query carries readable placeholders like &lt;startDate&gt; instead
	 * of the generated CLMS_ACCTIDand0_left= form.
	 * <p>
	 * Each incoming struct declares how widely it applies. A COLUMN level struct
	 * claims every comparison against that column name, TABLE narrows that to one
	 * table, OPERATOR narrows it further to one operator, and OPERATORU pins a
	 * single comparison by its exact key. Anything left unclaimed afterwards falls
	 * back to the constant that was in the original query.
	 *
	 * @param incomingStructs the parameters the user defined
	 * @param detailsLookup   each incoming struct to the ParamStruct holding its
	 *                        user facing name and display type
	 */
	public void fillParameters(List<ParamStructDetails> incomingStructs,
			Map<ParamStructDetails, ParamStruct> detailsLookup) {
		// first replace the incoming structs with the user defined param names
		for (int paramIndex = 0; paramIndex < incomingStructs.size(); paramIndex++) {
			ParamStructDetails thisStruct = incomingStructs.get(paramIndex);
			LEVEL thisStructLevel = thisStruct.getLevel();
			if (thisStructLevel == LEVEL.DATASOURCE) {
				// not relevant for the query
				continue;
			}

			ParamStruct pStruct = detailsLookup.get(thisStruct);
			// normally this is for using [<paramanme>] vs just <paramname>
			// but here we are using it to indicate that quoting is defined by the FE
			boolean noQuote = ParamStruct.PARAM_FILL_USE_ARRAY_TYPES.contains(pStruct.getModelDisplay());
			String userDefinedParamName = pStruct.getParamName();

			if (thisStructLevel == LEVEL.OPERATORU) {
				// this level names one exact comparison, so it is a direct lookup
				ParamStructDetails targetStruct = operatorTableColumnParamIndex.get(thisStruct.getParamKey());
				if (targetStruct != null) {
					applyUserParamName(targetStruct, userDefinedParamName, noQuote);
				}
				continue;
			}

			for (String key : operatorTableColumnParamIndex.keySet()) {
				ParamStructDetails targetStruct = operatorTableColumnParamIndex.get(key);
				if (matchesLevel(targetStruct, thisStruct, thisStructLevel)) {
					applyUserParamName(targetStruct, userDefinedParamName, noQuote);
				}
			}
		}

		/*
		 * replace all the other structs with the default values already present in the
		 * query this does not take in inputs it goes through all the remaining structs
		 * generated THROUGH the parsing and places those back to the default values
		 */
		Iterator<ParamStructDetails> paramIterator = paramToExpressionMap.keySet().iterator();
		while (paramIterator.hasNext()) {
			ParamStructDetails structDetails = paramIterator.next();
			fillExpressionsWithValue(paramToExpressionMap.get(structDetails),
					String.valueOf(structDetails.getCurrentValue()));
		}
	}

	/**
	 * Whether a parsed parameter falls within the scope an incoming struct claims.
	 * The levels nest, so TABLE also has to match the column and OPERATOR also has
	 * to match the table.
	 *
	 * @param targetStruct the parsed parameter
	 * @param thisStruct   the parameter the user defined
	 * @param level        the scope thisStruct claims
	 * @return true when targetStruct is in scope
	 */
	private static boolean matchesLevel(ParamStructDetails targetStruct, ParamStructDetails thisStruct, LEVEL level) {
		if (!targetStruct.getColumnName().equals(thisStruct.getColumnName())) {
			return false;
		}
		if (level == LEVEL.COLUMN) {
			return true;
		}
		if (!targetStruct.getTableName().equals(thisStruct.getTableName())) {
			return false;
		}
		if (level == LEVEL.TABLE) {
			return true;
		}
		return level == LEVEL.OPERATOR && targetStruct.getOperator().equals(thisStruct.getOperator());
	}

	/**
	 * Swap a parsed parameter's expressions over to the user facing placeholder and
	 * drop it from the fill map, so the default value pass leaves it alone.
	 *
	 * @param targetStruct         the parsed parameter to replace
	 * @param userDefinedParamName the name the user gave it
	 * @param noQuote              true when the front end supplies its own quoting
	 */
	private void applyUserParamName(ParamStructDetails targetStruct, String userDefinedParamName, boolean noQuote) {
		String quote = null;
		if (noQuote || targetStruct.getQuote() == QUOTE.NO) {
			quote = "";
		} else if (targetStruct.getQuote() == QUOTE.DOUBLE) {
			quote = "\"";
		} else if (targetStruct.getQuote() == QUOTE.SINGLE) {
			quote = "'";
		}

		fillExpressionsWithValue(paramToExpressionMap.get(targetStruct),
				quote + "<" + userDefinedParamName + ">" + quote);

		// remove this struct from the overall so it wont fill
		paramToExpressionMap.remove(targetStruct);
	}

	/**
	 * Register a constant found in the query as a parameter, adding it to all four
	 * levels of the index chain so it can later be replaced at any scope. Calling
	 * this twice for the same column, table, and operator reuses the existing
	 * parameter and just tracks the extra expression against it.
	 *
	 * @param columnName          the column being compared
	 * @param constantValue       the constant the query had in it
	 * @param operationName       the operator plus the branch it sits on, unique
	 *                            per comparison
	 * @param actualOperationName the bare operator, e.g. = or between.start
	 * @param constantType        one of string, double, long, date, timestamp
	 * @param exprToTrack         the expression node whose value gets replaced
	 * @param tableName           the table or alias the column came from
	 * @param defQuery            a query that lists the possible values, for the
	 *                            front end to offer as options
	 * @return the {@link #operatorTableColumnParamIndex} key for this parameter
	 */
	public String makeParameters(String columnName, Object constantValue, String operationName,
			String actualOperationName, String constantType, GenExpression exprToTrack, String tableName,
			String defQuery) {
		String tableAliasName = tableName;
		if (tableAlias.containsKey(tableAliasName)) {
			tableName = tableAlias.get(tableAliasName);
		}

		// add it to the column index first
		List<String> tableColumnList = new ArrayList<String>();
		if (this.columnTableIndex.containsKey(columnName)) {
			tableColumnList = columnTableIndex.get(columnName);
		}

		String tableColumnComposite = tableName + "_" + columnName;

		if (!tableColumnList.contains(tableColumnComposite)) {
			tableColumnList.add(tableColumnComposite);
		}
		columnTableIndex.put(columnName, tableColumnList);

		// next add the operator
		// need to see if the operator exists
		// if so i need to pop and do the left and right magic
		List<String> operatorTableColumnList = new ArrayList<String>();
		if (this.columnTableOperatorIndex.containsKey(tableColumnComposite)) {
			operatorTableColumnList = columnTableOperatorIndex.get(tableColumnComposite);
		}

		String tableColumnOperatorComposite = tableColumnComposite + operationName;
		if (!operatorTableColumnList.contains(tableColumnOperatorComposite)) {
			operatorTableColumnList.add(tableColumnOperatorComposite);
		}

		columnTableOperatorIndex.put(tableColumnComposite, operatorTableColumnList);

		ParamStructDetails daStruct = null;
		if (!operatorTableColumnParamIndex.containsKey(tableColumnOperatorComposite)) {
			String context = "";
			String contextPart = "";
			if (contextExpression.size() > 0) {
				context = contextExpression.pop();
				// get the context part
				contextPart = GenExpression.printQS(exprToTrack, null) + "";
			}
			daStruct = new ParamStructDetails();
			daStruct.setColumnName(columnName);
			daStruct.setTableAlias(tableAliasName);
			daStruct.setTableName(tableName);
			daStruct.setCurrentValue(constantValue);
			daStruct.setOperator(actualOperationName);
			daStruct.setuOperator(operationName); // this is the unique operator so that it can be pegged
			daStruct.setContext(context);
			daStruct.setContextPart(contextPart);
			daStruct.setDefQuery(defQuery);

			// need to get the current select struct to add to this

			if (constantType.equalsIgnoreCase("string")) {
				daStruct.setType(PixelDataType.CONST_STRING);
				daStruct.setQuote(QUOTE.SINGLE);
			} else if (constantType.equalsIgnoreCase("double")) {
				daStruct.setType(PixelDataType.CONST_DECIMAL);
				daStruct.setQuote(QUOTE.NO);
			} else if (constantType.equalsIgnoreCase("long")) {
				daStruct.setType(PixelDataType.CONST_INT);
				daStruct.setQuote(QUOTE.NO);
			} else if (constantType.equalsIgnoreCase("date")) {
				daStruct.setType(PixelDataType.CONST_DATE);
				daStruct.setQuote(QUOTE.SINGLE);
			} else if (constantType.equalsIgnoreCase("timestamp")) {
				daStruct.setType(PixelDataType.CONST_TIMESTAMP);
				daStruct.setQuote(QUOTE.SINGLE);
			}

			operatorTableColumnParamIndex.put(tableColumnOperatorComposite, daStruct);
			if (context.length() > 0) {
				contextExpression.push(context);
			}
		} else {
			daStruct = operatorTableColumnParamIndex.get(tableColumnOperatorComposite);
		}
		List<GenExpression> allExpressions = new ArrayList<GenExpression>();
		// now add this gen expression to it
		if (paramToExpressionMap.containsKey(daStruct)) {
			allExpressions = paramToExpressionMap.get(daStruct);
		}
		allExpressions.add(exprToTrack);
		paramToExpressionMap.put(daStruct, allExpressions);
		paramStringToParamMap.put(tableColumnOperatorComposite, daStruct);

		uniqueCounter++;

		return tableColumnOperatorComposite;
	}

	/**
	 * Every parameter discovered while parsing.
	 *
	 * @return the parsed parameters
	 */
	public List<ParamStructDetails> getParams() {
		List<ParamStructDetails> allParams = new ArrayList<ParamStructDetails>();
		allParams.addAll(paramToExpressionMap.keySet());
		return allParams;
	}

	/**
	 * Record that an expression calls a function, so callers can later find or
	 * neutralize every call to it.
	 *
	 * @param functionName the function being called
	 * @param expr         the expression that calls it
	 */
	public void addFunctionExpression(String functionName, GenExpression expr) {
		List<GenExpression> exprList = null;
		if (functionExpressionMapper.containsKey(functionName)) {
			exprList = functionExpressionMapper.get(functionName);
		} else {
			exprList = new ArrayList<GenExpression>();
		}

		if (!exprList.contains(expr)) {
			exprList.add(expr);
		}

		functionExpressionMapper.put(functionName, exprList);
	}

	/**
	 * Trace a projection back down through the nested selects that produced it,
	 * following the name as it gets re-aliased at each level.
	 * <p>
	 * Known anomaly: the trace is wrong when a join condition precedes the filter
	 * condition.
	 *
	 * @param starter       the expression to search from
	 * @param name          the projection name to trace
	 * @param selectLineage accumulates the composite FROM items at each level, pass
	 *                      null to start
	 * @param columnLineage accumulates the matching selector at each level, pass
	 *                      null to start
	 * @param allInstances  accumulates every expression that references the name,
	 *                      pass null to start
	 * @param level         the current nesting depth, pass 0 to start
	 * @return {selectLineage, columnLineage, allInstances, deepest level reached}
	 */
	public static Object[] getLineage(GenExpression starter, String name,
			Map<Integer, List<GenExpression>> selectLineage, Map<Integer, GenExpression> columnLineage,
			List<GenExpression> allInstances, int level) {
		if (selectLineage == null) {
			selectLineage = new HashMap<Integer, List<GenExpression>>();
		}
		if (columnLineage == null) {
			columnLineage = new HashMap<Integer, GenExpression>();
		}

		if (allInstances == null) {
			allInstances = new ArrayList<GenExpression>();
		}

		GenExpression identifiedSelector = null;

		List<GenExpression> levelLineage = null;
		if (selectLineage.containsKey(level)) {
			levelLineage = selectLineage.get(level);
		} else {
			levelLineage = new ArrayList<GenExpression>();
		}

		String newName = name;
		// find if this is even there as the first projection
		if (!(starter instanceof OperationExpression)) // make sure this is not a union
		{
			for (int selectorIndex = 0; selectorIndex < starter.nselectors.size(); selectorIndex++) {
				// check to see if the selector is here if yes, then find the from
				// if the from is not a simple from then pass to that
				GenExpression curSelector = starter.nselectors.get(selectorIndex);

				String selectorAlias = curSelector.leftAlias;
				String selectorColumn = curSelector.getLeftExpr();

				// there is a possibility this is a functional expression
				if (curSelector instanceof FunctionExpression) {
					selectorColumn = getColumnFromFunctionExpression((FunctionExpression) curSelector, false);
				}

				// remove the quotes
				if (selectorAlias != null) {
					selectorAlias = selectorAlias.replace("`", "");
					selectorAlias = selectorAlias.replace("'", "");
					selectorAlias = selectorAlias.replace("\"", "");

					if (name.contentEquals(selectorAlias)) {
						allInstances.add(curSelector);
						identifiedSelector = curSelector;
						// if it is a function and the selector alias is set to null.. use the column
						// itself
						if (curSelector instanceof FunctionExpression) {
							newName = getColumnFromFunctionExpression((FunctionExpression) curSelector, false);
						} else if (selectorColumn != null) {
							newName = selectorColumn;
						} else {
							newName = selectorAlias;
						}

						columnLineage.put(level + 1, curSelector);

						break;
					}
				}
				// compare both
				if (name.contentEquals(selectorColumn)) {
					allInstances.add(curSelector);
					identifiedSelector = curSelector;
					if (curSelector instanceof FunctionExpression) {
						newName = getColumnFromFunctionExpression((FunctionExpression) curSelector, false);
					} else if (selectorColumn != null) {
						newName = selectorColumn;
					} else {
						newName = selectorAlias;
					}

					columnLineage.put(level + 1, curSelector);

					break;
				}
			}
		}

		// groupby
		// do I care about groupby ?
		// given we are neutralizing now..
		// I think we should add the groupby as well
		// the only reason is if possibly this was not there in the selector ?
		for (int groupIndex = 0; groupIndex < starter.ngroupBy.size(); groupIndex++) {
			GenExpression curSelector = starter.ngroupBy.get(groupIndex);
			String selectorAlias = curSelector.leftAlias;
			String selectorColumn = curSelector.getLeftExpr();

			// there is a possibility this is a functional expression
			if (curSelector instanceof FunctionExpression) {
				selectorColumn = getColumnFromFunctionExpression((FunctionExpression) curSelector, false);
			}

			if (selectorAlias != null) {
				// remove the quotes
				selectorAlias = selectorAlias.replace("`", "");
				selectorAlias = selectorAlias.replace("'", "");
				selectorAlias = selectorAlias.replace("\"", "");

				if (name.contentEquals(selectorAlias)) {
					allInstances.add(curSelector);
					if (identifiedSelector == null) {
						identifiedSelector = curSelector;
						classLogger.debug("Left expression {}", curSelector.getLeftExpr());
						if (curSelector instanceof FunctionExpression) {
							newName = getColumnFromFunctionExpression((FunctionExpression) curSelector, false);
						} else if (selectorColumn != null) {
							newName = selectorColumn;
						} else {
							newName = selectorAlias;
						}
						columnLineage.put(level + 1, curSelector);
					}
					break;
				}
			}
			// compare both
			if (name.contentEquals(selectorColumn)) {
				allInstances.add(curSelector);
				identifiedSelector = curSelector;
				if (curSelector instanceof FunctionExpression) {
					newName = getColumnFromFunctionExpression((FunctionExpression) curSelector, false);
				} else if (selectorColumn != null) {
					newName = selectorColumn;
				} else {
					newName = selectorAlias;
				}

				columnLineage.put(level + 1, curSelector);

				break;
			}

		}

		// filters
		// add the filters
		if (starter.filter != null) {
			List<GenExpression> filterExpressions = getSelectorInComposite(name, starter.filter, null);
			allInstances.addAll(filterExpressions);
		}
		// process the from to see if it is a table or a full select
		// if this is not a composite we are all set
		// we need to now do this for each of the gen expressions
		{
			if (starter.from != null && starter.from.composite) {
				// !levelLineage.contains(starter.from))
				levelLineage.add(starter.from);
			}
			// allInstances.add(starter);
		}

		// now come the joins
		for (int joinIndex = 0; joinIndex < starter.joins.size(); joinIndex++) {
			GenExpression curJoin = starter.joins.get(joinIndex);
			List<GenExpression> allColumns = getSelectorInComposite(name, curJoin.body, null);
			// find if this join is the one
			// if the body is composite
			if (curJoin.from.composite) {
				levelLineage.add(curJoin.from); // search for next level
			}
			allInstances.addAll(allColumns);
			// the body may be composite or the body may be simple
		}

		// TODO: Union

		// also process final query
		// need to track the name at each level as well as w

		// add these back
		selectLineage.put(level, levelLineage);

		// only the first branch is followed, so a name that resolves through more than
		// one composite FROM item traces down whichever came first
		if (!levelLineage.isEmpty()) {
			return getLineage(levelLineage.get(0), newName, selectLineage, columnLineage, allInstances, level + 1);
		}

		Object[] lineage = new Object[4];
		lineage[0] = selectLineage;
		lineage[1] = columnLineage;
		lineage[2] = allInstances;
		lineage[3] = level;

		return lineage;
	}

	/**
	 * Pull the single column a function wraps, descending through nested functions.
	 *
	 * @param expr  the function expression
	 * @param table true to qualify the column with its table
	 * @return the column name, or null when the function does not wrap exactly one
	 *         column
	 */
	public static String getColumnFromFunctionExpression(FunctionExpression expr, boolean table) {
		String retString = null;

		if (expr.expressions.size() == 1) {
			GenExpression gep = expr.expressions.get(0);

			if (gep instanceof FunctionExpression) {
				retString = getColumnFromFunctionExpression((FunctionExpression) gep, table);
			} else if (gep.operation != null && gep.operation.equalsIgnoreCase("column")) {
				String tableName = gep.tableName;
				retString = gep.getLeftExpr();
				if (table) {
					retString = tableName + "." + retString;
				}
			}
		}

		return retString;

	}

	/**
	 * Find every comparison inside a composite expression that references a given
	 * column, recursing through both sides of each operator.
	 *
	 * @param name         the column name or alias to look for
	 * @param qs           the composite expression to search
	 * @param selectorList accumulates the matches, pass null to start
	 * @return the comparisons that reference the name
	 */
	public static List<GenExpression> getSelectorInComposite(String name, GenExpression qs,
			List<GenExpression> selectorList) {
		if (selectorList == null) {
			selectorList = new ArrayList<GenExpression>();
		}

		List<GenExpression> nextIterator = new ArrayList<GenExpression>();

		if (qs.leftItem != null) {
			if (qs.leftItem instanceof GenExpression && !((GenExpression) qs.leftItem).composite
					&& ((GenExpression) qs.leftItem).getOperation().equalsIgnoreCase("Column")) {
				GenExpression leftItem = (GenExpression) qs.leftItem;
				// do the left alias magic

				// this is where we need to do the paranthesis again I think
				String selectorName = leftItem.leftAlias;
				if (selectorName == null) {
					selectorName = leftItem.getLeftExpr();
				}

				if (name.contentEquals(selectorName)) {
					selectorList.add(qs);
					if (selectorList.contains(qs.parent.rightItem)) {
						selectorList.add(qs.parent);
					}
				}
			} else if (qs.leftItem instanceof GenExpression) {
				nextIterator.add((GenExpression) qs.leftItem);
			}
		}

		if (qs.rightItem != null) {
			if (qs.rightItem instanceof GenExpression && !((GenExpression) qs.rightItem).composite
					&& ((GenExpression) qs.rightItem).getOperation().equalsIgnoreCase("Column")) {
				GenExpression rightItem = (GenExpression) qs.rightItem;
				// do the left alias magic

				// this is where we need to do the paranthesis again I think
				String selectorName = rightItem.leftAlias;
				if (selectorName == null) {
					selectorName = rightItem.getLeftExpr();
				}

				if (name.contentEquals(selectorName)) {
					selectorList.add(qs);
					if (selectorList.contains(qs.parent.leftItem)) {
						selectorList.add(qs.parent);
					}
				}
			} else if (qs.rightItem instanceof GenExpression) {
				nextIterator.add((GenExpression) qs.rightItem);
			}
		}

		if (nextIterator.size() > 0) {
			for (int iterIndex = 0; iterIndex < nextIterator.size(); iterIndex++) {
				getSelectorInComposite(name, nextIterator.get(iterIndex), selectorList);
			}
		}
		return selectorList;
	}

	/**
	 * Resolve a projection name to the physical table and column it comes from, by
	 * tracing it to the deepest select that still names it.
	 *
	 * @param starter        the expression to search from
	 * @param projectionName the projection name to resolve
	 * @return table.column, or null when the name cannot be traced to a column
	 */
	@SuppressWarnings("unchecked")
	public static String getPhysicalColumnName(GenExpression starter, String projectionName) {
		Object[] output = getLineage(starter, projectionName, null, null, null, 0);

		int level = (Integer) output[3] + 1;

		Map<Integer, GenExpression> columnLineage = (Map<Integer, GenExpression>) output[1];

		GenExpression selector = null;

		// find the latest columnLineage
		do {
			selector = columnLineage.get(level);
			level--;
		} while (selector == null && level >= 0);

		if (selector != null) {
			if (selector instanceof FunctionExpression) {
				return getColumnFromFunctionExpression((FunctionExpression) selector, true);
			} else {
				return selector.tableName + "." + selector.getLeftExpr();
			}
		}
		return null;
	}

	/**
	 * Mark every expression that references a projection as neutralized, at every
	 * level of nesting, so it renders as a no-op rather than being dropped.
	 *
	 * @param starter        the expression to search from
	 * @param projectionName the projection to neutralize
	 * @param neutralize     true to neutralize, false to restore
	 */
	@SuppressWarnings("unchecked")
	public static void neutralizeSelector(GenExpression starter, String projectionName, boolean neutralize) {
		Object[] output = getLineage(starter, projectionName, null, null, null, 0);

		List<GenExpression> instanceList = (List<GenExpression>) output[2];
		for (int instanceIndex = 0; instanceIndex < instanceList.size(); instanceIndex++) {
			GenExpression curExpression = instanceList.get(instanceIndex);
			curExpression.neutralize = neutralize;
		}
	}

	/**
	 * The physical columns every call to a function reads, falling back to the
	 * projection name when a column cannot be resolved.
	 *
	 * @param functionName the function to look up
	 * @return the columns its calls read, empty when the function is not used
	 */
	public List<String> getColumnsForFunction(String functionName) {
		List<String> retList = new ArrayList<String>();
		if (functionExpressionMapper.containsKey(functionName)) {
			List<GenExpression> allExprs = functionExpressionMapper.get(functionName);
			for (int exprIndex = 0; exprIndex < allExprs.size(); exprIndex++) {
				GenExpression curSelector = allExprs.get(exprIndex);
				String curName = null;

				if (curSelector instanceof FunctionExpression) {
					curName = getColumnFromFunctionExpression((FunctionExpression) curSelector, false);
				} else if (curSelector.leftAlias != null) {
					curName = curSelector.leftAlias;
				} else {
					curName = curSelector.getLeftExpr();
				}
				classLogger.debug("curName is set to {}", curName);
				String physicalName = getPhysicalColumnName(root, curName);
				classLogger.debug("Physical name {}", physicalName);

				if (physicalName != null) {
					retList.add(physicalName);
				} else {
					retList.add(curName);
				}
			}
		}

		return retList;
	}

	/**
	 * Mark every call to a function as neutralized, so it renders without the
	 * function wrapper.
	 *
	 * @param starter      unused, kept for symmetry with
	 *                     {@link #neutralizeSelector(GenExpression, String, boolean)}
	 * @param functionName the function to neutralize
	 * @param neutralize   true to neutralize, false to restore
	 */
	public void neutralizeFunction(GenExpression starter, String functionName, boolean neutralize) {
		if (functionExpressionMapper.containsKey(functionName)) {
			List<GenExpression> allExprs = functionExpressionMapper.get(functionName);
			for (int exprIndex = 0; exprIndex < allExprs.size(); exprIndex++) {
				FunctionExpression curSelector = (FunctionExpression) allExprs.get(exprIndex);
				curSelector.neutralizeFunction = neutralize;
			}
		}

	}

	/**
	 * Wrap one of a select's selectors in a function call, keeping whatever alias
	 * the selector already had.
	 *
	 * @param select       the expression for the select to modify
	 * @param selectorName the selector to wrap, matched by alias or column name
	 * @param functionName the function to wrap it in
	 */
	public void addFunctionToSelector(GenExpression select, String selectorName, String functionName) {
		GenExpression selector = null;
		for (int selectIndex = 0; selectIndex < select.nselectors.size(); selectIndex++) {
			GenExpression curSelector = select.nselectors.get(selectIndex);
			String selectorAlias = curSelector.leftAlias;
			String selectorColumn = curSelector.getLeftExpr();

			// there is a possibility this is a functional expression
			if (curSelector instanceof FunctionExpression) {
				selectorColumn = getColumnFromFunctionExpression((FunctionExpression) curSelector, false);
			}

			if ((selectorAlias != null && selectorName.equalsIgnoreCase(selectorAlias))
					|| selectorName.equalsIgnoreCase(selectorColumn)) {
				selector = curSelector;
				break;
			}
		}

		if (selector != null) {
			FunctionExpression funExpression = new FunctionExpression();
			funExpression.operation = "function";
			funExpression.setExpression(functionName);
			funExpression.expressions.add(selector);
			String alias = selector.leftAlias;
			if (alias == null) {
				alias = selector.getLeftExpr();
			}

			funExpression.leftAlias = alias;
			select.nselectors.remove(selector);
			select.nselectors.add(funExpression);
		}
	}

	/**
	 * Re-render a query with the incoming parameters left as placeholders and every
	 * other parameter filled back in with its original constant.
	 *
	 * @param originalQuery   the SQL to transform
	 * @param incomingStructs the parameters to leave as placeholders
	 * @return the transformed query, or null when it could not be parsed
	 */
	public static String transformQueryWithParams(String originalQuery, List<ParamStructDetails> incomingStructs) {
		String retQuery = null;
		try {
			SqlParser parse2 = new SqlParser();
			parse2.parameterize = true;

			GenExpressionWrapper wrapper = parse2.processQuery(originalQuery);
			for (int paramIndex = 0; paramIndex < incomingStructs.size(); paramIndex++) {
				ParamStructDetails thisStruct = incomingStructs.get(paramIndex);
				String paramStructDetailsKey = thisStruct.getParamKey();

				if (wrapper.operatorTableColumnParamIndex.containsKey(paramStructDetailsKey)) {
					ParamStructDetails targetStruct = wrapper.operatorTableColumnParamIndex.get(paramStructDetailsKey);
					// remove this struct from the overall so it wont fill
					wrapper.paramToExpressionMap.remove(targetStruct);
				}
			}
			wrapper.fillParameters();
			retQuery = GenExpression.printQS(wrapper.root, null) + "";
		} catch (Exception e) {
			classLogger.error("Failed to apply the parameters to query {}", originalQuery, e);
		}

		return retQuery;
	}

	/**
	 * Transform the query and replace the param struct with the user defined param
	 * names via the lookup.
	 *
	 * @param originalQuery   the SQL to transform
	 * @param incomingStructs the parameters the user defined
	 * @param detailsLookup   each incoming struct to the ParamStruct holding its
	 *                        user facing name and display type
	 * @return the transformed query
	 * @throws Exception if the SQL cannot be parsed
	 */
	public static String transformQueryWithParams(String originalQuery, List<ParamStructDetails> incomingStructs,
			Map<ParamStructDetails, ParamStruct> detailsLookup) throws Exception {
		String retQuery = null;
		SqlParser parse2 = new SqlParser();
		parse2.parameterize = true;

		GenExpressionWrapper wrapper = parse2.processQuery(originalQuery);
		wrapper.fillParameters(incomingStructs, detailsLookup);
		retQuery = GenExpression.printQS(wrapper.root, null) + "";

		return retQuery;
	}

	/**
	 * Every generated placeholder in the query, mapped to the value it currently
	 * stands for. Given
	 *
	 * <pre>
	 * SELECT actor_name, title, gender FROM actor
	 * WHERE gender &gt; &lt;actor_genderand0_left&gt;
	 *   AND title IN (SELECT title FROM mv WHERE director = &lt;mv_directorand1_left=&gt;)
	 *   AND actor_name IN (&lt;actor_actor_namein2&gt;)
	 * </pre>
	 *
	 * the keys are actor_genderand0_left, mv_directorand1_left= and
	 * actor_actor_namein2.
	 *
	 * @return placeholder name to its current value
	 */
	public Map<String, Object> getAllParamNames() {
		Map<String, Object> retList = new HashMap<String, Object>();

		Iterator<String> paramKeys = paramStringToParamMap.keySet().iterator();
		while (paramKeys.hasNext()) {
			String paramName = paramKeys.next();
			retList.put(paramName, paramStringToParamMap.get(paramName).getCurrentValue());
		}

		return retList;
	}

	/**
	 * The value a placeholder currently stands for, which the front end can
	 * substitute directly.
	 *
	 * @param paramName a placeholder name, e.g. actor_genderand0_left
	 * @return its current value, or null when the placeholder is unknown
	 */
	public Object getCurrentValueOfParam(String paramName) {
		ParamStructDetails daStruct = paramStringToParamMap.get(paramName);
		if (daStruct != null) {
			return daStruct.getCurrentValue();
		}
		return null;
	}

	/**
	 * Set the value a placeholder stands for. The new value takes effect on the
	 * next call to {@link #fillParameters()}.
	 *
	 * @param paramName a placeholder name, e.g. actor_genderand0_left
	 * @param value     the new value
	 * @return true when the placeholder was found and updated
	 */
	public boolean setCurrentValueOfParam(String paramName, Object value) {
		ParamStructDetails daStruct = paramStringToParamMap.get(paramName);
		if (daStruct != null) {
			daStruct.setCurrentValue(value);
			return true;
		}
		return false;
	}

	/**
	 * A query that lists the values a placeholder could take, for the front end to
	 * offer as options.
	 *
	 * @param paramName a placeholder name, e.g. actor_genderand0_left
	 * @return the options query, or null when the placeholder is unknown
	 */
	public String getQueryForParam(String paramName) {
		ParamStructDetails daStruct = paramStringToParamMap.get(paramName);
		if (daStruct != null) {
			return daStruct.getDefQuery();
		}
		return null;
	}

	/**
	 * Render the expression tree back out as SQL.
	 *
	 * @param validate true to prove the result by parsing it back
	 * @return the rendered query
	 * @throws Exception if the tree cannot be rendered, or if validate is set and
	 *                   the result does not parse
	 */
	public String generateQuery(boolean validate) throws Exception {
		String finalQuery = GenExpression.printQS(root, new StringBuffer()).toString();
		classLogger.debug("Generated query {}", finalQuery);

		// the real test is can I parse it back
		if (validate) {
			CCJSqlParserUtil.parse(finalQuery);
		}
		return finalQuery;
	}

}

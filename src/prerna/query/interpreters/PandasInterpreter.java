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
package prerna.query.interpreters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.py.PandasFrame;
import prerna.ds.py.PandasSyntaxHelper;
import prerna.ds.py.PyTranslator;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.BetweenQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter.FILTER_TYPE;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.IQuerySelector.SELECTOR_TYPE;
import prerna.query.querystruct.selectors.IQuerySort;
import prerna.query.querystruct.selectors.QueryArithmeticSelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector.ORDER_BY_DIRECTION;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.selectors.QueryIfSelector;
import prerna.reactor.IReactor;
import prerna.reactor.qs.SubQueryExpression;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.util.Utility;

public class PandasInterpreter extends AbstractQueryInterpreter {

	private static final Logger classLogger = LogManager.getLogger(PandasInterpreter.class);

	private String frameName = null;
	private String wrapperFrameName = null;

	private Map<String, SemossDataType> colDataTypes;

	private StringBuilder selectorCriteria;
	private StringBuilder filterCriteria;
	private StringBuilder havingCriteria;
	private StringBuilder renameCriteria = new StringBuilder("");
	private StringBuilder groupCriteria = new StringBuilder("");
	private StringBuilder dateCriteria = new StringBuilder("");
	private StringBuilder arithmeticCriteria = new StringBuilder();
	private StringBuilder caseWhenCriteria = new StringBuilder();
	private StringBuilder aggCriteria = new StringBuilder("");
	private StringBuilder aggCriteria2 = new StringBuilder("");
	private StringBuilder orderBy = new StringBuilder("");
	private StringBuilder ascending = new StringBuilder("");
	private StringBuilder overrideQuery = null;

	private StringBuilder normalizer = new StringBuilder(".to_dict('split')['data']");

	private boolean isHavingFilter;
	private List<StringBuilder> havingList = new ArrayList<StringBuilder>();

	private static final List<String> DATE_FUNCTION_LIST = new ArrayList<String>(5);
	static {
		DATE_FUNCTION_LIST.add(QueryFunctionHelper.YEAR);
		DATE_FUNCTION_LIST.add(QueryFunctionHelper.QUARTER);
		DATE_FUNCTION_LIST.add(QueryFunctionHelper.MONTH_NAME);
		DATE_FUNCTION_LIST.add(QueryFunctionHelper.WEEK);
		DATE_FUNCTION_LIST.add(QueryFunctionHelper.DAY_NAME);
	}

	private Map<String, StringBuilder> dateHash = null;
	private Map<String, StringBuilder> arithmeticHash = null;
	private Map<String, StringBuilder> caseWhenHash = null;
	private List<String> dateKeys = null;
	private List<String> arithmeticKeys = null;
	private List<String> caseWhenKeys = null;
	private int caseWhenCount;
	private List<StringBuilder> renameColList = null;

	private Map<String, StringBuilder> aggHash = null;
	private Map<String, StringBuilder> aggHash2 = null;
	private Map<String, StringBuilder> orderHash = null;
	private Map<String, SemossDataType> typesHash = null;
	private Map<String, String> aliasHash = null;

	// Experiment
	private List<String> caseWhenFunctionList = null;
	private boolean caseWhenFunction;

	private Map<String, String> functionMap = null;

	Map<String, Boolean> processedSelector = new HashMap<>();

	ArrayList<String> aggKeys = new ArrayList<>();

	ArrayList<String> headers = null;

	// this is the headers being kept on the array list being generated
	ArrayList<String> actHeaders = null;

	int groupIndex = 0;

	ArrayList<SemossDataType> types = null;

	long start = 0;
	long end = 500;

	PyTranslator pyt = null;

	boolean scalar = false;

	// this is because we need to handle subquery
	private transient PandasFrame pandasFrame;

	/**
	 * Registers the column -> SemossDataType map used throughout query building to
	 * decide type-specific pandas syntax (e.g. string vs. date vs. numeric
	 * filters). Immediately normalizes the keys via {@link #updateTypes()}.
	 */
	public void setDataTypeMap(Map<String, SemossDataType> dataTypeMap) {
		this.colDataTypes = dataTypeMap;
		updateTypes();
	}

	/**
	 * Normalizes the keys of {@code colDataTypes} by stripping any "table__column"
	 * qualifier down to the bare column name, so later lookups (which use plain
	 * pandas column names) resolve correctly. Does not emit query text itself.
	 */
	private void updateTypes() {
		Map<String, SemossDataType> newTypesMap = new HashMap<>();
		for (String k : this.colDataTypes.keySet()) {
			String newK = null;
			if (k.contains("__")) {
				newK = k.split("__")[1];
			} else {
				newK = k;
			}

			newTypesMap.put(newK, this.colDataTypes.get(k));
		}
		this.colDataTypes = newTypesMap;
	}

	/**
	 * Returns whether the composed query collapses to a single scalar value (set
	 * during {@link #closeAll()} for a lone aggregate with no group by), which
	 * changes how the result is normalized/serialized.
	 */
	public boolean isScalar() {
		return scalar;
	}

	/** Sets the source PandasFrame, needed to execute nested subquery filters. */
	public void setPandasFrame(PandasFrame pandasFrame) {
		this.pandasFrame = pandasFrame;
	}

	/**
	 * Builds and returns the complete single-line pandas expression for the
	 * SelectQueryStruct. After resetting all fragment buffers, it calls
	 * {@link #fillParts()} to populate each criteria fragment and
	 * {@link #closeAll()} to finalize them, then concatenates the fragments onto
	 * the frame wrapper ({@code wrapperFrameName}, which resolves to
	 * {@code ...cache['data']}) in execution order:
	 * {@code <frame> + dateCriteria + arithmeticCriteria + filterCriteria +
	 * havingCriteria + groupCriteria + aggCriteria2 + caseWhenCriteria +
	 * renameCriteria + selectorCriteria + [.drop_duplicates()] + orderBy +
	 * .iloc[start:end] + .to_dict('split')}. Honors LIMIT/OFFSET from the query
	 * struct and returns any {@code overrideQuery} verbatim when one was supplied.
	 *
	 * @return the full pandas query string to evaluate against the DataFrame
	 */
	@Override
	public String composeQuery() {
		StringBuilder query = new StringBuilder();

		headers = new ArrayList<>();
		groupIndex = 0;
		actHeaders = new ArrayList<>();
		types = new ArrayList<>();
		selectorCriteria = new StringBuilder("");
		groupCriteria = new StringBuilder("");
		aggCriteria = new StringBuilder("");
		filterCriteria = new StringBuilder("");
		havingCriteria = new StringBuilder("");
		scalar = false;
		functionMap = new HashMap<>();
		aggHash = new HashMap<>();
		aggKeys = new ArrayList<>();
		aggHash2 = new HashMap<>();
		typesHash = new HashMap<>();
		orderHash = new HashMap<>();
		aliasHash = new HashMap<>();
		orderBy = new StringBuilder("");
		normalizer = new StringBuilder(".to_dict('split')");
		ascending = new StringBuilder("");

		dateCriteria = new StringBuilder("");
		dateHash = new HashMap<>();
		dateKeys = new ArrayList<>();
		arithmeticCriteria = new StringBuilder("");
		arithmeticHash = new HashMap<>();
		arithmeticKeys = new ArrayList<>();
		caseWhenCriteria = new StringBuilder("");
		caseWhenHash = new HashMap<>();
		caseWhenFunctionList = new ArrayList<>();
		caseWhenKeys = new ArrayList<>();
		renameColList = new ArrayList<>();

		caseWhenCount = 0;

		long limit = 500;
		start = 0;
		end = limit;

		if (((SelectQueryStruct) this.qs).getOffset() > 0) {
			start = ((SelectQueryStruct) this.qs).getOffset();
		}
		if (((SelectQueryStruct) this.qs).getLimit() != 0) {
			end = (start + ((SelectQueryStruct) this.qs).getLimit());
		}

		fillParts();
		closeAll();

		StringBuilder cachedFrame = new StringBuilder(wrapperFrameName);

		if (overrideQuery == null) {
			query.append(cachedFrame).append(dateCriteria).append(arithmeticCriteria).append(this.filterCriteria)
					.append(this.havingCriteria).append(this.groupCriteria).append(this.aggCriteria2)
					.append(caseWhenCriteria).append(renameCriteria).append(this.selectorCriteria);

			if (!scalar && aggCriteria2.toString().isEmpty()) {
				query.append(addDistinct(((SelectQueryStruct) this.qs).isDistinct()));
			}
			query.append(scalar ? "" : orderBy).append(addLimitOffset(start, end)).append(normalizer);
		} else {
			query = overrideQuery;
			if (actHeaders != null && actHeaders.size() > 0) {
				headers = actHeaders;
			}
		}
		return query.toString();
	}

	/**
	 * Populates every query fragment buffer from the SelectQueryStruct. For each
	 * query part (FILTER, HAVING, SELECT, SORT, AGGREGATE, GROUP), it either copies
	 * a pre-built fragment supplied directly in the query struct's part map, or
	 * generates it by delegating: {@link #addFilters()} -> {@code filterCriteria},
	 * {@link #addHavings()} -> {@code havingCriteria}, {@link #addSelectors()} ->
	 * {@code selectorCriteria}, {@link #processOrderBy()} -> {@code orderBy},
	 * {@link #genAggString()} -> {@code aggCriteria2},
	 * {@link #processGroupSelectors()} -> {@code groupCriteria}. Finally it always
	 * runs {@link #genIfElseString()}, {@link #genDateFunctionString()} and
	 * {@link #genArithmeticString()} to assemble the {@code .assign(...)} fragments
	 * for CASE WHEN, date-part and arithmetic derived columns.
	 */
	private void fillParts() {
		SelectQueryStruct sqs = (SelectQueryStruct) qs;
		Map partMap = sqs.getParts();

		if (partMap.containsKey(SelectQueryStruct.Query_Part.QUERY)) {
			overrideQuery = new StringBuilder(partMap.get(SelectQueryStruct.Query_Part.QUERY) + "");
		}
		if (partMap.containsKey(SelectQueryStruct.Query_Part.FILTER)) {
			filterCriteria = new StringBuilder(partMap.get(SelectQueryStruct.Query_Part.FILTER) + "");
		} else {
			addFilters();
		}
		if (partMap.containsKey(SelectQueryStruct.Query_Part.HAVING)) {
			havingCriteria = new StringBuilder(partMap.get(SelectQueryStruct.Query_Part.HAVING) + "");
		} else {
			addHavings();
		}
		if (partMap.containsKey(SelectQueryStruct.Query_Part.SELECT)) {
			selectorCriteria = new StringBuilder(partMap.get(SelectQueryStruct.Query_Part.SELECT) + "");
		} else {
			addSelectors();
		}

		if (partMap.containsKey(SelectQueryStruct.Query_Part.SORT)) {
			orderBy = new StringBuilder(partMap.get(SelectQueryStruct.Query_Part.SORT) + "");
		} else {
			processOrderBy();
		}
		if (partMap.containsKey(SelectQueryStruct.Query_Part.AGGREGATE)) {
			aggCriteria2 = new StringBuilder(partMap.get(SelectQueryStruct.Query_Part.AGGREGATE) + "");
		} else {
			genAggString();
		}
		if (partMap.containsKey(SelectQueryStruct.Query_Part.GROUP)) {
			groupCriteria = new StringBuilder(partMap.get(SelectQueryStruct.Query_Part.GROUP) + "");
		} else {
			processGroupSelectors();
		}
		genIfElseString();
		genDateFunctionString();
		genArithmeticString();
	}

	/**
	 * Emits the SQL DISTINCT fragment: returns {@code .drop_duplicates()} when the
	 * query is distinct, otherwise an empty string. Appended by
	 * {@link #composeQuery()} right after the selector projection.
	 *
	 * @param distinct whether the query struct requested distinct rows
	 * @return {@code ".drop_duplicates()"} or {@code ""}
	 */
	private String addDistinct(boolean distinct) {
		if (distinct) {
			return ".drop_duplicates()";
		}
		return "";
	}

	/**
	 * Finalizes/wraps the partially built fragment buffers into valid pandas syntax
	 * before {@link #composeQuery()} concatenates them. Specifically it: closes the
	 * aggregation into {@code .agg(...).reset_index()} (or a random-column
	 * {@code .assign(tmp=0).groupby('tmp')...} wrapper when there is no group by
	 * but multiple headers, or collapses to a scalar for a single header); wraps
	 * the selector list into the projection {@code [[ ... ]]} and builds the
	 * {@code .rename(columns={...})} fragment; wraps having into
	 * {@code .groupby([...]).filter(lambda x: ...)}; closes group by with
	 * {@code ], sort=False)} (plus {@code .count().reset_index()} when only group
	 * columns are selected); wraps the filter mask into {@code .loc[ ... ]}; and
	 * closes order by into {@code sort_values([...], ascending=[...])}. Skips any
	 * part that was supplied pre-built in the query struct's part map.
	 */
	public void closeAll() {
		SelectQueryStruct sqs = (SelectQueryStruct) qs;
		Map partMap = sqs.getParts();

		if (this.aggCriteria2.toString().length() > 0 && !partMap.containsKey(SelectQueryStruct.Query_Part.AGGREGATE)) {
			if (!((SelectQueryStruct) this.qs).getGroupBy().isEmpty()) {
				this.aggCriteria = aggCriteria.append("})").append(".reset_index()");
				this.aggCriteria2 = aggCriteria2.append(")").append(".reset_index()");
			} else if (headers.size() > 1) {
				String tempCol = Utility.getRandomString(6);
				this.aggCriteria2 = new StringBuilder(".assign(" + tempCol + "=0).groupby('" + tempCol + "')")
						.append(aggCriteria2).append(").reset_index().drop('" + tempCol + "',axis=1)");
			} else if (headers.size() == 1) {
				this.aggCriteria = aggCriteria.append("}).reset_index()");
				this.aggCriteria2 = aggCriteria2.append(").reset_index()");
				normalizer = new StringBuilder(".to_dict('split')['data'][0][1]");
				aggCriteria2 = aggCriteria;
				scalar = true;
			}
		}

		if (this.selectorCriteria.length() > 0 && !partMap.containsKey(SelectQueryStruct.Query_Part.SELECT)
				&& !scalar) {
			StringBuilder tempSelectorBuilder = new StringBuilder("[[");
			this.selectorCriteria = tempSelectorBuilder.append(this.selectorCriteria).append("]]");

			if (!renameColList.isEmpty()) {
				renameCriteria.append(".rename(columns={");
				for (int i = 0; i < renameColList.size(); i++) {
					if (i == 0) {
						renameCriteria.append(renameColList.get(i));
					} else {
						renameCriteria.append(",").append(renameColList.get(i));
					}
				}
				renameCriteria.append("})");
			}

		} else if (!partMap.containsKey(SelectQueryStruct.Query_Part.SELECT)) {
			this.selectorCriteria.delete(0, selectorCriteria.length());
		}

		if (havingCriteria.length() > 0 && !partMap.containsKey(SelectQueryStruct.Query_Part.HAVING)) {
			StringBuilder tempGroupCriteria = new StringBuilder(groupCriteria);
			havingCriteria = tempGroupCriteria.append("]).filter(lambda x: ").append(havingCriteria).append(")");
		}

		if (groupCriteria.length() > 0 && !partMap.containsKey(SelectQueryStruct.Query_Part.GROUP)) {
			groupCriteria.append("], sort=False)");
			// when doing a group by, it creates a different object
			List<IQuerySelector> groupSelectors = ((SelectQueryStruct) this.qs).getGroupBy();
			if (actHeaders.size() == groupSelectors.size()) {
				// to convert to dataframe we need to append
				groupCriteria.append(".count().reset_index()");
			}
		}
		if (filterCriteria.length() > 0 && !partMap.containsKey(SelectQueryStruct.Query_Part.FILTER)) {
			filterCriteria = new StringBuilder(".loc[").append(filterCriteria).append("]");
		}
		if (orderBy.length() != 0 && !partMap.containsKey(SelectQueryStruct.Query_Part.SORT)) {
			// combine it
			orderBy.append("],").append(ascending).append("])");
		}
	}

	/**
	 * Builds the {@code orderBy} fragment ({@code .sort_values([...],
	 * ascending=[...])}) from the query struct's combined ORDER BY selectors. For
	 * each column sort it resolves the alias to the actual output column name, maps
	 * ASC/DESC to {@code True}/{@code False}, and delegates to
	 * {@link #addOrder(StringBuilder, String)} to accumulate the column and
	 * ascending lists.
	 */
	private void processOrderBy() {
		List<IQuerySort> qcos = ((SelectQueryStruct) this.qs).getCombinedOrderBy();
		for (int orderIndex = 0; orderIndex < qcos.size(); orderIndex++) {
			IQuerySort sortOp = qcos.get(orderIndex);
			if (sortOp.getQuerySortType() == IQuerySort.QUERY_SORT_TYPE.COLUMN) {
				QueryColumnOrderBySelector orderBy = (QueryColumnOrderBySelector) sortOp;
				String sort = null;
				String alias = orderBy.getAlias();
				if (alias.length() == 0) {
					alias = orderBy.getTable();
				}
				ORDER_BY_DIRECTION sortDir = orderBy.getSortDir();
				if (sortDir == ORDER_BY_DIRECTION.ASC) {
					sort = "True";
				} else if (sortDir == ORDER_BY_DIRECTION.DESC) {
					sort = "False";
				}
				StringBuilder orderByClause = null;
				if (orderHash.containsKey(alias)) {
					orderByClause = orderHash.get(alias);
				}

				if (orderByClause == null && aliasHash.containsKey(alias)) {
					orderByClause = new StringBuilder("'" + aliasHash.get(alias) + "'");
				}

				if (orderByClause != null) {
					// check if it is aggregate
					// at this point the alias does it
					// addOrder(orderByClause, sort);
					if (orderHash.containsKey(alias)) {
						addOrder(new StringBuilder(alias), sort);
					} else {
						addOrder(new StringBuilder(aliasHash.get(alias)), sort);
					}
				}
			}
		}
	}

	/**
	 * Appends one column to the {@code orderBy} fragment, opening it as
	 * {@code .sort_values(['col'} on the first call and comma-separating subsequent
	 * columns, while accumulating the matching {@code ascending=[...]} list (closed
	 * later in {@link #closeAll()}).
	 *
	 * @param curOrder the output column name to sort on
	 * @param asc      {@code "True"} for ascending, {@code "False"} for descending
	 */
	private void addOrder(StringBuilder curOrder, String asc) {
		// I need to find out which are the pieces I need to drop
		if (orderBy.length() == 0) {
			orderBy = new StringBuilder(".sort_values([");
			ascending = new StringBuilder("ascending=[");
		} else {
			orderBy.append(",");
			ascending.append(",");
		}

		// add the ascending
		ascending.append(asc);

		// add the order by
		orderBy.append("'").append(curOrder).append("'");
	}

	/**
	 * Builds the LIMIT/OFFSET fragment {@code .iloc[start:end]} (the trailing end
	 * index is omitted when {@code end <= 0}, meaning "no upper bound"). Appended
	 * near the end of the query by {@link #composeQuery()}.
	 *
	 * @param start row offset (inclusive)
	 * @param end   exclusive end row, or {@code <= 0} for open-ended
	 * @return the {@code .iloc[...]} slice fragment
	 */
	private String addLimitOffset(long start, long end) {
		StringBuilder sb = new StringBuilder();
		sb.append(".iloc[" + start + ":");
		if (end > 0) {
			sb.append(end);
		}
		sb.append("]");
		return sb.toString();
	}

	/**
	 * Convenience entry point that builds the WHERE-clause {@code filterCriteria}
	 * (the boolean mask later wrapped as {@code .loc[...]}) from the query struct's
	 * combined filters, delegating to
	 * {@link #addFilters(List, String, StringBuilder, boolean)}.
	 */
	public void addFilters() {
		addFilters(qs.getCombinedFilters().getFilters(), this.wrapperFrameName, this.filterCriteria, false);
	}

	/**
	 * Convenience entry point that builds the HAVING-clause {@code havingCriteria}
	 * (later wrapped as {@code .groupby([...]).filter(lambda x: ...)}) from the
	 * query struct's having filters, delegating to
	 * {@link #addHavingFilters(List, String, StringBuilder, boolean)}.
	 */
	public void addHavings() {
		addHavingFilters(qs.getHavingFilters().getFilters(), this.wrapperFrameName, this.havingCriteria, false);
	}

	/**
	 * Builds the {@code selectorCriteria} fragment, i.e. the comma-separated list
	 * of quoted column names that {@link #closeAll()} wraps into the SELECT
	 * projection {@code [['col1','col2', ...]]}. Each selector is resolved via
	 * {@link #processSelector} (which may also register date/arithmetic/case-when
	 * derived columns), and the method tracks output headers and their
	 * SemossDataTypes so the produced pandas result stays in sync with PandasFrame.
	 */
	public void addSelectors() {
		this.selectorCriteria = new StringBuilder();
		List<IQuerySelector> selectors = qs.getSelectors();

		for (int i = 0; i < selectors.size(); i++) {
			IQuerySelector selector = selectors.get(i);
			SELECTOR_TYPE selectorType = selector.getSelectorType();
			String newHeader = processSelector(selector, wrapperFrameName, true, true);

			if (i == 0) {
				this.selectorCriteria.append(newHeader);
			} else {
				this.selectorCriteria.append(",").append(newHeader);
			}

			StringBuilder sb = new StringBuilder(newHeader);
			newHeader = newHeader.replace("'", "");
			headers.add(newHeader);
			orderHash.put(newHeader, sb);

			if (selectorType == IQuerySelector.SELECTOR_TYPE.FUNCTION && ((QueryFunctionSelector) selector)
					.getFunction().equalsIgnoreCase(QueryFunctionHelper.UNIQUE_GROUP_CONCAT)) {
				actHeaders.add(((QueryFunctionSelector) selector).getAllQueryColumns().get(0).getAlias());
			} else {
				actHeaders.add(newHeader);
			}
			if (selectorType == IQuerySelector.SELECTOR_TYPE.COLUMN) {
				SemossDataType curType = this.colDataTypes.get(((QueryColumnSelector) selector).getColumn());
				typesHash.put(newHeader, curType);
			}
		}
		for (String header : headers) {
			if (typesHash.containsKey(header)) {
				types.add(typesHash.get(header));
			}
		}
	}

	/**
	 * Returns the output column headers collected while building the SELECT
	 * projection.
	 */
	public String[] getHeaders() {
		if (headers != null) {
			String[] headerArray = new String[this.headers.size()];
			this.headers.toArray(headerArray);
			return headerArray;
		}
		return null;
	}

	/**
	 * Returns the SemossDataTypes of the output columns, aligned with
	 * {@link #getHeaders()}.
	 */
	public SemossDataType[] getTypes() {
		if (headers != null) {
			SemossDataType[] typeArray = new SemossDataType[this.headers.size()];
			this.types.toArray(typeArray);
			return typeArray;
		}
		return null;
	}

	/**
	 * Central dispatcher that resolves a single selector to the pandas text used to
	 * reference/derive it, routing by selector type to the specialized processor:
	 * COLUMN -> {@link #processColumnSelector}, CONSTANT ->
	 * {@link #processConstantSelector}, FUNCTION ->
	 * {@link #processFunctionSelector} (aggregate or date-part), ARITHMETIC ->
	 * {@link #processArithmeticSelector}, IF_ELSE ->
	 * {@link #processIfElseSelector}. Returns the fragment/column reference
	 * (typically a quoted column name like {@code 'col'} or a {@code frame['col']}
	 * accessor) that callers splice into larger fragments.
	 *
	 * @param useTable when {@code true}, qualifies the reference with the
	 *                 frame/table name
	 * @return the pandas reference/expression for this selector, or {@code null} if
	 *         the type is unsupported
	 */
	public String processSelector(IQuerySelector selector, String tableName, boolean includeTableName, boolean useAlias,
			boolean... useTable) {
		SELECTOR_TYPE selectorType = selector.getSelectorType();
		String tableNameForCol = null;

		if (useTable != null && useTable.length > 0 && useTable[0]) {
			tableNameForCol = tableName;
		}

		if (selector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.COLUMN) {
			return processColumnSelector((QueryColumnSelector) selector, tableNameForCol);
		} else if (selectorType == IQuerySelector.SELECTOR_TYPE.CONSTANT) {
			return processConstantSelector((QueryConstantSelector) selector);
		} else if (selectorType == IQuerySelector.SELECTOR_TYPE.FUNCTION) {
			return processFunctionSelector((QueryFunctionSelector) selector, tableName);
		} else if (selectorType == IQuerySelector.SELECTOR_TYPE.ARITHMETIC) {
			return processArithmeticSelector((QueryArithmeticSelector) selector, tableName, includeTableName, useAlias,
					useTable);
		} else if (selectorType == IQuerySelector.SELECTOR_TYPE.IF_ELSE) {
			return processIfElseSelector((QueryIfSelector) selector, tableName);
		} else {
			return null;
		}
	}

	/**
	 * Registers a date-part extraction derived column and contributes one clause to
	 * the {@code dateCriteria} fragment. Emits (into {@code dateHash}, keyed by
	 * alias) the assignment
	 * {@code <alias>=<col>.apply(pd.to_datetime).dt.<field>.values} for the
	 * requested field (year/quarter/month/week/day), which
	 * {@link #genDateFunctionString()} later wraps into
	 * {@code .assign(<alias>=..., ...)}. Returns the quoted alias {@code 'alias'}
	 * so the derived column can be projected/referenced.
	 *
	 * @param selector  the date function selector (function + inner column)
	 * @param tableName the frame/table reference for the underlying column
	 */
	private String processDateFunctionSelector(QueryFunctionSelector selector, String tableName) {
		IQuerySelector innerSelector = selector.getInnerSelector().get(0);
		StringBuilder sb = new StringBuilder();
		String functionName = selector.getFunction();
		String pandasFunction = QueryFunctionHelper.convertFunctionToPandasSyntax(functionName);
		String alias = selector.getAlias();
		String columnName = processSelector(innerSelector, tableName, true, true, true);

		dateKeys.add(alias);
		sb.append(alias).append("=").append(columnName).append(".apply(pd.to_datetime).").append(pandasFunction)
				.append(".values");

		dateHash.put(alias, sb);
		typesHash.put(alias, SemossDataType.STRING);

		// Add to functionMap. I need an example of this working, for processAgg and
		// processDAte
		functionMap.put(pandasFunction + columnName, selector.getAlias());
		return "'" + alias.toString() + "'";
	}

	/**
	 * Assembles the {@code dateCriteria} fragment by joining every per-alias date
	 * clause registered by {@link #processDateFunctionSelector} into a single
	 * {@code .assign(<alias1>=..., <alias2>=..., ...)} call (empty if no date-part
	 * selectors were used).
	 */
	private void genDateFunctionString() {
		for (String key : dateKeys) {
			if (dateHash.containsKey(key)) {
				if (dateCriteria.length() != 0) {
					dateCriteria.append(",");
				}
				dateCriteria.append(dateHash.get(key));
			}
		}

		if (dateCriteria.length() > 0) {
			dateCriteria = new StringBuilder(".assign(").append(dateCriteria).append(")");
		}
	}

	/**
	 * Routes a FUNCTION selector to the right builder: a date-part function
	 * (year/quarter/month/week/day) goes to {@link #processDateFunctionSelector}
	 * (contributing to {@code dateCriteria}), anything else is treated as an
	 * aggregation and goes to {@link #processAggSelector} (contributing to
	 * {@code aggCriteria2}). Returns the quoted alias for the derived column.
	 *
	 * @param selector  the function selector
	 * @param tableName the frame/table reference
	 */
	private String processFunctionSelector(QueryFunctionSelector selector, String tableName) {
		if (DATE_FUNCTION_LIST.contains(selector.getFunction())) {
			return processDateFunctionSelector(selector, tableName);
		} else {
			return processAggSelector(selector);
		}
	}

	/**
	 * Translates a SQL CASE WHEN (IF_ELSE) selector into a derived column that will
	 * become part of the {@code caseWhenCriteria} fragment. Builds the per-alias
	 * lambda body {@code <precedent> if(<condition>) else <antecedent-or-np.nan>}
	 * (using {@link #processFilter(IQueryFilter, String)} for the condition and
	 * {@link #processSelector} for the value branches) and stores it in
	 * {@code caseWhenHash}; {@link #genIfElseString()} later wraps these into
	 * {@code .assign(<alias>=<frame>.apply(lambda x: ... , axis=1).values)}.
	 * Handles column and function selectors and column-to-values / values-to-column
	 * conditions; column-to-column and arithmetic conditions are not yet supported.
	 * Returns the quoted alias for the new column.
	 *
	 * @param selector  the CASE WHEN selector
	 * @param tableName the frame/table reference
	 */
	private String processIfElseSelector(QueryIfSelector selector, String tableName) {
		IQueryFilter filter = selector.getCondition();
		IQuerySelector precedent = selector.getPrecedent();
		IQuerySelector antecedent = selector.getAntecedent();
		caseWhenFunction = false;

		String alias = selector.getAlias();
		if (alias.length() == 0) {
			alias = "CASE_WHEN_" + caseWhenCount;
			caseWhenCount += 1;
		}

		StringBuilder filterBuilder = new StringBuilder(processSelector(precedent, tableName, false, false, false));
		filterBuilder.append(" if(").append(processFilter(filter, tableName)).append(") else ");

		if (antecedent != null) {
			filterBuilder.append(processSelector(antecedent, tableName, false, false, false));
		} else {
			filterBuilder.append("np.nan");
		}
		if (caseWhenFunction) {
			caseWhenFunctionList.add(alias);
			caseWhenFunction = false;
		}

		caseWhenKeys.add(alias);
		caseWhenHash.put(alias, filterBuilder);

		typesHash.put(alias, SemossDataType.convertStringToDataType(selector.getDataType()));

		return "'" + alias + "'";
	}

	/**
	 * Assembles the {@code caseWhenCriteria} fragment from the per-alias CASE WHEN
	 * bodies registered by {@link #processIfElseSelector}, producing
	 * {@code .assign(<alias>=<frame>.apply(lambda x: <val> if(<cond>) else <val>,
	 * axis=1).values, ...)}. For aggregate-backed case-when columns it applies the
	 * lambda over the grouped/aggregated frame instead of the raw frame (empty if
	 * no CASE WHEN selectors were used).
	 */
	private void genIfElseString() {
		for (String key : caseWhenKeys) {
			// Map<String, String> tempHash = caseWhen.get(key);
			if (caseWhenHash.containsKey(key)) {
				if (caseWhenCriteria.length() > 0) {
					caseWhenCriteria.append(",");
				}
				caseWhenCriteria.append(key).append("=");

				if (caseWhenFunctionList.contains(key)) {
					caseWhenCriteria.append(this.wrapperFrameName).append(groupCriteria).append("])")
							.append(aggCriteria2).append(").apply(lambda x: ").append(caseWhenHash.get(key))
							.append(",axis=1).values");
				} else {
					caseWhenCriteria.append(this.frameName).append(".apply(lambda x: ").append(caseWhenHash.get(key))
							.append(",axis=1).values");
				}
			}
		}
		if (caseWhenCriteria.length() > 0) {
			caseWhenCriteria = new StringBuilder(".assign(").append(caseWhenCriteria).append(")");
		}
	}

	/**
	 * Resolves a plain COLUMN selector to its pandas reference. When the alias
	 * differs from the column name it records a {@code 'column':'alias'} entry in
	 * {@code renameColList} (which {@link #closeAll()} folds into the
	 * {@code .rename(columns={...})} fragment). Returns {@code tableName['alias']}
	 * when a table/frame qualifier is requested, otherwise just the quoted alias
	 * {@code 'alias'} for use in the selector projection.
	 *
	 * @param selector  the column selector
	 * @param tableName frame/table reference, or {@code null} for an unqualified
	 *                  name
	 */
	private String processColumnSelector(QueryColumnSelector selector, String tableName) {
		StringBuilder sb = new StringBuilder();
		String columnName = selector.getColumn();
		String alias = selector.getAlias();
		// proper initialization
		if (renameColList == null) {
			renameColList = new ArrayList<>();
		}
		if (aliasHash == null) {
			aliasHash = new HashMap<>();
		}

		if (!columnName.equals(alias)) {
			sb.append("'" + columnName + "'").append(":").append("'" + alias + "'");
			renameColList.add(sb);
		}
		aliasHash.put(columnName, alias);
		if (tableName != null) {
			return new StringBuffer(tableName).append("['").append(alias).append("']") + "";
		} else {
			return "'" + alias + "'";
		}
	}

	/**
	 * Sets the frame references used to build every fragment: {@code frameName} is
	 * the raw DataFrame handle and {@code wrapperFrameName} is the cache wrapper
	 * (resolving to {@code ...cache['data']}) that {@link #composeQuery()} uses as
	 * the base the criteria are appended onto.
	 */
	public void setDataTableName(String frameName, String wrapperFrameName) {
		this.frameName = frameName;
		this.wrapperFrameName = wrapperFrameName;
	}

	/**
	 * Resolves a CONSTANT selector to a literal pandas token spliced inline into
	 * the query. Numbers are emitted bare, strings are double-quoted, and a
	 * subquery-expression constant is executed here and replaced by its scalar
	 * result (a bare number, a quoted string, or {@code pd.NA} if it yields
	 * nothing).
	 *
	 * @param selector the constant selector
	 * @return the literal pandas token
	 */
	private String processConstantSelector(QueryConstantSelector selector) {
		Object constant = selector.getConstant();
		if (constant instanceof SubQueryExpression) {
			ITask innerTask = null;
			try {
				innerTask = ((SubQueryExpression) constant).generateQsTask();
				innerTask.setLogger(logger);
				if (innerTask.hasNext()) {
					Object value = innerTask.next().getValues()[0];
					if (value instanceof Number) {
						return value.toString();
					} else {
						return "\"" + constant + "\"";
					}
				}
			} catch (Exception e) {
				classLogger.error("Error executing the subquery to resolve the constant selector value", e);
			} finally {
				if (innerTask != null) {
					try {
						innerTask.close();
					} catch (IOException e) {
						classLogger.error("Error closing the subquery task", e);
					}
				}
			}

			// if this doesn't return anything...
			return "pd.NA";
		} else if (constant instanceof Number) {
			return constant.toString();
		} else {
			return "\"" + constant + "\"";
		}
	}

	/**
	 * Iterates the query struct's GROUP BY selectors and builds the
	 * {@code groupCriteria} fragment by delegating each column to
	 * {@link #processGroupSelector(QueryColumnSelector)}. Only column group-bys are
	 * supported; any other selector type raises an IllegalArgumentException.
	 */
	private void processGroupSelectors() {
		List<IQuerySelector> groupSelectors = ((SelectQueryStruct) this.qs).getGroupBy();

		QueryColumnSelector queryColumnSelector = null;
		for (int sIndex = 0; sIndex < groupSelectors.size(); sIndex++) {
			IQuerySelector groupBySelector = groupSelectors.get(sIndex);
			if (groupBySelector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.COLUMN) {
				queryColumnSelector = (QueryColumnSelector) groupSelectors.get(sIndex);
				processGroupSelector(queryColumnSelector);
			} else {
				String errorMessage = "Cannot group by non QueryColumnSelector type yet...";
				logger.error(errorMessage);
				throw new IllegalArgumentException(errorMessage);
			}
		}
	}

	/**
	 * Appends one column to the {@code groupCriteria} fragment, opening it as
	 * {@code .groupby(['col'} on the first column and comma-separating the rest
	 * ({@link #closeAll()} closes it with {@code ], sort=False)}). Also reorders
	 * this grouped column to the front of the output header list so grouped keys
	 * lead the result columns.
	 */
	private void processGroupSelector(QueryColumnSelector selector) {
		if (groupCriteria.length() == 0) {
			groupCriteria.append(".groupby([");
		} else {
			groupCriteria.append(",");
		}

		groupCriteria.append("'").append(selector.getColumn()).append("'");

		if (actHeaders.contains(selector.getColumn())) {
			int index = actHeaders.indexOf(selector.getColumn());
			actHeaders.remove(selector.getColumn());
			// headers.remove(selector.getTable());
			actHeaders.add(groupIndex, headers.get(index));
			groupIndex++;
		}

		// headers.add(groupIndex, selector.getTable());
		// we dont know how many groups would it be.. so updating
		if (processedSelector.containsKey(selector.getColumn())) {
			processedSelector.put(selector.getColumn(), Boolean.TRUE);
			headers.add(selector.getColumn());
		}
	}

	/**
	 * Returns the map of pandas-function+column keys to their output alias, built
	 * up while processing aggregate and date selectors (used to reconcile generated
	 * column names with their aliases).
	 */
	public Map<String, String> functionMap() {
		return this.functionMap;
	}

	/**
	 * Assembles the aggregation fragments from the per-column clauses registered by
	 * {@link #processAggSelector}. Builds {@code aggCriteria2} as the
	 * named-aggregation form {@code .agg(<alias>=('<col>','<func>'), ...)} (the
	 * primary fragment concatenated by {@link #composeQuery()}; {@link #closeAll()}
	 * appends {@code ).reset_index()}), alongside the legacy dict form
	 * {@code aggCriteria} ({@code .agg({'<col>':['<func>'], ...})}). Also handles
	 * the case of a GROUP BY with HAVING but no explicit aggregate by folding in
	 * the HAVING aggregate, or dropping the group-by fragment entirely when neither
	 * is present.
	 */
	private void genAggString() {
		aggCriteria = new StringBuilder("");

		if (aggKeys.size() == 0 && !((SelectQueryStruct) this.qs).getGroupBy().isEmpty()) {
			if (havingList.size() == 0) {
				groupCriteria.delete(0, groupCriteria.length());
			} else {
				aggCriteria2.append(havingList.get(0));
			}
		}

		for (int cIndex = 0; cIndex < aggKeys.size(); cIndex++) {
			String colKey = aggKeys.get(cIndex);
			// I need to replace this with aggHash2
			if (aggHash.containsKey(colKey)) {
				if (aggCriteria.length() != 0) {
					aggCriteria.append(",");
				}
				aggCriteria.append(aggHash.get(colKey)).append("]");
			}
			// aggCriteria.append(aggHash.get(colKey));
			if (aggHash2.containsKey(colKey)) {
				if (aggCriteria2.length() != 0) {
					aggCriteria2.append(",");
				}
				aggCriteria2.append(aggHash2.get(colKey));
			}
		}

		if (aggCriteria.length() > 0 || aggCriteria2.length() > 0) {
			aggCriteria = new StringBuilder(".agg({").append(aggCriteria);
			aggCriteria2 = new StringBuilder(".agg(").append(aggCriteria2);
		}
		// just a way to say the override was added by this guy and not coming from
		// outside
		if (overrideQuery != null && overrideQuery.length() > 0 && aggHash2.size() > 0
				&& !((SelectQueryStruct) qs).getParts().containsKey(SelectQueryStruct.Query_Part.QUERY)) {
			overrideQuery.append("]");
		}
	}

	/**
	 * Registers one aggregate selector and stages its aggregation clause. Emits
	 * (into {@code aggHash2}, keyed by alias) the named-aggregation clause
	 * {@code <alias>=('<col>','<func>')} that {@link #genAggString()} joins into
	 * {@code .agg(...)}, and also records the dict-form clause and the output type.
	 * As a special case, an ungrouped min/max is redirected into an
	 * {@code overrideQuery} of the form {@code <frame>['<col>'].min()/max()}.
	 * Returns the quoted alias for projection. (Still to be harmonized with
	 * addSelectors.)
	 *
	 * @param selector the aggregate function selector
	 */
	private String processAggSelector(QueryFunctionSelector selector) {
		// if it is using a function.. usually it is an aggregation
		String function = selector.getFunction();
		String columnName = selector.getAllQueryColumns().get(0).getAlias();

		// you need to get to the column selector and then get the alias
		String pandasFunction = QueryFunctionHelper.convertFunctionToPandasSyntax(function);
		StringBuilder aggBuilder = new StringBuilder("");
		StringBuilder aggBuilder2 = new StringBuilder("");

		// I also need to keep track of the alias here so I can use that in the sort
		// later
		// I need to get the alias here
		String aggAlias = selector.getAlias();
		// format is
		// mv.drop_duplicates().groupby(['Genre']).agg(Mango =
		// ('Studio','count')).iloc[0:2000]
		// mango is the name of the alias.. no quotes
		aggBuilder2.append(aggAlias).append("=('").append(columnName).append("' , '").append(pandasFunction)
				.append("')");

		if (aggHash.containsKey(columnName)) {
			aggBuilder = aggHash.get(columnName);
			aggBuilder.append(",");
		} else {
			aggBuilder.append("'").append(columnName).append("':[");
		}
		aggBuilder.append("'" + pandasFunction + "'");

		orderHash.put(selector.getAlias(), new StringBuilder("('").append(columnName).append("')"));

		// headers.add(selector.getAlias());
		aggHash.put(columnName, aggBuilder);
		// adding it through alias
		aggHash2.put(aggAlias, aggBuilder2);

		aggKeys.add(columnName);
		// also add the alias name
		if (!aggKeys.contains(aggAlias)) {
			aggKeys.add(aggAlias);
		}

		// if it is a group concat.. dont add this to actual headers here.. since it
		// will get added during group by
		functionMap.put(pandasFunction + columnName, selector.getAlias());

		// I can avoid all of this by creating a dataframe and imputing.. but let us see
		// how far we can inline this
		// I am going to assume that this is the same type as header for most operations
		SemossDataType curType = this.colDataTypes.get(columnName);

		if (curType == SemossDataType.STRING || curType == SemossDataType.BOOLEAN) {
			// types.add(SemossDataType.INT);
			typesHash.put(aggAlias, SemossDataType.INT);
		} else if (curType == SemossDataType.INT && pandasFunction.equalsIgnoreCase("mean")) {
			// types.add(SemossDataType.DOUBLE);
			typesHash.put(aggAlias, SemossDataType.DOUBLE);
		} else {
			// types.add(curType);
			typesHash.put(aggAlias, curType);
		}

		// if the groupby is empty then this is just simple min and max
		// need to revisit min and max
		// quick fix for min and max
		// I do need to honor the filter here
		if (((SelectQueryStruct) this.qs).getGroupBy().isEmpty()
				&& (pandasFunction.contains("min") || pandasFunction.contains("max"))) {
			if (overrideQuery == null || overrideQuery.length() == 0) {
				overrideQuery = new StringBuilder("[");
			} else {
				overrideQuery.append(",");
			}
			overrideQuery.append(wrapperFrameName).append("['").append(columnName).append("'].").append(pandasFunction)
					.append("()");
		}

		return "'" + aggAlias + "'";
	}

	/**
	 * Registers an arithmetic derived column and contributes one clause to the
	 * {@code arithmeticCriteria} fragment. Resolves both operands via
	 * {@link #processSelector} (each may be a column or numeric constant), enforces
	 * INT/DOUBLE operands, and emits (into {@code arithmeticHash}, keyed by alias)
	 * {@code <alias>=(<left> <op> <right>).values} (adding
	 * {@code .replace([np.inf,-np.inf], np.nan)} for division) which
	 * {@link #genArithmeticString()} wraps into {@code .assign(...)}. Returns the
	 * quoted alias for the new column.
	 *
	 * @param selector the arithmetic selector (left op right)
	 */
	private String processArithmeticSelector(QueryArithmeticSelector selector, String tableName,
			boolean includeTableName, boolean useAlias, boolean... useTable) {
		IQuerySelector leftSelector = selector.getLeftSelector();
		IQuerySelector rightSelector = selector.getRightSelector();

		String mathExpr = selector.getMathExpr();
		String alias = selector.getAlias();
		String lColumnName = processSelector(leftSelector, tableName, includeTableName, useAlias, true);
		String rColumnName = processSelector(rightSelector, tableName, includeTableName, useAlias, true);

		StringBuilder sb = new StringBuilder();
		StringBuilder replace = new StringBuilder();

		SemossDataType leftDataType = SemossDataType.convertStringToDataType(leftSelector.getDataType());
		if (leftDataType == null) {
			if (leftSelector.getQueryStructName().contains("__")) {
				leftDataType = this.colDataTypes.get(leftSelector.getQueryStructName().split("__")[1]);
			} else {
				leftDataType = this.colDataTypes.get(leftSelector.getQueryStructName());
			}
		}
		SemossDataType rightDataType = SemossDataType.convertStringToDataType(rightSelector.getDataType());
		if (rightDataType == null) {
			if (rightSelector.getQueryStructName().contains("__")) {
				rightDataType = this.colDataTypes.get(rightSelector.getQueryStructName().split("__")[1]);
			} else {
				rightDataType = this.colDataTypes.get(rightSelector.getQueryStructName());
			}
			rightDataType = this.colDataTypes.get(rightSelector.getQueryStructName());
		}
		if (!(leftDataType == SemossDataType.INT || leftDataType == SemossDataType.DOUBLE)
				|| !(rightDataType == SemossDataType.INT || rightDataType == SemossDataType.DOUBLE)) {
			throw new IllegalArgumentException("Arithmetic selectors must be of type INT or DOUBLE.");
		}
		if (mathExpr.equals("/")) {
			replace.append(".replace([np.inf, -np.inf], np.nan)");
		}

		arithmeticKeys.add(alias);
		sb.append(alias).append("=(").append(lColumnName).append(mathExpr).append(rColumnName).append(")")
				.append(replace).append(".values");
		arithmeticHash.put(alias, sb);

		if (leftDataType == SemossDataType.DOUBLE || rightDataType == SemossDataType.DOUBLE || mathExpr.equals("/")) {
			// types.add(SemossDataType.DOUBLE);
			typesHash.put(alias, SemossDataType.DOUBLE);
		} else {
			// types.add(SemossDataType.INT);
			typesHash.put(alias, SemossDataType.INT);
		}
		return "'" + alias + "'";
	}

	/**
	 * Assembles the {@code arithmeticCriteria} fragment by joining the per-alias
	 * clauses registered by {@link #processArithmeticSelector} into a single
	 * {@code .assign(<alias>=(<expr>).values, ...)} call (empty if no arithmetic
	 * selectors were used).
	 */
	private void genArithmeticString() {
		for (String key : arithmeticKeys) {
			if (arithmeticHash.containsKey(key)) {
				if (arithmeticCriteria.length() != 0) {
					arithmeticCriteria.append(",");
				}
				arithmeticCriteria.append(arithmeticHash.get(key));
			}
		}
		if (arithmeticCriteria.length() > 0) {
			arithmeticCriteria = new StringBuilder(".assign(").append(arithmeticCriteria).append(")");
		}
	}

	/*
	 * 
	 * end adding selectors
	 * 
	 */

	/*
	 * 
	 * start adding filters
	 * 
	 */

	/**
	 * Builds the WHERE-clause boolean mask into {@code builder} (normally
	 * {@code filterCriteria}, later wrapped as {@code .loc[<mask>]}). Each filter
	 * is converted via
	 * {@link #processFilter(IQueryFilter, String, boolean, boolean...)} and the
	 * resulting boolean expressions are combined with pandas {@code &}.
	 *
	 * @param filters   the list of WHERE filters
	 * @param tableName the frame/table reference used inside the mask
	 * @param builder   the fragment buffer to append the mask to
	 * @param useAlias  whether to reference columns by alias
	 */
	public void addFilters(List<IQueryFilter> filters, String tableName, StringBuilder builder, boolean useAlias) {
		for (IQueryFilter filter : filters) {
			StringBuilder filterSyntax = processFilter(filter, tableName, useAlias);
			if (filterSyntax != null) {
				if (builder.length() > 0) {
					builder.append(" & ");
				}
				builder.append(filterSyntax.toString());
			}
		}
	}

	/**
	 * Lambda-flavored filter dispatcher used for the CASE WHEN / IF_ELSE condition.
	 * Unlike the {@code .loc[...]} overload, this produces a boolean expression in
	 * terms of a per-row {@code x} (e.g. {@code x['col'] == ...}) suitable for
	 * embedding inside {@code apply(lambda x: ...)}. Recurses through AND/OR nodes
	 * and delegates SIMPLE nodes to
	 * {@link #processSimpleQueryFilter(SimpleQueryFilter, String)}.
	 *
	 * @param filter    the filter tree of the CASE WHEN condition
	 * @param tableName the frame/table reference
	 * @return the lambda-body boolean expression, or {@code null} if unsupported
	 */
	private StringBuilder processFilter(IQueryFilter filter, String tableName) {
		IQueryFilter.QUERY_FILTER_TYPE filterType = filter.getQueryFilterType();
		if (filterType == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
			return processSimpleQueryFilter((SimpleQueryFilter) filter, tableName);
		} else if (filterType == IQueryFilter.QUERY_FILTER_TYPE.AND) {
			return processAndQueryFilter((AndQueryFilter) filter, tableName);
		} else if (filterType == IQueryFilter.QUERY_FILTER_TYPE.OR) {
			return processOrQueryFilter((OrQueryFilter) filter, tableName);
		}
		return null;
	}

	/**
	 * Lambda-flavored simple-filter handler for the CASE WHEN condition. Depending
	 * on the filter type (col-to-values, values-to-col, col-to-query) it delegates
	 * to {@link #createLambdaFilter} (or {@link #createSubqueryLambdaFilter}) to
	 * build the {@code x[...] <op> value} lambda-body expression, reversing the
	 * comparator when the column is on the right-hand side.
	 *
	 * @param filter    the simple filter
	 * @param tableName the frame/table reference
	 * @return the lambda-body boolean expression, or {@code null} if unsupported
	 */
	private StringBuilder processSimpleQueryFilter(SimpleQueryFilter filter, String tableName) {
		NounMetadata leftComp = filter.getLComparison();
		NounMetadata rightComp = filter.getRComparison();
		String thisComparator = filter.getComparator();
		FILTER_TYPE fType = filter.getSimpleFilterType();

		if (fType == FILTER_TYPE.COL_TO_VALUES) {
			return createLambdaFilter(leftComp, rightComp, thisComparator, tableName);
		} else if (fType == FILTER_TYPE.VALUES_TO_COL) {
			return createLambdaFilter(rightComp, leftComp, IQueryFilter.getReverseNumericalComparator(thisComparator),
					tableName);
		} else if (fType == FILTER_TYPE.COL_TO_COL) {
			// TODO: need to implement
		} else if (fType == FILTER_TYPE.COL_TO_QUERY) {
			return createSubqueryLambdaFilter(leftComp, rightComp, thisComparator, tableName);
		} else if (fType == FILTER_TYPE.QUERY_TO_COL) {
			return createSubqueryLambdaFilter(rightComp, leftComp,
					IQueryFilter.getReverseNumericalComparator(thisComparator), tableName);
		}
		return null;
	}

	/**
	 * Builds the per-row lambda-body boolean expression for a column-vs-values
	 * comparison used inside a CASE WHEN {@code apply(lambda x: ...)}. Emits
	 * type-aware pandas per each value: numeric/string equality and inequality
	 * ({@code (x[col] <op> val)}), date/timestamp comparisons via
	 * {@code x[col].strftime(...)}, substring search via
	 * {@code in x[col].casefold()}, and begins/ends via
	 * {@code x[col].casefold().startswith/endswith(...)}, joining multiple values
	 * with {@code and}/{@code or} as appropriate.
	 *
	 * @param leftComp       the column-side comparison operand
	 * @param rightComp      the value(s) operand
	 * @param thisComparator the comparator/operator
	 * @param tableName      the frame/table reference
	 * @return the lambda-body boolean expression
	 */
	private StringBuilder createLambdaFilter(NounMetadata leftComp, NounMetadata rightComp, String thisComparator,
			String tableName) {
		// Get the data object to be filtered on as well as data type
		IQuerySelector leftSelector = (IQuerySelector) leftComp.getValue();
		SemossDataType leftDataType = SemossDataType.convertStringToDataType(leftSelector.getDataType());

		String leftSelectorExpression = processSelector(leftSelector, tableName, true, false, false);

		if (leftDataType == null) {
			if (leftSelector.getQueryStructName().contains("__")) {
				leftDataType = this.colDataTypes.get(leftSelector.getQueryStructName().split("__")[1]);
			} else {
				leftDataType = this.colDataTypes.get(leftSelector.getQueryStructName());
			}
		}

		List<Object> objects = new ArrayList<>();
		if (rightComp.getValue() instanceof List) {
			objects.addAll((List<Object>) rightComp.getValue());
		} else {
			objects.add(rightComp.getValue());
		}

		StringBuilder retBuilder = new StringBuilder();

		if (PandasSyntaxHelper.OPERATOR_LIST.contains(thisComparator)) {
			for (int i = 0; i < objects.size(); i++) {
				if (retBuilder.length() > 0) {
					if (thisComparator.equals("!=")) {
						retBuilder.append(" and ");
					} else if (thisComparator.equals("==")) {
						retBuilder.append(" or ");
					} else {
						throw new IllegalArgumentException(
								"Cannot pass multiple filter values when using oprator " + thisComparator);
					}
				}
				if (leftDataType == SemossDataType.INT || leftDataType == SemossDataType.DOUBLE) {
					retBuilder.append("(x[").append(leftSelectorExpression).append("]").append(thisComparator)
							.append(objects.get(i)).append(")");
				} else if (leftDataType == SemossDataType.DATE) {
					retBuilder.append("(x[").append(leftSelectorExpression).append("].strftime('%Y-%m-%d')")
							.append(thisComparator).append("'").append(objects.get(i)).append("')");
				} else if (leftDataType == SemossDataType.TIMESTAMP) {
					retBuilder.append("(x[").append(leftSelectorExpression).append("].strftime('%Y-%m-%d %H-%M:%s')")
							.append(thisComparator).append("'").append(objects.get(i)).append("')");
				} else if (leftDataType == SemossDataType.STRING) {
					retBuilder.append("(x[").append(leftSelectorExpression).append("]").append(thisComparator)
							.append("'").append(objects.get(i)).append("')");
				} else {
					throw new IllegalArgumentException("Unsupported data type " + leftDataType);
				}
			}
		} else if (thisComparator.equals(SEARCH_COMPARATOR) || thisComparator.equals(NOT_SEARCH_COMPARATOR)) {
			for (int i = 0; i < objects.size(); i++) {
				if (retBuilder.length() > 0) {
					if (thisComparator.equals(NOT_SEARCH_COMPARATOR)) {
						retBuilder.append(" and ");
					} else {
						retBuilder.append(" or ");
					}
				}
				retBuilder.append("('").append(objects.get(i).toString().toLowerCase()).append("'");
				;
				if (thisComparator.equals(NOT_SEARCH_COMPARATOR)) {
					retBuilder.append(" not ");
				}
				if (leftDataType == SemossDataType.DATE) {
					retBuilder.append("in x[").append(leftSelectorExpression).append("].strftime('%Y-%m-%d')");
				} else if (leftDataType == SemossDataType.TIMESTAMP) {
					retBuilder.append("in x[").append(leftSelectorExpression).append("].strftime('%Y-%m-%d %H-%M:%s')");
				} else {
					retBuilder.append("in x[").append(leftSelectorExpression).append("].casefold()");
				}
				retBuilder.append(")");
			}
		} else if (thisComparator.equals(BEGINS_COMPARATOR) || thisComparator.equals(ENDS_COMPARATOR)) {
			String function = thisComparator.equals(BEGINS_COMPARATOR) ? "startswith" : "endswith";
			for (int i = 0; i < objects.size(); i++) {
				if (retBuilder.length() > 0) {
					retBuilder.append(" or ");
				}
				if (leftDataType == SemossDataType.STRING) {
					retBuilder.append("(").append("x[").append(leftSelectorExpression).append("].casefold().")
							.append(function).append("('").append(objects.get(i).toString().toLowerCase())
							.append("'))");
				} else if (leftDataType == SemossDataType.DATE) {
					retBuilder.append("(").append("x[").append(leftSelectorExpression).append("].strftime('%Y-%m-%d').")
							.append(function).append("('").append(objects.get(i).toString().toLowerCase())
							.append("'))");
				} else if (leftDataType == SemossDataType.TIMESTAMP) {
					retBuilder.append("(").append("x[").append(leftSelectorExpression)
							.append("].strftime('%Y-%m-%d %H-%M:%s').").append(function).append("('")
							.append(objects.get(i).toString().toLowerCase()).append("'))");
				} else {
					throw new IllegalArgumentException(
							"Unsupported data type " + leftDataType + " for filter operator +" + thisComparator);
				}
			}
		} else if (thisComparator.equals(NOT_BEGINS_COMPARATOR) || thisComparator.equals(NOT_ENDS_COMPARATOR)) {
			String function = thisComparator.equals(NOT_BEGINS_COMPARATOR) ? "startswith" : "endswith";
			for (int i = 0; i < objects.size(); i++) {
				if (retBuilder.length() > 0) {
					retBuilder.append(" and ");
				}
				if (leftDataType == SemossDataType.STRING) {
					retBuilder.append("(").append("not x[").append(leftSelectorExpression).append("].casefold().")
							.append(function).append("('").append(objects.get(i).toString().toLowerCase())
							.append("'))");
				} else if (leftDataType == SemossDataType.DATE) {
					retBuilder.append("(").append("not x[").append(leftSelectorExpression)
							.append("].strftime('%Y-%m-%d').").append(function).append("('")
							.append(objects.get(i).toString().toLowerCase()).append("'))");
				} else if (leftDataType == SemossDataType.TIMESTAMP) {
					retBuilder.append("(").append("not x[").append(leftSelectorExpression)
							.append("].strftime('%Y-%m-%d %H-%M:%s').").append(function).append("('")
							.append(objects.get(i).toString().toLowerCase()).append("'))");
				} else {
					throw new IllegalArgumentException(
							"Unsupported data type " + leftDataType + " for filter operator " + thisComparator);
				}
			}
		} else {
			throw new IllegalArgumentException("Unsupported operator type used. ");
		}

		return retBuilder;
	}

	/**
	 * Lambda-filter variant for a column-to-subquery CASE WHEN condition: executes
	 * the right-side subquery against the PandasFrame, flattens its first column to
	 * a value list, then delegates to {@link #createLambdaFilter} to produce the
	 * {@code x[...]} lambda-body expression against those values.
	 *
	 * @param leftComp       the column-side operand
	 * @param rightComp      the subquery operand (a SelectQueryStruct)
	 * @param thisComparator the comparator/operator
	 * @param tableName      the frame/table reference
	 * @return the lambda-body boolean expression
	 */
	private StringBuilder createSubqueryLambdaFilter(NounMetadata leftComp, NounMetadata rightComp,
			String thisComparator, String tableName) {
		// flush out the right side to a list of values
		SelectQueryStruct subQs = (SelectQueryStruct) rightComp.getValue();
		IRawSelectWrapper subQueryValues = this.pandasFrame.query(subQs);
		List<Object> values = new ArrayList<>();
		while (subQueryValues.hasNext()) {
			values.add(subQueryValues.next().getValues()[0]);
		}
		NounMetadata newRightComp = new NounMetadata(values,
				SemossDataType.convertToPixelDataType(subQueryValues.getTypes()[0]));

		return createLambdaFilter(leftComp, newRightComp, thisComparator, tableName);
	}

	/**
	 * Lambda-flavored OR combiner for the CASE WHEN condition: joins the child
	 * filter expressions with Python {@code or} and parenthesizes them, producing a
	 * lambda-body boolean suitable for {@code apply(lambda x: ...)}.
	 *
	 * @param filter    the OR filter node
	 * @param tableName the frame/table reference
	 * @return the combined lambda-body boolean expression
	 */
	private StringBuilder processOrQueryFilter(OrQueryFilter filter, String tableName) {
		StringBuilder sb = new StringBuilder();
		List<IQueryFilter> filterList = filter.getFilterList();
		for (int i = 0; i < filterList.size(); i++) {
			if (i == 0) {
				sb.append("(");
			} else {
				sb.append(") or (");
			}
			sb.append(processFilter(filter, tableName));
		}
		return sb.append(")");
	}

	/**
	 * Lambda-flavored AND combiner for the CASE WHEN condition: joins the child
	 * filter expressions with Python {@code and} and parenthesizes them, producing
	 * a lambda-body boolean suitable for {@code apply(lambda x: ...)}.
	 *
	 * @param filter    the AND filter node
	 * @param tableName the frame/table reference
	 * @return the combined lambda-body boolean expression
	 */
	private StringBuilder processAndQueryFilter(AndQueryFilter filter, String tableName) {
		StringBuilder sb = new StringBuilder();
		List<IQueryFilter> filterList = filter.getFilterList();
		for (int i = 0; i < filterList.size(); i++) {
			if (i == 0) {
				sb.append("(");
			} else {
				sb.append(") and (");
			}
			sb.append(processFilter(filter, tableName));
		}
		return sb.append(")");
	}

	/**
	 * Mask-flavored filter dispatcher for WHERE ({@code .loc[<mask>]}) and HAVING
	 * ({@code .filter(lambda x: ...)}) clauses, selected by the
	 * {@code isHavingFilter} flag. Routes by filter type: SIMPLE ->
	 * {@link #processSimpleHavingFilter} or
	 * {@link #processSimpleQueryFilter(SimpleQueryFilter, String, boolean, boolean...)},
	 * AND/OR -> the varargs
	 * {@code processAndQueryFilter}/{@code processOrQueryFilter} combiners, BETWEEN
	 * -> {@link #processBetweenQueryFilter}. Unlike the two-argument overload, the
	 * WHERE branch builds vectorized frame-level boolean masks (e.g.
	 * {@code frame['col'].isin(...)}) rather than per-row {@code lambda x}
	 * expressions.
	 *
	 * @param filter    the filter node
	 * @param tableName the frame/table reference
	 * @param useAlias  whether columns are referenced by alias
	 * @return the boolean mask / having expression, or {@code null} if unsupported
	 */
	private StringBuilder processFilter(IQueryFilter filter, String tableName, boolean useAlias, boolean... useTable) {
		IQueryFilter.QUERY_FILTER_TYPE filterType = filter.getQueryFilterType();
		if (filterType == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE && isHavingFilter) {
			return processSimpleHavingFilter((SimpleQueryFilter) filter, tableName, useAlias, useTable);
		} else if (filterType == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE && !isHavingFilter) {
			return processSimpleQueryFilter((SimpleQueryFilter) filter, tableName, useAlias, useTable);
		} else if (filterType == IQueryFilter.QUERY_FILTER_TYPE.AND) {
			return processAndQueryFilter((AndQueryFilter) filter, tableName, useAlias, useTable);
		} else if (filterType == IQueryFilter.QUERY_FILTER_TYPE.OR) {
			return processOrQueryFilter((OrQueryFilter) filter, tableName, useAlias, useTable);
		} else if (filterType == IQueryFilter.QUERY_FILTER_TYPE.BETWEEN) {
			return processBetweenQueryFilter((BetweenQueryFilter) filter, tableName, useAlias, useTable);
		}
		return null;
	}

	/**
	 * Mask-flavored OR combiner for WHERE/HAVING. Joins child filter expressions
	 * with the pandas vectorized {@code |} for a WHERE mask, or Python {@code or}
	 * when {@code isHavingFilter} is set, parenthesizing each operand.
	 *
	 * @param filter    the OR filter node
	 * @param tableName the frame/table reference
	 * @param useAlias  whether columns are referenced by alias
	 * @return the combined boolean mask / having expression
	 */
	private StringBuilder processOrQueryFilter(OrQueryFilter filter, String tableName, boolean useAlias,
			boolean... useTable) {
		StringBuilder filterBuilder = new StringBuilder();
		List<IQueryFilter> filterList = filter.getFilterList();
		int numOrs = filterList.size();
		for (int i = 0; i < numOrs; i++) {
			if (i == 0) {
				filterBuilder.append("(");
			} else {
				if (isHavingFilter) {
					filterBuilder.append(") or (");
				} else {
					filterBuilder.append(" ) | ( ");
				}
			}
			filterBuilder.append(processFilter(filterList.get(i), tableName, useAlias, useTable));
		}
		filterBuilder.append(")");
		return filterBuilder;
	}

	/**
	 * Mask-flavored AND combiner for WHERE/HAVING. Joins child filter expressions
	 * with the pandas vectorized {@code &} for a WHERE mask, or Python {@code and}
	 * when {@code isHavingFilter} is set, parenthesizing each operand.
	 *
	 * @param filter    the AND filter node
	 * @param tableName the frame/table reference
	 * @param useAlias  whether columns are referenced by alias
	 * @return the combined boolean mask / having expression
	 */
	private StringBuilder processAndQueryFilter(AndQueryFilter filter, String tableName, boolean useAlias,
			boolean... useTable) {
		StringBuilder filterBuilder = new StringBuilder();
		List<IQueryFilter> filterList = filter.getFilterList();
		int numAnds = filterList.size();
		for (int i = 0; i < numAnds; i++) {
			if (i == 0) {
				filterBuilder.append("(");
			} else {
				if (isHavingFilter) {
					filterBuilder.append(") and (");
				} else {
					filterBuilder.append(") & (");
				}
			}
			filterBuilder.append(processFilter(filterList.get(i), tableName, useAlias, useTable));
		}
		filterBuilder.append(")");
		return filterBuilder;
	}

	/**
	 * Builds a BETWEEN range predicate as
	 * {@code ((<col> >= start) & (<col> <= end))}. For a plain column it produces a
	 * WHERE-mask expression (numeric direct comparison, or date/timestamp via
	 * {@code .apply(pd.to_datetime)...}); for a function column it produces a
	 * HAVING-style {@code (x['col'].<func>() >= start)
	 * & (x['col'].<func>() <= end)} expression and registers the aggregate clause
	 * in {@code havingList} so a group aggregate can be synthesized.
	 *
	 * @param filter    the BETWEEN filter (column, start, end)
	 * @param tableName the frame/table reference
	 * @param useAlias  whether columns are referenced by alias
	 * @return the range predicate expression
	 */
	private StringBuilder processBetweenQueryFilter(BetweenQueryFilter filter, String tableName, boolean useAlias,
			boolean... useTable) {
		StringBuilder retBuilder = new StringBuilder("((");
		StringBuilder havingsAggBuilder = new StringBuilder();

		IQuerySelector selector = filter.getColumn();

		if (selector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.COLUMN) {
			SemossDataType selectorDataType = SemossDataType.convertStringToDataType(selector.getDataType());
			if (selectorDataType == null) {
				if (selector.getQueryStructName().contains("__")) {
					selectorDataType = this.colDataTypes.get(selector.getQueryStructName().split("__")[1]);
				} else {
					selectorDataType = this.colDataTypes.get(selector.getQueryStructName());
				}
			}

			String columnName = processSelector(selector, tableName, true, useAlias, true);
			if (selectorDataType == SemossDataType.INT || selectorDataType == SemossDataType.DOUBLE) {
				retBuilder.append(columnName).append(" >= ").append(filter.getStart()).append(") & (")
						.append(columnName).append(" <= ").append(filter.getEnd()).append(")");
			} else if (selectorDataType == SemossDataType.DATE) {
				retBuilder.append(columnName).append(".apply(pd.to_datetime).dt.date >= pd.to_datetime('")
						.append(filter.getStart()).append("').date()) & (").append(columnName)
						.append(".apply(pd.to_datetime).dt.date <= pd.to_datetime('").append(filter.getEnd())
						.append("').date())");
			} else if (selectorDataType == SemossDataType.TIMESTAMP) {
				retBuilder.append(columnName).append(".apply(od.to_datetime) >= pd.to_datetime('")
						.append(filter.getStart()).append("')) & (").append(columnName)
						.append(".apply(pd.to_datetime) <= pd.to_datetime('").append(filter.getEnd()).append("'))");
			} else {
				throw new IllegalArgumentException("Invalid column input.");
			}
		} else if (selector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.FUNCTION) {
			String function = ((QueryFunctionSelector) selector).getFunction();
			String pandasFunction = QueryFunctionHelper.convertFunctionToPandasSyntax(function);
			String columnName = selector.getAllQueryColumns().get(0).getAlias();

			havingsAggBuilder.append(selector.getAlias()).append("=('").append(columnName).append("','")
					.append(pandasFunction).append("')");
			havingList.add(havingsAggBuilder);

			retBuilder.append("x['").append(columnName).append("'].").append(pandasFunction).append("() >= ")
					.append(filter.getStart()).append(") & (x['").append(columnName).append("'].")
					.append(pandasFunction).append("() <= ").append(filter.getEnd()).append(")");
		}
		return retBuilder.append(")");
	}

	/**
	 * Mask-flavored simple-filter handler for the WHERE clause. Dispatches by
	 * filter type to the appropriate builder that emits a vectorized frame-level
	 * boolean mask: col-to-col -> {@link #addSelectorToSelectorFilter}, col/values
	 * -> {@link #addSelectorToValuesFilter}, col/query ->
	 * {@link #addSelectorToQueryFilter}, col/lambda ->
	 * {@link #addSelectorToLambda}, reversing the comparator when the column is on
	 * the right-hand side.
	 *
	 * @param filter    the simple filter
	 * @param tableName the frame/table reference
	 * @param useAlias  whether columns are referenced by alias
	 * @return the boolean mask expression, or {@code null} if unsupported
	 */
	private StringBuilder processSimpleQueryFilter(SimpleQueryFilter filter, String tableName, boolean useAlias,
			boolean... useTable) {
		NounMetadata leftComp = filter.getLComparison();
		NounMetadata rightComp = filter.getRComparison();
		String thisComparator = filter.getComparator();

		FILTER_TYPE fType = filter.getSimpleFilterType();
		if (fType == FILTER_TYPE.COL_TO_COL) {
			return addSelectorToSelectorFilter(leftComp, rightComp, thisComparator, tableName, useAlias, useTable);
		} else if (fType == FILTER_TYPE.COL_TO_VALUES) {
			return addSelectorToValuesFilter(leftComp, rightComp, thisComparator, tableName, useAlias, useTable);
		} else if (fType == FILTER_TYPE.VALUES_TO_COL) {
			return addSelectorToValuesFilter(rightComp, leftComp,
					IQueryFilter.getReverseNumericalComparator(thisComparator), tableName, useAlias, useTable);
		} else if (fType == FILTER_TYPE.COL_TO_QUERY) {
			return addSelectorToQueryFilter(leftComp, rightComp, thisComparator, tableName, useAlias, useTable);
		} else if (fType == FILTER_TYPE.QUERY_TO_COL) {
			return addSelectorToQueryFilter(rightComp, leftComp,
					IQueryFilter.getReverseNumericalComparator(thisComparator), tableName, useAlias, useTable);
		} else if (fType == FILTER_TYPE.COL_TO_LAMBDA) {
			return addSelectorToLambda(leftComp, rightComp, thisComparator, tableName, useAlias);
		} else if (fType == FILTER_TYPE.LAMBDA_TO_COL) {
			// same logic as above, just switch the order and reverse the comparator if it
			// is numeric
			return addSelectorToLambda(rightComp, leftComp, IQueryFilter.getReverseNumericalComparator(thisComparator),
					tableName, useAlias);
		} else if (fType == FILTER_TYPE.VALUE_TO_VALUE) {
			// WHY WOULD YOU DO THIS!!!
		}
		return null;
	}

	/**
	 * Resolves a column-to-lambda WHERE filter by executing the right-side reactor
	 * to obtain a concrete value, then delegating to
	 * {@link #addSelectorToValuesFilter} to build the frame-level boolean mask.
	 * Only scalar-producing lambdas are supported.
	 *
	 * @param leftComp       the column-side operand
	 * @param rightComp      the lambda/reactor operand
	 * @param thisComparator the comparator/operator
	 * @param tableName      the frame/table reference
	 * @param useAlias       whether the column is referenced by alias
	 * @return the boolean mask expression
	 */
	private StringBuilder addSelectorToLambda(NounMetadata leftComp, NounMetadata rightComp, String thisComparator,
			String tableName, boolean useAlias) {
		// need to evaluate the lambda on the right
		IReactor reactor = (IReactor) rightComp.getValue();
		NounMetadata nounEvaluated = reactor.execute();

		Map<String, Object> mergeMetadata = reactor.mergeIntoQsMetadata();
		if (mergeMetadata.get(IReactor.MERGE_INTO_QS_FORMAT).equals(IReactor.MERGE_INTO_QS_FORMAT_SCALAR)) {
			return addSelectorToValuesFilter(leftComp, nounEvaluated, thisComparator, tableName, useAlias);
		}

		throw new IllegalArgumentException("Unknown qs format to merge");
	}

	/**
	 * Builds a column-to-column WHERE-mask expression comparing two resolved
	 * selectors (e.g. {@code lSelector == rSelector}, with NA-aware handling for
	 * equality/inequality and like/not-like search operators). Contributes to the
	 * {@code filterCriteria} mask.
	 *
	 * @param leftComp       the left column operand
	 * @param rightComp      the right column operand
	 * @param thisComparator the comparator/operator
	 * @param tableName      the frame/table reference
	 * @param useAlias       whether columns are referenced by alias
	 * @return the boolean mask expression
	 */
	private StringBuilder addSelectorToSelectorFilter(NounMetadata leftComp, NounMetadata rightComp,
			String thisComparator, String tableName, boolean useAlias, boolean... useTable) {
		// get the left side
		IQuerySelector leftSelector = (IQuerySelector) leftComp.getValue();
		IQuerySelector rightSelector = (IQuerySelector) rightComp.getValue();

		String lSelector = processSelector(leftSelector, tableName, true, useAlias, useTable);
		String rSelector = processSelector(rightSelector, tableName, true, useAlias, useTable);

		StringBuilder filterBuilder = new StringBuilder();
		if (thisComparator.equals("!=") || thisComparator.equals("<>")) {
			filterBuilder.append("( !(").append(lSelector).append(" == ").append(rSelector)
					// account for NA
					.append(") | ( is.na(").append(lSelector).append(") & !is.na(").append(rSelector)
					.append(") ) | ( !is.na(").append(lSelector).append(") & is.na(").append(rSelector).append(")) )");
		} else if (thisComparator.equals(SEARCH_COMPARATOR)) {
			// some operation
			filterBuilder.append("as.character(").append(lSelector).append(") %like% as.character(").append(rSelector)
					.append(")");
		} else if (thisComparator.equals(NOT_SEARCH_COMPARATOR)) {
			// some operation
			filterBuilder.append("!(as.character(").append(lSelector).append(") %like% as.character(").append(rSelector)
					.append("))");
		} else {
			if (thisComparator.equals("==")) {
				filterBuilder.append("(").append(lSelector).append(" == ").append(rSelector)
						// account for NA
						.append(" | is.na(").append(lSelector).append(") & is.na(").append(rSelector).append(") )");
			} else {
				// other op
				filterBuilder.append(lSelector).append(" ").append(thisComparator).append(" ").append(rSelector);
			}
		}

		return filterBuilder;
	}

	/**
	 * Core builder of the WHERE-clause boolean mask (the {@code filterCriteria}
	 * later wrapped as {@code .loc[...]}) for a column-vs-value(s) comparison.
	 * Produces type- and operator-specific vectorized pandas: string equality via
	 * {@code frame[col].isin((...))} (negated with {@code ~}), substring search via
	 * {@code frame[col].str.contains('v',case=False)}, begins/ends via
	 * {@code frame[col].str.casefold().str.startswith/endswith(...)},
	 * date/timestamp comparisons via {@code frame[col].apply(pd.to_datetime)...},
	 * and numeric via {@code frame[col].apply(lambda x: x <op> v)}, OR-combined
	 * ({@code |}) across values. Also emits a {@code frame[col].isna()} /
	 * {@code ~...isna()} clause when a null value is being matched.
	 *
	 * @param leftComp       the column-side operand
	 * @param rightComp      the value(s) operand
	 * @param thisComparator the comparator/operator
	 * @param tableName      the frame/table reference
	 * @param useAlias       whether the column is referenced by alias
	 * @return the boolean mask expression
	 */
	private StringBuilder addSelectorToValuesFilter(NounMetadata leftComp, NounMetadata rightComp,
			String thisComparator, String tableName, boolean useAlias, boolean... useTable) {
		IQuerySelector leftSelector = (IQuerySelector) leftComp.getValue();
		SemossDataType leftDataType = SemossDataType.convertStringToDataType(leftSelector.getDataType());

		String leftSelectorExpression = processSelector(leftSelector, tableName, true, useAlias, useTable);

		if (leftDataType == null) {
			if (leftSelector.getQueryStructName().contains("__")) {
				leftDataType = this.colDataTypes.get(leftSelector.getQueryStructName().split("__")[1]);
			} else {
				leftDataType = this.colDataTypes.get(leftSelector.getQueryStructName());
			}
		}

		List<Object> objects = new ArrayList<>();
		boolean multi = false;
		if (rightComp.getValue() instanceof List) {
			objects.addAll((List<Object>) rightComp.getValue());
			multi = true;
		} else {
			objects.add(rightComp.getValue());
		}

		boolean addNullCheck = objects.remove(null);
		if (leftDataType != null && SemossDataType.isNotString(leftDataType)) {
			if (objects.remove("null") || objects.remove("nan")
					|| (thisComparator.equals("==") && objects.remove(""))) {
				addNullCheck = true;
			}
		}
		if (!addNullCheck) {
			// are we searching for null?
			addNullCheck = IQueryInterpreter.getAllSearchComparators().contains(thisComparator)
					&& (objects.contains("n") || objects.contains("nu") || objects.contains("nul")
							|| objects.contains("null"));
		}

		StringBuilder filterBuilder = new StringBuilder("(");
		StringBuilder retBuilder = new StringBuilder();

		if (addNullCheck) {
			if (thisComparator.equals("==") || IQueryInterpreter.getPosSearchComparators().contains(thisComparator)) {
				filterBuilder.append(wrapperFrameName).append("[").append(leftSelectorExpression).append("].isna())");
			} else if (thisComparator.equals("!=") || thisComparator.equals("<>")
					|| IQueryInterpreter.getPosSearchComparators().contains(thisComparator)) {
				filterBuilder.append("~").append(wrapperFrameName).append("[").append(leftSelectorExpression)
						.append("].isna())");
			}
		} else {
			filterBuilder = null;
		}

		if (leftDataType == SemossDataType.STRING || leftDataType == SemossDataType.FACTOR
				|| leftDataType == SemossDataType.DATE || leftDataType == SemossDataType.TIMESTAMP) {
			String myFilterFormatted = PandasSyntaxHelper.createPandasColVec(objects, leftDataType);

			if ((leftDataType == SemossDataType.STRING || leftDataType == SemossDataType.FACTOR)
					&& (thisComparator.equals("==") || thisComparator.equals("!="))) {
				retBuilder.append("(");
				if (thisComparator.equals("!=")) {
					retBuilder.append("~");
				}
				retBuilder.append(wrapperFrameName).append("[").append(leftSelectorExpression).append("].isin")
						.append(myFilterFormatted).append(")");
			} else if (thisComparator.equals(SEARCH_COMPARATOR) || thisComparator.equals(NOT_SEARCH_COMPARATOR)) {
				for (int i = 0; i < objects.size(); i++) {
					if (retBuilder.length() > 0) {
						retBuilder.append(" | ");
					}
					retBuilder.append("(");
					if (thisComparator.equals(NOT_SEARCH_COMPARATOR)) {
						retBuilder.append("~");
					}
					if (leftDataType == SemossDataType.DATE) {
						retBuilder.append(wrapperFrameName).append("[").append(leftSelectorExpression)
								.append("].apply(pd.to_datetime).dt.strftime('%Y-%m-%d').str.contains('")
								.append(objects.get(i)).append("',case=False)");
					} else if (leftDataType == SemossDataType.TIMESTAMP) {
						retBuilder.append(wrapperFrameName).append("[").append(leftSelectorExpression)
								.append("].apply(pd.to_dateime).dt.strftime('%Y-%m-%d %H:%M:%s').str.contains('")
								.append(objects.get(i)).append("',case=False)");
					} else {
						retBuilder.append(wrapperFrameName).append("[").append(leftSelectorExpression)
								.append("].str.contains('").append(objects.get(i)).append("',case=False)");
					}
					retBuilder.append(")");
				}
			} else if (thisComparator.equals(BEGINS_COMPARATOR) || thisComparator.equals(ENDS_COMPARATOR)) {
				String function = thisComparator.equals(BEGINS_COMPARATOR) ? "startswith" : "endswith";
				for (int i = 0; i < objects.size(); i++) {
					if (retBuilder.length() > 0) {
						retBuilder.append(" | ");
					}
					retBuilder.append("(").append(wrapperFrameName).append("[").append(leftSelectorExpression)
							.append("].str.casefold().str.").append(function).append("('")
							.append(objects.get(i).toString().toLowerCase()).append("'))");
				}
			} else if (thisComparator.equals(NOT_BEGINS_COMPARATOR) || thisComparator.equals(NOT_ENDS_COMPARATOR)) {
				String function = thisComparator.equals(NOT_BEGINS_COMPARATOR) ? "startswith" : "endswith";
				for (int i = 0; i < objects.size(); i++) {
					if (retBuilder.length() > 0) {
						retBuilder.append(" | ");
					}
					retBuilder.append("(~").append(wrapperFrameName).append("[").append(leftSelectorExpression)
							.append("].str.casefold().str.").append(function).append("('")
							.append(objects.get(i).toString().toLowerCase()).append("'))");
				}
			} else if (leftDataType != SemossDataType.STRING && leftDataType != SemossDataType.FACTOR) {
				if (multi) {
					if (!(thisComparator.equals("==") || thisComparator.equals("!="))) {
						throw new IllegalArgumentException("Unsupported operand argument '" + thisComparator
								+ "' for filtering by multiple values.");
					}
					retBuilder.append("(");
					if (thisComparator.equals("!=")) {
						retBuilder.append("~");
					}
					retBuilder.append(wrapperFrameName).append("[").append(leftSelectorExpression)
							.append("].apply(pd.to_datetime).isin").append(myFilterFormatted).append(")");
				} else {
					retBuilder.append("(").append(wrapperFrameName).append("[").append(leftSelectorExpression)
							.append("].apply(pd.to_datetime) ").append(thisComparator).append(" pd.to_datetime('")
							.append(objects.get(0)).append("'))");
				}
			} else {
				throw new IllegalArgumentException(
						"Unsupported operand argument '" + thisComparator + "' for type String.");
			}
		} else {
			if (!PandasSyntaxHelper.OPERATOR_LIST.contains(thisComparator)) {
				throw new IllegalArgumentException(
						"Unsupported operand argument '" + thisComparator + "' for type Numeric.");
			}
			for (int i = 0; i < objects.size(); i++) {
				if (retBuilder.length() > 0) {
					retBuilder.append(" | ");
				}
				retBuilder.append("(").append(wrapperFrameName).append("[").append(leftSelectorExpression)
						.append("].apply(lambda x: x").append(thisComparator);
				if (objects.get(i) instanceof String && ((String) objects.get(i)).isEmpty()) {
					retBuilder.append("\"\"").append("))");
				} else {
					retBuilder.append(objects.get(i)).append("))");
				}
			}
		}

		if (filterBuilder != null) {
			if (retBuilder.length() > 0) {
				retBuilder = new StringBuilder("(").append(filterBuilder).append(" & ").append(retBuilder).append(")");
			}
		}
		return retBuilder;
	}

	/**
	 * Resolves a column-to-subquery WHERE filter by executing the right-side
	 * subquery against the PandasFrame, flattening its first column to a value
	 * list, then delegating to {@link #addSelectorToValuesFilter} to build the
	 * frame-level boolean mask.
	 *
	 * @param leftComp       the column-side operand
	 * @param rightComp      the subquery operand (a SelectQueryStruct)
	 * @param thisComparator the comparator/operator
	 * @param tableName      the frame/table reference
	 * @param useAlias       whether the column is referenced by alias
	 * @return the boolean mask expression
	 */
	private StringBuilder addSelectorToQueryFilter(NounMetadata leftComp, NounMetadata rightComp, String thisComparator,
			String tableName, boolean useAlias, boolean... useTable) {
		// flush out the right side to a list of values
		SelectQueryStruct subQs = (SelectQueryStruct) rightComp.getValue();
		IRawSelectWrapper subQueryValues = this.pandasFrame.query(subQs);
		List<Object> values = new ArrayList<>();
		while (subQueryValues.hasNext()) {
			values.add(subQueryValues.next().getValues()[0]);
		}
		NounMetadata newRightComp = new NounMetadata(values,
				SemossDataType.convertToPixelDataType(subQueryValues.getTypes()[0]));

		return addSelectorToValuesFilter(leftComp, newRightComp, thisComparator, tableName, useAlias, useTable);
	}

	/**
	 * Builds the HAVING expression into {@code builder} (normally
	 * {@code havingCriteria}, later wrapped by {@link #closeAll()} as
	 * {@code .groupby([...]).filter(lambda x: <expr>)}). Sets the
	 * {@code isHavingFilter} flag so {@link #processFilter} emits having-style
	 * per-group lambda expressions, converts each filter, joins them with Python
	 * {@code and}, and requires a GROUP BY to be present.
	 *
	 * @param filters   the list of HAVING filters
	 * @param tableName the frame/table reference
	 * @param builder   the fragment buffer to append the having expression to
	 * @param useAlias  whether columns are referenced by alias
	 */
	public void addHavingFilters(List<IQueryFilter> filters, String tableName, StringBuilder builder,
			boolean useAlias) {
		if (filters.size() > 0) {
			isHavingFilter = true;
		}
		if (isHavingFilter && ((SelectQueryStruct) this.qs).getGroupBy().isEmpty()) {
			throw new IllegalArgumentException(
					"Invalid query statement. A GroupBy(...) is required for filtering by functions.");
		}
		for (IQueryFilter filter : filters) {
			StringBuilder filterSyntax = processFilter(filter, tableName, useAlias);

			if (builder.length() > 0) {
				builder.append(" and ");
			}
			if (filterSyntax != null) {
				builder.append(filterSyntax.toString());
			}
		}
		isHavingFilter = false;
	}

	/**
	 * Handles a simple HAVING filter by delegating to {@link #createHavingFilter}
	 * to build the per-group {@code (x['col'].<func>() <op> value)} expression
	 * (reversing the comparator for values-to-column), which becomes part of the
	 * {@code .filter(lambda x: ...)} having fragment. Currently only
	 * column-to-value comparisons are supported.
	 *
	 * @param filter    the simple having filter
	 * @param tableName the frame/table reference
	 * @param useAlias  whether columns are referenced by alias
	 * @return the having expression, or {@code null} if unsupported
	 */
	private StringBuilder processSimpleHavingFilter(SimpleQueryFilter filter, String tableName, boolean useAlias,
			boolean... useTable) {
		NounMetadata leftComp = filter.getLComparison();
		NounMetadata rightComp = filter.getRComparison();
		String thisOperator = filter.getComparator();

		FILTER_TYPE fType = filter.getSimpleFilterType();
		if (fType == FILTER_TYPE.COL_TO_VALUES) {
			return createHavingFilter(leftComp, rightComp, thisOperator, tableName, useAlias, useTable);
		} else if (fType == FILTER_TYPE.VALUES_TO_COL) {
			return createHavingFilter(rightComp, leftComp, IQueryFilter.getReverseNumericalComparator(thisOperator),
					tableName, useAlias, useTable);
		}
		return null;
	}

	/**
	 * Builds the per-group HAVING predicate
	 * {@code (x['<col>'].<func>() <op> value)} for each comparison value, forming
	 * the body that {@link #closeAll()} wraps as
	 * {@code .groupby([...]).filter(lambda x: ...)}. Also registers the aggregate
	 * clause {@code <alias>=('<col>','<func>')} in {@code havingList}, which
	 * {@link #genAggString()} uses to synthesize an aggregation when the query has
	 * no explicit aggregate selector.
	 *
	 * @param leftComp  the aggregate-function column operand
	 * @param rightComp the comparison value(s)
	 * @param operator  the comparator/operator
	 * @param tableName the frame/table reference
	 * @param useAlias  whether columns are referenced by alias
	 * @return the having predicate expression
	 */
	private StringBuilder createHavingFilter(NounMetadata leftComp, NounMetadata rightComp, String operator,
			String tableName, boolean useAlias, boolean... useTable) {
		IQuerySelector leftSelector = (IQuerySelector) leftComp.getValue();
		QueryFunctionSelector selector = (QueryFunctionSelector) leftSelector;

		String function = selector.getFunction();
		String columnName = selector.getAllQueryColumns().get(0).getAlias();
		String pandasFunction = QueryFunctionHelper.convertFunctionToPandasSyntax(function);

		List<Object> values = new ArrayList<Object>();
		if (rightComp.getValue() instanceof List) {
			values.addAll((List<Object>) rightComp.getValue());
		} else {
			values.add(rightComp.getValue());
		}

		StringBuilder havingsAggBuilder = new StringBuilder();
		StringBuilder retBuilder = new StringBuilder();

		havingsAggBuilder.append(selector.getAlias()).append("=('").append(columnName).append("','")
				.append(pandasFunction).append("')");
		havingList.add(havingsAggBuilder);

		for (int index = 0; index < values.size(); index++) {
			retBuilder.append("(x['").append(columnName).append("'].").append(pandasFunction).append("()")
					.append(operator).append(values.get(index)).append(")");
		}
		return retBuilder;
	}

	/** Sets the PyTranslator used to execute pandas/python against the frame. */
	public void setPyTranslator(PyTranslator pyt) {
		this.pyt = pyt;
	}
}

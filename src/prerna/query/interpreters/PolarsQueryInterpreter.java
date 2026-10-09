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
/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * Licensed under the Apache License, Version 2.0 or GPLv2, as applicable.
 *******************************************************************************/
package prerna.query.interpreters;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AbstractListFilter;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.IQuerySort;
import prerna.query.querystruct.selectors.QueryArithmeticSelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class PolarsQueryInterpreter extends AbstractQueryInterpreter {

	private static final Gson GSON = new GsonBuilder().serializeNulls().disableHtmlEscaping().create();

	private GenRowFilters frameFilters = new GenRowFilters();

	public void setFrameFilters(GenRowFilters frameFilters) {
		this.frameFilters = frameFilters == null ? new GenRowFilters() : frameFilters;
	}

	@Override
	public String composeQuery() {
		if (!(this.qs instanceof SelectQueryStruct)) {
			throw new IllegalArgumentException("Polars supports SelectQueryStruct queries only");
		}
		return GSON.toJson(buildPlan((SelectQueryStruct) this.qs));
	}

	public Map<String, Object> buildPlan(SelectQueryStruct selectQs) {
		Map<String, Object> plan = new LinkedHashMap<>();
		List<Map<String, Object>> selectors = new ArrayList<>();
		for (IQuerySelector selector : selectQs.getSelectors()) {
			selectors.add(convertSelector(selector));
		}
		plan.put("selectors", selectors);

		List<Map<String, Object>> groupBy = new ArrayList<>();
		for (IQuerySelector selector : selectQs.getGroupBy()) {
			groupBy.add(convertSelector(selector));
		}
		plan.put("groupBy", groupBy);
		plan.put("filter", combineFilters(selectQs.getCombinedFilters(), this.frameFilters));
		plan.put("having", convertFilters(selectQs.getHavingFilters()));
		plan.put("distinct", selectQs.isDistinct());
		plan.put("offset", Math.max(selectQs.getOffset(), 0));
		plan.put("limit", selectQs.getLimit());

		List<Map<String, Object>> sorts = new ArrayList<>();
		for (IQuerySort sort : selectQs.getCombinedOrderBy()) {
			if (!(sort instanceof QueryColumnOrderBySelector)) {
				throw new IllegalArgumentException(
						"Polars currently supports column order-by selectors only");
			}
			QueryColumnOrderBySelector columnSort = (QueryColumnOrderBySelector) sort;
			if (columnSort.isPrimKeyColumn()) {
				continue;
			}
			Map<String, Object> sortPlan = new LinkedHashMap<>();
			sortPlan.put("column", cleanColumn(columnSort.getColumn()));
			sortPlan.put("descending",
					columnSort.getSortDir() == QueryColumnOrderBySelector.ORDER_BY_DIRECTION.DESC);
			sortPlan.put("nullsLast", false);
			sorts.add(sortPlan);
		}
		plan.put("sort", sorts);
		return plan;
	}

	public static Map<String, Object> convertFilters(GenRowFilters filters) {
		if (filters == null || filters.isEmpty()) {
			return null;
		}
		List<Map<String, Object>> children = new ArrayList<>();
		for (IQueryFilter filter : filters.getFilters()) {
			children.add(convertFilter(filter));
		}
		if (children.size() == 1) {
			return children.get(0);
		}
		Map<String, Object> combined = new LinkedHashMap<>();
		combined.put("kind", "and");
		combined.put("children", children);
		return combined;
	}

	private static Map<String, Object> combineFilters(GenRowFilters queryFilters, GenRowFilters frameFilters) {
		List<Map<String, Object>> children = new ArrayList<>();
		Map<String, Object> query = convertFilters(queryFilters);
		Map<String, Object> frame = convertFilters(frameFilters);
		if (query != null) {
			children.add(query);
		}
		if (frame != null) {
			children.add(frame);
		}
		if (children.isEmpty()) {
			return null;
		}
		if (children.size() == 1) {
			return children.get(0);
		}
		Map<String, Object> combined = new LinkedHashMap<>();
		combined.put("kind", "and");
		combined.put("children", children);
		return combined;
	}

	private static Map<String, Object> convertFilter(IQueryFilter filter) {
		switch (filter.getQueryFilterType()) {
		case SIMPLE:
			return convertSimpleFilter((SimpleQueryFilter) filter);
		case AND:
		case OR:
			AbstractListFilter listFilter = (AbstractListFilter) filter;
			Map<String, Object> listPlan = new LinkedHashMap<>();
			listPlan.put("kind", filter.getQueryFilterType().toString().toLowerCase());
			List<Map<String, Object>> children = new ArrayList<>();
			for (IQueryFilter child : listFilter.getFilterList()) {
				children.add(convertFilter(child));
			}
			listPlan.put("children", children);
			return listPlan;
		default:
			throw new IllegalArgumentException(
					"Unsupported Polars filter type " + filter.getQueryFilterType());
		}
	}

	private static Map<String, Object> convertSimpleFilter(SimpleQueryFilter filter) {
		NounMetadata left = filter.getLComparison();
		NounMetadata right = filter.getRComparison();
		String comparator = filter.getComparator();
		boolean leftColumn = isColumn(left);
		boolean rightColumn = isColumn(right);
		if (!leftColumn && rightColumn) {
			NounMetadata swap = left;
			left = right;
			right = swap;
			comparator = IQueryFilter.getReverseNumericalComparator(comparator);
		} else if (!leftColumn) {
			throw new IllegalArgumentException(
					"Polars filters require at least one column operand");
		}

		Map<String, Object> plan = new LinkedHashMap<>();
		plan.put("kind", "simple");
		plan.put("left", convertOperand(left));
		plan.put("comparator", comparator);
		plan.put("right", convertOperand(right));
		return plan;
	}

	private static boolean isColumn(NounMetadata noun) {
		return noun.getNounType() == PixelDataType.COLUMN || noun.getValue() instanceof QueryColumnSelector;
	}

	private static Map<String, Object> convertOperand(NounMetadata noun) {
		Map<String, Object> operand = new LinkedHashMap<>();
		if (isColumn(noun)) {
			operand.put("kind", "column");
			operand.put("column", extractColumn(noun.getValue()));
		} else {
			operand.put("kind", "value");
			operand.put("value", noun.getValue());
		}
		return operand;
	}

	private static String extractColumn(Object value) {
		if (value instanceof QueryColumnSelector) {
			QueryColumnSelector selector = (QueryColumnSelector) value;
			return selector.isPrimKeyColumn() ? selector.getTable() : selector.getColumn();
		}
		return cleanColumn(String.valueOf(value));
	}

	private static Map<String, Object> convertSelector(IQuerySelector selector) {
		Map<String, Object> plan = new LinkedHashMap<>();
		switch (selector.getSelectorType()) {
		case COLUMN:
			QueryColumnSelector column = (QueryColumnSelector) selector;
			plan.put("kind", "column");
			plan.put("column", column.isPrimKeyColumn() ? column.getTable() : cleanColumn(column.getColumn()));
			break;
		case CONSTANT:
			plan.put("kind", "literal");
			plan.put("value", ((QueryConstantSelector) selector).getConstant());
			break;
		case ARITHMETIC:
			QueryArithmeticSelector arithmetic = (QueryArithmeticSelector) selector;
			plan.put("kind", "arithmetic");
			plan.put("operator", arithmetic.getMathExpr());
			plan.put("left", convertSelector(arithmetic.getLeftSelector()));
			plan.put("right", convertSelector(arithmetic.getRightSelector()));
			break;
		case FUNCTION:
			QueryFunctionSelector function = (QueryFunctionSelector) selector;
			plan.put("kind", "function");
			String functionName = function.getFunction();
			if (function.isDistinct() && "COUNT".equalsIgnoreCase(functionName)) {
				functionName = "UNIQUE_COUNT";
			}
			plan.put("function", functionName);
			List<Map<String, Object>> inputs = new ArrayList<>();
			for (IQuerySelector input : function.getInnerSelector()) {
				inputs.add(convertSelector(input));
			}
			plan.put("inputs", inputs);
			break;
		default:
			throw new IllegalArgumentException(
					"Unsupported Polars selector type " + selector.getSelectorType());
		}
		plan.put("alias", selector.getAlias());
		return plan;
	}

	private static String cleanColumn(String column) {
		if (column != null && column.contains("__")) {
			return column.split("__", 2)[1];
		}
		return column;
	}
}

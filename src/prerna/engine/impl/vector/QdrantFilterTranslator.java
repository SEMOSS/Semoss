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
package prerna.engine.impl.vector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.BetweenQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter.FILTER_TYPE;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public final class QdrantFilterTranslator {

	private QdrantFilterTranslator() {
	}

	public static Map<String, Object> translate(List<IQueryFilter> filters) {
		if (filters == null || filters.isEmpty()) {
			return null;
		}
		List<Map<String, Object>> mustClauses = new ArrayList<>();
		for (IQueryFilter f : filters) {
			Map<String, Object> clause = process(f);
			if (clause != null) {
				mustClauses.add(clause);
			}
		}
		if (mustClauses.isEmpty()) {
			return null;
		}
		Map<String, Object> root = new LinkedHashMap<>();
		root.put("must", mustClauses);
		return root;
	}

	private static Map<String, Object> process(IQueryFilter filter) {
		if (filter == null) {
			return null;
		}
		IQueryFilter.QUERY_FILTER_TYPE t = filter.getQueryFilterType();
		if (t == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
			return processSimple((SimpleQueryFilter) filter);
		}
		if (t == IQueryFilter.QUERY_FILTER_TYPE.AND) {
			return processGroup((AndQueryFilter) filter, "must");
		}
		if (t == IQueryFilter.QUERY_FILTER_TYPE.OR) {
			return processGroup((OrQueryFilter) filter, "should");
		}
		if (t == IQueryFilter.QUERY_FILTER_TYPE.BETWEEN) {
			return processBetween((BetweenQueryFilter) filter);
		}
		if (t == IQueryFilter.QUERY_FILTER_TYPE.FUNCTION) {
			throw new IllegalArgumentException("FUNCTION filter type not supported for Qdrant");
		}
		return null;
	}

	private static Map<String, Object> processGroup(IQueryFilter group, String key) {
		List<IQueryFilter> children;
		if (group instanceof AndQueryFilter) {
			children = ((AndQueryFilter) group).getFilterList();
		} else if (group instanceof OrQueryFilter) {
			children = ((OrQueryFilter) group).getFilterList();
		} else {
			return null;
		}
		List<Map<String, Object>> clauses = new ArrayList<>();
		for (IQueryFilter c : children) {
			Map<String, Object> clause = process(c);
			if (clause != null) {
				clauses.add(clause);
			}
		}
		if (clauses.isEmpty()) {
			return null;
		}
		Map<String, Object> wrapper = new LinkedHashMap<>();
		wrapper.put(key, clauses);
		return wrapper;
	}

	private static Map<String, Object> processBetween(BetweenQueryFilter filter) {
		String key = columnKey(filter.getColumn());
		if (key == null) {
			return null;
		}
		Map<String, Object> range = new LinkedHashMap<>();
		range.put("gte", filter.getStart());
		range.put("lte", filter.getEnd());

		Map<String, Object> condition = new LinkedHashMap<>();
		condition.put("key", key);
		condition.put("range", range);
		return condition;
	}

	private static Map<String, Object> processSimple(SimpleQueryFilter filter) {
		FILTER_TYPE fType = filter.getSimpleFilterType();
		NounMetadata left = filter.getLComparison();
		NounMetadata right = filter.getRComparison();
		String comparator = filter.getComparator();
		if (comparator != null) {
			comparator = comparator.trim();
		}

		if (fType == FILTER_TYPE.COL_TO_VALUES) {
			return buildValueCondition(columnKey(left), right.getValue(), comparator);
		}
		if (fType == FILTER_TYPE.VALUES_TO_COL) {
			return buildValueCondition(columnKey(right), left.getValue(),
					IQueryFilter.getReverseNumericalComparator(comparator));
		}
		throw new IllegalArgumentException("Unsupported simple filter type for Qdrant: " + fType);
	}

	private static Map<String, Object> buildValueCondition(String key, Object value, String comparator) {
		if (key == null) {
			return null;
		}
		if (comparator == null) {
			comparator = "==";
		}
		if (isPointIdKey(key)) {
			return buildHasIdCondition(value, comparator);
		}
		Map<String, Object> condition = new LinkedHashMap<>();
		condition.put("key", key);

		switch (comparator) {
		case "==":
			if (value instanceof Collection) {
				Map<String, Object> match = new HashMap<>();
				match.put("any", normalizeCollection((Collection<?>) value));
				condition.put("match", match);
			} else {
				Map<String, Object> match = new HashMap<>();
				match.put("value", normalizeScalar(value));
				condition.put("match", match);
			}
			break;
		case "!=":
		case "<>":
			Map<String, Object> excludeRoot = new LinkedHashMap<>();
			List<Map<String, Object>> mustNot = new ArrayList<>();
			Map<String, Object> inner = new LinkedHashMap<>();
			inner.put("key", key);
			Map<String, Object> innerMatch = new HashMap<>();
			if (value instanceof Collection) {
				innerMatch.put("any", normalizeCollection((Collection<?>) value));
			} else {
				innerMatch.put("value", normalizeScalar(value));
			}
			inner.put("match", innerMatch);
			mustNot.add(inner);
			excludeRoot.put("must_not", mustNot);
			return excludeRoot;
		case ">":
			condition.put("range", singleRange("gt", value));
			break;
		case ">=":
			condition.put("range", singleRange("gte", value));
			break;
		case "<":
			condition.put("range", singleRange("lt", value));
			break;
		case "<=":
			condition.put("range", singleRange("lte", value));
			break;
		case "?like":
		case "?begins":
		case "?ends":
			Map<String, Object> textMatch = new HashMap<>();
			textMatch.put("text", value == null ? "" : value.toString());
			condition.put("match", textMatch);
			break;
		default:
			throw new IllegalArgumentException("Comparator not supported for Qdrant filter: " + comparator);
		}
		return condition;
	}

	private static boolean isPointIdKey(String key) {
		return "id".equalsIgnoreCase(key) || "_id".equalsIgnoreCase(key)
				|| "point_id".equalsIgnoreCase(key);
	}

	private static Map<String, Object> buildHasIdCondition(Object value, String comparator) {
		List<Object> ids;
		if (value instanceof Collection) {
			ids = normalizeCollection((Collection<?>) value);
		} else {
			ids = new ArrayList<>();
			Object scalar = normalizeScalar(value);
			if (scalar != null) {
				ids.add(scalar);
			}
		}
		if (ids.isEmpty()) {
			throw new IllegalArgumentException(
					"has_id filter requires at least one point id. An empty list would silently match everything under negation (delete-by-filter foot-gun).");
		}
		Map<String, Object> condition = new LinkedHashMap<>();
		condition.put("has_id", ids);
		if ("!=".equals(comparator) || "<>".equals(comparator)) {
			Map<String, Object> exclude = new LinkedHashMap<>();
			List<Map<String, Object>> mustNot = new ArrayList<>();
			mustNot.add(condition);
			exclude.put("must_not", mustNot);
			return exclude;
		}
		return condition;
	}

	private static Map<String, Object> singleRange(String op, Object value) {
		Map<String, Object> range = new LinkedHashMap<>();
		range.put(op, value);
		return range;
	}

	private static String columnKey(NounMetadata nm) {
		if (nm == null) {
			return null;
		}
		Object v = nm.getValue();
		if (v instanceof IQuerySelector) {
			return columnKey((IQuerySelector) v);
		}
		return v == null ? null : v.toString();
	}

	private static String columnKey(IQuerySelector selector) {
		if (selector == null) {
			return null;
		}
		return selector.getQueryStructName();
	}

	private static Object normalizeScalar(Object value) {
		if (value instanceof Number || value instanceof Boolean) {
			return value;
		}
		return value == null ? null : value.toString();
	}

	private static List<Object> normalizeCollection(Collection<?> values) {
		List<Object> out = new ArrayList<>(values.size());
		for (Object v : values) {
			out.add(normalizeScalar(v));
		}
		return out;
	}
}

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
import java.util.List;
import java.util.Map;

import prerna.query.querystruct.filters.AbstractListFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter.FILTER_TYPE;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Translates SEMOSS {@link IQueryFilter}s into a Chroma {@code where} clause — e.g.
 * {@code Filter(Source == ["a","b"])} becomes {@code {"Source": {"$in": ["a","b"]}}}, with nested
 * {@code $and}/{@code $or} groups. Supports column-to-values filters
 * ({@code $eq}/{@code $in}/{@code $ne}/{@code $nin}/{@code $gt}/{@code $gte}/{@code $lt}/{@code $lte});
 * unsupported filter shapes are skipped rather than throwing.
 */
public final class ChromaVectorQueryFilterTranslationHelper {

	private static final String AND = "$and";
	private static final String OR = "$or";

	private ChromaVectorQueryFilterTranslationHelper() {
	}

	/** Combine top-level filters into a single AND-ed Chroma {@code where} clause, or {@code null} if none. */
	public static Map<String, Object> toWhere(List<IQueryFilter> filters) {
		if (filters == null || filters.isEmpty()) {
			return null;
		}
		List<Map<String, Object>> clauses = new ArrayList<>();
		for (IQueryFilter filter : filters) {
			Map<String, Object> clause = translate(filter);
			if (clause != null) {
				clauses.add(clause);
			}
		}
		return combine(AND, clauses);
	}

	private static Map<String, Object> translate(IQueryFilter filter) {
		if (filter == null) {
			return null;
		}
		switch (filter.getQueryFilterType()) {
			case SIMPLE:
				return translateSimple((SimpleQueryFilter) filter);
			case AND:
				return translateGroup(AND, (AbstractListFilter) filter);
			case OR:
				return translateGroup(OR, (AbstractListFilter) filter);
			default:
				return null;
		}
	}

	private static Map<String, Object> translateGroup(String operator, AbstractListFilter filter) {
		List<Map<String, Object>> clauses = new ArrayList<>();
		List<IQueryFilter> filterList = filter.getFilterList();
		if (filterList != null) {
			for (IQueryFilter child : filterList) {
				Map<String, Object> clause = translate(child);
				if (clause != null) {
					clauses.add(clause);
				}
			}
		}
		return combine(operator, clauses);
	}

	/** Collapse clauses under a boolean operator; one clause is returned as-is, none yields {@code null}. */
	private static Map<String, Object> combine(String operator, List<Map<String, Object>> clauses) {
		if (clauses.isEmpty()) {
			return null;
		}
		if (clauses.size() == 1) {
			return clauses.get(0);
		}
		Map<String, Object> combined = new HashMap<>();
		combined.put(operator, clauses);
		return combined;
	}

	private static Map<String, Object> translateSimple(SimpleQueryFilter filter) {
		// only column-to-values filters map to a Chroma where clause
		if (filter.getSimpleFilterType() != FILTER_TYPE.COL_TO_VALUES) {
			return null;
		}
		String column = extractColumn(filter.getLComparison());
		if (column == null) {
			return null;
		}
		List<Object> values = normalizeToList(filter.getRComparison().getValue());
		if (values.isEmpty()) {
			return null;
		}
		Map<String, Object> condition = buildCondition(filter.getComparator(), values);
		if (condition == null) {
			return null;
		}
		Map<String, Object> clause = new HashMap<>();
		clause.put(column, condition);
		return clause;
	}

	private static Map<String, Object> buildCondition(String comparator, List<Object> values) {
		boolean multi = values.size() > 1;
		Map<String, Object> condition = new HashMap<>();
		switch (comparator) {
			case "==":
			case "=":
				condition.put(multi ? "$in" : "$eq", multi ? values : values.get(0));
				break;
			case "!=":
			case "<>":
				condition.put(multi ? "$nin" : "$ne", multi ? values : values.get(0));
				break;
			case ">":
				condition.put("$gt", values.get(0));
				break;
			case ">=":
				condition.put("$gte", values.get(0));
				break;
			case "<":
				condition.put("$lt", values.get(0));
				break;
			case "<=":
				condition.put("$lte", values.get(0));
				break;
			default:
				return null;
		}
		return condition;
	}

	/** Column name from a simple filter's left comparison. */
	private static String extractColumn(NounMetadata leftComparison) {
		if (leftComparison == null) {
			return null;
		}
		Object value = leftComparison.getValue();
		if (value instanceof IQuerySelector) {
			return ((IQuerySelector) value).getQueryStructName();
		}
		return value == null ? null : value.toString();
	}

	/** Flatten a right-comparison value to a list (handles a single value or a collection). */
	private static List<Object> normalizeToList(Object value) {
		List<Object> values = new ArrayList<>();
		if (value == null) {
			return values;
		}
		if (value instanceof Collection<?>) {
			for (Object element : (Collection<?>) value) {
				if (element != null) {
					values.add(element);
				}
			}
		} else {
			values.add(value);
		}
		return values;
	}
}

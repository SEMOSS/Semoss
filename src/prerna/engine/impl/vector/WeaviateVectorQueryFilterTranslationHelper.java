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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import io.weaviate.client6.v1.api.collections.query.Filter;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter.FILTER_TYPE;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Translates SEMOSS {@link IQueryFilter}s into the Weaviate v6 {@link Filter}
 * model, mirroring what {@link RestVectorQueryFilterTranslationHelper} does for
 * Open/Elastic Search. The top-level list of filters is AND'd together.
 *
 * Note: equality filters compare against whole property values, so the
 * filterable metadata properties (source, modality, divider, part) must be
 * created with FIELD tokenization for exact matching to be reliable.
 */
public final class WeaviateVectorQueryFilterTranslationHelper {

	private WeaviateVectorQueryFilterTranslationHelper() {

	}

	/**
	 * Translates a list of filters into a single Weaviate filter, AND'ing the
	 * top-level entries together.
	 *
	 * @param filters the SEMOSS filters to translate
	 * @return the combined Weaviate filter, or null if there is nothing to apply
	 */
	public static Filter translate(List<IQueryFilter> filters) {
		return combineList(filters, true);
	}

	/**
	 * @param filters the filters to translate and combine
	 * @param and     true to AND the results together, false to OR
	 * @return the combined filter, or null if nothing translated
	 */
	private static Filter combineList(List<IQueryFilter> filters, boolean and) {
		if (filters == null || filters.isEmpty()) {
			return null;
		}
		List<Filter> children = new ArrayList<>();
		for (IQueryFilter queryFilter : filters) {
			Filter translated = translateFilter(queryFilter);
			if (translated != null) {
				children.add(translated);
			}
		}
		if (children.isEmpty()) {
			return null;
		}
		if (children.size() == 1) {
			return children.get(0);
		}
		Filter[] operands = children.toArray(new Filter[0]);
		return and ? Filter.and(operands) : Filter.or(operands);
	}

	/**
	 * @param queryFilter the filter to translate
	 * @return the equivalent Weaviate filter
	 */
	private static Filter translateFilter(IQueryFilter queryFilter) {
		IQueryFilter.QUERY_FILTER_TYPE type = queryFilter.getQueryFilterType();
		if (type == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
			return translateSimpleFilter((SimpleQueryFilter) queryFilter);
		} else if (type == IQueryFilter.QUERY_FILTER_TYPE.AND) {
			return combineList(((AndQueryFilter) queryFilter).getFilterList(), true);
		} else if (type == IQueryFilter.QUERY_FILTER_TYPE.OR) {
			return combineList(((OrQueryFilter) queryFilter).getFilterList(), false);
		}
		throw new IllegalArgumentException(
				"Filters with a Query Filter Type of " + type + " are not supported for Weaviate vector databases");
	}

	/**
	 * @param filter the simple filter to translate
	 * @return the equivalent Weaviate filter
	 */
	private static Filter translateSimpleFilter(SimpleQueryFilter filter) {
		FILTER_TYPE fType = filter.getSimpleFilterType();
		String comparator = filter.getComparator();
		if (fType == FILTER_TYPE.COL_TO_VALUES) {
			return buildColToValuesFilter(filter.getLComparison(), filter.getRComparison(), comparator);
		} else if (fType == FILTER_TYPE.VALUES_TO_COL) {
			// same logic, just switch the order and reverse the comparator if it is numeric
			return buildColToValuesFilter(filter.getRComparison(), filter.getLComparison(),
					IQueryFilter.getReverseNumericalComparator(comparator));
		}
		throw new IllegalArgumentException(
				"Filter with a Filter Type of " + fType + " is not supported for Weaviate vector databases");
	}

	/**
	 * @param colComp    the column side of the comparison
	 * @param valComp    the value(s) side of the comparison
	 * @param comparator the comparator between them
	 * @return the equivalent Weaviate filter, or null if there are no values
	 */
	private static Filter buildColToValuesFilter(NounMetadata colComp, NounMetadata valComp, String comparator) {
		String property = normalizeProperty(String.valueOf(colComp.getValue()));
		List<Object> values = normalizeToList(valComp.getValue());
		if (values.isEmpty()) {
			return null;
		}

		switch (comparator) {
		case "==":
			return equalityFilter(property, values, false);
		case "!=":
		case "<>":
			return equalityFilter(property, values, true);
		case "<":
			return rangeFilter(property, values.get(0), "<");
		case "<=":
			return rangeFilter(property, values.get(0), "<=");
		case ">":
			return rangeFilter(property, values.get(0), ">");
		case ">=":
			return rangeFilter(property, values.get(0), ">=");
		case "?like":
			return Filter.property(property).like("*" + values.get(0) + "*");
		case "?nlike":
			return Filter.not(Filter.property(property).like("*" + values.get(0) + "*"));
		case "?begins":
			return Filter.property(property).like(values.get(0) + "*");
		case "?nbegins":
			return Filter.not(Filter.property(property).like(values.get(0) + "*"));
		case "?ends":
			return Filter.property(property).like("*" + values.get(0));
		case "?nends":
			return Filter.not(Filter.property(property).like("*" + values.get(0)));
		default:
			throw new IllegalArgumentException(
					"Comparator '" + comparator + "' is not supported for Weaviate vector databases");
		}
	}

	/**
	 * Builds an equality (or, when multiple values, a "matches any of") filter,
	 * optionally negated for the != case.
	 *
	 * @param property the property to filter on
	 * @param values   the value(s) to match
	 * @param negate   true for != / &lt;&gt;
	 * @return the equality filter
	 */
	private static Filter equalityFilter(String property, List<Object> values, boolean negate) {
		Filter equality;
		if (values.size() == 1) {
			equality = Filter.property(property).eq(String.valueOf(values.get(0)));
		} else {
			String[] valueArray = values.stream().map(String::valueOf).toArray(String[]::new);
			equality = Filter.property(property).containsAny(valueArray);
		}
		return negate ? Filter.not(equality) : equality;
	}

	/**
	 * @param property   the property to filter on
	 * @param value      the single comparison value
	 * @param comparator one of &lt;, &lt;=, &gt;, &gt;=
	 * @return the range filter
	 */
	private static Filter rangeFilter(String property, Object value, String comparator) {
		Double numericValue = tryParseDouble(value);
		Filter.FilterBuilder builder = Filter.property(property);
		switch (comparator) {
		case "<":
			return numericValue != null ? builder.lt(numericValue.doubleValue()) : builder.lt(String.valueOf(value));
		case "<=":
			return numericValue != null ? builder.lte(numericValue.doubleValue()) : builder.lte(String.valueOf(value));
		case ">":
			return numericValue != null ? builder.gt(numericValue.doubleValue()) : builder.gt(String.valueOf(value));
		case ">=":
			return numericValue != null ? builder.gte(numericValue.doubleValue()) : builder.gte(String.valueOf(value));
		default:
			throw new IllegalArgumentException(
					"Comparator '" + comparator + "' is not supported for Weaviate vector databases");
		}
	}

	/**
	 * Weaviate lowercases the first letter of property names, and vector filter
	 * columns may carry a table prefix (table__Column); normalize to the stored
	 * property name.
	 *
	 * @param column the raw column name from the filter
	 * @return the normalized property name
	 */
	private static String normalizeProperty(String column) {
		if (column == null || column.isEmpty()) {
			return column;
		}
		String name = column;
		int prefixSeparator = name.lastIndexOf("__");
		if (prefixSeparator > -1) {
			name = name.substring(prefixSeparator + 2);
		}
		if (name.isEmpty()) {
			return name;
		}
		return Character.toLowerCase(name.charAt(0)) + name.substring(1);
	}

	/**
	 * @param value the value to parse
	 * @return the numeric value, or null if it is not a number
	 */
	private static Double tryParseDouble(Object value) {
		if (value instanceof Number) {
			return ((Number) value).doubleValue();
		}
		try {
			return Double.valueOf(String.valueOf(value));
		} catch (NumberFormatException e) {
			return null;
		}
	}

	/**
	 * @param values a single value or a collection of values
	 * @return a flat list of non-null values
	 */
	private static List<Object> normalizeToList(Object values) {
		if (values instanceof String || values instanceof Number) {
			return Collections.singletonList(values);
		} else if (values instanceof Collection<?>) {
			return ((Collection<?>) values).stream().filter(Objects::nonNull).collect(Collectors.toList());
		}
		throw new IllegalArgumentException(
				"Unsupported filter value type: " + (values == null ? "null" : values.getClass().getName()));
	}

}

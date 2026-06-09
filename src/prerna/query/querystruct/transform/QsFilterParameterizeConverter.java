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
package prerna.query.querystruct.transform;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class QsFilterParameterizeConverter {

	private static final Logger classLogger = LogManager.getLogger(QsFilterParameterizeConverter.class);

	private QsFilterParameterizeConverter() {

	}

	public static IQueryFilter modifyFilter(IQueryFilter filter, String colToParameterize,
			Map<String, List<String>> colToComparators) {
		if (filter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
			return convertSimpleQueryFilter((SimpleQueryFilter) filter, colToParameterize, colToComparators);
		} else if (filter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.AND) {
			return convertAndQueryFilter((AndQueryFilter) filter, colToParameterize, colToComparators);
		} else if (filter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.OR) {
			return convertOrQueryFilter((OrQueryFilter) filter, colToParameterize, colToComparators);
		}

		return null;
	}

	public static IQueryFilter convertOrQueryFilter(OrQueryFilter filter, String colToParameterize,
			Map<String, List<String>> colToComparators) {
		OrQueryFilter newFilter = new OrQueryFilter();
		for (IQueryFilter f : filter.getFilterList()) {
			IQueryFilter newF = modifyFilter(f, colToParameterize, colToComparators);
			newFilter.addFilter(newF);
		}
		return newFilter;
	}

	public static IQueryFilter convertAndQueryFilter(AndQueryFilter filter, String colToParameterize,
			Map<String, List<String>> colToComparators) {
		AndQueryFilter newFilter = new AndQueryFilter();
		for (IQueryFilter f : filter.getFilterList()) {
			IQueryFilter newF = modifyFilter(f, colToParameterize, colToComparators);
			newFilter.addFilter(newF);
		}
		return newFilter;
	}

	public static IQueryFilter convertSimpleQueryFilter(SimpleQueryFilter filter, String colToParameterize,
			Map<String, List<String>> colToComparators) {
		NounMetadata newL = null;
		NounMetadata newR = null;

		boolean parameterizeLeft = false;
		boolean parameterizeRight = false;

		NounMetadata origL = filter.getLComparison();
		if (origL.getNounType() == PixelDataType.COLUMN) {
			IQuerySelector selector = (IQuerySelector) origL.getValue();
			if (selector.getAlias().equals(colToParameterize)) {
				parameterizeRight = true;
			}
		}
		NounMetadata origR = filter.getRComparison();
		if (origR.getNounType() == PixelDataType.COLUMN) {
			IQuerySelector selector = (IQuerySelector) origL.getValue();
			if (selector.getAlias().equals(colToParameterize)) {
				parameterizeLeft = true;
			}
		}

		String comparator = filter.getComparator();
		if (parameterizeLeft) {
			// keep the same right
			newR = origR;
			// create the new left hand side
			newL = new NounMetadata(
					"<" + colToParameterize + "__" + IQueryFilter.getSimpleNameForComparator(comparator) + ">",
					PixelDataType.CONST_STRING);
			addToColToParam(colToComparators, colToParameterize, comparator);

		} else if (parameterizeRight) {
			// keep the same left
			newL = origL;
			// create the new right hand side
			newR = new NounMetadata(
					"<" + colToParameterize + "__" + IQueryFilter.getSimpleNameForComparator(comparator) + ">",
					PixelDataType.CONST_STRING);
			addToColToParam(colToComparators, colToParameterize, comparator);

		} else {
			// return the original
			return filter;
		}

		SimpleQueryFilter newF = new SimpleQueryFilter(newL, filter.getComparator(), newR);
		return newF;
	}

	private static void addToColToParam(Map<String, List<String>> colToComparators, String col, String comparator) {
		List<String> colComparators = null;
		if (colToComparators.containsKey(col)) {
			colComparators = colToComparators.get(col);
		} else {
			colComparators = new Vector<>();
			colToComparators.put(col, colComparators);
		}
		classLogger.info("Found filter on column = " + col + " with comparator = " + comparator);
		colComparators.add(comparator);
	}

	///////////////////////////////////////////////////////////

	public static void findSelectorsForAlias(IQueryFilter filter, String colToParameterize, List<String> qsList) {
		if (filter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
			searchSimpleQueryFilter((SimpleQueryFilter) filter, colToParameterize, qsList);
		} else if (filter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.AND) {
			searchAndQueryFilter((AndQueryFilter) filter, colToParameterize, qsList);
		} else if (filter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.OR) {
			searchOrQueryFilter((OrQueryFilter) filter, colToParameterize, qsList);
		}
	}

	private static void searchOrQueryFilter(OrQueryFilter filter, String colToParameterize, List<String> qsList) {
		for (IQueryFilter f : filter.getFilterList()) {
			findSelectorsForAlias(f, colToParameterize, qsList);
		}
	}

	private static void searchAndQueryFilter(AndQueryFilter filter, String colToParameterize, List<String> qsList) {
		for (IQueryFilter f : filter.getFilterList()) {
			findSelectorsForAlias(f, colToParameterize, qsList);
		}
	}

	private static void searchSimpleQueryFilter(SimpleQueryFilter filter, String colToParameterize,
			List<String> qsList) {
		NounMetadata origL = filter.getLComparison();
		if (origL.getNounType() == PixelDataType.COLUMN) {
			IQuerySelector selector = (IQuerySelector) origL.getValue();
			if (selector.getAlias().equals(colToParameterize)) {
				qsList.add(selector.getQueryStructName());
			}
		}
		NounMetadata origR = filter.getRComparison();
		if (origR.getNounType() == PixelDataType.COLUMN) {
			IQuerySelector selector = (IQuerySelector) origL.getValue();
			if (selector.getAlias().equals(colToParameterize)) {
				qsList.add(selector.getQueryStructName());
			}
		}
	}

}

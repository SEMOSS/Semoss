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
	
	private static final Logger logger = LogManager.getLogger(QsFilterParameterizeConverter.class);
	
	private QsFilterParameterizeConverter() {

	}
	
	public static IQueryFilter modifyFilter(IQueryFilter filter, String colToParameterize, Map<String, List<String>> colToComparators) {
		if(filter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
			return convertSimpleQueryFilter((SimpleQueryFilter) filter, colToParameterize, colToComparators);
		} else if(filter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.AND) {
			return convertAndQueryFilter((AndQueryFilter) filter, colToParameterize, colToComparators);
		} else if(filter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.OR) {
			return convertOrQueryFilter((OrQueryFilter) filter, colToParameterize, colToComparators);
		}
		
		return null;
	}

	public static IQueryFilter convertOrQueryFilter(OrQueryFilter filter, String colToParameterize, Map<String, List<String>> colToComparators) {
		OrQueryFilter newFilter = new OrQueryFilter();
		for(IQueryFilter f : filter.getFilterList()) {
			IQueryFilter newF = modifyFilter(f, colToParameterize, colToComparators);
			newFilter.addFilter(newF);
		}
		return newFilter;
	}

	public static IQueryFilter convertAndQueryFilter(AndQueryFilter filter, String colToParameterize, Map<String, List<String>> colToComparators) {
		AndQueryFilter newFilter = new AndQueryFilter();
		for(IQueryFilter f : filter.getFilterList()) {
			IQueryFilter newF = modifyFilter(f, colToParameterize, colToComparators);
			newFilter.addFilter(newF);
		}
		return newFilter;
	}

	public static IQueryFilter convertSimpleQueryFilter(SimpleQueryFilter filter, String colToParameterize, Map<String, List<String>> colToComparators) {
		NounMetadata newL = null;
		NounMetadata newR = null;

		boolean parameterizeLeft = false;
		boolean parameterizeRight = false;
		
		NounMetadata origL = filter.getLComparison();
		if(origL.getNounType() == PixelDataType.COLUMN) {
			IQuerySelector selector = (IQuerySelector) origL.getValue();
			if(selector.getAlias().equals(colToParameterize)) {
				parameterizeRight = true;
			}
		}
		NounMetadata origR = filter.getRComparison();
		if(origR.getNounType() == PixelDataType.COLUMN) {
			IQuerySelector selector = (IQuerySelector) origL.getValue();
			if(selector.getAlias().equals(colToParameterize)) {
				parameterizeLeft = true;
			}
		}
		
		if(parameterizeLeft) {
			// keep the same right
			newR = origR;
			// create the new left hand side
			String comparator = filter.getComparator();
			newL = new NounMetadata("<" + colToParameterize + "__" + IQueryFilter.getSimpleNameForComparator(comparator) + ">", PixelDataType.CONST_STRING);
			addToColToParam(colToComparators, colToParameterize, comparator);

		} else if(parameterizeRight) {
			// keep the same left
			newL = origL;
			// create the new right hand side
			String comparator = filter.getComparator();
			newR = new NounMetadata("<" + colToParameterize + "__" + IQueryFilter.getSimpleNameForComparator(comparator) + ">", PixelDataType.CONST_STRING);
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
		if(colToComparators.containsKey(col)) {
			colComparators = colToComparators.get(col);
		} else {
			colComparators = new Vector<>();
			colToComparators.put(col, colComparators);
		}
		logger.info("Found filter on column = " + col + " with comparator = " + comparator);
		colComparators.add(comparator);
	}
	
	///////////////////////////////////////////////////////////
	
	
	public static void findSelectorsForAlias(IQueryFilter filter, String colToParameterize, List<String> qsList) {
		if(filter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
			searchSimpleQueryFilter((SimpleQueryFilter) filter, colToParameterize, qsList);
		} else if(filter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.AND) {
			searchAndQueryFilter((AndQueryFilter) filter, colToParameterize, qsList);
		} else if(filter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.OR) {
			searchOrQueryFilter((OrQueryFilter) filter, colToParameterize, qsList);
		}
	}

	private static void searchOrQueryFilter(OrQueryFilter filter, String colToParameterize, List<String> qsList) {
		for(IQueryFilter f : filter.getFilterList()) {
			findSelectorsForAlias(f, colToParameterize, qsList);
		}
	}

	private static void searchAndQueryFilter(AndQueryFilter filter, String colToParameterize, List<String> qsList) {
		for(IQueryFilter f : filter.getFilterList()) {
			findSelectorsForAlias(f, colToParameterize, qsList);
		}
	}

	private static void searchSimpleQueryFilter(SimpleQueryFilter filter, String colToParameterize, List<String> qsList) {
		NounMetadata origL = filter.getLComparison();
		if(origL.getNounType() == PixelDataType.COLUMN) {
			IQuerySelector selector = (IQuerySelector) origL.getValue();
			if(selector.getAlias().equals(colToParameterize)) {
				qsList.add(selector.getQueryStructName());
			}
		}
		NounMetadata origR = filter.getRComparison();
		if(origR.getNounType() == PixelDataType.COLUMN) {
			IQuerySelector selector = (IQuerySelector) origL.getValue();
			if(selector.getAlias().equals(colToParameterize)) {
				qsList.add(selector.getQueryStructName());
			}
		}
	}
	
}

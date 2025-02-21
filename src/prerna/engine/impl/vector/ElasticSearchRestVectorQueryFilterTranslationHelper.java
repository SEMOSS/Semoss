package prerna.engine.impl.vector;

import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter.FILTER_TYPE;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public final class ElasticSearchRestVectorQueryFilterTranslationHelper {
	
	/**
	 * 
	* @param filters
	 * @return
	 */
	public static JsonArray addFilters(List<IQueryFilter> filters) {
		JsonArray filterArray = new JsonArray();
		
		for(IQueryFilter filter : filters) {
			JsonObject filterSyntax = processFilter(filter);
			if(filterSyntax != null) {
				filterArray.add(filterSyntax);
			}
		}
		
		if (filterArray.size() == 0) {
			throw new IllegalArgumentException("Unable to generate filter");
		}
		return filterArray;
	}
	
	/**
	 * 
	 * @param filter
	 * @return
	 */
	private static JsonObject processFilter(IQueryFilter filter) {
		// logic taken from SqlInterpreter.processFilter
		IQueryFilter.QUERY_FILTER_TYPE filterType = filter.getQueryFilterType();
		if(filterType == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
			return processSimpleQueryFilter((SimpleQueryFilter) filter);
		} 
//		else if(filterType == IQueryFilter.QUERY_FILTER_TYPE.AND) {
//			return processAndQueryFilter((AndQueryFilter) filter);
//		} else if(filterType == IQueryFilter.QUERY_FILTER_TYPE.OR) {
//			return processOrQueryFilter((OrQueryFilter) filter);
//		} else if(filterType == IQueryFilter.QUERY_FILTER_TYPE.FUNCTION) {
//			throw new IllegalArgumentException("Filters with a Query Filter Type of Function are not supported for FAISS vector databases");
//		}else if(filterType == IQueryFilter.QUERY_FILTER_TYPE.BETWEEN) {
//			throw new IllegalArgumentException("Filters with a Query Filter Type of Between are not supported for FAISS vector databases");
//		}
		return null;
	}
	
	/**
	 * 
	 * @param filter
	 * @return
	 */
	private static JsonObject processSimpleQueryFilter(SimpleQueryFilter filter) {
		NounMetadata leftComp = filter.getLComparison();
		NounMetadata rightComp = filter.getRComparison();
		String thisComparator = filter.getComparator();
		
		FILTER_TYPE fType = filter.getSimpleFilterType();
				
		if(fType == FILTER_TYPE.COL_TO_VALUES) {
			return addSelectorToValuesFilter(leftComp, rightComp, thisComparator);
		}
//		if(fType == FILTER_TYPE.COL_TO_COL) {
//			return addSelectorToSelectorFilter(leftComp, rightComp, thisComparator);
//		} else if(fType == FILTER_TYPE.COL_TO_VALUES) {
//			return addSelectorToValuesFilter(leftComp, rightComp, thisComparator);
//		} else if(fType == FILTER_TYPE.VALUES_TO_COL) {
//			// same logic as above, just switch the order and reverse the comparator if it is numeric
//			return addSelectorToValuesFilter(rightComp, leftComp, IQueryFilter.getReverseNumericalComparator(thisComparator));
//		} else if(fType == FILTER_TYPE.COL_TO_QUERY) {
//			throw new IllegalArgumentException("Filter of with a Filter Type of COL_TO_QUERY are not supported for FAISS vector databases");
//		} else if(fType == FILTER_TYPE.QUERY_TO_COL) {
//			throw new IllegalArgumentException("Filter of with a Filter Type of QUERY_TO_COL are not supported for FAISS vector databases");
//		} else if(fType == FILTER_TYPE.COL_TO_LAMBDA) {
//			throw new IllegalArgumentException("Filter of with a Filter Type of COL_TO_LAMBDA are not supported for FAISS vector databases");
//		} else if(fType == FILTER_TYPE.LAMBDA_TO_COL) {
//			// same logic as above, just switch the order and reverse the comparator if it is numeric
//			throw new IllegalArgumentException("Filter of with a Filter Type of LAMBDA_TO_COL are not supported for FAISS vector databases");
//		} else if(fType == FILTER_TYPE.VALUE_TO_VALUE) {
//			// WHY WOULD YOU DO THIS!!!
//			throw new IllegalArgumentException("Filter of with a Filter Type of VALUE_TO_VALUE are not supported for FAISS vector databases");
//		} 
		return null;
	}
		
	/**
	 * Add filter for a column to values
	 * @param filters 
	 * @param leftComp
	 * @param rightComp
	 * @param thisComparator
	 */
	private static JsonObject addSelectorToValuesFilter(NounMetadata leftComp, NounMetadata rightComp, String thisComparator) {
	
		JsonObject jsonBuilder = new JsonObject();
		{
			JsonObject match = new JsonObject();
			{
				match.addProperty(leftComp.getValue().toString(), rightComp.getValue().toString());
			}
			jsonBuilder.add("match", match);
		}
		return jsonBuilder;
	}
}

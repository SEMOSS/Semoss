package prerna.engine.impl.vector;

import java.math.BigDecimal;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter.FILTER_TYPE;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public final class ElasticSearchRestVectorQueryFilterTranslationHelper {
	

	public static void processFilter(IQueryFilter queryFilter, JsonArray filter, JsonArray should, JsonArray must_not) {
		
		IQueryFilter.QUERY_FILTER_TYPE filterType = queryFilter.getQueryFilterType();
		if(filterType == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
			addSimpleQueryFilter((SimpleQueryFilter) queryFilter, filter, must_not);
		} else if(filterType == IQueryFilter.QUERY_FILTER_TYPE.AND) {
			addAndFilter((AndQueryFilter) queryFilter, filter, must_not);
		} else if(filterType == IQueryFilter.QUERY_FILTER_TYPE.OR) {
			addOrFilter((OrQueryFilter) queryFilter, should, must_not);
		} else if(filterType == IQueryFilter.QUERY_FILTER_TYPE.FUNCTION) {
			throw new IllegalArgumentException("Filters with a Query Filter Type of Function are not supported for Elastic Search vector databases");
		} else if(filterType == IQueryFilter.QUERY_FILTER_TYPE.BETWEEN) {
			throw new IllegalArgumentException("Filters with a Query Filter Type of Between are not supported for Elastic Search vector databases");
		}
		
	}
	
	private static void addSimpleQueryFilter(SimpleQueryFilter filter, JsonArray targetArray, JsonArray must_not) {
		if (filter.getComparator().equals("!=")) {
			must_not.add(processSimpleQueryFilter(filter));
		} else {
			targetArray.add(processSimpleQueryFilter(filter));
		}
	}
	
	public static JsonObject processSimpleQueryFilter(SimpleQueryFilter filter) {
		NounMetadata leftComp = filter.getLComparison();
		NounMetadata rightComp = filter.getRComparison();
		String thisComparator = filter.getComparator();
		
		FILTER_TYPE fType = filter.getSimpleFilterType();

		if(fType == FILTER_TYPE.COL_TO_COL) {
			throw new IllegalArgumentException("Filter of with a Filter Type of COL_TO_COL are not supported for FAISS vector databases");
		} else if(fType == FILTER_TYPE.COL_TO_VALUES) {
			return addSelectorToValuesFilter(leftComp, rightComp, thisComparator);
		} else if(fType == FILTER_TYPE.VALUES_TO_COL) {
			throw new IllegalArgumentException("Filter of with a Filter Type of VALUES_TO_COL are not supported for FAISS vector databases");
		} else if(fType == FILTER_TYPE.COL_TO_QUERY) {
			throw new IllegalArgumentException("Filter of with a Filter Type of COL_TO_QUERY are not supported for FAISS vector databases");
		} else if(fType == FILTER_TYPE.QUERY_TO_COL) {
			throw new IllegalArgumentException("Filter of with a Filter Type of QUERY_TO_COL are not supported for FAISS vector databases");
		} else if(fType == FILTER_TYPE.COL_TO_LAMBDA) {
			throw new IllegalArgumentException("Filter of with a Filter Type of COL_TO_LAMBDA are not supported for FAISS vector databases");
		} else if(fType == FILTER_TYPE.LAMBDA_TO_COL) {
			// same logic as above, just switch the order and reverse the comparator if it is numeric
			throw new IllegalArgumentException("Filter of with a Filter Type of LAMBDA_TO_COL are not supported for FAISS vector databases");
		} else if(fType == FILTER_TYPE.VALUE_TO_VALUE) {
			// WHY WOULD YOU DO THIS!!!
			throw new IllegalArgumentException("Filter of with a Filter Type of VALUE_TO_VALUE are not supported for FAISS vector databases");
		}
		return null;
	}
	
	private static void addAndFilter(AndQueryFilter queryFilter, JsonArray targetArray, JsonArray must_not) {
		
		List<IQueryFilter> filterList = queryFilter.getFilterList();
		int numAnds = filterList.size();
		for(int i = 0; i < numAnds; i++) {
			IQueryFilter filter2 = filterList.get(i);
			if (filter2.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
				addSimpleQueryFilter((SimpleQueryFilter) filterList.get(i), targetArray, must_not);
			} else if (filter2.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.AND) {
				addAndFilter((AndQueryFilter) filterList.get(i), targetArray, must_not);
			}
		}
        // Add other filter types if needed
    }
	
	private static void addOrFilter(OrQueryFilter queryFilter, JsonArray targetArray, JsonArray must_not) {
		
		List<IQueryFilter> filterList = queryFilter.getFilterList();
		int numAnds = filterList.size();
		for(int i = 0; i < numAnds; i++) {
			IQueryFilter filter2 = filterList.get(i);
			if (filter2.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
				addSimpleQueryFilter((SimpleQueryFilter) filterList.get(i), targetArray, must_not);
			} else if (filter2.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.OR) {
				addOrFilter((OrQueryFilter) filterList.get(i), targetArray, must_not);
			}
		}
        // Add other filter types if needed
    }

//	public static JsonArray processAndQueryFilter(AndQueryFilter queryFilter, JsonArray filter, JsonArray should, JsonArray must_not) {
//		JsonArray filtersArray = new JsonArray();
//		List<IQueryFilter> filterList = queryFilter.getFilterList();
//		int numAnds = filterList.size();
//		for(int i = 0; i < numAnds; i++) {
//			filtersArray.push(processFilter(filterList.get(i), filter, should, must_not));
//			IQueryFilter filter2 = filterList.get(i);
//		}
//		return filtersArray;
//	}
		
	private static JsonObject addSelectorToValuesFilter(NounMetadata leftComp, NounMetadata rightComp, String thisComparator) {
		
		if (thisComparator.equals("==") || thisComparator.equals("!=")) {
			JsonObject jsonBuilder = new JsonObject();
			{
				JsonObject match = new JsonObject();
				{
					match.addProperty(leftComp.getValue().toString(), rightComp.getValue().toString());
				}
				jsonBuilder.add("match", match);
			}
			return jsonBuilder;
		} else if (thisComparator.equals("<") || thisComparator.equals(">")) {
			JsonObject jsonBuilder = new JsonObject();
			{
				JsonObject range = new JsonObject();
				{
					JsonObject column = new JsonObject();
					{
						if (thisComparator.equals("<")) {							
							column.addProperty("lte", new BigDecimal(rightComp.getValue().toString()));
						}
						if (thisComparator.equals(">")) {							
							column.addProperty("gte", new BigDecimal(rightComp.getValue().toString()));
						}
					}
					range.add(leftComp.getValue().toString(), column);
				}
				jsonBuilder.add("range", range);
			}
			return jsonBuilder;
		} else if (thisComparator.equals("?like")) {
			JsonObject jsonBuilder = new JsonObject();
			{
				JsonObject wildcard = new JsonObject();
				{
					wildcard.addProperty(leftComp.getValue().toString(), "*" + rightComp.getValue().toString() + "*");
				}
				jsonBuilder.add("wildcard", wildcard);
			}
			return jsonBuilder;
		} else if (thisComparator.equals("?begins")) {
			JsonObject jsonBuilder = new JsonObject();
			{
				JsonObject prefix = new JsonObject();
				{
					prefix.addProperty(leftComp.getValue().toString(), rightComp.getValue().toString());
				}
				jsonBuilder.add("prefix", prefix);
			}
			return jsonBuilder;
		} else if (thisComparator.equals("?ends")) {
			JsonObject jsonBuilder = new JsonObject();
			{
				JsonObject regexp = new JsonObject();
				{
					regexp.addProperty(leftComp.getValue().toString(), ".*" + rightComp.getValue().toString() + "$");
				}
				jsonBuilder.add("regexp", regexp);
			}
			return jsonBuilder;
		}
		
		return null;
		
	}
}

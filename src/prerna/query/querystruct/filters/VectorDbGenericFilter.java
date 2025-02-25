package prerna.query.querystruct.filters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.query.interpreters.sql.SqlInterpreter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class VectorDbGenericFilter extends SqlInterpreter {
	
	public Object processFilter(IQueryFilter filter, VectorDatabaseTypeEnum vectorDbType) {
        switch (filter.getQueryFilterType()) {
            case SIMPLE:
                return processSimpleQueryFilter(filter, vectorDbType);
            case AND:
                return processAndOrQueryFilter((AndQueryFilter) filter, vectorDbType, "AND", "must");
            case OR:
                return processAndOrQueryFilter((OrQueryFilter) filter, vectorDbType, "OR", "should");
            default:
                return null;
        }
    }
	
	private Object processSimpleQueryFilter(IQueryFilter filter, VectorDatabaseTypeEnum vectorDbType) {
        SimpleQueryFilter simpleFilter = (SimpleQueryFilter) filter;
        NounMetadata leftComp = simpleFilter.getLComparison();
        NounMetadata rightComp = simpleFilter.getRComparison();
        String comparator = simpleFilter.getComparator().trim();

        if (vectorDbType == VectorDatabaseTypeEnum.MILVUS) {
            return createMilvusFilterQuery(leftComp, rightComp, comparator);
        } else if (vectorDbType == VectorDatabaseTypeEnum.OPEN_SEARCH) {
            return createOpenSearchFilterQuery(leftComp, rightComp, comparator);
        }
		return null;
    }

    private Object processAndOrQueryFilter(IQueryFilter filter, VectorDatabaseTypeEnum vectorDbType, String logicalOperation, String elasticBooleanKey) {
        List<IQueryFilter> filterList;
        
        if (filter instanceof AndQueryFilter) {
            filterList = ((AndQueryFilter) filter).getFilterList();
        } else if (filter instanceof OrQueryFilter) {
            filterList = ((OrQueryFilter) filter).getFilterList();
        } else {
            throw new IllegalArgumentException("Unsupported query filter type: " + filter.getClass().getSimpleName());
        }
        
        if (vectorDbType == VectorDatabaseTypeEnum.MILVUS) {
            StringBuilder filterBuilder = new StringBuilder();
            for (int i = 0; i < filterList.size(); i++) {
                if (i > 0) filterBuilder.append(" ").append(logicalOperation).append(" ");
                filterBuilder.append(processFilter(filterList.get(i), vectorDbType));
            }
            return filterBuilder;
        } else if (vectorDbType == VectorDatabaseTypeEnum.OPEN_SEARCH)  {
            JsonObject boolQuery = new JsonObject();
            JsonArray conditions = new JsonArray();
            for (IQueryFilter subFilter : filterList) {
                conditions.add((JsonObject) processFilter(subFilter, vectorDbType));
            }
            boolQuery.add(elasticBooleanKey, conditions);
            JsonObject wrapper = new JsonObject();
            wrapper.add("bool", boolQuery);
            return wrapper;
        }
		return null;
    }

    private StringBuilder createMilvusFilterQuery(NounMetadata leftComp, NounMetadata rightComp, String comparator) {
    	comparator = comparator.trim();
    	IQuerySelector leftSelector = (IQuerySelector) leftComp.getValue();
		String leftDataType = leftSelector.getDataType();
		
		List<Object> objects = new ArrayList<>();
		// ugh... this is gross
		if(rightComp.getValue() instanceof Collection) {
			objects.addAll( (Collection) rightComp.getValue());
		} else {
			objects.add(rightComp.getValue());
		}
		
		StringBuilder filterBuilder = new StringBuilder();
			filterBuilder.append(leftSelector).append(" ");

			String getFormatedFilter = getFormatedObject(leftDataType, objects, comparator);

			switch (comparator.toUpperCase()) {
			    case "==": case "!=": case ">": case "<": case "<=": case ">=": 
			    case "IN": case "LIKE":
			        filterBuilder.append(comparator).append(" ").append(getFormatedFilter);
			        break;
			    default:
			        filterBuilder.append(comparator).append(" ").append(getFormatedFilter);
			}
			
		return filterBuilder;
	}

    private JsonObject createOpenSearchFilterQuery(NounMetadata leftComp, NounMetadata rightComp, String comparator) {
        JsonObject termFilter = new JsonObject();
        JsonObject condition = new JsonObject();
        String fieldName = leftComp.getValue().toString();
        Object value = rightComp.getValue();

        if (comparator.equals("==")) {
	        condition.addProperty(fieldName, value.toString());
	        termFilter.add("term", condition);
	    } else if (comparator.equals(">") || comparator.equals("<") || comparator.equals(">=") || comparator.equals("<=")) {
	        JsonObject rangeCondition = new JsonObject();
	        
	        switch (comparator) {
	            case ">":
	                rangeCondition.addProperty("gt", value.toString());
	                break;
	            case "<":
	                rangeCondition.addProperty("lt", value.toString());
	                break;
	            case ">=":
	                rangeCondition.addProperty("gte", value.toString());
	                break;
	            case "<=":
	                rangeCondition.addProperty("lte", value.toString());
	                break;
	        }
	        JsonObject fieldObject = new JsonObject();
	        fieldObject.add(fieldName, rangeCondition);
	        termFilter.add("range", fieldObject);
	    } else if (comparator.equals("IN")) {
	        JsonArray values = new JsonArray();
	        for (Object val : (List<?>) value) {
	            values.add(val.toString());
	        }
	        condition.add(fieldName, values);
	        termFilter.add("terms", condition);
	    }
	    
	    return termFilter;
	}

}

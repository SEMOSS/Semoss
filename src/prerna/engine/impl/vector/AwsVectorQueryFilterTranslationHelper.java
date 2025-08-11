package prerna.engine.impl.vector;

import java.util.List;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;

public class AwsVectorQueryFilterTranslationHelper {

    public StringBuilder processAwsFilter(IQueryFilter filter) {
        return processFilterRecursive(filter);
    }

    private StringBuilder processFilterRecursive(IQueryFilter filters) {
        IQueryFilter.QUERY_FILTER_TYPE filterType = filters.getQueryFilterType();
        switch (filterType) {
            case SIMPLE:
                return processSimpleFilter((SimpleQueryFilter) filters);

            case AND:
                return processAndFilter((AndQueryFilter) filters);

            case OR:
                return processOrFilter((OrQueryFilter) filters);

            default:
                throw new UnsupportedOperationException("Unsupported filter type: " + filterType);
        }
    }

    private StringBuilder processAndFilter(AndQueryFilter filter) {
    	StringBuilder sb = new StringBuilder();
        sb.append("{ \"$and\": [");

        List<IQueryFilter> filterList = filter.getFilterList();
        for (int i = 0; i < filterList.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(processFilterRecursive(filterList.get(i)));
        }

        sb.append("] }");
        return sb;
    }

    private StringBuilder processOrFilter(OrQueryFilter filter) {
    	 StringBuilder sb = new StringBuilder();
         sb.append("{ \"$or\": [");

         List<IQueryFilter> filterList = filter.getFilterList();
         for (int i = 0; i < filterList.size(); i++) {
             if (i > 0) sb.append(", ");
             sb.append(processFilterRecursive(filterList.get(i)));
         }

         sb.append("] }");
         return sb;
     }
    
    private StringBuilder processSimpleFilter(SimpleQueryFilter filter) {
        String field = ((IQuerySelector) filter.getLComparison().getValue()).toString();
        Object value = filter.getRComparison().getValue();
        String comparator = normalizeComparator(filter.getComparator());

        StringBuilder sb = new StringBuilder();
        sb.append("{ \"").append(field).append("\": { \"").append(comparator).append("\": ");

        if (value instanceof String) {
            sb.append("\"").append(value).append("\"");
        } else {
            sb.append(value);
        }

        sb.append(" } }");
        return sb;
    }

    private String normalizeComparator(String raw) {
        switch (raw.trim().toUpperCase()) {
            case "==": return "$eq";
            case "!=": return "$ne";
            case ">":  return "$gt";
            case ">=": return "$gte";
            case "<":  return "$lt";
            case "<=": return "$lte";
            default: throw new IllegalArgumentException("Unsupported comparator: " + raw);
        }
    }

}


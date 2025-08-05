package prerna.engine.impl.vector;

import java.util.Collection;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;

public class AwsVectorQueryFilterTranslationHelper {

    public JsonObject processAwsFilter(List<IQueryFilter> metaFilters) {
        return processFilterRecursive(metaFilters);
    }

    private JsonObject processFilterRecursive(List<IQueryFilter> metaFilters) {
        IQueryFilter.QUERY_FILTER_TYPE filterType = ((AndQueryFilter) metaFilters).getQueryFilterType();

        switch (filterType) {
            case SIMPLE:
                return processSimpleFilter((SimpleQueryFilter) metaFilters);

            case AND:
                return processAndFilter((AndQueryFilter) metaFilters);

            case OR:
                return processOrFilter((OrQueryFilter) metaFilters);

            default:
                throw new UnsupportedOperationException("Unsupported filter type: " + filterType);
        }
    }

    private JsonObject processAndFilter(AndQueryFilter filter) {
        JsonArray conditionArray = new JsonArray();
        for (IQueryFilter sub : filter.getFilterList()) {
            conditionArray.add(processFilterRecursive((List<IQueryFilter>) sub));
        }
        JsonObject wrapper = new JsonObject();
        wrapper.add("$and", conditionArray);
        return wrapper;
    }

    private JsonObject processOrFilter(OrQueryFilter filter) {
        JsonArray conditionArray = new JsonArray();
        for (IQueryFilter sub : filter.getFilterList()) {
            conditionArray.add(processFilterRecursive((List<IQueryFilter>) sub));
        }
        JsonObject wrapper = new JsonObject();
        wrapper.add("$or", conditionArray);
        return wrapper;
    }


    private JsonObject processSimpleFilter(SimpleQueryFilter filter) {
        String field = ((IQuerySelector) filter.getLComparison().getValue()).toString();
        Object value = filter.getRComparison().getValue();
        String comparator = normalizeComparator(filter.getComparator());

        JsonObject filterJson = new JsonObject();
        JsonObject condition = new JsonObject();

        switch (comparator) {
            case "$eq":
            case "$ne":
            case "$gt":
            case "$gte":
            case "$lt":
            case "$lte":
                addPrimitive(condition, comparator, value);
                break;

            case "$in":
            case "$nin":
                addArray(condition, comparator, value);
                break;

            case "$exists":
                condition.addProperty(comparator, Boolean.parseBoolean(value.toString()));
                break;

            default:
                throw new UnsupportedOperationException("Unsupported comparator: " + comparator);
        }

        filterJson.add(field, condition);
        return filterJson;
    }

    private void addPrimitive(JsonObject obj, String op, Object value) {
        if (value instanceof Number) {
            obj.addProperty(op, ((Number) value).doubleValue());
        } else if (value instanceof Boolean) {
            obj.addProperty(op, (Boolean) value);
        } else {
            obj.addProperty(op, value.toString());
        }
    }

    private void addArray(JsonObject obj, String op, Object value) {
        JsonArray array = new JsonArray();
        if (value instanceof Collection<?>) {
            for (Object val : (Collection<?>) value) {
                array.add(val.toString());
            }
        } else {
            array.add(value.toString());
        }
        obj.add(op, array);
    }

    private String normalizeComparator(String raw) {
        switch (raw.trim().toUpperCase()) {
            case "==": return "$eq";
            case "!=": return "$ne";
            case ">":  return "$gt";
            case ">=": return "$gte";
            case "<":  return "$lt";
            case "<=": return "$lte";
            case "IN": return "$in";
            case "NOT IN": return "$nin";
            case "$EXISTS":
            case "EXISTS": return "$exists";
            default: return raw.startsWith("$") ? raw : "$" + raw.toLowerCase();
        }
    }
}


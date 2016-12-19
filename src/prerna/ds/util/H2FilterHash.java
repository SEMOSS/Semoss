package prerna.ds.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.ds.AbstractTableDataFrame;

public class H2FilterHash {

	Map<String, Map<String, Set<Object>>> filterHash;	

	public H2FilterHash(Map<String, Map<String, Set<Object>>> filterHash) {
		this.filterHash = filterHash;
	}
	
	public H2FilterHash() {
		this.filterHash = new HashMap<>();
	}
	
	/**
	 * Aggregates filters for a columnheader In the case of a numerical filter
	 * such as greater than, less than, filters are replaced
	 * 
	 * @param columnHeader
	 *            - column filters to modify
	 * @param values
	 *            - values to add to filters
	 * @param comparator
	 */
	public void addFilters(String columnHeader, List<Object> values, String comparator) {
		if (filterHash.get(columnHeader) == null) {
			setFilters(columnHeader, values, comparator);
		} else {
			Map<String, Set<Object>> innerMap = filterHash.get(columnHeader);
			if (innerMap.get(comparator) == null
					|| (!comparator.equals(AbstractTableDataFrame.Comparator.EQUAL) && !comparator.equals(AbstractTableDataFrame.Comparator.NOT_EQUAL))) {
				innerMap.put(comparator, new HashSet<>(values));
			} else {
				innerMap.get(comparator).addAll(values);
			}
		}
	}

	/**
	 * Overwrites filters for a specific column with the values and comparator
	 * specified
	 * 
	 * @param columnHeader
	 * @param values
	 * @param comparator
	 */
	public void setFilters(String columnHeader, List<Object> values, String comparator) {
		Map<String, Set<Object>> innerMap = new HashMap<>();
		innerMap.put(comparator, new HashSet<>(values));
		filterHash.put(columnHeader, innerMap);
	}

	/**
	 * Clears the filters for the columnHeader
	 * 
	 * @param columnHeader
	 */
	public void removeFilter(String columnHeader) {
		filterHash.remove(columnHeader);
	}

	/**
	 * Clears all filters associated with the main table
	 */
	public void clearFilters() {
		filterHash.clear();
	}


	public String makeFilterSubQuery() {
		String filterStatement = "";
		if (filterHash.keySet().size() > 0) {

			List<String> filteredColumns = new ArrayList<String>(filterHash.keySet());
			for (int x = 0; x < filteredColumns.size(); x++) {

				String header = filteredColumns.get(x);
				// String tableHeader = joinMode ? translateColumn(header) :
				// header;
				String tableHeader = header;

				Map<String, Set<Object>> innerMap = filterHash.get(header);
				int i = 0;
				for (String comparator : innerMap.keySet()) {
					if (i > 0) {
						filterStatement += " AND ";
					}
					switch (comparator) {

					case "=": {
						Set<Object> filterValues = innerMap.get(comparator);
						String listString = getQueryStringList(filterValues);
						filterStatement += tableHeader + " in " + listString;
						break;
					}
					case "!=": {
						Set<Object> filterValues = innerMap.get(comparator);
						String listString = getQueryStringList(filterValues);
						filterStatement += tableHeader + " not in " + listString;
						break;
					}
					
					case "<":
					case ">":
					case "<=":
					case ">=": {
						Set<Object> filterValues = innerMap.get(comparator);
						String listString = filterValues.iterator().next().toString();
						filterStatement += tableHeader + " "+comparator+" " + listString;
						break;
					}
						
					default: {
						Set<Object> filterValues = innerMap.get(comparator);
						String listString = getQueryStringList(filterValues);
						filterStatement += tableHeader + " in " + listString;
					}

					}
					i++;
				}

				// put appropriate ands
				if (x < filteredColumns.size() - 1) {
					filterStatement += " AND ";
				}
			}

			if (filterStatement.length() > 0) {
				filterStatement = " WHERE " + filterStatement;
			}
		}

		return filterStatement;
	}

	public Map<String, Map<String, Set<Object>>> getFilterHash() {
		return this.filterHash;
	}
	
	private String getQueryStringList(Set<Object> values) {
		String listString = "(";

		Iterator<Object> iterator = values.iterator();
		int i = 0;
		while (iterator.hasNext()) {
			Object value = iterator.next();
			value = RdbmsFrameUtility.cleanInstance(value.toString());
			listString += "'" + value + "'";
			if (i < values.size() - 1) {
				listString += ", ";
			}
			i++;
		}

		listString += ")";
		return listString;
	}
}

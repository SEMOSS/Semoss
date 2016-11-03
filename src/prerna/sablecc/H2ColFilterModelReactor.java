package prerna.sablecc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.algorithm.api.IMetaData;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.AbstractTableDataFrame;
import prerna.ds.AbstractTableDataFrame.Comparator;
import prerna.ds.h2.H2Builder;
import prerna.ds.h2.H2Frame;
import prerna.ds.h2.H2Joiner;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.util.Utility;

public class H2ColFilterModelReactor extends ColFilterModelReactor {

	@Override
	public Iterator process() {

		// TODO change filter word to include underscores for spaces
		// get table data
		H2Frame table = (H2Frame) myStore.get("G");
		String tableName = table.getTableName();
		// get column filtered
		Vector<String> colVector = (Vector) myStore.get(PKQLEnum.COL_DEF);
		String col = (String) colVector.get(0);
		// get filtered word
		String filterWord = (String) myStore.get("filterWord");
		// query strings
		String limitOffset = "";
		String filteredQuery = "";
		String unfilterQuery = "";
		Map<String, HashSet<String>> filteredValues = new HashMap<>();
		Map<String, HashSet<String>> visibleValues = new HashMap<>();

		// if limit and offset options exists
		if (myStore.containsKey(PKQLEnum.MATH_PARAM)) {
			limitOffset = getLimitOffset();
		}
		
		//clean filter word
		if (table != null && filterWord != null && table.getDataType(col).equals(IMetaData.DATA_TYPES.STRING)) {
			filterWord = Utility.cleanString(filterWord, true, true, false);
		}

		// Get filterHash
		Object[] joinedMode = table.getBuilder().getFilterHashJoinedMode();
		tableName = (String) joinedMode[0];
		Map<String, Map<AbstractTableDataFrame.Comparator, Set<Object>>> filterHash = (Map<String, Map<Comparator, Set<Object>>>) joinedMode[1];

		Map<String, ArrayList<String>> filteredMap = new HashMap<>();
		Map<String, ArrayList<String>> unfilteredMap = new HashMap<>();

		// set up filterHash and create conditions
		// to add to filteredMap and unfilteredMap
		// {"col": ["col = "value1", "col = value2"], "col2":["col2 = "value3",
		// "col2 = "value4"]}
		if (filterHash != null) {
			for (String key : filterHash.keySet()) {
				Map<Comparator, Set<Object>> comp = filterHash.get(key);
				ArrayList<String> unfilterCol = new ArrayList<>();
				ArrayList<String> filterCol = new ArrayList<>();
				for (Comparator compKey : comp.keySet()) {
					String sqlComparison = getQueryCompString(compKey);
					Set<Object> values = comp.get(compKey);
					for (Object s : values) {
						s = "\'" + s + "\'";
						//filterCol negates the comparison in the filterHash to exclude the column from the query
						if (key.equals(col)) {
							filterCol.add(key + getQueryNegationCompString(compKey) + s);
						} else {
							filterCol.add(key + sqlComparison + s);
						}
						unfilterCol.add(key + sqlComparison + s);
					}
				}
				filteredMap.put(key, filterCol);
				unfilteredMap.put(key, unfilterCol);
			}
		}
		// Create queries
		unfilterQuery = buildQuery2(col, tableName, filterWord, unfilteredMap, limitOffset);
		filteredQuery = buildQuery(col, tableName, filterWord, filteredMap, limitOffset);

		// Get values from queries
		HashSet<String> unfilteredList = table.getBuilder().getHashSetFromQuery(unfilterQuery);
		HashSet<String> filteredList = new HashSet<String>();

		// only query for filtered values if column is in the filterHash
		if (filterHash.containsKey(col)) {
			filteredList = table.getBuilder().getHashSetFromQuery(filteredQuery);
		}

		visibleValues.put(col, unfilteredList);
		filteredValues.put(col, filteredList);

		// store results
		Map<String, Object> retMap = new HashMap<String, Object>();
		retMap.put("unfilteredValues", visibleValues);
		retMap.put("filteredValues", filteredValues);
		retMap.put("minMax", table.getMinMaxValues(col));
		myStore.put("filterRS", retMap);
		myStore.put("RESPONSE", STATUS.SUCCESS.toString());
		myStore.put("STATUS", STATUS.SUCCESS);

		return null;
	}

	/**
	 * This method is used to build the query to grab all the data with filters
	 * and account for exclusions
	 * 
	 * @param col
	 *            column being filtered
	 * @param tableName
	 * @param word
	 *            filter word being used
	 * @param conditions
	 *            a list of conditions to be added to the query grouped by
	 *            column value and conditions for that column
	 * 
	 * @param limitOffset
	 * @return query string
	 */
	private String buildQuery(String col, String tableName, String word, Map<String, ArrayList<String>> conditions,
			String limitOffset) {

		String query = "SELECT DISTINCT " + col + " FROM " + tableName;
		boolean whereUsed = false;
		boolean trailingWhere = false;

		// add filterWord to query
		if (word != null && word.length() > 0) {
			query += " WHERE UPPER(" + col + ") = UPPER(" + word.trim() + ")";
			whereUsed = true;
		}

		// additional conditions
		if (conditions.size() > 0) {
			if (!whereUsed) {
				query += " WHERE ";
				trailingWhere = true;
			} else {
				query += " AND ";
			}
			for (String statement : conditions.keySet()) {
				ArrayList ors = conditions.get(statement);
				if (ors.size() > 0) {
					trailingWhere = false;
					query += " ( ";
					for (int i = 0; i < ors.size(); i++) {
						if (!statement.equals(col)) {
							query += ors.get(i) + " OR  ";
						} else {
							query += ors.get(i) + " AND ";
						}
					}
					// remove last OR chars
					query = query.substring(0, query.length() - " OR  ".length());
					// used for another column
					query += " ) AND ";
				}
			}
			// remove last AND chars
			if (trailingWhere) {
				query = query.substring(0, query.length() - " WHERE ".length());
			} else {
				query = query.substring(0, query.length() - " AND ".length());
			}
		}

		// limit and offset
		query += limitOffset + ";";

		return query;
	}

	/**
	 * This method is used to build the query to grab all the data with filters
	 * possibilities
	 * 
	 * @param col
	 *            column being filtered
	 * @param tableName
	 * @param word
	 *            filter word being used
	 * @param conditions
	 *            a list of conditions to be added to the query grouped by
	 *            column value and conditions for that column
	 * 
	 * @param limitOffset
	 * @return query string
	 */
	private String buildQuery2(String col, String tableName, String word, Map<String, ArrayList<String>> conditions,
			String limitOffset) {

		String query = "SELECT DISTINCT " + col + " FROM " + tableName;
		boolean whereUsed = false;
		boolean trailingWhere = false;

		// add filterWord to query
		if (word != null && word.length() > 0) {
			query += " WHERE UPPER(" + col + ") = UPPER(" + word.trim() + ")";
			whereUsed = true;
		}

		// additional conditions
		if (conditions.size() > 0) {
			if (!whereUsed) {
				query += " WHERE ";
				trailingWhere = true;
			} else {
				query += " AND ";
			}
			for (String statement : conditions.keySet()) {
				ArrayList ors = conditions.get(statement);
				if (ors.size() > 0) {
					trailingWhere = false;
					query += " ( ";
					for (int i = 0; i < ors.size(); i++) {
							query += ors.get(i) + " OR  ";
					}
					// remove last OR chars
					query = query.substring(0, query.length() - " OR  ".length());
					// used for another column
					query += " ) AND ";
				}
			}
			// remove last AND chars
			if (trailingWhere) {
				query = query.substring(0, query.length() - " WHERE ".length());
			} else {
				query = query.substring(0, query.length() - " AND ".length());
			}
		}

		// limit and offset
		query += limitOffset + ";";

		return query;
	}

	private String getLimitOffset() {
		String filters = "";
		Map<String, Object> options = (Map<String, Object>) myStore.get(PKQLEnum.MATH_PARAM);
		for (String key : options.keySet()) {
			filters += " " + key.toUpperCase() + " " + options.get(key);
		}
		return filters;
	}

	public String getQueryCompString(Comparator compKey) {
		String sqlComparison = "";
		switch (compKey) {
		case EQUAL: {
			sqlComparison = " = ";
			break;
		}
		case NOT_EQUAL: {
			sqlComparison = " != ";
			break;
		}
		case LESS_THAN: {
			sqlComparison = " < ";
			break;
		}
		case GREATER_THAN: {
			sqlComparison = " > ";
			break;
		}
		case GREATER_THAN_EQUAL: {
			sqlComparison = " >= ";
			break;
		}
		case LESS_THAN_EQUAL: {
			sqlComparison = " <= ";
			break;
		}
		default: {
			sqlComparison = " = ";

		}
		}

		return sqlComparison;
	}

	public String getQueryNegationCompString(Comparator compKey) {
		String sqlComparison = "";
		switch (compKey) {
		case EQUAL: {
			sqlComparison = " != ";
			break;
		}
		case NOT_EQUAL: {
			sqlComparison = " = ";
			break;
		}
		case LESS_THAN: {
			sqlComparison = " >= ";
			break;
		}
		case GREATER_THAN: {
			sqlComparison = " <= ";
			break;
		}
		case GREATER_THAN_EQUAL: {
			sqlComparison = " < ";
			break;
		}
		case LESS_THAN_EQUAL: {
			sqlComparison = " > ";
			break;
		}
		default: {
			sqlComparison = " != ";

		}
		}

		return sqlComparison;
	}
}

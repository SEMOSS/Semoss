package prerna.ds.nativeframe;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ds.AbstractTableDataFrame;
import prerna.ds.QueryStruct;
import prerna.ds.TinkerFrame;
import prerna.ds.h2.H2Iterator;
import prerna.engine.api.IEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.rdf.query.builder.IQueryInterpreter;
import prerna.util.Utility;

public class NativeFrameBuilder {
	
	private static final Logger LOGGER = LogManager.getLogger(NativeFrameBuilder.class.getName());
	
	private String engineName;
	private RDBMSNativeEngine coreEngine;
	
	QueryStruct queryStruct;
	
	Map<String, Map<AbstractTableDataFrame.Comparator, Set<Object>>> filterHash = new HashMap<>();
	
	//need to determine which filters are the more narrow filters
	public Map <String, Map<String, List>> temporalFilters = new Hashtable<>();
	public Map <String, Map<String, List>> dbfilters = new Hashtable<>();
	
	// TODO: this is a horrible variable name
	// this actually holds a portion of the select query
	private String tableName;

	public NativeFrameBuilder() {

	}

	public void setConnection(String engineName) {
		IEngine coreEngine = Utility.getEngine(engineName);
		if(coreEngine instanceof RDBMSNativeEngine) {
			this.engineName = engineName;
			this.coreEngine = (RDBMSNativeEngine) coreEngine;
		} else {
			throw new IllegalArgumentException("Engine must be of type RDBMS");
		}
	}
	
	public void setView(String tableName) {
		this.tableName = tableName;
	}
	
	public void mergeQueryStruct(QueryStruct qs) {
		if(this.queryStruct == null) {
			this.queryStruct = qs;
		} else {
			//merge query struct
			this.queryStruct.merge(qs);
		}
		
		//no need to track filters here, they are tracked as db and temporal filters
		this.queryStruct.andfilters = new Hashtable<>();
		
		refreshView();
	}
	
	/**
	 * 
	 * @param filters
	 * 
	 * This method produces the most restrictive filter object during the merge
	 */
	public void mergeDBFilters(Map <String, Map<String, List>> filters) {
		this.dbfilters = mergeFilters(this.dbfilters, filters);
		
		refreshView();
	}
	
	/**
	 * 
	 * @param filters1
	 * @param filters2
	 * @return
	 * 
	 * return the combination of filters1 and filters2 such that it produces the most restrictive filtering
	 */
	private Map<String, Map<String, List>> mergeFilters(Map <String, Map<String, List>> filters1, Map <String, Map<String, List>> filters2) {
		Map<String, Map<String, List>> retFilters = new Hashtable<>();
		
		retFilters.putAll(filters2);
		
		for(String key : filters1.keySet()) {
			Map<String, List> hash1 = filters1.get(key);
			Map<String, List> newHash = new Hashtable<String, List>();
			retFilters.put(key, newHash);
			//need to determine which set is more restrictive
			//assumption is that smaller vector is a subset of bigger vector
			if(filters2.containsKey(key)) {
				Map<String, List> hash2 = filters2.get(key);
				for(String relationKey : hash1.keySet()) {
					List v;
					if(hash2.containsKey(relationKey)) {
						List v2 = hash2.get(relationKey);
						List v1 = hash1.get(relationKey);
						switch(relationKey) {
						case "=": {
							//pick the smaller vector
							if(v2.size() < v1.size()) {
								newHash.put(relationKey, v2);
							} else {
								newHash.put(relationKey, v1);
							}
							break;
						}
						case "!=": {
							//pick the bigger vector
							if(v2.size() < v1.size()) {
								newHash.put(relationKey, v1);
							} else {
								newHash.put(relationKey, v2);
							}
							break;
						}
						case "<=":
						case "<": {
							//pick the smaller number
							Object thisObj = v2.get(0);
							Object incomingObj = v1.get(0);
							
							Double thisDbl = Double.NEGATIVE_INFINITY;
							Double incomingDbl = Double.POSITIVE_INFINITY;
							
							if(thisObj instanceof Double) {
								thisDbl = (Double)thisObj;
							} else {
								//i shouldn't need to cast to string here, pkql should be taking care of that
							}
							
							if(incomingObj instanceof Double) {
								incomingObj = (Double)incomingObj;
							} else {
								//i shouldn't need to cast to string here, pkql should be taking care of that
							}
							
							if(thisDbl > incomingDbl) {
								Vector newVec = new Vector(0);
								newVec.add(incomingObj);
								hash2.put(relationKey, newVec);
							} else {
								Vector newVec = new Vector(0);
								newVec.add(thisObj);
								hash2.put(relationKey, newVec);
							}
							
							break;
						}
						case ">=":
						case ">": {
							//pick the bigger number
							Object thisObj = v2.get(0);
							Object incomingObj = v1.get(0);
							
							Double thisDbl = Double.POSITIVE_INFINITY;
							Double incomingDbl = Double.NEGATIVE_INFINITY;
							
							if(thisObj instanceof Double) {
								thisDbl = (Double)thisObj;
							} else {
								//i shouldn't need to cast to string here, pkql should be taking care of that
							}
							
							if(incomingObj instanceof Double) {
								incomingObj = (Double)incomingObj;
							} else {
								//i shouldn't need to cast to string here, pkql should be taking care of that
							}
							
							if(thisDbl < incomingDbl) {
								Vector newVec = new Vector(0);
								newVec.add(incomingObj);
								newHash.put(relationKey, newVec);
							} else {
								Vector newVec = new Vector(0);
								newVec.add(thisObj);
								newHash.put(relationKey, newVec);
							}
							
							break;
						}						
						}
					} else {
						v = new Vector();
						v.addAll(hash1.get(relationKey));
						newHash.put(relationKey, v);
					}
				}
			} 
			
			//these are new filters, add them to the db filters
			else {
				for(String relationKey : hash1.keySet()) {
					Vector v = new Vector();
					v.addAll(hash1.get(relationKey));
					newHash.put(relationKey, v);
				}
				retFilters.put(key, newHash);
			}
		}
		
		return retFilters;
	}
	
	/**
	 * 
	 * @return
	 * 
	 * return a copy of the query struct with the db filters
	 */
	public QueryStruct getQueryStruct() {
		QueryStruct copy = this.queryStruct.deepCopy();
		copy.andfilters = this.dbfilters;
		return copy;
	}
	
	public Map<String, Map<String, List>> getTempFilters() {
		return this.temporalFilters;
	}
	
	private QueryStruct mergeQueryStructWithFilters() {
		QueryStruct qs = this.queryStruct.deepCopy();
		qs.andfilters = mergeFilters(this.dbfilters, this.temporalFilters);
		return qs;
	}
	
	private void refreshView() {
		IEngine engine = Utility.getEngine(this.engineName);
		IQueryInterpreter interpreter = engine.getQueryInterpreter();
		interpreter.setQueryStruct(mergeQueryStructWithFilters());
		String selectQuery = interpreter.composeQuery();
		String partialSelect = selectQuery.substring(selectQuery.indexOf(" FROM ")+6, selectQuery.length());
		setView(partialSelect);
	}
	
	public String getEngineName() {
		return this.engineName;
	}

	/*************************** TEST **********************************************/
	public static void main(String[] a) throws Exception {

		
	}

	// Test method
	public static void testDB() throws Exception {
		Class.forName("org.mariadb.jdbc.Driver");
		Connection conn = DriverManager
				.getConnection("jdbc:mariadb://localhost:3306/moviedb?user=root&password=password");

		Statement stmt = conn.createStatement();
		String query = "select * from Movie_Data";
		// String query =
		// "select t.title, s.studio from title t, studio s where t.title = s.title_fk";
		// query =
		// "select t.title, concat(t.title,':', s.studio), s.studio from title t, studio s where t.title = s.title_fk";
		// ResultSet rs = stmt.executeQuery("SELECT * FROM TITLE");
		ResultSet rs = stmt.executeQuery(query);
		// stmt.execute("CREATE TABLE MOVIES AS SELECT * From CSVREAD('../Movie.csv')");

		ResultSetMetaData rsmd = rs.getMetaData();
		int columnCount = rsmd.getColumnCount();

		ArrayList records = new ArrayList();
		System.err.println("Number of columns " + columnCount);

		while (rs.next()) {
			Object[] data = new Object[columnCount];
			for (int colIndex = 0; colIndex < columnCount; colIndex++) {
				data[colIndex] = rs.getObject(colIndex + 1);
				System.out.print(rsmd.getColumnType(colIndex + 1));
				System.out.print(rsmd.getColumnName(colIndex + 1) + ":");
				System.out.print(rs.getString(colIndex + 1));
				System.out.print(">>>>" + rsmd.getTableName(colIndex + 1)
						+ "<<<<");
			}
			System.out.println();
		}

		// add application code here
		conn.close();
	}
	
	
	/*************************** END TEST **********************************************/

	
	/*************************** ITERATOR **********************************************/
	
	/**
	 * 
	 * @param selectors
	 * @return
	 * 
	 * returns an iterator that returns data from the selectors
	 */
	public Iterator buildIterator(List<String> selectors) {
		String selectQuery = makeSelect(tableName, selectors);
		
		long startTime = System.currentTimeMillis();
		ResultSet rs = executeQuery(selectQuery);
		long endTime = System.currentTimeMillis();
		
		LOGGER.info("Executed Select Query on NATIVE FRAME: "+(endTime - startTime)+" ms");
		
		return new H2Iterator(rs);
	}

	//build the new way to create iterator with all the options
	/**
	 * 
	 * @param options - options needed to build the iterator
	 * @return
	 * 
	 * returns an iterator based on the options parameter
	 */
	public Iterator buildIterator(Map<String, Object> options) {
//		String tableName = joinMode ? this.viewTableName : this.tableName;

		
		String sortDir = (String)options.get(TinkerFrame.SORT_BY_DIRECTION);

		//dedup = true indicates remove duplicates
		//TODO: rename de_dup to something more meaningful for cases outside of tinkergraph
		Boolean dedup = (Boolean) options.get(TinkerFrame.DE_DUP);
		if(dedup == null) dedup = false;

		//how many rows to get
		Integer limit = (Integer) options.get(TinkerFrame.LIMIT);

		//at which row to start
		Integer offset = (Integer) options.get(TinkerFrame.OFFSET);

		//column to sort by
		String sortBy = (String)options.get(TinkerFrame.SORT_BY);

		//selectors to gather
		List<String> selectors = (List<String>) options.get(TinkerFrame.SELECTORS);
//		if(joinMode) {
//			selectors = translateColumns(selectors);
//		}
		String selectQuery;

		if(dedup) {
			selectQuery = makeSelectDistinct(tableName, selectors);
		} else {
			selectQuery = makeSelect(tableName, selectors);
		}
		
		//temporary filters to apply only to this iterator
		Map<String, List<Object>> temporalBindings = (Map<String, List<Object>>) options.get(TinkerFrame.TEMPORAL_BINDINGS); 
		Map<String, AbstractTableDataFrame.Comparator> compHash = new HashMap<String, AbstractTableDataFrame.Comparator>();
		for(String key : temporalBindings.keySet()) {
			compHash.put(key, AbstractTableDataFrame.Comparator.EQUAL);
		}

		//create a new filter substring and add/replace old filter substring
		String temporalFiltering = makeFilterSubQuery(temporalBindings, compHash); // default comparator is equals
		if(temporalFiltering != null && temporalFiltering.length() > 0) {
			if(selectQuery.contains(" WHERE ")) {
				temporalFiltering = temporalFiltering.replaceFirst(" WHERE ", "");
				selectQuery = selectQuery + temporalFiltering;
			} else {
				selectQuery = selectQuery + temporalFiltering;
			}
		}

		if(sortBy != null) {

//			if(joinMode) {
//				sortBy = translateColumn(sortBy);
//			}
			selectQuery += " sort by " + sortBy;
			//			if(sortDir != null) {
			//				
			//			}
		}
		if(limit != null && limit > 0) {
			selectQuery += " limit "+limit;
		}
		if(offset != null && offset > 0) {
			selectQuery += " offset "+offset;
		}

		long startTime = System.currentTimeMillis();
		ResultSet rs = executeQuery(selectQuery);
		
		long endTime = System.currentTimeMillis();
		LOGGER.info("Executed Select Query on NATIVE FRAME: "+(endTime - startTime)+" ms");
		return new H2Iterator(rs);
	}
	
	/*************************** END ITERATOR **********************************************/


	/*************************** READ **********************************************/
	
	//get scaled version of above method
//	public List<Object[]> getScaledData(String tableName, List<String> selectors, Map<String, String> headerTypeMap, String column, Object value, Double[] maxArr, Double[] minArr) {
//		int cindex = selectors.indexOf(column);
//		if(tableName == null) tableName = this.tableName;
//
//		List<Object[]> data;
//		String[] types = new String[headerTypeMap.size()];
//
//		int index = 0;
//		for(String selector : selectors) {
//			types[index] = headerTypeMap.get(selector);
//			index++;
//		}
//
//
//		try {
//			String selectQuery = makeSpecificSelect(tableName, selectors, column, value);
//			ResultSet rs = executeQuery(selectQuery);
//
//			if(rs != null) {
//
//				ResultSetMetaData rsmd = rs.getMetaData();
//				int NumOfCol = rsmd.getColumnCount();
//				data = new ArrayList<>(NumOfCol);
//				while (rs.next()){
//					Object[] row = new Object[NumOfCol];
//
//					for(int i = 1; i <= NumOfCol; i++) {
//						Object val = rs.getObject(i);
//						if(cindex != (i-1) && (types[i-1].equalsIgnoreCase("int") || types[i-1].equalsIgnoreCase("double"))) {
//							row[i-1] = ( ((Number)val).doubleValue() - minArr[i-1])/(maxArr[i-1] - minArr[i-1]);
//						} else {
//							row[i-1] = val;
//						}
//					}
//					data.add(row);
//				}
//
//				return data;
//			}
//
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
//
//		return new ArrayList<Object[]>(0);
//	}

	/**
	 * Get the column values from the table as an Object array
	 * 
	 * @param columnHeader
	 * @param distinct - true indicates only distinct values, false otherwise
	 * @return
	 */
	public Object[] getColumn(String columnHeader, boolean distinct) {
		String tableName = this.tableName;

		ArrayList<Object> column = new ArrayList<>();

		List<String> headers = new ArrayList<String>(1);
		headers.add(columnHeader);
		ResultSet rs;
		String selectQuery;
		if(distinct) {
			selectQuery = makeSelectDistinct(tableName, headers);
		} else {
			selectQuery = makeSelect(tableName, headers);
		}
		try {
			rs = executeQuery(selectQuery);
			while(rs.next()) {
				column.add(rs.getObject(1));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return column.toArray();
	}

	/**
	 * This method returns the max/min/count/sum/avg of a column
	 * returns null if statType parameter is invalid
	 * 
	 * @param columnHeader
	 * @param statType
	 * @return
	 */
//	public Double getStat(String columnHeader, String statType) {
//		String tableName = this.tableName;
////		if(joinMode) {
////			tableName = this.viewTableName;
////			columnHeader = translateColumn(columnHeader);
////		} else {
////			
////		}
////		String tableName = joinMode ? this.viewTableName : this.tableName;
//		ResultSet rs = executeQuery(makeFunction(columnHeader, statType, tableName));
//		try {
//			if(rs.next()) {
//				return rs.getDouble(1);
//			}
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
//		return null;
//	}

//	public Map<Map<Object, Object>, Object> getStat(String columnHeader, String statType, List<String> groupByCols) {
//		String tableName = this.tableName;
////		if(joinMode) {
////			tableName = this.viewTableName;
////			columnHeader = translateColumn(columnHeader);
////			groupByCols = translateColumns(groupByCols);
////		} else {
////			
////		}
////		String tableName = joinMode ? this.viewTableName : this.tableName;
//		ResultSet rs = executeQuery(makeFunction(columnHeader, statType, tableName, groupByCols));
//		try {
//			Map<Map<Object, Object>, Object> results = new Hashtable<Map<Object, Object>, Object>();
//			ResultSetMetaData metaData = rs.getMetaData();
//			int numReturns = metaData.getColumnCount();
//
//			int[] typeArr = new int[numReturns];
//			// index starts at 1 for metaData
//			for(int i = 1; i <= numReturns; i++) {
//				typeArr[i-1] = metaData.getColumnType(i);
//			}
//
//			while(rs.next()) {
//				// first return is the stat routine
//				Double val = rs.getDouble(1);
//				// get the unique set of group by values
//				Map<Object, Object> groupByVals = new Hashtable<Object, Object>();
//				for(int i = 2; i <= numReturns; i++) {
//					// group by cols are added in the same position as the list passed in
//					groupByVals.put(groupByCols.get(i-2), rs.getObject(i));
//				}
//				// add the row into the result set
//				results.put(groupByVals, val);
//			}
//
//			return results;
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
//		return null;
//	}
	
	/**
	 * This method returns the max/min/count/sum/avg of a column
	 * returns null if statType parameter is invalid
	 * 
	 * @param columnHeader
	 * @param statType
	 * @return
	 */
	public Double getStat(String columnHeader, String statType, boolean ignoreFilter) {
		String tableName = this.tableName;
//		if(joinMode) {
//			tableName = this.viewTableName;
//			columnHeader = translateColumn(columnHeader);
//		} else {
//			
//		}
////		String tableName = joinMode ? this.viewTableName : this.tableName;
//		columnHeader = cleanHeader(columnHeader);
		
		String function;
		if(ignoreFilter) {
			function = makeFunctionIgnoreFilter(columnHeader, statType, tableName);
		} else {
			function = makeFunction(columnHeader, statType, tableName);
		}
		
		ResultSet rs = executeQuery(function);
		try {
			if(rs.next()) {
				return rs.getDouble(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	/*************************** END READ **********************************************/
	
	
	/*************************** FILTER ******************************************/

	/**
	 * Aggregates filters for a columnheader
	 * In the case of a numerical filter such as greater than, less than, filters are replaced
	 * 
	 * @param columnHeader - column filters to modify
	 * @param values - values to add to filters
	 * @param comparator
	 */
//	public void addFilters(String columnHeader, List<Object> values, Comparator comparator) {
//		//always overwrite for numerical filters (greater than, less than, etc.)
//		//    	if(filterHash.get(columnHeader) == null || (comparator != Comparator.EQUAL && comparator != Comparator.NOT_EQUAL)) {
//		//    		setFilters(columnHeader, values, comparator);
//		//    	} else {
//		//    		filterHash.get(columnHeader).addAll(values);
//		//    		filterComparator.put(columnHeader, comparator);
//		//    	}
//
//		if(filterHash.get(columnHeader) == null) {
//			setFilters(columnHeader, values, comparator);
//		} else {
//			Map<Comparator, Set<Object>> innerMap = filterHash.get(columnHeader);
//			if(innerMap.get(comparator) == null || (comparator != Comparator.EQUAL && comparator != Comparator.NOT_EQUAL)) {
//				innerMap.put(comparator, new HashSet<>(values));
//			} else {
//				innerMap.get(comparator).addAll(values);
//			}
//		}
//	}

	public void addFilters(String columnHeader, List<Object> values, String comparator) {
		if(this.temporalFilters.containsKey(columnHeader)) {
			Map<String, List> curFilters = this.temporalFilters.get(columnHeader);
			if(curFilters.containsKey(comparator)) {
				Set<Object> set = new HashSet<>();
				set.addAll(curFilters.get(comparator));
				set.addAll(values);
				Vector newVec = new Vector();
				newVec.addAll(set);
				curFilters.put(comparator, newVec);
			} else {
				Vector newVec = new Vector();
				newVec.addAll(values);
				curFilters.put(comparator, newVec);
			}
		} else {
			setFilters(columnHeader, values, comparator);
		}
		
		refreshView();
	}
	/**
	 * Overwrites filters for a specific column with the values and comparator specified
	 * @param columnHeader
	 * @param values
	 * @param comparator
	 */
	public void setFilters(String columnHeader, List<Object> values, AbstractTableDataFrame.Comparator comparator) {
		//    	filterHash.put(columnHeader, values);
		//    	filterComparator.put(columnHeader, comparator);

		Map<AbstractTableDataFrame.Comparator, Set<Object>> innerMap = new HashMap<>();
		innerMap.put(comparator, new HashSet<>(values));
		filterHash.put(columnHeader, innerMap);
	}
	
	public void setFilters(String columnHeader, List<Object> values, String comparator) {
		List newVals = new Vector();
		newVals.addAll(values);
		Map<String, List> newTable = new Hashtable<String, List>();
		newTable.put(comparator, newVals);
		this.temporalFilters.put(columnHeader, newTable);
		
		refreshView();
	}

	/**
	 * Clears the filters for the columnHeader
	 * @param columnHeader
	 */
	public void removeFilter(String columnHeader) {
		//    	filterHash.remove(columnHeader);
		//    	filterComparator.remove(columnHeader);

		filterHash.remove(columnHeader);
		temporalFilters.remove(columnHeader);
		
		refreshView();
	}

	/**
	 * Clears all filters associated with the main table
	 */
	public void clearFilters() {
		//    	filterHash.clear();
		//    	filterComparator.clear();

		filterHash.clear();
		temporalFilters.clear();
		
		refreshView();
	}

	/**
	 * 
	 * @param filterHash
	 * @param filterComparator
	 * @return
	 */
	private String makeFilterSubQuery(Map<String, List<Object>> filterHash, Map<String, AbstractTableDataFrame.Comparator> filterComparator) {
		//need translation of filter here
		String filterStatement = "";
		if(filterHash.keySet().size() > 0) {

			List<String> filteredColumns = new ArrayList<String>(filterHash.keySet());
			for(int x = 0; x < filteredColumns.size(); x++) {

				String header = filteredColumns.get(x);
//				String tableHeader = joinMode ? translateColumn(header) : header;
				String tableHeader = header;
				
				AbstractTableDataFrame.Comparator comparator = filterComparator.get(header);

				switch(comparator) {
				case EQUAL:{
					List<Object> filterValues = filterHash.get(header);
					String listString = getQueryStringList(filterValues);
					filterStatement += tableHeader+" in " + listString;
					break;
				}
				case NOT_EQUAL: {
					List<Object> filterValues = filterHash.get(header);
					String listString = getQueryStringList(filterValues);
					filterStatement += tableHeader+" not in " + listString;
					break;
				}
				case LESS_THAN: {
					List<Object> filterValues = filterHash.get(header);
					String listString = filterValues.get(0).toString();
					filterStatement += tableHeader+" < " + listString;
					break;
				}
				case GREATER_THAN: {
					List<Object> filterValues = filterHash.get(header);
					String listString = filterValues.get(0).toString();
					filterStatement += tableHeader+" > " + listString;
					break;
				}
				case GREATER_THAN_EQUAL: {
					List<Object> filterValues = filterHash.get(header);
					String listString = filterValues.get(0).toString();
					filterStatement += tableHeader+" >= " + listString;
					break;
				}
				case LESS_THAN_EQUAL: {
					List<Object> filterValues = filterHash.get(header);
					String listString = filterValues.get(0).toString();
					filterStatement += tableHeader+" <= " + listString;
					break;
				}
				default: {
					List<Object> filterValues = filterHash.get(header);
					String listString = getQueryStringList(filterValues);

					filterStatement += tableHeader+" in " + listString;
				}
				}

				//put appropriate ands
				if(x < filteredColumns.size() - 1) {
					filterStatement += " AND ";
				}
			}

			if(filterStatement.length() > 0) {
				filterStatement = " WHERE " + filterStatement;
			}
		}

		return filterStatement;
	}

	private String makeFilterSubQuery(Map<String, Map<AbstractTableDataFrame.Comparator, Set<Object>>> filterHash) {
		//need to also pass in translationMap
//		if(joinMode) filterHash = joiner.getJoinedFilterHash(viewTableName);

		String filterStatement = "";
		if(filterHash.keySet().size() > 0) {

			List<String> filteredColumns = new ArrayList<String>(filterHash.keySet());
			for(int x = 0; x < filteredColumns.size(); x++) {

				String header = filteredColumns.get(x);
//				String tableHeader = joinMode ? translateColumn(header) : header;
				String tableHeader = header;
				
				Map<AbstractTableDataFrame.Comparator, Set<Object>> innerMap = filterHash.get(header);
				int i = 0;
				for(AbstractTableDataFrame.Comparator comparator : innerMap.keySet()) {
					if(i > 0) {
						filterStatement += " AND ";
					}
					switch(comparator) {

					case EQUAL:{
						Set<Object> filterValues = innerMap.get(comparator);
						String listString = getQueryStringList(filterValues);
						filterStatement += tableHeader+" in " + listString;
						break;
					}
					case NOT_EQUAL: {
						Set<Object> filterValues = innerMap.get(comparator);
						String listString = getQueryStringList(filterValues);
						filterStatement += tableHeader+" not in " + listString;
						break;
					}
					case LESS_THAN: {
						Set<Object> filterValues = innerMap.get(comparator);
						String listString = filterValues.iterator().next().toString();
						filterStatement += tableHeader+" < " + listString;
						break;
					}
					case GREATER_THAN: {
						Set<Object> filterValues = innerMap.get(comparator);
						String listString = filterValues.iterator().next().toString();
						filterStatement += tableHeader+" > " + listString;
						break;
					}
					case GREATER_THAN_EQUAL: {
						Set<Object> filterValues = innerMap.get(comparator);
						String listString = filterValues.iterator().next().toString();
						filterStatement += tableHeader+" >= " + listString;
						break;
					}
					case LESS_THAN_EQUAL: {
						Set<Object> filterValues = innerMap.get(comparator);
						String listString = filterValues.iterator().next().toString();
						filterStatement += tableHeader+" <= " + listString;
						break;
					}
					default: {
						Set<Object> filterValues = innerMap.get(comparator);
						String listString = getQueryStringList(filterValues);
						filterStatement += tableHeader+" in " + listString;
					}

					}
					i++;
				}

				//put appropriate ands
				if(x < filteredColumns.size() - 1) {
					filterStatement += " AND ";
				}
			}

			if(filterStatement.length() > 0) {
				filterStatement = " WHERE " + filterStatement;
			}
		}

		return filterStatement;
	}

	/**
	 * 
	 * @param selectors - list of headers to grab from table
	 * @return
	 * 
	 * return the filtered values portion of the filter model that is returned by the H2Frame
	 */
	public Map<String, List<Object>> getFilteredValues(List<String> selectors) {

		//Header name -> [val1, val2, ...]
		//Ex: Studio -> [WB, Universal]
		//	WB and Universal are filtered from Studio column
		String tableName = this.tableName;
//		Map<String, Map<Comparator, Set<Object>>> filterHash = this.filterHash2;
//		if(joinMode) {
//			tableName = this.viewTableName;
//			filterHash = joiner.getJoinedFilterHash(viewTableName);
//		}

		Map<String, List<Object>> returnFilterMap = new HashMap<>();

		try {
			for(String selector : selectors) {
				
//				String thisSelector = joinMode ? translateColumn(selector) : selector;
				
				if(filterHash.get(selector) != null) {
					String query = makeNotSelect(tableName, selector);
					ResultSet rs = executeQuery(query);

					List<Object> filterData = null;
					if(rs != null) {
						//						ResultSetMetaData rsmd = rs.getMetaData();
						filterData = new ArrayList<>();
						while (rs.next()){
							Object nextValue = rs.getObject(1);
							filterData.add(nextValue);
						}
					}

					returnFilterMap.put(selector, filterData);
				} else {
					returnFilterMap.put(selector, new ArrayList<Object>());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return returnFilterMap;
	}

	/*************************** END FILTER **************************************/
	
	/*************************** QUERY BUILDERS **************************************/
	

	//make a select query
	private String makeSelect(String tableName, List<String> selectors) {

		String selectStatement = "SELECT ";

		for(int i = 0; i < selectors.size(); i++) {
			String selector = selectors.get(i);
			//    		selector = cleanHeader(selector);

			if(i < selectors.size() - 1) {
				selectStatement += selector + ", ";
			}
			else {
				selectStatement += selector;	
			}
		}

		//    	String filterSubQuery = makeFilterSubQuery(this.filterHash, this.filterComparator);
		String filterSubQuery = makeFilterSubQuery(this.filterHash);
		selectStatement += " FROM " + tableName + filterSubQuery;
		return selectStatement;
	}

//	private String makeSpecificSelect(String tableName, List<String> selectors, String columnHeader, Object value) {
//		value = cleanInstance(value.toString());
//
//		//SELECT column1, column2, column3
//		String selectStatement = "SELECT ";
//		for(int i = 0; i < selectors.size(); i++) {
//			String selector = selectors.get(i);
//
//			if(i < selectors.size() - 1) {
//				selectStatement += selector + ", ";
//			}
//			else {
//				selectStatement += selector;
//			}
//		}
//
//		//SELECT column1, column2, column3 from table1
//		selectStatement += " FROM " + tableName;
//		//		String filterSubQuery = makeFilterSubQuery(this.filterHash, this.filterComparator);
//		String filterSubQuery = makeFilterSubQuery(this.filterHash); 
//		if(filterSubQuery.length() > 1) {
//			selectStatement += filterSubQuery;
//			selectStatement += " AND " + columnHeader + " = " + "'"+value+"'";
//		} else {
//			selectStatement += " WHERE " + columnHeader + " = " + "'"+value+"'";
//		}
//
//		return selectStatement;
//	}

	//make a select query
	private String makeSelectDistinct(String tableName, List<String> selectors) {

		String selectStatement = "SELECT DISTINCT ";

		for(int i = 0; i < selectors.size(); i++) {
			String selector = selectors.get(i);

			if(i < selectors.size() - 1) {
				selectStatement += selector + ",";
			}
			else {
				selectStatement += selector;
			}
		}

		//    	String filterSubQuery = makeFilterSubQuery(this.filterHash, this.filterComparator);
		String filterSubQuery = makeFilterSubQuery(this.filterHash);
		selectStatement += " FROM " + tableName + filterSubQuery;

		return selectStatement;
	}


	/**
	 * This method returns all the distinct values that are filtered out for a particular column
	 * This method is mainly used to retrieve values for the filter model displayed on the front end
	 * 
	 * @param tableName - table in which selector exists
	 * @param selector - column to grab values from
	 * @return
	 */
	private String makeNotSelect(String tableName, String selector) {
		String selectStatement = "SELECT DISTINCT ";

		selectStatement += selector + " FROM " + tableName;    	

		String filterStatement = "";

		Map<AbstractTableDataFrame.Comparator, Set<Object>> filterMap = filterHash.get(selector);
		int i = 0;
		//what ever is listed in the filter hash, we want the get the values that would be the logical opposite
		//i.e. if filter hash indicates 'X < 0.9 AND X > 0.8', return 'X > =0.9 OR X <= 0.8'
		for(AbstractTableDataFrame.Comparator comparator : filterMap.keySet()) {
			if(i > 0) {
				filterStatement += " OR ";
			}
			Set<Object> filterValues = filterMap.get(comparator);
			if(filterValues.size() == 0) continue;

			if(comparator.equals(AbstractTableDataFrame.Comparator.EQUAL)) {
				String listString = getQueryStringList(filterValues);
				filterStatement += selector+" NOT IN " + listString;
			} else if(comparator.equals(AbstractTableDataFrame.Comparator.NOT_EQUAL)) {
				String listString = getQueryStringList(filterValues);
				filterStatement += selector+" IN " + listString;
			} else if(comparator.equals(AbstractTableDataFrame.Comparator.GREATER_THAN)) {
				filterStatement += selector+" <= "+filterValues.iterator().next().toString();
			} else if(comparator.equals(AbstractTableDataFrame.Comparator.GREATER_THAN_EQUAL)) {
				filterStatement += selector+" < "+filterValues.iterator().next().toString();
			} else if(comparator.equals(AbstractTableDataFrame.Comparator.LESS_THAN)) {
				filterStatement += selector+" >= "+filterValues.iterator().next().toString();
			} else if(comparator.equals(AbstractTableDataFrame.Comparator.LESS_THAN_EQUAL)) {
				filterStatement += selector+" > "+filterValues.iterator().next().toString();
			}
			i++;
		}

		selectStatement += " WHERE " + filterStatement;

		return selectStatement;
	}

	private String getQueryStringList(List<Object> values) {
		String listString = "(";

		for(int i = 0; i < values.size(); i++) {
			Object value = values.get(i);
			value = cleanInstance(value.toString());
			listString += "'"+value+"'";
			if(i < values.size() - 1) {
				listString += ", ";
			}
		}

		listString+=")";
		return listString;
	}

	private String getQueryStringList(Set<Object> values) {
		String listString = "(";

		Iterator<Object> iterator = values.iterator();
		int i = 0;
		while(iterator.hasNext()) {
			Object value = iterator.next();
			value = cleanInstance(value.toString());
			listString += "'"+value+"'";
			if(i < values.size() - 1) {
				listString += ", ";
			}
			i++;
		}

		listString+=")";
		return listString;
	}

	private String makeFunction(String column, String function, String tableName) {
		String functionString = "SELECT ";
		switch(function.toUpperCase()) {
		case "COUNT": functionString += "COUNT("+column+")"; break;
		case "AVERAGE": functionString += "AVG("+column+")"; break;
		case "MIN": functionString += "MIN("+column+")"; break;
		case "MAX": functionString += "MAX("+column+")"; break;
		case "SUM": functionString += "SUM("+column+")"; break;
		default: functionString += column;
		}

		functionString += "FROM "+tableName;
		return functionString;
	}
	
	private String makeFunctionIgnoreFilter(String column, String function, String tableName) {
		String functionString = "SELECT ";
		switch(function.toUpperCase()) {
		case "COUNT": functionString += "COUNT("+column+")"; break;
		case "AVERAGE": functionString += "AVG("+column+")"; break;
		case "MIN": functionString += "MIN("+column+")"; break;
		case "MAX": functionString += "MAX("+column+")"; break;
		case "SUM": functionString += "SUM("+column+")"; break;
		default: functionString += column;
		}

		functionString += "FROM "+tableName;
		return functionString;
	}

//	private String makeFunction(String column, String function, String tableName, List<String> groupByCols) {
//		if(groupByCols == null || groupByCols.isEmpty()) {
//			return makeFunction(column, function, tableName);
//		}
//
//		// clean all the headers and group by cols
////		List<String> cleanGroupByCols = new Vector<String>();
////		for(String col : groupByCols) {
////			cleanGroupByCols.add(cleanHeader(col));
////		}
//
//		// the first return is the column you are modifying
//		String functionString = "SELECT DISTINCT ";
//		switch(function.toUpperCase()) {
//		case "COUNT": functionString += "COUNT("+column+")"; break;
//		case "AVERAGE": functionString += "AVG("+column+")"; break;
//		case "MIN": functionString += "MIN("+column+")"; break;
//		case "MAX": functionString += "MAX("+column+")"; break;
//		case "SUM": functionString += "SUM("+column+")"; break;
//		default: functionString += column;
//		}
//
//		// also want to return the group by cols
//		for(String col : groupByCols) {
//			functionString += ", " + col;
//		}
//
//		// add group by
//		functionString += " FROM " + tableName + " GROUP BY ";
//		boolean isFirst = true;
//		for(String col : groupByCols) {
//			if(isFirst) {
//				functionString += col;
//				isFirst = false;
//			} else {
//				functionString += ", " + col;
//			}
//		}
//
//		return functionString;
//	}
	
	/*************************** END QUERY BUILDERS **************************************/
	
	/*************************** QUERY EXECUTION **********************************************/
	
	//use this when result set is expected
	private ResultSet executeQuery(String query) {
		LOGGER.info("EXECUTING QUERY:	"+query);
		Map<String, Object> execData = coreEngine.execQuery(query);
		ResultSet rs = (ResultSet) execData.get(RDBMSNativeEngine.RESULTSET_OBJECT);
		return rs;
	}
	
	//use this when result set is not expected back
//	private void runQuery(String query) throws Exception{
//		query = query.trim().toUpperCase();
//		if(query.startsWith("DROP")) {
//			if(query.startsWith("DROP VIEW")) {
//				getConnection().createStatement().execute(query);
//			} else {
//				throw new IllegalArgumentException("CAN ONLY DROP VIEWS IN NATIVE MODE");
//			}
//		} else {
//			getConnection().createStatement().execute(query);
//		}
//	}
//
//	//use this when result set is expected
//	private ResultSet executeQuery(String query) {
//		try {
//			return getConnection().createStatement().executeQuery(query);
//		} catch(SQLException e) {
//			e.printStackTrace();
//		}
//		return null;
//	}
//	
//	public void runExternalQuery(String query) throws Exception {
//		query = query.toUpperCase().trim();
//		if(query.startsWith("CREATE VIEW")) {
//			runQuery(query);
//		} 
//
//		else if(query.startsWith("CREATE OR REPLACE VIEW")) {
//			runQuery(query);
//		}
//		
//		else if(query.startsWith("CREATE TEMPORARY TABLE")) {
//			runQuery(query);
//		}
//		//possibly need to drop view table externally as well
//		else if(query.startsWith("DROP VIEW")) {
//			runQuery(query);
//		} 
//
//		else {
//			//could also run select queries directly
//			throw new IllegalArgumentException("Can only run 'create view/create or replace view/drop view' queries externally");
//		}
//	}
	/*************************** END QUERY EXECUTION **********************************************/

	
	/*************************** UTILITY **********************************************/
	
	private String cleanInstance(String value) {
		return value.replace("'", "''");
	}
	
	/*************************** END UTILITY **********************************************/
	
	/*----------------- ONLY WANT TO DELETE VIEWS HERE, NOT DATABASE DATA----------------------------------*/ 
	/*************************** DELETE **********************************************/

//	protected void dropView() {
//		String dropViewQuery = "DROP VIEW "+this.tableName;
//		try {
//			runQuery(dropViewQuery);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
	
	/*************************** END DELTE **********************************************/
	
	
}

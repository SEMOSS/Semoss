package prerna.ds.sqlserver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ds.AbstractTableDataFrame;
import prerna.ds.h2.H2Builder;
import prerna.ds.h2.H2Iterator;
import prerna.ds.util.RdbmsFrameUtility;
import prerna.ds.util.RdbmsQueryBuilder;

public class SqlServerBuilder extends H2Builder {

	private static final Logger LOGGER = LogManager.getLogger(SqlServerBuilder.class.getName());

	{
		typeConversionMap.clear();
		typeConversionMap.put("DOUBLE", "DECIMAL(18,5)");
		typeConversionMap.put("NUMBER", "DECIMAL(18,5)");
		typeConversionMap.put("FLOAT", "DECIMAL(18,5)");
		typeConversionMap.put("LONG", "DECIMAL(18,5)");
		typeConversionMap.put("STRING", "VARCHAR(800)");
		typeConversionMap.put("DATE", "DATE");
		typeConversionMap.put("TIMESTAMP", "DATE");
	}
	
	public SqlServerBuilder() {
		super();
		this.isInMem = false;
		try {
			Statement statement = getConnection().createStatement();
			statement.execute("USE semosssqlserver");
			statement.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public Connection getConnection() {
		if (this.conn == null) {
			try {
				Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
				String url = "jdbc:sqlserver://semosssqlserver.database.windows.net:1433;database=semosssqlserver;";
				this.conn = DriverManager.getConnection(url, "semossadmin@semosssqlserver", "S3m0ss!123");
				
				LOGGER.info("Successfully connected to MS SQL");
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return this.conn;
	}
	
	public String[] getHeaders(String tableName) {
		List<String> headers = new ArrayList<String>();

		String columnQuery = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + tableName + "'";
		ResultSet rs = executeQuery(columnQuery);
		try {
			while (rs.next()) {
				String header = rs.getString("COLUMN_NAME");
				headers.add(header);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return headers.toArray(new String[] {});
	}
	
	protected String getDataType(String tableName, String colName) {
		String query = "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + tableName + "'" + 
				" AND COLUMN_NAME = '" + colName + "'";
		ResultSet rs = executeQuery(query);
		try {
			if(rs.next()) {
				return rs.getString(1).toUpperCase();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public Iterator buildIterator(Map<String, Object> options) {
		String tableName = getReadTable();

		String sortDir = (String) options.get(AbstractTableDataFrame.SORT_BY_DIRECTION);

		// dedup = true indicates remove duplicates
		// TODO: rename de_dup to something more meaningful for cases outside of
		// tinkergraph
		Boolean dedup = (Boolean) options.get(AbstractTableDataFrame.DE_DUP);
		if (dedup == null)
			dedup = false;

		Boolean ignoreFilters = (Boolean) options.get(AbstractTableDataFrame.IGNORE_FILTERS);
		if (ignoreFilters == null)
			ignoreFilters = false;

		// how many rows to get
		Integer limit = (Integer) options.get(AbstractTableDataFrame.LIMIT);

		// at which row to start
		Integer offset = (Integer) options.get(AbstractTableDataFrame.OFFSET);

		// column to sort by
		String sortBy = (String) options.get(AbstractTableDataFrame.SORT_BY);

		// selectors to gather
		List<String> selectors = (List<String>) options.get(AbstractTableDataFrame.SELECTORS);
		if(selectors.isEmpty()) {
			selectors = Arrays.asList(getHeaders(this.tableName));
		}
		selectors = cleanHeaders(selectors);
		
		
		boolean hasLimit = false;
		boolean hasOffset = false;
		boolean hasSort = false;
		if(offset != null && offset > 0) {
			hasOffset = true;
		} 
		if (limit != null && limit > 0) {
			hasLimit = true;
		}
		if (sortBy != null) {
			sortBy = cleanHeader(sortBy);
			// add an index to the table to make the sort much faster
			// note h2 view does not support index
			addColumnIndex(tableName, sortBy);
			hasSort = true;
		} else {
			if(hasLimit && hasOffset) {
				// to do limit/offset in Sql Server, we must have an order by
				// so just order the first column
				sortBy = selectors.get(0);
				addColumnIndex(tableName, sortBy);
				hasSort = true;
			}
		}
		
		String selectQuery = RdbmsQueryBuilder.makeSelect(tableName, selectors, dedup);
		// we have a limit but no offset, we got to add a top
		if(hasLimit && !hasOffset) {
			if(dedup) {
				selectQuery = selectQuery.replace("SELECT DISTINCT ", "SELECT DISTINCT TOP(" + limit + ") ");
			} else {
				selectQuery = selectQuery.replace("SELECT ", "SELECT TOP(" + limit + ") ");
			}
		}
		if(!ignoreFilters) {
			selectQuery += makeFilterSubQuery();
		}

		// temporary filters to apply only to this iterator
		Map<String, List<Object>> temporalBindings = (Map<String, List<Object>>) options.get(AbstractTableDataFrame.TEMPORAL_BINDINGS);
		Map<String, AbstractTableDataFrame.Comparator> compHash = new HashMap<String, AbstractTableDataFrame.Comparator>();
		for (String key : temporalBindings.keySet()) {
			compHash.put(key, AbstractTableDataFrame.Comparator.EQUAL);
		}

		// create a new filter substring and add/replace old filter substring
		String temporalFiltering = makeFilterSubQuery(temporalBindings, compHash); // default comparator is equals
		if (temporalFiltering != null && temporalFiltering.length() > 0) {
			if (selectQuery.contains(" WHERE ")) {
				temporalFiltering = temporalFiltering.replaceFirst(" WHERE ", "");
				selectQuery = selectQuery + temporalFiltering;
			} else {
				selectQuery = selectQuery + temporalFiltering;
			}
		}

		if(hasSort) {
			selectQuery += " ORDER BY " + sortBy + " ASC ";
		}
		
		if(hasLimit && hasOffset) {
			selectQuery += " OFFSET " + offset + " ROWS FETCH NEXT " + limit + " ROWS ONLY";
		} else if(hasOffset && !hasLimit){
			selectQuery += " OFFSET " + offset;
		}

		long startTime = System.currentTimeMillis();
		System.out.println("TABLE NAME IS: " + this.tableName);
		System.out.println("RUNNING QUERY : " + selectQuery);
		ResultSet rs = executeQuery(selectQuery);
		long endTime = System.currentTimeMillis();
		LOGGER.info("Executed Select Query on H2 FRAME: " + (endTime - startTime) + " ms");
		return new H2Iterator(rs);
	}
	
	public boolean isEmpty(String tableName) {
		// first check if the table exists
		if (tableExists(tableName)) {
			// now check if there is at least one row
			String query = "SELECT TOP(1) * FROM " + tableName;
			ResultSet rs = executeQuery(query);
			try {
				if (rs.next()) {
					return false;
				}
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				if (rs != null) {
					try {
						rs.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			}
		}
		return true;
	}

	public void setSchema() {
		LOGGER.info("Currently cannot change schema...");
	}
	
	protected void mergeTables(String tableName1, String tableName2, Hashtable<Integer, Integer> matchers, String[] oldHeaders, String[] newHeaders, String join) {
		getConnection();

		// want to create indices on the join columns to speed up the process
		for (Integer table1JoinIndex : matchers.keySet()) {
			Integer table2JoinIndex = matchers.get(table1JoinIndex);

			String table1JoinCol = newHeaders[table1JoinIndex];
			String table2JoinCol = oldHeaders[table2JoinIndex];

			addColumnIndex(tableName1, table1JoinCol);
			addColumnIndex(tableName2, table2JoinCol);
			// note that this creates indices on table1 and table2
			// but these tables are later dropped so no indices are kept
			// through the flow
		}

		// now I need to create a join query
		// first the froms
		
		String froms = " FROM " + tableName1 + " AS  A ";
		String joins = " " + join + " " + tableName2 + " AS B ON (";

		Enumeration<Integer> keys = matchers.keys();
		for (int jIndex = 0; jIndex < matchers.size(); jIndex++) {
			Integer newIndex = keys.nextElement();
			Integer oldIndex = matchers.get(newIndex);

			String oldCol = oldHeaders[oldIndex];
			String newCol = newHeaders[newIndex];

			// need to make sure the data types are good to go
			String oldColType = getDataType(tableName1, oldCol);
			String newColType = getDataType(tableName2, newCol);

			// syntax modification for each addition join column
			if (jIndex != 0) {
				joins = joins + " AND ";
			}

			if(oldColType.equals(newColType)) {
				// data types are the same, no need to do anything
				joins = joins + "A." + oldCol + " = " + "B." + newCol;
			} else {
				// data types are different... 
				// if both are different numbers -> convert both to double
				// else -> convert to strings

				if( (oldColType.equals("DOUBLE") || oldColType.equals("INT") )
						&& (newColType.equals("DOUBLE") || newColType.equals("INT") ) ) {
					// both are numbers
					if(!oldColType.equals("DOUBLE")) {
						joins = joins + " A." + oldCol;
					} else {
						joins = joins + " CAST(A." + oldCol + " AS DOUBLE)";
					}
					joins = joins + " = ";
					if(!newColType.equals("DOUBLE")) {
						joins = joins + " B." + newCol;
					} else {
						joins = joins + " CAST(B." + newCol + " AS DOUBLE)";
					}
				}
				// case when old col type is double and new col type is string
				else if( (oldColType.equals("DOUBLE") || oldColType.equals("INT") )
						&& newColType.equals("VARCHAR") ) 
				{
					// if it is not a double, convert it
					if(!oldColType.equals("DOUBLE")) {
						joins = joins + " CAST(A." + oldCol + " AS DOUBLE)";
					} else {
						joins = joins + " A." + oldCol;
					}
					joins = joins + " = ";

					// new col is a string
					// so cast to double
					joins = joins + " CAST(B." + newCol + " AS DOUBLE)";
				}
				// case when old col type is string and new col type is double
				else if(  oldColType.equals("VARCHAR") && 
						(newColType.equals("DOUBLE") || newColType.equals("INT") ) ) 
				{
					// old col is a string
					// so cast to double
					joins = joins + " CAST(A." + oldCol + " AS DOUBLE)";
					joins = joins + " = ";
					// if it is not a double, convert it
					if(!newColType.equals("DOUBLE")) {
						joins = joins + " B." + newCol;
					} else {
						joins = joins + " CAST(B." + newCol + " AS DOUBLE)";
					}
				}
				else {
					// not sure... just make everything a string
					if(oldColType.equals("VARCHAR")) {
						joins = joins + " A." + oldCol;
					} else {
						joins = joins + " CAST( A." + oldCol + " AS VARCHAR(800))";
					}
					joins = joins + " = ";
					if(newColType.equals("VARCHAR")) {
						joins = joins + " B." + newCol;
					} else {
						joins = joins + " CAST(B." + newCol + " AS VARCHAR(800))";
					}
				}
			}
		}

		joins = joins + " )";

		// first table A
		String selectors = "";
		for (int oldIndex = 0; oldIndex < oldHeaders.length; oldIndex++) {
			if (oldIndex == 0)
				selectors = "A." + oldHeaders[oldIndex];
			else
				selectors = selectors + " , " + "A." + oldHeaders[oldIndex];
		}

		// next table 2
		for (int newIndex = 0; newIndex < newHeaders.length; newIndex++) {
			if (!matchers.containsKey(newIndex))
				selectors = selectors + " , " + "B." + newHeaders[newIndex];
		}

		
		String origTableName = tableName1;
		// now create a third table
		String tempTableName = RdbmsFrameUtility.getNewTableName();
		String finalQuery = "SELECT " + selectors + " INTO " + tempTableName + " " + froms + " " + joins;

		System.out.println(finalQuery);

		try {
			long start = System.currentTimeMillis();
			runQuery(finalQuery);
			long end = System.currentTimeMillis();
			System.out.println("TIME FOR JOINING TABLES = " + (end - start) + " ms");

			// Statement stmt = conn.createStatement();
			// stmt.execute(finalQuery);

			runQuery(RdbmsQueryBuilder.makeDropTable(tableName1));

			// DONT DROP THIS due to need to preserve for outer joins, method
			// outside will handle dropping new table
			// runQuery(makeDropTable(tableName2));

			// rename back to the original table
			runQuery("EXEC SP_RENAME '" + tempTableName + "' , '" + origTableName + "'");

			// this created a new table
			// need to clear the index map
			clearColumnIndexMap();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public LinkedHashMap<String, String> connectToExistingTable(String tableName) {
		String query = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='"
				+ tableName + "'";
		this.conn = getConnection();
		try {
			if(this.conn.isClosed()) {
				this.conn = null;
				this.conn = getConnection();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		LinkedHashMap<String, String> dataTypeMap = new LinkedHashMap<String, String>();
		ResultSet rs = executeQuery(query);
		try {
			while(rs.next()) {
				String colName = rs.getString(1).toUpperCase();
				String dataType = rs.getString(2).toUpperCase();
				dataTypeMap.put(colName, dataType);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		if(dataTypeMap.isEmpty()) {
			throw new IllegalArgumentException("Table name " + tableName + " does not exist or is empty");
		}
		
		this.tableName = tableName;
		return dataTypeMap;
	}
	
}

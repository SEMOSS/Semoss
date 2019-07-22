package prerna.ds.rdbms;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.ds.EmptyIteratorException;
import prerna.engine.api.IHeadersDataRow;
import prerna.poi.main.HeadersException;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class RdbmsFrameBuilder {

	private static final String CLASS_NAME = RdbmsFrameBuilder.class.getName();
	private Logger logger = LogManager.getLogger(CLASS_NAME);
	
	protected Connection conn;
	protected String schema;
	protected AbstractSqlQueryUtil queryUtil;
	
	// keep track of the indices that exist in the table for optimal speed in sorting
	protected Map<String, String> columnIndexMap = new Hashtable<String, String>();
	protected Map<String, String> multiColumnIndexMap = new Hashtable<String, String>();
	
	public RdbmsFrameBuilder(Connection conn, String schema, AbstractSqlQueryUtil util) {
		this.conn = conn;
		this.schema = schema;
		this.queryUtil = util;
	}
	
	protected Map<String, String> typeConversionMap = new HashMap<String, String>(); {
		typeConversionMap.clear();
		
		typeConversionMap.put("INT", "INT");
		typeConversionMap.put("LONG", "INT");
		
		typeConversionMap.put("NUMBER", "DOUBLE");
		typeConversionMap.put("FLOAT", "DOUBLE");
		typeConversionMap.put("DOUBLE", "DOUBLE");

		typeConversionMap.put("DATE", "DATE");
		typeConversionMap.put("TIMESTAMP", "TIMESTAMP");
		
		typeConversionMap.put("STRING", "VARCHAR(800)");
	}
	
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	
	/*
	 * Write methods
	 */
	
	/**
	 * Add a row into a table
	 * @param tableName
	 * @param columnNames
	 * @param values
	 * @param types
	 */
	public void addRow(String tableName, String[] columnNames, Object[] values, String[] types) {
		boolean create = true;
		types = cleanTypes(types);

		// create table if it does not already exist
		try {
			if (!this.queryUtil.tableExists(this.conn, tableName, this.schema)) {
				String createTable = this.queryUtil.createTable(tableName, columnNames, types);
				runQuery(createTable);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			create = false;
		}

		// add the row to the table
		try {
			if(create) {
				String insert = this.queryUtil.insertIntoTable(tableName, columnNames, types, values);
				runQuery(insert);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
	
	/**
	 * Add rows into a table based on an iterator
	 * This will create the table / alter the table to have the column names of the iterator if not currently present
	 * @param iterator
	 * @param tableName
	 * @param typesMap
	 */
	public void addRowsViaIterator(Iterator<IHeadersDataRow> iterator, String tableName, Map<String, SemossDataType> typesMap) {
		try {
			// keep a batch size so we dont get heapspace
			final int batchSize = 5000;
			int count = 0;

			PreparedStatement ps = null;
			SemossDataType[] types = null;
			String[] strTypes = null;

			// we loop through every row of the csv
			while (iterator.hasNext()) {
				IHeadersDataRow headerRow = iterator.next();
				Object[] nextRow = headerRow.getValues();

				// need to set values on the first iteration
				if (ps == null) {
					String[] headers = headerRow.getHeaders();
					headers = HeadersException.getInstance().getCleanHeaders(headers);
					// get the data types
					types = new SemossDataType[headers.length];
					strTypes = new String[headers.length];
					for (int i = 0; i < types.length; i++) {
						types[i] = typesMap.get(headers[i]);
						strTypes[i] = SemossDataType.convertDataTypeToString(types[i] );
					}
					// alter the table to have the column information if not
					// already present
					// this will also create a new table if the table currently
					// doesn't exist
					alterTableNewColumns(tableName, headers, strTypes);

					// set the PS based on the headers
					ps = createInsertPreparedStatement(tableName, headers);
				}

				// we need to loop through every value and cast appropriately
				for (int colIndex = 0; colIndex < nextRow.length; colIndex++) {
					SemossDataType type = types[colIndex];
					if (type == SemossDataType.INT) {
						if(nextRow[colIndex] instanceof Number) {
							ps.setInt(colIndex + 1, ((Number) nextRow[colIndex]).intValue());
						} else {
							Integer value = Utility.getInteger(nextRow[colIndex] + "");
							if (value != null) {
								ps.setInt(colIndex + 1, value);
							} else {
								ps.setNull(colIndex + 1, java.sql.Types.DOUBLE);
							}
						}
					} else if(type == SemossDataType.DOUBLE) {
						if(nextRow[colIndex] instanceof Number) {
							ps.setDouble(colIndex + 1, ((Number) nextRow[colIndex]).doubleValue());
						} else {
							Double value = Utility.getDouble(nextRow[colIndex] + "");
							if (value != null) {
								ps.setDouble(colIndex + 1, value);
							} else {
								ps.setNull(colIndex + 1, java.sql.Types.DOUBLE);
							}
						}
					} else if (type == SemossDataType.DATE) {
						if (nextRow[colIndex] == null) {
							ps.setNull(colIndex + 1, java.sql.Types.DATE);
						} else if(nextRow[colIndex] instanceof SemossDate) {
							Date d = ((SemossDate) nextRow[colIndex]).getDate();
							if(d != null) {
								ps.setDate(colIndex + 1, new java.sql.Date( d.getTime() ) );
							} else {
								ps.setNull(colIndex + 1, java.sql.Types.DATE);
							}
						} else {
							SemossDate value = SemossDate.genDateObj(nextRow[colIndex] + "");
							if (value != null) {
								ps.setDate(colIndex + 1, new java.sql.Date(value.getDate().getTime()));
							} else {
								ps.setNull(colIndex + 1, java.sql.Types.DATE);
							}
						}
					} else if (type == SemossDataType.TIMESTAMP) {
						if (nextRow[colIndex] == null) {
							ps.setNull(colIndex + 1, java.sql.Types.DATE);
						} else if(nextRow[colIndex] instanceof SemossDate) {
							Date d = ((SemossDate) nextRow[colIndex]).getDate();
							if(d != null) {
								ps.setTimestamp(colIndex + 1, new java.sql.Timestamp( d.getTime() ) );
							} else {
								ps.setNull(colIndex + 1, java.sql.Types.TIMESTAMP);
							}
						} else {
							SemossDate value = SemossDate.genTimeStampDateObj(nextRow[colIndex] + "");
							if (value != null) {
								ps.setTimestamp(colIndex + 1, new java.sql.Timestamp(value.getDate().getTime()));
							} else {
								ps.setNull(colIndex + 1, java.sql.Types.TIMESTAMP);
							}
						}
					} else {
						if(nextRow[colIndex] == null) {
							ps.setNull(colIndex + 1, java.sql.Types.VARCHAR);
						} else {
							String value = nextRow[colIndex] + "";
							if(value.length() > 800) {
								value = value.substring(0, 796) + "...";
							}
							ps.setString(colIndex + 1, value + "");
						}
					}
				}
				// add it
				ps.addBatch();

				// batch commit based on size
				if (++count % batchSize == 0) {
					logger.info("Executing batch .... row num = " + count);
					ps.executeBatch();
				}
			}

			if(ps == null) {
				throw new EmptyIteratorException("Query returned no data");
			}
			
			// well, we are done looping through now
			logger.info("Executing final batch .... row num = " + count);
			ps.executeBatch(); // insert any remaining records
			ps.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Create a prepared statement in order to perform bulk inserts into a table
	 * @param TABLE_NAME			The name of the table
	 * @param columns				The columns that will be used in the inserting
	 * @return The prepared statement
	 */
	public PreparedStatement createInsertPreparedStatement(final String TABLE_NAME, final String[] columns) {
		String sql = this.queryUtil.createInsertPreparedStatementString(TABLE_NAME, columns);

		PreparedStatement ps = null;
		try {
			// create the prepared statement using the sql query defined
			ps = this.conn.prepareStatement(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return ps;
	}

	/**
	 * Create a prepared statement in order to perform a bulk update
	 * @param TABLE_NAME			The name of the table
	 * @param columnsToUpdate		The columns to update
	 * @param whereColumns			The conditions of the update
	 * @return
	 */
	public PreparedStatement createUpdatePreparedStatement(final String TABLE_NAME, final String[] columnsToUpdate, final String[] whereColumns) {
		// generate the sql for the prepared statement
		String sql = this.queryUtil.createUpdatePreparedStatementString(TABLE_NAME, columnsToUpdate, whereColumns);
		
		PreparedStatement ps = null;
		try {
			// create the prepared statement using the sql query defined
			ps = this.conn.prepareStatement(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return ps;
	}
	
	private PreparedStatement createPreparedStatement(String sql) {
		PreparedStatement ps = null;
		try {
			// create the prepared statement using the sql query defined
			ps = this.conn.prepareStatement(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return ps;
	}
	
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////

	
	/**
	 * Alter the table to make sure it has the following headers
	 * @param tableName
	 * @param headers
	 * @param types
	 */
	public void alterTableNewColumns(String tableName, String[] headers, String[] types) {
		types = cleanTypes(types);
		try {
			if (this.queryUtil.tableExists(this.conn, tableName, this.schema)) {
				List<String> newHeaders = new ArrayList<String>();
				List<String> newTypes = new ArrayList<String>();

				// determine the new headers and types
				List<String> currentHeaders = this.queryUtil.getTableColumns(this.conn, tableName, this.schema);
				for (int i = 0; i < headers.length; i++) {
					if (!currentHeaders.contains(headers[i].toUpperCase())) {
						// these are the columns to create
						newHeaders.add(headers[i]);
						newTypes.add(types[i]);
					}
				}

				// if we have new headers add them to the table
				if (!newHeaders.isEmpty()) {
					// if there is an index
					// definitely get rid of it
					// or this takes forever on big data
					List<String[]> indicesToAdd = new Vector<String[]>();
					Set<String> colIndexMapKeys = new HashSet<String>(this.columnIndexMap.keySet());
					for(String tableColConcat : colIndexMapKeys) {
						// table name and col name are appended together with +++
						String[] tableCol = tableColConcat.split("\\+\\+\\+");
						indicesToAdd.add(tableCol);
						removeColumnIndex(tableCol[0], tableCol[1]);
					}
					if(this.queryUtil.allowMultiAddColumn()) {
						String alterQuery = this.queryUtil.alterTableAddColumns(tableName, newHeaders.toArray(new String[]{}), newTypes.toArray(new String[]{}));
						logger.debug("ALTERING TABLE: " + alterQuery);
						runQuery(alterQuery);
						logger.debug("DONE ALTER TABLE");
					} else {
						// must look through all the headers + types
						for(int i = 0; i < newHeaders.size(); i++) {
							String alterQuery = this.queryUtil.alterTableAddColumn(tableName, newHeaders.get(i), newTypes.get(i));
							logger.debug("ALTERING TABLE: " + alterQuery);
							runQuery(alterQuery);
							logger.debug("DONE ALTER TABLE");
						}
					}
					for(String[] tableColIndex : indicesToAdd ) {
						addColumnIndex(tableColIndex[0], tableColIndex[1]);
					}
				}
			} else {
				// if table doesn't exist then create one with headers and types
				String createTable =  queryUtil.createTable(tableName, headers, types);
				logger.info("Generating SQL table");
				logger.debug("CREATING TABLE: " + createTable);
				runQuery(createTable);
				logger.info("Finished generating SQL table");
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		}
	}
	
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////

	/*
	 * Indexing
	 */
	
	public void addColumnIndex(String tableName, String colName) {
		if (!columnIndexMap.containsKey(tableName + "+++" + colName)) {
			long start = System.currentTimeMillis();

			String indexSql = null;
			logger.info("Generating index on SQL Table on column = " + colName);
			logger.debug("CREATING INDEX ON TABLE = " + tableName + " ON COLUMN = " + colName);
			try {
				String indexName = colName + "_INDEX_" + getRandomValues();
				indexSql = queryUtil.createIndex(indexName, tableName, colName);
				runQuery(indexSql);
				columnIndexMap.put(tableName + "+++" + colName, indexName);
				
				long end = System.currentTimeMillis();

				logger.debug("TIME FOR INDEX CREATION = " + (end - start) + " ms");
				logger.info("Finished generating indices on SQL Table on column = " + colName);
			} catch (Exception e) {
				logger.debug("ERROR WITH INDEX !!! " + indexSql);
				e.printStackTrace();
			}
		}
	}
	
	public void addColumnIndex(String tableName, String[] colNames) {
		String multiColIndexName = StringUtils.join(colNames, "__");
		if (!multiColumnIndexMap.containsKey(tableName + "+++" + multiColIndexName)) {
			logger.info("Generating index on SQL Table columns = " + StringUtils.join(colNames,", "));
			logger.debug("CREATING INDEX ON TABLE = " + tableName + " ON COLUMNS = " + multiColIndexName);
			try {
				long start = System.currentTimeMillis();
				String indexName = multiColIndexName + "_INDEX_" + getRandomValues();
				String indexSql = queryUtil.createIndex(indexName, tableName, Arrays.asList(colNames));
				runQuery(indexSql);
				multiColumnIndexMap.put(tableName + "+++" + multiColIndexName, indexName);
				long end = System.currentTimeMillis();
				logger.debug("TIME FOR INDEX CREATION = " + (end - start) + " ms");
				logger.info("Finished generating indices on SQL Table on columns = " + StringUtils.join(colNames, ", "));
			} catch (Exception e) {
				logger.debug("ERROR WITH INDEX !!! " + multiColIndexName);
				e.printStackTrace();
			}
		}
	}

	protected void removeColumnIndex(String tableName, String colName) {
		if (columnIndexMap.containsKey(tableName + "+++" + colName)) {
			logger.info("Removing index on SQL Table column = " + colName);
			logger.debug("DROPPING INDEX ON TABLE = " + tableName + " ON COLUMN = " + colName);
			String indexName = columnIndexMap.remove(tableName +  "+++" + colName);
			try {
				runQuery(queryUtil.dropIndex(indexName, tableName));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public void removeColumnIndex(String tableName, String[] colNames) {
		String multiColIndexName = StringUtils.join(colNames, "__");
		if (multiColumnIndexMap.containsKey(tableName + "+++" + multiColIndexName)) {
			logger.info("DROPPING INDEX ON TABLE = " + tableName + " ON COLUMNS = " + multiColIndexName);
			String indexName = multiColumnIndexMap.remove(tableName +  "+++" + multiColIndexName);
			try {
				runQuery(queryUtil.dropIndex(indexName, tableName));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public boolean columnIndexed(String tableName, String colName) {
		if (columnIndexMap.containsKey(tableName + "+++" + colName)) {
			return true;
		}
		return false;
	}
	
	public void removeAllIndexes() {
		for(String key : columnIndexMap.keySet()) {
			String[] split = key.split("\\+\\+\\+");
			this.removeColumnIndex(split[0], split[1]);
		}
		for(String key : multiColumnIndexMap.keySet()) {
			String[] split = key.split("\\+\\+\\+");
			this.removeColumnIndex(split[0], split[1].split("__"));
		}
	}
	
	
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////
	
	/*
	 * Utility methods
	 */
	
	/**
	 * Execute a query
	 * @param query
	 * @throws Exception
	 */
	public void runQuery(String query) throws Exception {
		long start = System.currentTimeMillis();
		logger.debug("Running frame query : " + query);
		Statement stmt = null;
		try {
			stmt = this.conn.createStatement();
			stmt.execute(query);
		} finally {
			if(stmt != null) {
				stmt.close();
			}
		}
		long end = System.currentTimeMillis();
		logger.debug("Time to execute = " + (end-start) + "ms");
	}
	
	/**
	 * Get the headers for a table
	 * @param tableName
	 * @return
	 */
	public String[] getHeaders(String tableName) {
		return this.queryUtil.getTableColumns(this.conn, tableName, this.schema).toArray(new String[]{});
	}
	
	/**
	 * See if the table is empty
	 * @param tableName
	 * @return
	 */
	public boolean isEmpty(String tableName) {
		// first check if the table exists
		if (this.queryUtil.tableExists(this.conn, tableName, this.schema)) {
			// now check if there is at least one row
			String query = "SELECT * FROM " + tableName + " LIMIT 1";
			Statement stmt = null;
			ResultSet rs = null;
			try {
				stmt = this.conn.createStatement();
				rs = stmt.executeQuery(query);
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
				if(stmt != null) {
					try {
						stmt.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			}
		}
		return true;
	}
	
	/**
	 * Get the number of elements in this table
	 * @param tableName
	 * @return
	 */
	public int getNumRecords(String tableName) {
		String query = "SELECT COUNT(*) * " + getHeaders(tableName).length + " FROM " + tableName;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = this.conn.createStatement();
			rs = stmt.executeQuery(query);
			while (rs.next()) {
				return rs.getInt(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if(rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if(stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}

		return 0;
	}
	
	/**
	 * Get the clean SQL type based on the string representation
	 * @param type
	 * @return
	 */
	protected String cleanType(String type) {
		if (type == null) {
			type = "VARCHAR(800)";
		}
		type = type.toUpperCase();
		if (typeConversionMap.containsKey(type)) {
			type = typeConversionMap.get(type);
		} else {
			if (typeConversionMap.containsValue(type)) {
				return type;
			}
			type = "VARCHAR(800)";
		}
		return type;
	}

	/**
	 * Get the clean SQL types based on the string representation
	 * @param types
	 * @return
	 */
	protected String[] cleanTypes(String[] types) {
		String[] cleanTypes = new String[types.length];
		for (int i = 0; i < types.length; i++) {
			cleanTypes[i] = cleanType(types[i]);
		}

		return cleanTypes;
	}
	
	/**
	 * Get random UUID values
	 * @return
	 */
	private String getRandomValues() {
		String uuid = UUID.randomUUID().toString();
		uuid = uuid.replaceAll("-", "_");
		// table names will be upper case because that is how it is set in
		// information schema
		return uuid.toUpperCase();
	}
	
	/**
	 * Set the logger for the builder
	 * @param logger
	 */
	public void setLogger(Logger logger) {
		this.logger = logger;
	}

}

package prerna.ds.rdbms;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.algorithm.api.SemossDataType;
import prerna.ds.shared.AbstractTableDataFrame;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.query.querystruct.transform.QSRenameTableConverter;
import prerna.rdf.engine.wrappers.RawRDBMSSelectWrapper;
import prerna.reactor.imports.ImportUtility;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.util.Constants;
import prerna.util.sql.AbstractSqlQueryUtil;

public abstract class AbstractRdbmsFrame extends AbstractTableDataFrame {

	private Logger logger = LogManager.getLogger(AbstractRdbmsFrame.class);
	
	protected Connection conn = null;
	protected String database = null;
	protected String schema = null;
	protected AbstractSqlQueryUtil util = null;
	protected RdbmsFrameBuilder builder = null;

	public AbstractRdbmsFrame() {
		this.frameName = "RDBMSFRAME_" + UUID.randomUUID().toString().toUpperCase().replaceAll("-", "_");
		try {
			this.initConnAndBuilder();
		} catch (Exception e) {
			throw new IllegalArgumentException("Error generating new sql frame", e);
		}
		this.originalName = this.frameName;
	}
	
	public AbstractRdbmsFrame(String tableName) {
		if(tableName != null && !tableName.isEmpty()) {
			this.frameName = tableName;
		} else {
			this.frameName = "RDBMSFRAME_" + UUID.randomUUID().toString().toUpperCase().replaceAll("-", "_");
		}
		try {
			this.initConnAndBuilder();
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error generating new sql frame", e);
		}
		this.originalName = this.frameName;
	}
	
	public AbstractRdbmsFrame(String[] headers) {
		this();

		// assume all types are string
		int numHeaders = headers.length;
		String[] types = new String[numHeaders];
		for(int i = 0; i < numHeaders; i++) {
			types[i] = "STRING";
		}

		ImportUtility.parseHeadersAndTypeIntoMeta(this, headers, types, this.frameName);
		this.builder.alterTableNewColumns(this.frameName, headers, types);
		syncHeaders();
		this.originalName = this.frameName;
	}
	
	public AbstractRdbmsFrame(String[] headers, String[] types) {
		this();
		ImportUtility.parseHeadersAndTypeIntoMeta(this, headers, types, this.frameName);
		this.builder.alterTableNewColumns(this.frameName, headers, types);
		syncHeaders();
		this.originalName = this.frameName;
	}
	
	/**
	 * This method needs to define the following class variables:
	 * Connection conn
	 * String schema
	 * AbstractSqlQueryUtil util
	 * RdbmsFrameBuilder builder
	 * This is necessary for this class and its subclasses to work
	 * @throws Exception
	 */
	protected abstract void initConnAndBuilder() throws Exception;
	
	//////////////////////////////////////////////////////////////////////////////////////////////////////
	
	public AbstractSqlQueryUtil getQueryUtil() {
		return this.util;
	}
	
	public RdbmsFrameBuilder getBuilder() {
		return this.builder;
	}
	
	public Connection getConn() {
		return this.conn;
	}
	
	///////////////////////////////////////////////////////////////////////////////////////////////////////
	
	/**
	 * Add iterator data into table
	 * Assuming this is the base frame table
	 * @param it
	 * @param typesMap
	 */
	public void addRowsViaIterator(Iterator<IHeadersDataRow> it, Map<String, SemossDataType> typesMap) {
		addRowsViaIterator(it, this.frameName, typesMap);
	}
	
	/**
	 * Add iterator data into the specified table
	 * @param it
	 * @param tableName
	 * @param typesMap
	 */
	public void addRowsViaIterator(Iterator<IHeadersDataRow> it, String tableName, Map<String, SemossDataType> typesMap) {
		this.builder.addRowsViaIterator(it, tableName, typesMap);
	}
	
	@Override
	public void addRow(Object[] values, String[] columnNames) {
		String[] types = new String[columnNames.length];
		for (int i = 0; i < types.length; i++) {
			types[i] = this.metaData.getHeaderTypeAsString(columnNames[i], this.frameName);
			if(types[i] == null) {
				types[i] = "STRING";
			}
		}

		// get table for headers
		this.addRow(this.frameName, columnNames, values, types);
	}

	/**
	 * Add a row into a table
	 * @param tableName
	 * @param columnNames
	 * @param values
	 * @param types
	 */
	public void addRow(String tableName, String[] columnNames, Object[] values, String[] types) {
		this.builder.addRow(tableName, columnNames, values, types);
	}
	
	public void addNewColumn(String[] newHeaders, String[] types, String tableName) {
		this.builder.alterTableNewColumns(tableName, newHeaders, types);
		for(int i = 0; i < newHeaders.length; i++) {
			this.metaData.addProperty(tableName, tableName + "__" + newHeaders[i]);
			this.metaData.setAliasToProperty(tableName + "__" + newHeaders[i], newHeaders[i]);
			this.metaData.setDataTypeToProperty(tableName + "__" + newHeaders[i], types[i]);
		}
	}
	
	@Override
	public void removeColumn(String columnHeader) {
		if(this.util.allowDropColumn()) {
			String dropColumnSql = this.util.alterTableDropColumn(this.frameName, columnHeader);
			try {
				this.builder.runQuery(dropColumnSql);
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
			}
		} else {
			// TODO: make new table not including this column and insert from table
			// TODO: make new table not including this column and insert from table
			// TODO: make new table not including this column and insert from table
		}
		
		this.metaData.dropProperty(this.frameName + "__" + columnHeader, this.frameName);
		syncHeaders();
	}

	public PreparedStatement createUpdatePreparedStatement(String[] columnsToUpdate, String[] whereColumns) {
		return this.builder.createUpdatePreparedStatement(this.frameName, columnsToUpdate, whereColumns);
	}
	
	public PreparedStatement createInsertPreparedStatement(String[] columns) {
		return this.builder.createInsertPreparedStatement(this.frameName, columns);
	}

	@Override
	public boolean isEmpty() {
		return this.builder.isEmpty(this.frameName);
	}

	@Override
	public long size(String tableName) {
		if(isEmpty()) {
			return 0;
		}
		return this.builder.getNumRecords(tableName);
	}

	@Override
	public IRawSelectWrapper query(String query) throws Exception {
		logger.info("Executing query...");
		long start = System.currentTimeMillis();
		RawRDBMSSelectWrapper it = RawRDBMSSelectWrapper.directExecutionViaConnection(this.conn, query, false);
		long end = System.currentTimeMillis();
		logger.info("Time to execute query on frame = " + (end-start) + "ms");
		return it;
	}
	
	@Override
	public IRawSelectWrapper query(SelectQueryStruct qs) throws Exception {
		logger.info("Generating SQL query...");
		qs = QSAliasToPhysicalConverter.getPhysicalQs(qs, this.metaData);
		if(!this.frameName.equals(this.originalName)) {
			Map<String, String> transformation = new HashMap<>();
			transformation.put(originalName, frameName);
			qs = QSRenameTableConverter.convertQs(qs, transformation, true);
		}
		IQueryInterpreter interp = getQueryInterpreter();
		interp.setQueryStruct(qs);
		interp.setLogger(this.logger);
		String iteratorQuery = interp.composeQuery();
		logger.info("Done generating SQL query");
		return query(iteratorQuery);
	}
	
	@Override
	public Object querySQL(String query) {
		Map<String, Object> retMap = new HashMap<>();
		List <List<Object>> data = new ArrayList<List<Object>>();
		
		HardSelectQueryStruct qs = new HardSelectQueryStruct();
		qs.setQuery(query);
		IRawSelectWrapper it = null;
		try {
			it = query(qs);
			while(it.hasNext()) {
				data.add( Arrays.asList(it.next().getValues()) );
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error executing sql: " + query);
		} finally {
			if(it != null) {
				try {
					it.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		retMap.put("data", data);
		retMap.put("types", SemossDataType.convertSemossDataTypeArrToStringArr( it.getTypes()) );
		retMap.put("columns", it.getHeaders());
		return retMap;
	}
	
	@Override
	public void close() {
		super.close();
		try {
			this.conn.close();
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}
	}
	
	@Override
	public DataFrameTypeEnum getFrameType() {
		return DataFrameTypeEnum.GRID;
	}
	
	
	@Override
	public IQueryInterpreter getQueryInterpreter() {
		return getQueryUtil().getInterpreter(this);
	}

	
	/////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////

	/*
	 * Legacy methods... do not require
	 */
	
	@Override
	public void processDataMakerComponent(DataMakerComponent component) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getDataMakerName() {
		return null;
	}

}

package prerna.ds.rdbms;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.shared.AbstractTableDataFrame;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.interpreters.sql.H2SqlInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.rdf.engine.wrappers.RawRDBMSSelectWrapper;
import prerna.sablecc2.reactor.imports.ImportUtility;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.util.sql.AbstractSqlQueryUtil;

public abstract class AbstractRdbmsFrame extends AbstractTableDataFrame {

	protected Connection conn = null;
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
			throw new IllegalArgumentException("Error generating new sql frame", e);
		}
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
	}
	
	public AbstractRdbmsFrame(String[] headers, String[] types) {
		this();
		ImportUtility.parseHeadersAndTypeIntoMeta(this, headers, types, this.frameName);
		this.builder.alterTableNewColumns(this.frameName, headers, types);
		syncHeaders();
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
	
	@Override
	public void removeColumn(String columnHeader) {
		if(this.util.allowDropColumn()) {
			String dropColumnSql = this.util.alterTableDropColumn(this.frameName, columnHeader);
			try {
				this.builder.runQuery(dropColumnSql);
			} catch (Exception e) {
				e.printStackTrace();
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
	public IRawSelectWrapper query(String query) {
		logger.info("Executing query...");
		long start = System.currentTimeMillis();
		RawRDBMSSelectWrapper it = new RawRDBMSSelectWrapper();
		it.directExecutionViaConnection(this.conn, query, false);
		long end = System.currentTimeMillis();
		logger.info("Time to execute query on frame = " + (end-start) + "ms");
		return it;
	}
	
	@Override
	public IRawSelectWrapper query(SelectQueryStruct qs) {
		logger.info("Generating SQL query...");
		qs = QSAliasToPhysicalConverter.getPhysicalQs(qs, this.metaData);
		H2SqlInterpreter interp = new H2SqlInterpreter(this);
		interp.setQueryStruct(qs);
		interp.setLogger(this.logger);
		String iteratorQuery = interp.composeQuery();
		logger.info("Done generating SQL query");
		return query(iteratorQuery);
	}
	
	@Override
	public void close() {
		super.close();
		try {
			this.conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
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

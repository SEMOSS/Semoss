package prerna.engine.impl.modifications;

import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngineModifier;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Constants;
import prerna.util.sql.AbstractSqlQueryUtil;

public class RdbmsModifier implements IEngineModifier {
	
	private static final Logger classLogger = LogManager.getLogger(RdbmsModifier.class);

	private RDBMSNativeEngine engine = null;
	private AbstractSqlQueryUtil queryUtil = null;
	private String database = null;
	private String schmea = null;
	
	@Override
	public void setEngine(IDatabaseEngine engine) {
		this.engine = (RDBMSNativeEngine) engine;
		this.queryUtil = this.engine.getQueryUtil();
		this.database = this.engine.getDatabase();
		this.schmea = this.engine.getSchema();
	}

	@Override
	public void addProperty(String tableName, String newColumn, String dataType) throws Exception {
		if(queryUtil.allowAddColumn()) {
			String sqlQuery = queryUtil.alterTableAddColumn(tableName, newColumn, dataType);
			try {
				this.engine.insertData(sqlQuery);
			} catch (SQLException e) {
				classLogger.error(e.getMessage());
				classLogger.error(Constants.STACKTRACE, e);	
				throw new SQLException("Error occurred to alter the table. See logs for details.");
			}
		} else {
			// we need to make a new temp table with the new column we are adding
			// we need to insert all the old values of the existing table
			// we need to drop the existing table (we will rename it in case the last operation fails and only drop at end)
			// we need to rename the temp table to the existing table name
			
			
		}
	}
	
	@Override
	public void editProperty(String tableName, String newColumn, String newDataType) throws Exception {
		if(queryUtil.allowAddColumn()) {
			String sqlQuery = queryUtil.modColumnType(tableName, newColumn, newDataType);
			try {
				this.engine.insertData(sqlQuery);
			} catch (SQLException e) {
				classLogger.error(e.getMessage());
				classLogger.error(Constants.STACKTRACE, e);	
				throw new SQLException("Error occurred to alter the table. See logs for details.");
			}
		} else {
			// we need to make a new temp table with the new column we are adding
			// we need to insert all the old values of the existing table
			// we need to drop the existing table (we will rename it in case the last operation fails and only drop at end)
			// we need to rename the temp table to the existing table name
			
			
		}
	}
	

	@Override
	public void removeProperty(String tableName, String columnName) throws Exception {
		if(queryUtil.allowDropColumn()) {
			String sqlQuery = queryUtil.alterTableDropColumn(tableName, columnName);
			try {
				this.engine.insertData(sqlQuery);
			} catch (SQLException e) {
				classLogger.error(e.getMessage());
				classLogger.error(Constants.STACKTRACE, e);	
				throw new SQLException("Error occurred to alter the table. See logs for details.");
			}
		} else {
			// we need to make a new temp table without the column we are dropping
			// we need to insert all the old values of the existing table
			// we need to drop the existing table (we will rename it in case the last operation fails and only drop at end)
			// we need to rename the temp table to the existing table name
			
			
		}
	}

	@Override
	public void addIndex(String tableName, String columnName, String indexName, boolean addIfExists) throws Exception {
		String indexQuery = null;
		if(addIfExists) {
			indexQuery = queryUtil.createIndex(indexName, tableName, columnName);
		} else {
			// first need to check if there is an existing index on the column
			String existingIndicesQuery = queryUtil.allIndexForTableQuery(tableName, this.database, this.schmea);
			IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, existingIndicesQuery);
			while(wrapper.hasNext()) {
				Object[] values = wrapper.next().getValues();
				if(values[0].toString().equalsIgnoreCase(indexName)) {
					throw new IllegalArgumentException("The index name defined already exists");
				}
				if(values[1].toString().equalsIgnoreCase(tableName)) {
					throw new IllegalArgumentException("Index already exists on the column with the name '" + values[1] + "'");
				}
			}
			// we need to see if the index already exists or not
			indexQuery = queryUtil.createIndex(indexName, tableName, columnName);
		}
		
		try {
			this.engine.insertData(indexQuery);
		} catch (SQLException e) {
			classLogger.error(e.getMessage());
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Could not add index. See logs for details.");
		}
	}

	@Override
	public void renameProperty(String existingConcept, String existingColumn, String newColumn) throws Exception {
		String query = queryUtil.modColumnName(existingConcept, existingColumn, newColumn);
		try {
			this.engine.insertData(query);
		} catch (SQLException e) {
			classLogger.error(e.getMessage());
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Could rename property. See logs for details.");
		}		
		
	}

	@Override
	public void renameConcept(String existingConcept, String newConcept) throws Exception {
		String query = queryUtil.alterTableName(existingConcept, newConcept);
		try {
			this.engine.insertData(query);
		} catch (SQLException e) {
			classLogger.error(e.getMessage());
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Could not alter table name. See logs for details.");
		}
	}
	
	
}

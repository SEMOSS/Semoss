package prerna.reactor.export;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.date.SemossDate;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.reactor.AbstractReactor;
import prerna.reactor.database.upload.rdbms.RdbmsUploadReactorUtility;
import prerna.reactor.task.TaskBuilderReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.EngineSyncUtility;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class ToDatabaseReactor extends TaskBuilderReactor {

	private static final Logger classLogger = LogManager.getLogger(ToDatabaseReactor.class);

	private static final String CLASS_NAME = ToDatabaseReactor.class.getName();

	private static final String TARGET_DATABASE = "targetDatabase";
	private static final String TARGET_TABLE = "targetTable";
	private static final String INSERT_ID_KEY = "insertId";
	private static final String IGNORE_OWL = "ignoreOWL";

	private String engineId = null;
	private String targetTable = null;
	private boolean override = false;
	private boolean newTable = false;
	private boolean genId = false;
	private boolean ignoreOWL = false;
	
	public ToDatabaseReactor() {
		this.keysToGet = new String[]{ 
				ReactorKeysEnum.TASK.getKey(), 
				TARGET_DATABASE, 
				TARGET_TABLE, 
				ReactorKeysEnum.OVERRIDE.getKey(), 
				INSERT_ID_KEY,
				IGNORE_OWL
				};
	}

	@Override
	public NounMetadata execute() {
		try {
			User user = this.insight.getUser();
			// throw error is user doesn't have rights to export data
			if(AbstractSecurityUtils.adminSetExporter() && !SecurityQueryUtils.userIsExporter(user)) {
				AbstractReactor.throwUserNotExporterError();
			}
			
			organizeKeys();
			this.task = getTask();
			
			this.engineId = this.keyValue.get(TARGET_DATABASE);
			this.engineId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), this.engineId);
			if(!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), this.engineId)) {
				throw new IllegalArgumentException("Database " + this.engineId + " does not exist or user does not have edit access to the app");
			}
			
			this.targetTable = this.keyValue.get(TARGET_TABLE);
			// clean up the table name
			this.targetTable = Utility.makeAlphaNumeric(this.targetTable);
			this.override = Boolean.parseBoolean( this.keyValue.get(ReactorKeysEnum.OVERRIDE.getKey())+"" );
			// checks if targetTable doesn't exist in the engine
			this.newTable = !MasterDatabaseUtility.getConceptsWithinDatabaseRDBMS(this.engineId).contains(this.targetTable);
			// boolean check if a unique id will be generated
			this.genId = Boolean.parseBoolean( this.keyValue.get(INSERT_ID_KEY)+"" );
			this.ignoreOWL = Boolean.parseBoolean( this.keyValue.get(IGNORE_OWL)+"" );

			if(this.newTable && this.override) {
				// you cannot override a table that doesn't exist
				throw new SemossPixelException(
						new NounMetadata("Table " + this.targetTable + " cannot be found to override. Please enter an existing table name or turn override to false",
								PixelDataType.CONST_STRING));
			}
		
			buildTask();
		} catch(Exception e) {
			// clean up
			throw e;
		} finally {
			if(this.task != null) {
				try {
					task.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		ClusterUtil.pushEngine(getEngineId());
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.MARKET_PLACE_ADDITION, PixelOperationType.FORCE_SAVE_DATA_EXPORT);
	}

	@Override
	protected void buildTask() {
		Logger logger = this.getLogger(CLASS_NAME);

		logger.info("Persisting data into table = " + this.targetTable);
		logger.info("Replace existing table data = " + this.override);

		// grab the engine
		IDatabaseEngine targetDatabase = Utility.getDatabase(engineId);
		// only for RDBMS right now
		if(!(targetDatabase instanceof IRDBMSEngine)) {
			throw new SemossPixelException(new NounMetadata("Can only persist data to a relational database at the moment", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
		}
		IRDBMSEngine targetEngine = (IRDBMSEngine) targetDatabase;
		AbstractSqlQueryUtil queryUtil = targetEngine.getQueryUtil();
		Map<String, String> typeConversionMap = queryUtil.getTypeConversionMap();
		// create prepared statement of all inserts from task
		List<Map<String, Object>> headerInfo = this.task.getHeaderInfo();
		
		int size = headerInfo.size();
		int targetSize;
		String[] headers;
		SemossDataType[] types = new SemossDataType[size];
		String[] sqlTypes;
		// if we generate an id need to add an id row to schema
		if(this.genId){
			headers = new String[size + 1];		
			sqlTypes = new String[size + 1];
			targetSize = size + 1;
			if(this.override || this.newTable){
				headers[0] = targetTable + RdbmsUploadReactorUtility.UNIQUE_ROW_ID;
			} else {
				headers[0] = targetTable;
			}
			sqlTypes[0] = "IDENTITY";
			for(int i = 0; i < size; i++) {
				Map<String, Object> hMap = headerInfo.get(i);
				headers[i + 1] = (String) hMap.get("alias");
				types[i] = SemossDataType.convertStringToDataType((String) hMap.get("dataType"));
				sqlTypes[i + 1] = typeConversionMap.get(types[i].toString());
			}
		} else {
			headers = new String[size];		
			sqlTypes = new String[size];
			targetSize = size;
			
			for(int i = 0; i < size; i++) {
				Map<String, Object> hMap = headerInfo.get(i);
				headers[i] = (String) hMap.get("alias");
				types[i] = SemossDataType.convertStringToDataType((String) hMap.get("dataType"));
				sqlTypes[i] = typeConversionMap.get(types[i].toString());
			}
		}

		
		// check if overriding to an existing table
		// that all the columns are there
		if(this.override) {
			// just update the metadata in case columns have changed
			logger.info("Deleting existing table data and creating new table");

			try {
				if(queryUtil.allowsIfExistsTableSyntax()) {
					targetEngine.insertData(queryUtil.dropTableIfExists(this.targetTable));
				} else {
					targetEngine.insertData(queryUtil.dropTable(this.targetTable));
				}
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new SemossPixelException(new NounMetadata("Error occurred trying to delete the existing table with message = " + e.getMessage(), PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			}
			
			try {
				targetEngine.insertData(queryUtil.createTable(this.targetTable, headers, sqlTypes));
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new SemossPixelException(new NounMetadata("Error occurred trying to create the new table with message = " + e.getMessage(), PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			}
			
			logger.info("Finished deleting existing table data and creating new table");
		// else if newTable create the table
		} else if(this.newTable) {
			// create the table
			logger.info("Creating new table ");
			try {
				targetEngine.insertData(queryUtil.createTable(this.targetTable, headers, sqlTypes));
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new SemossPixelException(new NounMetadata("Error occurred trying to create the new table with message = " + e.getMessage(), PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			}
			logger.info("Finished creating new table");
		} else {
			// # of columns must match
			List<String> cols = MasterDatabaseUtility.getSpecificConceptProperties(targetTable, engineId);
			if(cols.size() != size) {
				throw new SemossPixelException(new NounMetadata("Header size does not match for existing table. Please create a new table or have override=true", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			}
		}
		
		Object[] getPreparedStatementArgs;
		// prepared statements differ if we generate id
		if(this.genId){
			getPreparedStatementArgs = new Object[headers.length];
			getPreparedStatementArgs[0] = targetTable;
			for (int headerIndex = 1; headerIndex < headers.length; headerIndex++) {
				getPreparedStatementArgs[headerIndex] = headers[headerIndex];
			}
		} else {
			getPreparedStatementArgs = new Object[headers.length + 1];
			getPreparedStatementArgs[0] = targetTable;
			for (int headerIndex = 0; headerIndex < headers.length; headerIndex++) {
				getPreparedStatementArgs[headerIndex + 1] = headers[headerIndex];
			}
		}
		PreparedStatement ps = null;
		try {
			ps = targetEngine.bulkInsertPreparedStatement(getPreparedStatementArgs);
		} catch (SQLException e) {
			throw new IllegalArgumentException("An error occurred creating the prepared statement with message = " + e.getMessage());
		}
		try {
			// keep a batch size so we dont get heapspace
			final int batchSize = 5000;
			int count = 0;
	
			logger.info("Begin inserting new data");
			// we loop through every row of task result
			while (this.task.hasNext()) {
				Object[] nextRow = this.task.next().getValues();
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
								ps.setNull(colIndex + 1, java.sql.Types.INTEGER);
							}
						}
					} else if(type == SemossDataType.DOUBLE) {
						if(nextRow[colIndex] instanceof Number) {
							double d = ((Number) nextRow[colIndex]).doubleValue();
							if(Double.isFinite(d)) {
								ps.setDouble(colIndex + 1, d);
							} else {
								ps.setNull(colIndex + 1, java.sql.Types.DOUBLE);
							}
						} else {
							Double value = Utility.getDouble(nextRow[colIndex] + "");
							if (value != null && Double.isFinite(value)) {
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
							SemossDate value = SemossDate.genDateObj(nextRow[colIndex] + "");
							if (value != null) {
								ps.setTimestamp(colIndex + 1, new java.sql.Timestamp(value.getDate().getTime()));
							} else {
								ps.setNull(colIndex + 1, java.sql.Types.TIMESTAMP);
							}
						}
					} else if (type == SemossDataType.BOOLEAN) {
						if(nextRow[colIndex] == null) {
							ps.setNull(colIndex + 1, java.sql.Types.BOOLEAN);
						} else {
							Boolean var = Boolean.parseBoolean(nextRow[colIndex] + "");
							ps.setBoolean(colIndex + 1, var);
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

			// well, we are done looping through now
			logger.info("Executing final batch .... row num = " + count);
			ps.executeBatch(); // insert any remaining records
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred persisting data into the database with message = " + e.getMessage());
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
				if(targetEngine.isConnectionPooling()) {
					try {
						ps.getConnection().close();
					} catch (SQLException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
		
		if(this.ignoreOWL) {
			logger.info("Ignoring any OWL modifications");
		} else {
			// if it is a new table
			// this is easy
			// just add everything
			if(this.override || this.newTable) {
				long start = System.currentTimeMillis();
				logger.info("Start to add the new exisitng concept from the OWL");
				try (WriteOWLEngine owlEngine = targetEngine.getOWLEngineFactory().getWriteOWL()){
					ClusterUtil.pullOwl(engineId, owlEngine);
	
					// if we are overwriting the existing value
					// we need to grab the current concept/columns
					// and we need to delete them
					// then do the add new table logic
					if(this.override) {
						long start2 = System.currentTimeMillis();
						logger.info("Need to first remove the exisitng concept from the OWL");
						owlEngine.removeConcept(targetTable);
						long end2 = System.currentTimeMillis();
						logger.info("Finished removing concept from the OWL. Total time = "+((end2-start2)/1000)+" seconds");
					}
					// choose the first column as the prim key
					owlEngine.addConcept(targetTable, null, null);
					owlEngine.addProp(targetTable, headers[0], sqlTypes[0]);
					// add all others as properties
					for(int i = 1; i < targetSize; i++) {
						owlEngine.addProp(targetTable, headers[i], sqlTypes[i], null);
					}
					
					try {
						logger.info("Persisting engine metadata and synchronizing with local master");
						owlEngine.export();
						Utility.synchronizeEngineMetadata(engineId);
						// also push to cloud
						ClusterUtil.pushOwl(engineId, owlEngine);
						EngineSyncUtility.clearEngineCache(this.engineId);
						logger.info("Finished persisting engine metadata and synchronizing with local master");
					} catch (IOException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
	
				} catch (InterruptedException | IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
				long end = System.currentTimeMillis();
				logger.info("Finished adding concept to the OWL. Total time = "+((end-start)/1000)+" seconds");
			}
		}
	}

	///////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////

	/**
	 * 
	 * @return
	 */
	private String getEngineId() {
		GenRowStruct grs = this.store.getNoun(TARGET_DATABASE);
		if(grs != null && !grs.isEmpty()) {
			NounMetadata noun = grs.getNoun(0);
			if(noun.getNounType() == PixelDataType.UPLOAD_RETURN_MAP) {
				Map<String, Object> uploadMap = (Map<String, Object>) noun.getValue();
				return uploadMap.get("app_id").toString();
			}
			return noun.getValue().toString();
		}
		
		List<String> strInput = this.curRow.getAllStrValues();
		if(strInput != null && !strInput.isEmpty()) {
			return strInput.get(0);
		}
		
		throw new IllegalArgumentException("Must define the engine to persist the data");
	}
	
}


package prerna.sablecc2.reactor.export;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAppUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.date.SemossDate;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ACTION_TYPE;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.app.metaeditor.concepts.RemoveOwlConceptReactor;
import prerna.sablecc2.reactor.task.TaskBuilderReactor;
import prerna.util.OWLER;
import prerna.util.Utility;

public class ToDatabaseReactor extends TaskBuilderReactor {

	private static final String CLASS_NAME = ToDatabaseReactor.class.getName();

	private static final String TARGET_DATABASE = "targetDatabase";
	private static final String TARGET_TABLE = "targetTable";
	private static final String INSERT_ID_KEY = "insertId";

	protected static Map<SemossDataType, String> typeConversionMap = new HashMap<SemossDataType, String>();
	static {
		typeConversionMap.put(SemossDataType.INT, "INT");
		typeConversionMap.put(SemossDataType.DOUBLE, "DOUBLE");
		typeConversionMap.put(SemossDataType.BOOLEAN, "BOOLEAN");
		typeConversionMap.put(SemossDataType.DATE, "DATE");
		typeConversionMap.put(SemossDataType.TIMESTAMP, "TIMESTAMP");
		typeConversionMap.put(SemossDataType.STRING, "VARCHAR(800)");
	}
	
	private String engineId = null;
	private String targetTable = null;
	private boolean override = false;
	private boolean newTable = false;
	private boolean genId = false;
	
	public ToDatabaseReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.TASK.getKey(), TARGET_DATABASE, TARGET_TABLE, ReactorKeysEnum.OVERRIDE.getKey(), INSERT_ID_KEY};
	}

	@Override
	public NounMetadata execute() {
		this.task = getTask();
		
		this.engineId = getEngineId();
		if(AbstractSecurityUtils.securityEnabled()) {
			this.engineId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), this.engineId);
			if(!SecurityAppUtils.userCanEditEngine(this.insight.getUser(), this.engineId)) {
				throw new IllegalArgumentException("Database " + this.engineId + " does not exist or user does not have edit access to the app");
			}
		} else {
			this.engineId = MasterDatabaseUtility.testEngineIdIfAlias(this.engineId);
			if(!MasterDatabaseUtility.getAllEngineIds().contains(this.engineId)) {
				throw new IllegalArgumentException("Database " + this.engineId + " does not exist");
			}
		}
		
		this.targetTable = getTargetTable();
		// clean up the table name
		this.targetTable = Utility.makeAlphaNumeric(this.targetTable);
		this.override = getOverride();
		// checks if targetTable doesn't exist in the engine
		this.newTable = !MasterDatabaseUtility.getConceptsWithinEngineRDBMS(this.engineId).contains(this.targetTable);
		// boolean check if a unique id will be generated
		this.genId = getInsertKey();
		if(this.newTable && this.override) {
			// you cannot override a table that doesn't exist
			throw new SemossPixelException(
					new NounMetadata("Table " + this.newTable + " cannot be found to override. Please enter an existing table name or turn override to false",
							PixelDataType.CONST_STRING));
		}
		
		try {
			buildTask();
		} catch(Exception e) {
			// clean up
			throw e;
		}
		ClusterUtil.reactorPushApp(getEngineId());
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.MARKET_PLACE_ADDITION);
	}

	@Override
	protected void buildTask() {
		Logger logger = this.getLogger(CLASS_NAME);

		logger.info("Persisting data into table = " + this.targetTable);
		logger.info("Replace existing table data = " + this.override);

		// grab the engine
		IEngine targetEngine = Utility.getEngine(engineId);

		// only for RDBMS right now
		if(!(targetEngine instanceof RDBMSNativeEngine)) {
			throw new SemossPixelException(new NounMetadata("Can only persist data to a relational database at the moment", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
		}

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
				headers[0] = targetTable + "_UNIQUE_ROW";
			} else {
				headers[0] = targetTable;
			}
			sqlTypes[0] = "IDENTITY";
			for(int i = 0; i < size; i++) {
				Map<String, Object> hMap = headerInfo.get(i);
				headers[i + 1] = (String) hMap.get("alias");
				types[i] = SemossDataType.convertStringToDataType((String) hMap.get("type"));
				sqlTypes[i + 1] = typeConversionMap.get(types[i]);
			}
		} else {
			headers = new String[size];		
			sqlTypes = new String[size];
			targetSize = size;
			
			for(int i = 0; i < size; i++) {
				Map<String, Object> hMap = headerInfo.get(i);
				headers[i] = (String) hMap.get("alias");
				types[i] = SemossDataType.convertStringToDataType((String) hMap.get("type"));
				sqlTypes[i] = typeConversionMap.get(types[i]);
			}
		}

		
		// check if overriding to an existing table
		// that all the columns are there
		if(this.override) {
			// just update the metadata in case columns have changed
			logger.info("Deleting existing table data and creating new table");

			String delete = "DROP TABLE IF EXISTS " + this.targetTable + ";";
			String create = RdbmsQueryBuilder.makeCreate(this.targetTable, headers, sqlTypes);
			try {
				targetEngine.insertData(delete);
				targetEngine.insertData(create);
			} catch (Exception e) {
				e.printStackTrace();
				throw new SemossPixelException(new NounMetadata("Error occured trying to delete and create the new table", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			}
			
			logger.info("Finished deleting existing table data and creating new table");
		// else if newTable create the table
		} else if(this.newTable) {
			// create the table
			logger.info("Creating new table ");
			String create = RdbmsQueryBuilder.makeCreate(targetTable, headers, sqlTypes);
			try {
				targetEngine.insertData(create);
			} catch (Exception e) {
				e.printStackTrace();
				throw new SemossPixelException(new NounMetadata("Error occured trying to create the new table", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			}
			logger.info("Finished creating new table");
		} else {
			// # of columns must match
			List<String> cols = MasterDatabaseUtility.getSpecificConceptPropertiesRDBMS(targetTable, engineId);
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
		PreparedStatement ps = (PreparedStatement) targetEngine.doAction(ACTION_TYPE.BULK_INSERT, getPreparedStatementArgs);

		// keep a batch size so we dont get heapspace
		final int batchSize = 5000;
		int count = 0;

		logger.info("Begin inserting new data");
		// we loop through every row of task result
		try {
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
							SemossDate value = SemossDate.genDateObj(nextRow[colIndex] + "");
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

			// well, we are done looping through now
			logger.info("Executing final batch .... row num = " + count);
			ps.executeBatch(); // insert any remaining records
			ps.close();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("An error occured persisting data into the database");
		}
		
		
		// if we are overwriting the existing value
		// we need to grab the current concept/columns
		// and we need to delete them
		// then do the add new table logic
		if(this.override) {
			// i will just do the delete portion here
			// since the add is the same in either case
			RemoveOwlConceptReactor remover = new RemoveOwlConceptReactor();
			remover.getNounStore().makeNoun(ReactorKeysEnum.APP.getKey()).addLiteral(this.engineId);
			remover.getNounStore().makeNoun(ReactorKeysEnum.CONCEPT.getKey()).addLiteral(this.targetTable);
			remover.setInsight(this.insight);
			remover.execute();
		}
		
		// if it is a new table
		// this is easy
		// just add everything
		if(this.override || this.newTable) {
			logger.info("Need to update the engine metadata for the new table");
			OWLER owler = new OWLER(targetEngine, targetEngine.getOWL());
			// choose the first column as the prim key
			owler.addConcept(targetTable, headers[0], sqlTypes[0]);
			// add all others as properties
			for(int i = 1; i < targetSize; i++) {
				owler.addProp(targetTable, headers[0], headers[i], sqlTypes[i], null);
			}
			
			logger.info("Persisting engine metadata and synchronizing with local master");
			try {
				owler.export();
				Utility.synchronizeEngineMetadata(engineId);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	///////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////

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
		
		throw new IllegalArgumentException("Must define the app to persist the data");
	}
	
	private String getTargetTable() {
		GenRowStruct grs = this.store.getNoun(TARGET_TABLE);
		if(grs != null && !grs.isEmpty()) {
			NounMetadata noun = grs.getNoun(0);
			return noun.getValue().toString();
		}
		
		// or it is the second string input
		List<String> strInput = this.curRow.getAllStrValues();
		if(strInput != null && !strInput.isEmpty()) {
			return strInput.get(1);
		}
		
		throw new IllegalArgumentException("Must define the table to persist the data into");
	}
	
	private boolean getOverride() {
		GenRowStruct boolGrs = this.store.getNoun(this.keysToGet[3]);
		if(boolGrs != null) {
			if(boolGrs.size() > 0) {
				List<Object> val = boolGrs.getValuesOfType(PixelDataType.BOOLEAN);
				return (boolean) val.get(0);
			}
		}
		
		List<NounMetadata> booleanInput = this.curRow.getNounsOfType(PixelDataType.BOOLEAN);
		if(booleanInput != null && !booleanInput.isEmpty()) {
			return (boolean) booleanInput.get(0).getValue();
		}
		
		return false;
	}
	
	private boolean getInsertKey() {
		GenRowStruct boolGrs = this.store.getNoun(this.keysToGet[4]);
		if(boolGrs != null) {
			if(boolGrs.size() > 0) {
				List<Object> val = boolGrs.getValuesOfType(PixelDataType.BOOLEAN);
				return (boolean) val.get(0);
			}
		}
		
		List<NounMetadata> booleanInput = this.curRow.getNounsOfType(PixelDataType.BOOLEAN);
		if(booleanInput != null && !booleanInput.isEmpty()) {
			return (boolean) booleanInput.get(0).getValue();
		}
		
		return false;
	}
}




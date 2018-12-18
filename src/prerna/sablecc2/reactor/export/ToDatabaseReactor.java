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
import prerna.auth.utils.SecurityQueryUtils;
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
import prerna.sablecc2.reactor.task.TaskBuilderReactor;
import prerna.util.OWLER;
import prerna.util.Utility;

public class ToDatabaseReactor extends TaskBuilderReactor {

	private static final String CLASS_NAME = ToDatabaseReactor.class.getName();

	private static final String TARGET_DATABASE = "targetDatabase";
	private static final String TARGET_TABLE = "targetTable";

	protected static Map<SemossDataType, String> typeConversionMap = new HashMap<SemossDataType, String>();
	static {
		typeConversionMap.put(SemossDataType.INT, "INT");
		typeConversionMap.put(SemossDataType.DOUBLE, "DOUBLE");
		typeConversionMap.put(SemossDataType.BOOLEAN, "BOOLEAN");
		typeConversionMap.put(SemossDataType.DATE, "DATE");
		typeConversionMap.put(SemossDataType.TIMESTAMP, "TIMESTAMP");
		typeConversionMap.put(SemossDataType.STRING, "VARCHAR(800)");
	}

	public ToDatabaseReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.TASK.getKey(), TARGET_DATABASE, TARGET_TABLE, ReactorKeysEnum.OVERRIDE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		this.task = getTask();
		buildTask();
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.MARKET_PLACE_ADDITION);
	}

	@Override
	protected void buildTask() {
		Logger logger = this.getLogger(CLASS_NAME);

		// first, make sure we have proper permissions
		String engineId = getEngineId();
		if(AbstractSecurityUtils.securityEnabled()) {
			engineId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), engineId);
			if(!SecurityQueryUtils.userCanEditEngine(this.insight.getUser(), engineId)) {
				throw new IllegalArgumentException("Database " + engineId + " does not exist or user does not have edit access to the app");
			}
		} else {
			engineId = MasterDatabaseUtility.testEngineIdIfAlias(engineId);
			if(!MasterDatabaseUtility.getAllEngineIds().contains(engineId)) {
				throw new IllegalArgumentException("Database " + engineId + " does not exist");
			}
		}

		String targetTable = getTargetTable();
		boolean overWrite = getOverWrite();
		
		logger.info("Persisting data into table = " + targetTable);
		logger.info("Replace existing table data = " + overWrite);

		// grab the engine
		IEngine targetEngine = Utility.getEngine(engineId);

		// only for RDBMS right now
		if(!(targetEngine instanceof RDBMSNativeEngine)) {
			throw new SemossPixelException(new NounMetadata("Can only persist data to a relational database at the moment", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
		}

		// create prepared statement of all inserts from task
		List<Map<String, Object>> headerInfo = this.task.getHeaderInfo();
		int size = headerInfo.size();
		String[] headers = new String[size];
		SemossDataType[] types = new SemossDataType[size];
		String[] sqlTypes = new String[size];
		for(int i = 0; i < size; i++) {
			Map<String, Object> hMap = headerInfo.get(i);
			headers[i] = (String) hMap.get("alias");
			types[i] = SemossDataType.convertStringToDataType((String) hMap.get("type"));
			sqlTypes[i] = typeConversionMap.get(types[i]);
		}
		
		// check if adding to an existing table
		// that all the columns are there
		boolean newTable = !MasterDatabaseUtility.getConceptsWithinEngineRDBMS(engineId).contains(targetTable);
		if(overWrite) {
			// just update the metadata in case columns have changed
			newTable = true;
			logger.info("Deleting existing table data and creating new table");

			String delete = "DROP TABLE IF EXISTS " + targetTable + ";";
			String create = RdbmsQueryBuilder.makeCreate(targetTable, headers, sqlTypes);
			try {
				targetEngine.insertData(delete);
				targetEngine.insertData(create);
			} catch (Exception e) {
				e.printStackTrace();
				throw new SemossPixelException(new NounMetadata("Error occured trying to delete and create the new table", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			}
			
			logger.info("Finished deleting existing table data and creating new table");
		} else if (newTable) {
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
				throw new SemossPixelException(new NounMetadata("Header size does not match for existing table. Please create a new table or have overwrite=true", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			}
		}

		// delete table if it exists and overwrite is true
		if (!newTable && overWrite) {
			logger.info("Deleting existing table data");

			String delete = "DROP TABLE IF EXISTS " + targetTable + ";";
			String create = RdbmsQueryBuilder.makeCreate(targetTable, headers, sqlTypes);
			try {
				targetEngine.insertData(delete);
				targetEngine.insertData(create);
			} catch (Exception e) {
				e.printStackTrace();
				throw new SemossPixelException(new NounMetadata("Error occured trying to delete and create the new table", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			}
			
			logger.info("Finished deleting existing table data");
		}

		Object[] getPreparedStatementArgs = new Object[headers.length + 1];
		getPreparedStatementArgs[0] = targetTable;
		for (int headerIndex = 0; headerIndex < headers.length; headerIndex++) {
			getPreparedStatementArgs[headerIndex + 1] = headers[headerIndex];
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
		
		// need to reload the metadata of this app in case the table is new
		if(newTable) {
			logger.info("Need to update the engine metadata for the new table");
			OWLER owler = new OWLER(targetEngine, targetEngine.getOWL());
			// choose the first column as the prim key
			owler.addConcept(targetTable, headers[0], sqlTypes[0]);
			// add all others as properties
			for(int i = 1; i < size; i++) {
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
	
	private boolean getOverWrite() {
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
}
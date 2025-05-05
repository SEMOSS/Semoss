	package prerna.reactor.storage;
	
	import java.io.File;
	import java.nio.file.Files;
	import java.nio.file.Paths;
	import java.util.Date;
	import java.util.Map;
	import java.io.Reader;
	import java.util.List;
	import java.text.SimpleDateFormat;
	import java.text.ParseException;
	import java.sql.Timestamp;
	
	
	import org.apache.commons.csv.CSVFormat;
	import org.apache.commons.csv.CSVParser;
	import org.apache.commons.csv.CSVRecord;
	import org.apache.logging.log4j.LogManager;
	import org.apache.logging.log4j.Logger;
	
	import prerna.auth.utils.SecurityAdminUtils;
	import prerna.auth.utils.SecurityEngineUtils;
	import prerna.engine.api.IDatabaseEngine;
	import prerna.engine.api.IStorageEngine;
	import prerna.sablecc2.om.GenRowStruct;
	import prerna.engine.impl.rdbms.RDBMSNativeEngine;
	import prerna.reactor.AbstractReactor;
	import prerna.sablecc2.om.PixelDataType;
	import prerna.sablecc2.om.ReactorKeysEnum;
	import prerna.sablecc2.om.nounmeta.NounMetadata;
	import prerna.util.Constants;
	import prerna.util.UploadInputUtility;
	import prerna.util.Utility;
	import prerna.util.sql.AbstractSqlQueryUtil;
	
	public class StorageToDatabaseReactor extends AbstractReactor {
		private static final Logger classLogger = LogManager.getLogger(StorageToDatabaseReactor.class);
		
		public StorageToDatabaseReactor() {
			this.keysToGet = new String[] {ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.TABLE.getKey(), 
					ReactorKeysEnum.STORAGE.getKey(), ReactorKeysEnum.STORAGE_PATH.getKey()};
		}
	
		@Override
		public NounMetadata execute() {
			organizeKeys();
	
			String databaseId = this.keyValue.get(ReactorKeysEnum.DATABASE.getKey());
			IDatabaseEngine database = Utility.getDatabase(databaseId);
			if(!SecurityAdminUtils.userIsAdmin(this.insight.getUser())) {
				if (!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), databaseId)) {
					throw new IllegalArgumentException("User does not have permission to reload the database");
				}
			}
	
			if(!(database instanceof RDBMSNativeEngine)) {
				throw new IllegalArgumentException("Database must be an RDBMS native engine");
			}
	
			IStorageEngine storage = getStorage();
			String storagePath = this.keyValue.get(ReactorKeysEnum.STORAGE_PATH.getKey());
			String fileLocation = Utility.normalizePath(UploadInputUtility.getFilePath(this.store, this.insight));
			if(!(new File(fileLocation).isDirectory())) {
				new File(fileLocation).mkdirs();
			}
			try {
				storage.copyToLocal(storagePath, fileLocation);
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Error occurred downloading storage file to local");
			}
			String[] storagePaths = storagePath.split("/");
			try (Reader reader = Files.newBufferedReader(Paths.get(fileLocation + storagePaths[storagePaths.length-1]));
				CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader().withTrim())) {
				Map<String, Integer> headerMap = csvParser.getHeaderMap();
				
				RDBMSNativeEngine rdbms = (RDBMSNativeEngine) database;
				AbstractSqlQueryUtil queryUtil = rdbms.getQueryUtil();
				final String tableName = this.keyValue.get(ReactorKeysEnum.TABLE.getKey());
				String dropQuery = queryUtil.dropTableIfExists(tableName);
				rdbms.removeData(dropQuery);
				String [] colNames = new String[headerMap.keySet().size()];
				for (int i = 0; i < headerMap.keySet().size(); i++) {
					colNames[i] = (String)headerMap.keySet().toArray()[i];
				}
				List<CSVRecord> csvRecords = csvParser.getRecords();
				String[] types = getTypes(queryUtil, csvRecords.get(0));
				String createQuery = queryUtil.createTable(tableName, colNames, types);
				rdbms.insertData(createQuery);
				for (CSVRecord record : csvRecords) {
					Object[] values = new Object[colNames.length];
					for (int i = 0; i < colNames.length; i++) {
						if (types[i].equalsIgnoreCase("TIMESTAMP")) {
					        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
							Date date = dateFormat.parse(record.get(colNames[i]));
							Timestamp timestamp = new Timestamp(date.getTime());
							values[i] = timestamp;
						} else {
							values[i] = record.get(colNames[i]);
						}
					}
					String insertQuery = queryUtil.insertIntoTable(tableName, colNames, types, values);
					rdbms.insertData(insertQuery);
				}
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Pull from S3 to database failed"); 
			}
			return new NounMetadata(true, PixelDataType.BOOLEAN);
		}
	
		private IStorageEngine getStorage() {
			GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.STORAGE.getKey());
			if(grs != null && !grs.isEmpty()) {
				return (IStorageEngine) grs.get(0);
			}
			
			List<NounMetadata> storageInputs = this.curRow.getNounsOfType(PixelDataType.STORAGE);
			if(storageInputs != null && !storageInputs.isEmpty()) {
				return (IStorageEngine) storageInputs.get(0).getValue();
			}
			
			throw new NullPointerException("No storage engine defined");
		}	
	
		private String[] getTypes(AbstractSqlQueryUtil queryUtil, CSVRecord csvRecord) {
				final String INTEGER_DATATYPE_NAME = queryUtil.getIntegerDataTypeName();
				final String VARCHAR = queryUtil.getVarcharDataTypeName();
		        String pattern = "yyyy-MM-dd HH:mm:ss";
	
				String [] types = new String[csvRecord.size()];
				
				for (int i = 0; i < csvRecord.size(); i++) {
					String record = csvRecord.get(i);
					if (isValidTimestamp(record, pattern)) {
						types[i] = "TIMESTAMP";
					} else if (isInteger(record)) {
						types[i] = INTEGER_DATATYPE_NAME;
					} else {
						types[i] = VARCHAR;
					}
				}
				return types; 
		}
		
	    private boolean isValidTimestamp(String timestamp, String pattern) {
	        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
	        sdf.setLenient(false);
	        try {
	            sdf.parse(timestamp);
	            return true;
	        } catch (ParseException e) {
	            return false;
	        }
	    }
	
	    public boolean isInteger(String str) {
	        if (str == null || str.isEmpty()) {
	            return false;
	        }
	
	        try {
	            Integer.parseInt(str);
	            return true;
	        } catch (NumberFormatException e) {
	            return false;
	        }
	    }
	
	}
package prerna.reactor.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.io.Reader;


import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabaseEngine;
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
		this.keysToGet = new String[] {ReactorKeysEnum.TABLE.getKey(), ReactorKeysEnum.STORAGE.getKey(), ReactorKeysEnum.STORAGE_PATH.getKey(), 
				ReactorKeysEnum.SPACE.getKey(), ReactorKeysEnum.FILE_PATH.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		try {
			String databaseId = this.keyValue.get(this.keysToGet[0]);
			IDatabaseEngine database = Utility.getDatabase(databaseId);
			if(!(database instanceof RDBMSNativeEngine)) {
				throw new IllegalArgumentException("Database must be an RDBMS native engine");
			}

			String fileLocation = Utility.normalizePath(UploadInputUtility.getFilePath(this.store, this.insight));

			try (Reader reader = Files.newBufferedReader(Paths.get(fileLocation));
				CSVParser csvParser = new CSVParser(reader, CSVFormat.POSTGRESQL_CSV.withFirstRecordAsHeader().withTrim())) {
				Map<String, Integer> headerMap = csvParser.getHeaderMap();
				
				RDBMSNativeEngine rdbms = (RDBMSNativeEngine) database;
				AbstractSqlQueryUtil queryUtil = rdbms.getQueryUtil();
				final String tableName = this.keyValue.get(ReactorKeysEnum.TABLE.getKey());
				String dropQuery = queryUtil.dropTableIfExists(tableName);
				rdbms.removeData(dropQuery);
				final String INTEGER_DATATYPE_NAME = queryUtil.getIntegerDataTypeName();
				final String VARCHAR = queryUtil.getVarcharDataTypeName();
				String [] colNames = new String[headerMap.keySet().size()];
				for (int i = 0; i < headerMap.keySet().size(); i++) {
					colNames[i] = (String)headerMap.keySet().toArray()[i];
				}
				String [] types = new String[] { VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, 
						VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, INTEGER_DATATYPE_NAME, INTEGER_DATATYPE_NAME};
				String createQuery = queryUtil.createTable(tableName, colNames, types);
				rdbms.insertData(createQuery);
				for (CSVRecord record : csvParser) {
					Object[] values = new Object[headerMap.keySet().size()];
					for (int i = 0; i < colNames.length; i++) {
						values[i] = record.get(colNames[i]);
					}
					String insertQuery = queryUtil.insertIntoTable(tableName, colNames, types, values);
					rdbms.insertData(insertQuery);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}

			return new NounMetadata(true, PixelDataType.BOOLEAN);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred downloading storage file to postgres");
		}
	}
}
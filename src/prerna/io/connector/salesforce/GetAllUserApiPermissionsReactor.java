package prerna.io.connector.salesforce;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabaseEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class GetAllUserApiPermissionsReactor extends AbstractReactor {
	
	private static final String TABLE = "USERAPIPERMISSION";

	private static final Logger classLogger = LogManager.getLogger(GetAllUserApiPermissionsReactor.class);

	@Override
	public NounMetadata execute() {
		try {
			IDatabaseEngine database = Utility.getDatabase(Constants.SECURITY_DB);
			String tableName = getTableName(database);
			if (tableName == null) {
				classLogger.error("User Api Permission table not found in database.");
				throw new SemossPixelException("User Api Permission table not found in database.");
			}
			
			String query = "SELECT ID, USERID, API_ID, TYPE FROM " + tableName;
			Object execResult = database.execQuery(query);
			if (!(execResult instanceof Map)) {
			    classLogger.error("Unexpected execQuery return type: {}", execResult == null ? "null" : execResult.getClass());
			    throw new SemossPixelException("Unexpected database response.");
			}
			@SuppressWarnings("unchecked")
			Map<String, Object> resultMap = (Map<String, Object>) execResult;
			Object rsObj = resultMap.get("RESULTSET_OBJECT");
			List<HashMap<String, String>> resultList = new ArrayList<>();
			
			if(rsObj instanceof ResultSet) {
				try(ResultSet rs = (ResultSet) rsObj){
					ResultSetMetaData metaData = rs.getMetaData();
					int columnCount = metaData.getColumnCount();
					
					// loop through all rows
					while (rs.next()) {
						HashMap<String, String> row = new HashMap<>();
						for(int i = 1; i <= columnCount; i++) {
							String columnName = metaData.getColumnName(i);
							String columnValue = rs.getString(i);
							row.put(columnName, columnValue);
						}
						resultList.add(row);
					}
				}
			} else {
				classLogger.error("No ResultSet returned from database query.");
				throw new SemossPixelException("No ResultSet returned from database query.");
			}
			
			return new NounMetadata(resultList, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("Error fetching User Api Permissions " + e.getMessage());
		}
	}

	// centralized table name lookup
	private String getTableName(IDatabaseEngine database) {
		try {
			List<String> tables = database.getPixelConcepts();
			for (String tableName : tables) {
				if (TABLE.equals(tableName)) {
					return tableName;
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
		return null;
	}
	
	@Override
	public String getReactorDescription() {
		return "Fetches all User API Permission records from the database and returns them for UI display.";
	}
}
